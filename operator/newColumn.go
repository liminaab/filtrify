package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/expr"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/lmnqlbridge"
	"github.com/liminaab/filtrify/types"
)

type NewColumnOperator struct {
}

// backtickIdentRe matches a backtick-quoted identifier, e.g. `ColumnName`, as used to
// reference a column inside a NewColumn statement.
var backtickIdentRe = regexp.MustCompile("`([^`]+)`")

// CollectNewColumnBatch scans steps for a maximal run of NewColumn steps (starting at
// steps[0]) that can be executed as a single combined SQL SELECT instead of one
// executeSQLQuery round-trip per step. Each round-trip re-scans and re-converts the full
// (growing) dataset through qlbridge's in-memory SQL engine, so this turns an O(steps)
// number of full-dataset passes into O(batches).
//
// A step ends the run (and is left for the caller to process on its own) when it has a
// GroupBy, contains an aggregation, has a malformed or duplicate-name statement, or reads
// a column produced by an earlier step already in this run: a single SQL SELECT evaluates
// every column against the original row, not against sibling columns being computed in the
// same statement, so such a step would silently see the wrong (pre-batch) value if merged.
func CollectNewColumnBatch(steps []*types.TransformationStep) ([]*NewColumnConfiguration, []string) {
	op := &NewColumnOperator{}
	introduced := make(map[string]bool)

	configs := make([]*NewColumnConfiguration, 0, len(steps))
	statements := make([]string, 0, len(steps))
	for _, step := range steps {
		if step.Operator != types.NewColumn {
			break
		}
		cfg, err := op.buildConfiguration(step.Configuration)
		if err != nil || cfg.GroupBy != "" {
			break
		}
		plainStatement, aggs, err := op.splitAggs(cfg.Statement)
		if err != nil || len(aggs) > 0 {
			break
		}
		selectedColName := op.findSelectedColumnName(cfg)
		selectedStatement := op.getSelectedStatement(cfg)
		if selectedColName == nil || selectedStatement == nil {
			break
		}
		if introduced[strings.ToLower(*selectedColName)] {
			// duplicate column name within this run - let the single-step path raise
			// the proper "column already exists" error
			break
		}
		conflict := false
		for _, m := range backtickIdentRe.FindAllStringSubmatch(*selectedStatement, -1) {
			if introduced[strings.ToLower(m[1])] {
				conflict = true
				break
			}
		}
		if conflict {
			break
		}
		configs = append(configs, cfg)
		statements = append(statements, plainStatement)
		introduced[strings.ToLower(*selectedColName)] = true
	}

	return configs, statements
}

// TransformNewColumnBatch executes a run of non-aggregating, non-groupby NewColumn
// statements (as collected by CollectNewColumnBatch) as one SELECT, adding all of their
// columns to dataset in a single pass instead of one executeSQLQuery call per statement.
func TransformNewColumnBatch(dataset *types.DataSet, statements []string) (*types.DataSet, error) {
	headers, columnTypeMap := extractHeadersAndTypeMap(dataset)
	headers, columnTypeMap, dataset = addKeyRowToDataset(headers, columnTypeMap, dataset)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(buildSelectStatement(headers))
	for _, statement := range statements {
		sb.WriteString(", ")
		sb.WriteString(statement)
	}
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)

	result, err := executeSQLQuery(sb.String(), dataset, columnTypeMap)
	if err != nil {
		return nil, err
	}

	result.Headers = buildHeaders(result, dataset)
	result = removeAndAssignRowKey(result)
	return result, nil
}

// TODO find better names for these variables
type NewColumnConfiguration struct {
	Statement string `json:"statement"`
	GroupBy   string `json:"groupby"`
}

func (t *NewColumnOperator) findSelectedColumnName(config *NewColumnConfiguration) *string {
	index := strings.LastIndex(config.Statement, " AS `")
	if index < 0 {
		return nil
	}
	subText := config.Statement[index+4:]
	selectedColumnName := strings.ReplaceAll(subText, "`", "")
	return &selectedColumnName
}

func (t *NewColumnOperator) getSelectedStatement(config *NewColumnConfiguration) *string {
	index := strings.LastIndex(config.Statement, " AS `")
	if index < 0 {
		return nil
	}
	selectedStatement := config.Statement[:index]
	return &selectedStatement
}

func (t *NewColumnOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	headers, columnTypeMap := extractHeadersAndTypeMap(dataset)
	plainAggs := make([]*types.DataColumn, 0)

	// let's check what is the name of the final result column
	// we need to make sure it doesn't exist in our dataset
	// if it does we need to return error
	selectedColName := t.findSelectedColumnName(typedConfig)
	if selectedColName != nil {
		for _, h := range headers {
			if strings.EqualFold(h, *selectedColName) {
				return nil, errors.New("column already exists")
			}
		}
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	// we can't select original columns if there is a group by statement
	if typedConfig.GroupBy == "" {
		headers, columnTypeMap, dataset = addKeyRowToDataset(headers, columnTypeMap, dataset)
		sb.WriteString(buildSelectStatement(headers))

		// we need to execute multiple queries here
		// first do we have any aggregations in statement?
		plainStatement, aggs, err := t.splitAggs(typedConfig.Statement)
		if err != nil {
			return nil, err
		}
		if len(plainStatement) > 0 {
			sb.WriteString(", ")
			sb.WriteString(plainStatement)
		}
		if len(aggs) > 0 {
			// we have a problem here
			// we have aggregations but we don't have a group by
			// we have to execute each of these aggregations like a seperate query
			for _, agg := range aggs {
				aggData, err := t.executePlainAggregation(agg, dataset, columnTypeMap)
				if err != nil {
					return nil, err
				}
				if len(aggData.Rows) > 1 {
					return nil, errors.New("invalid aggregation command")
				}
				aggRow := aggData.Rows[0]
				if len(aggRow.Columns) > 1 {
					return nil, errors.New("invalid aggregation command")
				}
				aggCol := aggRow.Columns[0]
				plainAggs = append(plainAggs, aggCol)
			}
		}

	} else {
		if len(typedConfig.Statement) > 0 {
			sb.WriteString(typedConfig.Statement)
		} else {
			sb.WriteString(buildSelectStatement(headers))
		}
	}
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	if typedConfig.GroupBy != "" {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(fmt.Sprintf("`%s`", typedConfig.GroupBy))
	}
	if err != nil {
		return nil, err
	}
	fullQuery := sb.String()

	result, err := executeSQLQuery(fullQuery, dataset, columnTypeMap)
	if err != nil {
		// TODO properly fix this in qlbridge
		// this is a really ugly workaround to select floats
		// right now the qlbridge driver doesn't support selecting float64
		selectedStatement := t.getSelectedStatement(typedConfig)
		if selectedStatement != nil {
			// let's try to parse this into a float
			// if it fails we return the original error
			// if it succeeds we return the result
			val, floatErr := strconv.ParseFloat(*selectedStatement, 64)
			if floatErr == nil {
				// ok let's add this value to all of the rows
				// now we need to merge result with plain aggregations
				for _, r := range dataset.Rows {
					r.Columns = append(r.Columns, &types.DataColumn{
						ColumnName: *selectedColName,
						CellValue: &types.CellValue{
							DataType:    types.DoubleType,
							DoubleValue: val,
						},
					})
				}
				dataset.Headers = buildHeaders(dataset, dataset)
				dataset = removeAndAssignRowKey(dataset)
				return dataset, nil
			}
		}

		return nil, err
	}

	// now we need to merge result with plain aggregations
	for _, r := range result.Rows {
		r.Columns = append(r.Columns, plainAggs...)
	}

	result.Headers = buildHeaders(result, dataset)
	result = removeAndAssignRowKey(result)
	return result, nil
}

func (t *NewColumnOperator) executePlainAggregation(aggrStatement string, ds *types.DataSet, existingColumnTypeMap map[string]types.CellDataType) (*types.DataSet, error) {
	q := fmt.Sprintf("SELECT %s FROM %s", aggrStatement, defaultTableName)
	result, err := executeSQLQuery(q, ds, existingColumnTypeMap)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *NewColumnOperator) hasAggCall(statement string) bool {
	statement = strings.ToLower(statement)
	allOps := lmnqlbridge.GetOperators()
	for key, op := range allOps {
		aggfn, hasAggFlag := op.(expr.AggFunc)
		if hasAggFlag {
			if aggfn.IsAgg() {
				fncCallKey := key + "("
				// let's make sure our statement contains a call to an aggregate function
				if strings.Contains(statement, fncCallKey) {
					return true
				}
			}
		}
	}
	return false
}

type selectState int

const (
	no_call selectState = iota
	in_call
	param_name
)

func (t *NewColumnOperator) splitStatements(statement string) []string {
	statements := make([]string, 0)
	statementStack := []selectState{no_call}
	statementBuilder := strings.Builder{}

	for _, c := range statement {
		switch c {
		case '`':
			if len(statementStack) > 0 && statementStack[len(statementStack)-1] == param_name {
				statementStack = statementStack[:len(statementStack)-1]
			} else {
				statementStack = append(statementStack, param_name)
			}
			statementBuilder.WriteRune(c)
			break
		case '(':
			if len(statementStack) > 0 && statementStack[len(statementStack)-1] != param_name {
				statementStack = append(statementStack, in_call)
			}
			statementBuilder.WriteRune(c)
			break
		case ')':
			if len(statementStack) > 0 && statementStack[len(statementStack)-1] != param_name {
				statementStack = statementStack[:len(statementStack)-1]
			}
			statementBuilder.WriteRune(c)
			break
		case ',':
			if len(statementStack) > 0 && statementStack[len(statementStack)-1] == no_call {
				statements = append(statements, statementBuilder.String())
				statementBuilder.Reset()
			} else {
				statementBuilder.WriteRune(c)
			}
			break
		default:
			statementBuilder.WriteRune(c)
		}
	}
	if statementBuilder.Len() > 0 {
		statements = append(statements, statementBuilder.String())
	}
	return statements
}

func (t *NewColumnOperator) splitAggs(statement string) (string, []string, error) {
	plainStatements := make([]string, 0)
	aggStatements := make([]string, 0)
	miniStatements := t.splitStatements(statement)
	if len(miniStatements) != 1 {
		return "", nil, errors.New("new column operator only supports one statement")
	}
	for _, ms := range miniStatements {
		if t.hasAggCall(ms) {
			aggStatements = append(aggStatements, ms)
		} else {
			plainStatements = append(plainStatements, ms)
		}
	}
	return strings.Join(plainStatements, ","), aggStatements, nil
}

func (t *NewColumnOperator) buildConfiguration(config string) (*NewColumnConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := NewColumnConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Statement) < 1 {
		return nil, errors.New("missing statement in newcolumn configuration")
	}

	return &typedConfig, nil
}

func (t *NewColumnOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
