package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"limina.com/dyntransformer/lmnqlbridge"
	"limina.com/dyntransformer/types"
)

type NewColumnOperator struct {
}

// TODO find better names for these variables
type NewColumnConfiguration struct {
	Statement string `json:"statement"`
	GroupBy   string `json:"groupby"`
}

func (t *NewColumnOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	headers, _ := extractHeadersAndTypeMap(dataset)
	plainAggs := make([]*types.DataColumn, 0)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	// we can't select original columns if there is a group by statement
	if typedConfig.GroupBy == "" {
		sb.WriteString(buildSelectStatement(headers))

		// we need to execute multiple queries here
		// first do we have any aggregations in statement?
		plainStatement, aggs := t.splitAggs(typedConfig.Statement)
		if len(plainStatement) > 0 {
			sb.WriteString(", ")
			sb.WriteString(plainStatement)
		}
		if len(aggs) > 0 {
			// we have a problem here
			// we have aggregations but we don't have a group by
			// we have to execute each of these aggregations like a seperate query
			for _, agg := range aggs {
				aggData, err := t.executePlainAggregation(agg, dataset)
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

	result, err := executeSQLQuery(fullQuery, dataset)
	if err != nil {
		return nil, err
	}

	// now we need to merge result with plain aggregations
	for _, r := range result.Rows {
		r.Columns = append(r.Columns, plainAggs...)
	}

	return result, nil
}

func (t *NewColumnOperator) executePlainAggregation(aggrStatement string, ds *types.DataSet) (*types.DataSet, error) {
	q := fmt.Sprintf("SELECT %s FROM %s", aggrStatement, defaultTableName)
	result, err := executeSQLQuery(q, ds)
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
			if statementStack[len(statementStack)-1] == param_name {
				statementStack = statementStack[:len(statementStack)-1]
			} else {
				statementStack = append(statementStack, param_name)
			}
			statementBuilder.WriteRune(c)
			break
		case '(':
			if statementStack[len(statementStack)-1] != param_name {
				statementStack = append(statementStack, in_call)
			}
			statementBuilder.WriteRune(c)
			break
		case ')':
			if statementStack[len(statementStack)-1] != param_name {
				statementStack = statementStack[:len(statementStack)-1]
			}
			statementBuilder.WriteRune(c)
			break
		case ',':
			if statementStack[len(statementStack)-1] == no_call {
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

func (t *NewColumnOperator) splitAggs(statement string) (string, []string) {
	plainStatements := make([]string, 0)
	aggStatements := make([]string, 0)
	miniStatements := t.splitStatements(statement)
	for _, ms := range miniStatements {
		if t.hasAggCall(ms) {
			aggStatements = append(aggStatements, ms)
		} else {
			plainStatements = append(plainStatements, ms)
		}
	}
	return strings.Join(plainStatements, ","), aggStatements
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

	return &typedConfig, nil
}

func (t *NewColumnOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
