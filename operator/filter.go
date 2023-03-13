package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type FilterOperator struct {
}

type FilterConfiguration struct {
	FilterCriteria *FilterCriteria `json:"filterCriteria"`
}

type FilterCriteria struct {
	NestedCriterias []*FilterCriteria `json:"nestedCriterias"`
	ChainWith       []string          `json:"chainWith"`
	Criteria        *Criteria         `json:"criteria"`
}

type Criteria struct {
	FieldName string `json:"field"`
	Operator  string `json:"operator"`
	Value     string `json:"value"`
}

func parsePercentage(data string) (float64, error) {
	newData := strings.ReplaceAll(data, " ", "")
	if strings.Contains(newData, "%") {
		newData = strings.ReplaceAll(data, "%", "")
		return strconv.ParseFloat(newData, 64)
	}
	return 0, errors.New("invalid percentage format")
}

func (t *FilterOperator) buildComparisonQuery(c *Criteria, colType types.CellDataType) (string, error) {
	switch colType {
	case types.IntType, types.LongType:
		i, err := strconv.ParseInt(c.Value, 10, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("`%s` %s %d", c.FieldName, c.Operator, i), nil
	case types.DoubleType:
		i, err := parsePercentage(c.Value)
		if err != nil {
			i, err = strconv.ParseFloat(c.Value, 64)
			if err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("`%s` %s %f", c.FieldName, c.Operator, i), nil
	case types.TimestampType:
		// TODO define format smartly - think about this
		return fmt.Sprintf("`%s` %s todate('%s')", c.FieldName, c.Operator, c.Value), nil
	default:
		return "", errors.New("invalid comparison on filter query")
	}
}

func (t *FilterOperator) buildContainsQuery(c *Criteria, colType types.CellDataType) (string, error) {
	switch colType {
	case types.StringType:
		return fmt.Sprintf("`%s` %s '%s'", c.FieldName, c.Operator, c.Value), nil
	default:
		return "", errors.New("invalid comparison on filter query")
	}
}

func (t *FilterOperator) buildEmptyQuery(c *Criteria, colType types.CellDataType) (string, error) {
	return fmt.Sprintf("(`%s` = NULL OR `%s` = '')", c.FieldName, c.FieldName), nil
}

func (t *FilterOperator) buildNotEmptyQuery(c *Criteria, colType types.CellDataType) (string, error) {
	return fmt.Sprintf("NOT (`%s` = NULL OR `%s` = '')", c.FieldName, c.FieldName), nil
}

func (t *FilterOperator) buildEqualsQuery(c *Criteria, colType types.CellDataType) (string, error) {

	switch colType {
	case types.IntType, types.LongType:
		i, err := strconv.ParseInt(c.Value, 10, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("`%s` %s %d", c.FieldName, c.Operator, i), nil
	case types.TimestampType:
		// TODO define format smartly - think about this
		return fmt.Sprintf("`%s` %s todate('%s')", c.FieldName, c.Operator, c.Value), nil
	case types.StringType:
		return fmt.Sprintf("`%s` %s '%s'", c.FieldName, c.Operator, c.Value), nil
	case types.DoubleType:
		i, err := strconv.ParseFloat(c.Value, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("`%s` %s %f", c.FieldName, c.Operator, i), nil
	case types.BoolType:
		data := strings.ToLower(c.Value)
		if data == "true" {
			return fmt.Sprintf("`%s` %s %s", c.FieldName, c.Operator, data), nil
		} else if data == "false" {
			return fmt.Sprintf("`%s` %s %s", c.FieldName, c.Operator, data), nil
		} else {
			return "", errors.New("invalid boolean value")
		}
	default:
		return "", errors.New("unknown column type in where clause")
	}
}

// TODO think about lists
func (t *FilterOperator) buildCriteriaText(c *Criteria, columnTypeMap map[string]types.CellDataType) (string, error) {
	// <
	// <=
	// >
	// >=
	// = (single)
	// = (list)
	// != (single)
	// != (list)
	// CONTAINS
	// NOT CONTAINS
	// IS EMPTY

	// we need to find out criteria's column type to be able to do this comparison
	colType, exists := columnTypeMap[c.FieldName]
	if !exists {
		return "", buildColumnNotExistsError(c.FieldName)
	}

	switch c.Operator {
	case "<", "<=", ">", ">=":
		// valid for numerical and timestamp
		return t.buildComparisonQuery(c, colType)
	case "=", "!=":
		// valid for all data types
		return t.buildEqualsQuery(c, colType)
	case "CONTAINS", "NOT CONTAINS":
		// valid for string
		return t.buildContainsQuery(c, colType)
	case "IS EMPTY":
		// valid for all
		return t.buildEmptyQuery(c, colType)
	case "IS NOT EMPTY":
		// valid for all
		return t.buildNotEmptyQuery(c, colType)
	default:
		return "", errors.New("unknown comparison operator in filter")
	}
}

func (t *FilterOperator) isListComparison(statement *FilterCriteria) bool {
	if statement.Criteria == nil {
		return false
	}
	val := statement.Criteria.Value
	if strings.HasPrefix(val, "(") && strings.HasSuffix(val, ")") {
		val = val[1 : len(val)-1]
		if strings.Contains(val, ",") {
			return true
		}
	}
	return false
}

func (t *FilterOperator) compileListComparisonStatements(statement *FilterCriteria) *FilterCriteria {
	newStatement := &FilterCriteria{
		Criteria: nil,
	}
	newStatement.NestedCriterias = make([]*FilterCriteria, 0)
	newStatement.ChainWith = make([]string, 0)
	val := statement.Criteria.Value
	val = val[1 : len(val)-1]
	values := strings.Split(val, ",")
	for i, v := range values {
		s := &FilterCriteria{
			Criteria: &Criteria{
				FieldName: statement.Criteria.FieldName,
				Operator:  statement.Criteria.Operator,
				Value:     strings.TrimSpace(v),
			},
		}
		newStatement.NestedCriterias = append(newStatement.NestedCriterias, s)
		if i != len(values)-1 {
			newStatement.ChainWith = append(newStatement.ChainWith, "OR")
		}
	}

	return newStatement
}

// this should be a recursive function
func (t *FilterOperator) buildWhereClause(statement *FilterCriteria, columnTypeMap map[string]types.CellDataType) (string, error) {
	if t.isListComparison(statement) {
		statement = t.compileListComparisonStatements(statement)
	} else if statement.Criteria != nil {
		// but is this a list comparison query? let's check that out

		// this is a simple query
		return t.buildCriteriaText(statement.Criteria, columnTypeMap)
	}
	var query strings.Builder
	var err error
	if len(statement.NestedCriterias)-1 != len(statement.ChainWith) {
		return "", errors.New("invalid where clause configuration")
	}

	for i, stmt := range statement.NestedCriterias {
		var q string
		if t.isListComparison(stmt) {
			stmt = t.compileListComparisonStatements(stmt)
			q, err = t.buildWhereClause(stmt, columnTypeMap)
		} else if stmt.Criteria != nil {
			// this is a simple statement
			q, err = t.buildCriteriaText(stmt.Criteria, columnTypeMap)
		} else {
			q, err = t.buildWhereClause(stmt, columnTypeMap)
		}

		if err != nil {
			return "", err
		}

		// we need to wrap the query inside the paranthesis
		_, err = query.WriteString("( ")
		if err != nil {
			return "", err
		}
		_, err = query.WriteString(q)
		if err != nil {
			return "", err
		}
		_, err = query.WriteString(" ) ")
		if err != nil {
			return "", err
		}

		if i != len(statement.NestedCriterias)-1 {
			query.WriteString(statement.ChainWith[i])
			query.WriteString(" ")
		}
	}

	return query.String(), err
}

func (t *FilterOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	return t.TransformTyped(dataset, typedConfig)
}

func (t *FilterOperator) TransformTyped(dataset *types.DataSet, typedConfig *FilterConfiguration) (*types.DataSet, error) {

	headers, colTypeMap := extractHeadersAndTypeMap(dataset)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(buildSelectStatement(headers))
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	sb.WriteString(" WHERE ")
	whereClause, err := t.buildWhereClause(typedConfig.FilterCriteria, colTypeMap)
	if err != nil {
		return nil, err
	}
	sb.WriteString(whereClause)
	fullQuery := sb.String()

	return executeSQLQuery(fullQuery, dataset, colTypeMap)
}

func (t *FilterOperator) buildConfiguration(config string) (*FilterConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := FilterConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if typedConfig.FilterCriteria == nil {
		return nil, errors.New("invalid configuration")
	}

	return &typedConfig, nil
}

func (t *FilterOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
