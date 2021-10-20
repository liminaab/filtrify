package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/lex"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"limina.com/dyntransformer/lmnqlbridge"
	"limina.com/dyntransformer/types"
)

type FilterOperator struct {
}

type FilterConfiguration struct {
	Statement *FilterStatement `json:"statement"`
}

type FilterStatement struct {
	Statements []*FilterStatement `json:"statements"`
	Conditions []string           `json:"conditions"`
	Criteria   *Criteria          `json:"criteria"`
}

type Criteria struct {
	FieldName string `json:"field"`
	Operator  string `json:"operator"`
	Value     string `json:"value"`
}

func (t *FilterOperator) buildComparisonQuery(c *Criteria, colType types.CellDataType) (string, error) {
	switch colType {
	case types.IntType, types.LongType:
		i, err := strconv.ParseInt(c.Value, 10, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %d", c.FieldName, c.Operator, i), nil
	case types.DoubleType:
		i, err := strconv.ParseFloat(c.Value, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %f", c.FieldName, c.Operator, i), nil
	case types.TimestampType:
		// TODO define format smartly - think about this
		return fmt.Sprintf("%s %s todate('2006-01-02T15:04:05', '%s')", c.FieldName, c.Operator, c.Value), nil
	default:
		return "", errors.New("invalid comparison on filter query")
	}
}

func (t *FilterOperator) buildContainsQuery(c *Criteria, colType types.CellDataType) (string, error) {
	switch colType {
	case types.StringType:
		return fmt.Sprintf("%s %s '%s'", c.FieldName, c.Operator, c.Value), nil
	default:
		return "", errors.New("invalid comparison on filter query")
	}
}

func (t *FilterOperator) buildEmptyQuery(c *Criteria, colType types.CellDataType) (string, error) {
	return fmt.Sprintf("%s IS NULL", c.FieldName), nil
}

func (t *FilterOperator) buildEqualsQuery(c *Criteria, colType types.CellDataType) (string, error) {

	switch colType {
	case types.IntType, types.LongType:
		i, err := strconv.ParseInt(c.Value, 10, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %d", c.FieldName, c.Operator, i), nil
	case types.TimestampType:
		// TODO define format smartly - think about this
		return fmt.Sprintf("%s %s todate('2006-01-02T15:04:05', '%s')", c.FieldName, c.Operator, c.Value), nil
	case types.StringType:
		return fmt.Sprintf("%s %s '%s'", c.FieldName, c.Operator, c.Value), nil
	case types.DoubleType:
		i, err := strconv.ParseFloat(c.Value, 64)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %f", c.FieldName, c.Operator, i), nil
	case types.BoolType:
		data := strings.ToLower(c.Value)
		if data == "true" {
			return fmt.Sprintf("%s %s %s", c.FieldName, c.Operator, data), nil
		} else if data == "false" {
			return fmt.Sprintf("%s %s %s", c.FieldName, c.Operator, data), nil
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

	// TODO make sure value is compatible with column type
	// we need to find out criteria's column type to be able to do this comparison
	colType, exists := columnTypeMap[c.FieldName]
	if !exists {
		return "", errors.New("column doesn't exist")
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
	default:
		return "", errors.New("unknown comparison operator in filter")
	}
}

// this should be a recursive function
func (t *FilterOperator) buildWhereClause(statement *FilterStatement, columnTypeMap map[string]types.CellDataType) (string, error) {
	if statement.Criteria != nil {
		// this is a simple query
		return t.buildCriteriaText(statement.Criteria, columnTypeMap)
	}
	var query strings.Builder
	var err error
	if len(statement.Statements)-1 != len(statement.Conditions) {
		return "", errors.New("invalid where clause configuration")
	}

	for i, stmt := range statement.Statements {

		var q string
		if stmt.Criteria != nil {
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

		if i != len(statement.Statements)-1 {
			query.WriteString(statement.Conditions[i])
			query.WriteString(" ")
		}
	}

	return query.String(), err
}

func (t *FilterOperator) Transform(dataset *types.DataSet, config string) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	headers, colTypeMap := extractHeadersAndTypeMap(dataset)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(headers, ","))
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	sb.WriteString(" WHERE ")
	whereClause, err := t.buildWhereClause(typedConfig.Statement, colTypeMap)
	if err != nil {
		return nil, err
	}
	sb.WriteString(whereClause)
	fullQuery := sb.String()

	// now let's run it
	// TODO wrap these on a common place
	exit := make(chan bool)
	inMemoryDataSource := lmnqlbridge.NewLmnInMemDataSource(exit)

	inMemoryDataSource.AddTable(defaultTableName, dataset)
	schemaName := RandStringBytesMaskImprSrcUnsafe(15)

	err = schema.RegisterSourceAsSchema(schemaName, inMemoryDataSource)
	if err != nil {
		return nil, err
	}
	defer func() {
		schema.DefaultRegistry().SchemaDrop(schemaName, schemaName, lex.TokenSchema)
	}()
	result, columns, err := lmnqlbridge.RunQLQuery(schemaName, fullQuery)
	if err != nil {
		return nil, err
	}
	ds := convertToDataSet(result, columns)
	return ds, nil
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

	if typedConfig.Statement == nil {
		return nil, errors.New("invalid configuration")
	}

	return &typedConfig, nil
}

func (t *FilterOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
