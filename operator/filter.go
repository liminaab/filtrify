package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"limina.com/dyntransformer/lmnqlbridge"
	"limina.com/dyntransformer/types"
)

const defaultTableName string = "ext"
const defaultColumnName string = "Column"
const defaultDBName string = "limina_db"

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

// TODO think about lists
func (t *FilterOperator) buildCriteriaText(c *Criteria) (string, error) {
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

	switch c.Operator {
	case "<", "<=", ">", "=", "!=":
		return fmt.Sprintf("%s %s %s", c.FieldName, c.Operator, c.Value), nil
	case "CONTAINS", "NOT CONTAINS":
		return fmt.Sprintf("%s %s %s", c.FieldName, c.Operator, c.Value), nil
	case "IS EMPTY":
		return fmt.Sprintf("%s %s %s", c.FieldName, c.Operator, c.Value), nil
	default:
		return "", errors.New("unknown operator")

	}
}

// this should be a recursive function
func (t *FilterOperator) buildWhereClause(statement *FilterStatement) (string, error) {
	if statement.Criteria != nil {
		// this is a simple query
		return t.buildCriteriaText(statement.Criteria)
	}
	var query strings.Builder
	var err error
	for _, stmt := range statement.Statements {

		var q string
		if stmt.Criteria != nil {
			// this is a simple statement
			q, err = t.buildCriteriaText(stmt.Criteria)
		} else {
			q, err = t.buildWhereClause(stmt)
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
	}

	return query.String(), err
}

func extractHeaders(dataset *types.DataSet) []string {
	columnLength := len(dataset.Rows[0].Columns)
	cols := make([]string, columnLength)
	for i := 0; i < columnLength; i++ {
		cols[i] = *dataset.Rows[0].Columns[i].ColumnName
	}

	return cols
}

// TODO think about rawinput vs converted input
func (t *FilterOperator) Transform(dataset *types.DataSet, config string) (*types.DataSet, error) {

	typedConfig, err := buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	headers := extractHeaders(dataset)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(headers, ","))
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	sb.WriteString(" WHERE ")
	whereClause, err := t.buildWhereClause(typedConfig.Statement)
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

	schema.RegisterSourceAsSchema(defaultDBName, inMemoryDataSource)
	result, columns, err := lmnqlbridge.RunQLQuery(defaultDBName, fullQuery)
	if err != nil {
		return nil, err
	}
	// so here we have our new data

	// TODO convert this to a common format
	result = append([][]string{columns}, result...)
	return &types.InputData{
		RawData:                  result,
		RawDataFirstLineIsHeader: true,
	}, nil
}

func buildConfiguration(config string) (*FilterConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := FilterConfiguration{}
	json.Unmarshal([]byte(config), &typedConfig)

	if typedConfig.Statement == nil {
		return nil, errors.New("invalid configuration")
	}

	return &typedConfig, nil
}

func (t *FilterOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := buildConfiguration(config)
	return typedConfig != nil, err
}
