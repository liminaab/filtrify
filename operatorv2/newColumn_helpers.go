package operatorv2

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/liminaab/filtrify/lmnqlbridge"
)

// Configuration parsing and statement splitting for the new column operator.
// These only read the configuration's statement and hold no state.

// TODO find better names for these variables
type NewColumnConfiguration struct {
	Statement string `json:"statement"`
	GroupBy   string `json:"groupby"`
}

func findSelectedColumnName(config *NewColumnConfiguration) *string {
	index := strings.LastIndex(config.Statement, " AS `")
	if index < 0 {
		return nil
	}
	subText := config.Statement[index+4:]
	selectedColumnName := strings.ReplaceAll(subText, "`", "")
	return &selectedColumnName
}

func getSelectedStatement(config *NewColumnConfiguration) *string {
	index := strings.LastIndex(config.Statement, " AS `")
	if index < 0 {
		return nil
	}
	selectedStatement := config.Statement[:index]
	return &selectedStatement
}

func hasAggCall(statement string) bool {
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

func splitStatements(statement string) []string {
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

func splitAggs(statement string) (string, []string, error) {
	plainStatements := make([]string, 0)
	aggStatements := make([]string, 0)
	miniStatements := splitStatements(statement)
	if len(miniStatements) != 1 {
		return "", nil, errors.New("new column operator only supports one statement")
	}
	for _, ms := range miniStatements {
		if hasAggCall(ms) {
			aggStatements = append(aggStatements, ms)
		} else {
			plainStatements = append(plainStatements, ms)
		}
	}
	return strings.Join(plainStatements, ","), aggStatements, nil
}

func buildNewColumnConfiguration(config string) (*NewColumnConfiguration, error) {
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
