package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"strings"
)

type IsBlank struct{}

// Type is string
func (m *IsBlank) Type() value.ValueType { return value.StringType }
func (m *IsBlank) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf(`expected 1 arg for IsBlank(ColumnName) but got %s`, n)
	}
	return isBlankEval, nil
}

func isBlankEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	val := vals[0]
	if val.Nil() {
		return value.NewBoolValue(true), true
	}
	switch val.Type() {
	case value.StringType:
		return value.NewBoolValue(strings.TrimSpace(val.ToString()) == ""), true
	case value.NilType:
		return value.NewBoolValue(true), true
	default:
		return value.NewBoolValue(false), true
	}
}
