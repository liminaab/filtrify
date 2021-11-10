package operator

import (
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Contains struct{}

// Type is Bool
func (m *Contains) Type() value.ValueType { return value.BoolType }
func (m *Contains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 args for contains(str_value, contains_this) but got %s", n)
	}
	return containsEval, nil
}

func containsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	left, leftOk := value.ValueToString(args[0])
	right, rightOk := value.ValueToString(args[1])

	if !leftOk {
		return value.BoolValueFalse, true
	}
	if !rightOk {
		return value.BoolValueFalse, true
	}
	if left == "" || right == "" {
		return value.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}
