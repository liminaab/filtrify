package operator

import (
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type NotContains struct{}

// Type is Bool
func (m *NotContains) Type() value.ValueType { return value.BoolType }
func (m *NotContains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for contains(str_value, contains_this) but got %s", n)
	}
	return notContainsEval, nil
}

func notContainsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	left, leftOk := value.ValueToString(args[0])
	right, rightOk := value.ValueToString(args[1])

	if !leftOk {
		// TODO:  this should be false, false?
		//        need to ensure doesn't break downstream
		return value.BoolValueTrue, true
	}
	if !rightOk {
		return value.BoolValueTrue, true
	}
	if left == "" || right == "" {
		return value.BoolValueTrue, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueFalse, true
	}
	return value.BoolValueTrue, true
}
