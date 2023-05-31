package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type OR struct{}

func (m *OR) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("expected at least 2 arg for OR(arg) but got %s", n)
	}
	return orEval, nil
}

func orEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	for _, arg := range args {
		boolVal := getArgBooleanValue(arg)
		if boolVal {
			return value.NewBoolValue(true), true
		}
	}

	return value.NewBoolValue(false), true
}

func (m *OR) Type() value.ValueType { return value.BoolType }
