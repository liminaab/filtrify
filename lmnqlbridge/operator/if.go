package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type IF struct{}

func (m *IF) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf("expected 3 arg for IF(arg) but got %s", n)
	}
	return ifEval, nil
}

func ifEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	condition := args[0]
	if condition == value.NilValueVal || condition == value.BoolValueFalse {
		return args[2], true
	}

	return args[1], true
}

func (m *IF) Type() value.ValueType { return value.UnknownType }
