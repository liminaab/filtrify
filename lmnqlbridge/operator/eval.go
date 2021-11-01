package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Eval struct{}

func (m *Eval) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for Eval(arg) but got %s", n)
	}
	return evalEval, nil
}

func evalEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	return args[0], true
}

func (m *Eval) Type() value.ValueType { return value.BoolType }
