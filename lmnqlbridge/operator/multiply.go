package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type MULTIPLY struct{}

func (m *MULTIPLY) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("expected at least 2 arg for MULTIPLY(arg) but got %s", n)
	}
	return multiplyEval, nil
}

func multiplyEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var total float64 = 1

	for _, arg := range args {
		if !arg.Type().IsNumeric() {
			return value.NewNumberNil(), false
		}
		numericArg, ok := arg.(value.NumericValue)
		if !ok {
			return value.NewNumberNil(), false
		}
		total *= numericArg.Float()
	}

	return value.NewNumberValue(total), true

}

func (m *MULTIPLY) Type() value.ValueType { return value.NumberType }
