package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type PLUS struct{}

func (m *PLUS) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("expected at least 2 arg for PLUS(arg) but got %s", n)
	}
	return plusEval, nil
}

func plusEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var total float64 = 0

	for _, arg := range args {
		if !arg.Type().IsNumeric() {
			return value.NewNumberNil(), false
		}
		numericArg, ok := arg.(value.NumericValue)
		if !ok {
			return value.NewNumberNil(), false
		}
		total += numericArg.Float()
	}

	return value.NewNumberValue(total), true

}

func (m *PLUS) Type() value.ValueType { return value.NumberType }
