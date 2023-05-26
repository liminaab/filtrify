package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type MINUS struct{}

func (m *MINUS) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for MINUS(arg) but got %s", n)
	}
	return minusEval, nil
}

func minusEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var total float64 = 0
	arg1 := args[0]
	arg2 := args[1]
	if !arg1.Type().IsNumeric() || !arg2.Type().IsNumeric() {
		return value.NewNumberNil(), false
	}
	numericArg1, ok := arg1.(value.NumericValue)
	if !ok {
		return value.NewNumberNil(), false
	}
	numericArg2, ok := arg2.(value.NumericValue)
	if !ok {
		return value.NewNumberNil(), false
	}
	total = numericArg1.Float() - numericArg2.Float()
	return value.NewNumberValue(total), true

}

func (m *MINUS) Type() value.ValueType { return value.NumberType }
