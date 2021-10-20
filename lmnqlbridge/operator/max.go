package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type MAX struct{}

func (m *MAX) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for MAX(arg) but got %s", n)
	}
	return maxEval, nil
}

func maxEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var firstVal float64 = 0
	switch val := args[0].(type) {
	case value.NumberValue:
		firstVal = val.Float()
	case value.IntValue:
		firstVal = float64(val.Int())
	default:
		return value.NewNumberNil(), false
	}
	var secondVal float64 = 0
	switch units := args[1].(type) {
	case value.NumberValue:
		secondVal = units.Float()
	case value.IntValue:
		secondVal = float64(units.Int())
	default:
		return value.NewNumberNil(), false
	}

	if firstVal >= secondVal {
		return value.NewNumberValue(firstVal), true
	}

	return value.NewNumberValue(secondVal), true

}

func (m *MAX) Type() value.ValueType { return value.NumberType }
