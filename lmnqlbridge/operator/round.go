package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

func RoundTo(x, unit float64) float64 {
	r := math.Pow(10, unit)
	return math.Round(x*r) / r
}

type Round struct{}

func (m *Round) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for Round(arg) but got %s", n)
	}
	return roundEval, nil
}

func roundEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var floatUnits float64 = 2
	switch units := args[1].(type) {
	case value.NumberValue:
		floatUnits = units.Float()
	case value.IntValue:
		floatUnits = float64(units.Int())
	default:
		return value.NewNumberNil(), false
	}
	switch val := args[0].(type) {
	case value.NumberValue:
		return value.NewNumberValue(RoundTo(val.Float(), floatUnits)), true
	case value.IntValue:
		return value.NewNumberValue(RoundTo(float64(val.Int()), floatUnits)), true
	default:
		return value.NewNumberNil(), false
	}
}

func (m *Round) Type() value.ValueType { return value.NumberType }
