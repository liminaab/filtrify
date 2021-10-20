package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

func FloorTo(x, unit float64) float64 {
	r := math.Pow(10, unit)
	return math.Floor(x*r) / r
}

type Floor struct{}

func (m *Floor) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for Round(arg) but got %s", n)
	}
	return floorEval, nil
}

func floorEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
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
		return value.NewNumberValue(FloorTo(val.Float(), floatUnits)), true
	case value.IntValue:
		return value.NewNumberValue(FloorTo(float64(val.Int()), floatUnits)), true
	default:
		return value.NewNumberNil(), false
	}
}

func (m *Floor) Type() value.ValueType { return value.NumberType }
