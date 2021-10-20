package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

func AbsFloat(x float64) float64 {
	return math.Float64frombits(math.Float64bits(x) &^ (1 << 63))
}

func Abs(x int64) int64 {
	if x > 0 {
		return x
	}
	return -x
}

type ABS struct{}

func (m *ABS) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for ABS(arg) but got %s", n)
	}
	return absEval, nil
}

func absEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	switch node := args[0].(type) {
	case value.NumberValue:
		return value.NewNumberValue(AbsFloat(node.Float())), true
	case value.IntValue:
		return value.NewIntValue(Abs(node.Int())), true
	default:
		return value.NewIntNil(), false
	}
}

func (m *ABS) Type() value.ValueType { return value.UnknownType }
