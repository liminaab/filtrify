package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type MinCol struct{}

// Type is NumberType
func (m *MinCol) Type() value.ValueType { return value.NumberType }
func (m *MinCol) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for first(arg) but got %s", n)
	}
	return minColEval, nil
}
func (m *MinCol) IsAgg() bool { return true }

func minColEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	numericVal := float64(0)
	val := vals[0]
	switch v := val.(type) {
	case value.StringsValue:
		for _, sv := range v.Val() {
			if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
				numericVal += fv
			} else {
				return value.NumberNaNValue, false
			}
		}
	case value.SliceValue:
		for _, sv := range v.Val() {
			if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
				numericVal += fv
			} else {
				return value.NumberNaNValue, false
			}
		}
	case value.StringValue:
		if fv, ok := value.StringToFloat64(v.Val()); ok {
			numericVal = fv
		}
	case value.NumericValue:
		numericVal = v.Float()
	default:
		return value.NumberNaNValue, false
	}
	return value.NewNumberValue(numericVal), true
}

func (m *MinCol) GetAggregator() aggr.AggregatorFactory {
	return NewMinCol
}

type minCol struct {
	val float64
}

func (m *minCol) Do(v value.Value) {
	var realVal float64 = 0
	switch vt := v.(type) {
	case value.IntValue:
		realVal = vt.Float()
	case value.NumberValue:
		realVal = vt.Val()
	}

	if realVal < m.val {
		m.val = realVal
	}
}
func (m *minCol) Result() interface{} {
	return m.val
}
func (m *minCol) Merge(a *aggr.AggPartial) {

}
func (m *minCol) Reset() { m.val = math.MaxFloat64 }

func NewMinCol() aggr.Aggregator {
	return &minCol{}
}
