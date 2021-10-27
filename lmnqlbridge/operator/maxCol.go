package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type MaxCol struct{}

// Type is NumberType
func (m *MaxCol) Type() value.ValueType { return value.NumberType }
func (m *MaxCol) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for first(arg) but got %s", n)
	}
	return maxColEval, nil
}
func (m *MaxCol) IsAgg() bool { return true }

func maxColEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
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

func (m *MaxCol) GetAggregator() aggr.AggregatorFactory {
	return NewMaxCol
}

type maxCol struct {
	val float64
}

func (m *maxCol) Do(v value.Value) {
	var realVal float64 = 0
	switch vt := v.(type) {
	case value.IntValue:
		realVal = vt.Float()
	case value.NumberValue:
		realVal = vt.Val()
	}

	if realVal > m.val {
		m.val = realVal
	}
}
func (m *maxCol) Result() interface{} {
	return m.val
}
func (m *maxCol) Merge(a *aggr.AggPartial) {

}
func (m *maxCol) Reset() { m.val = 0 }

func NewMaxCol() aggr.Aggregator {
	return &maxCol{}
}
