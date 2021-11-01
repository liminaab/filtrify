package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type WeightedAverage struct{}

// Type is NumberType
func (m *WeightedAverage) Type() value.ValueType { return value.NumberType }
func (m *WeightedAverage) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 args for weightedAverage(arg, arg, ...) but got %s", n)
	}
	return weightedAverageEval, nil
}
func (m *WeightedAverage) IsAgg() bool { return true }

func weightedAverageEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	avg := []float64{0, 0}
	for i, val := range vals {
		switch v := val.(type) {
		case value.StringsValue:
			for _, sv := range v.Val() {
				if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
					avg[i] = fv
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.SliceValue:
			for _, sv := range v.Val() {
				if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
					avg[i] = fv
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.StringValue:
			if fv, ok := value.StringToFloat64(v.Val()); ok {
				avg[i] += fv
			}
		case value.NumericValue:
			avg[i] += v.Float()
		}
	}
	v := []value.Value{value.NewNumberValue(avg[0] * avg[1]), value.NewNumberValue(avg[1])}
	return value.NewSliceValues(v), true
}

func (m *WeightedAverage) GetAggregator() aggr.AggregatorFactory {
	return NewWeightedAverage
}

type weightedAverage struct {
	ct float64
	n  float64
}

func (m *weightedAverage) Do(v value.Value) {
	if !v.Type().IsSlice() {
		panic("invalid type")
	}
	sliceVal := v.Value().([]value.Value)
	if len(sliceVal) != 2 {
		panic("invalid type")
	}
	switch vt := sliceVal[0].(type) {
	case value.NumberValue:
		m.n += vt.Val()
	}

	switch vt := sliceVal[1].(type) {
	case value.NumberValue:
		m.ct += vt.Val()
	}
}
func (m *weightedAverage) Result() interface{} {
	val := m.n / float64(m.ct)
	return math.Round(val*100) / 100
}
func (m *weightedAverage) Merge(a *aggr.AggPartial) {
	m.ct += a.Ct
	m.n += a.N
}
func (m *weightedAverage) Reset() { m.n = 0; m.ct = 0 }

func NewWeightedAverage() aggr.Aggregator {
	return &weightedAverage{}
}
