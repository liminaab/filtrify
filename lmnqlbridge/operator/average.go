package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Average struct{}

// Type is NumberType
func (m *Average) Type() value.ValueType { return value.NumberType }
func (m *Average) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("expected 1 or more args for avg(arg, arg, ...) but got %s", n)
	}
	return averageEval, nil
}
func (m *Average) IsAgg() bool { return true }

func averageEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	avg := float64(0)
	ct := 0
	for _, val := range vals {
		switch v := val.(type) {
		case value.StringsValue:
			for _, sv := range v.Val() {
				if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.SliceValue:
			for _, sv := range v.Val() {
				if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
					avg += fv
					ct++
				} else {
					return value.NumberNaNValue, false
				}
			}
		case value.StringValue:
			if fv, ok := value.StringToFloat64(v.Val()); ok {
				avg += fv
				ct++
			}
		case value.NumericValue:
			avg += v.Float()
			ct++
		}
	}
	if ct > 0 {
		return value.NewNumberValue(avg / float64(ct)), true
	}
	return value.NumberNaNValue, false
}

func (m *Average) GetAggregator() aggr.AggregatorFactory {
	return NewAverage
}

type average struct {
	ct int64
	n  float64
}

func (m *average) Do(v value.Value) {
	m.ct++
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
}
func (m *average) Result() interface{} {
	return m.n / float64(m.ct)
}
func (m *average) Merge(a *aggr.AggPartial) {
	m.ct += a.Ct
	m.n += a.N
}
func (m *average) Reset() { m.n = 0; m.ct = 0 }

func NewAverage() aggr.Aggregator {
	return &average{}
}
