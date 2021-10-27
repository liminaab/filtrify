package operator

import (
	"fmt"
	"math"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Sum struct{}

// Type is number
func (m *Sum) Type() value.ValueType { return value.NumberType }

// IsAgg yes sum is an agg.
func (m *Sum) IsAgg() bool { return true }

func (m *Sum) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 1 {
		return nil, fmt.Errorf("Expected 1 or more args for Sum(arg, arg, ...) but got %s", n)
	}
	return sumEval, nil
}

func sumEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	sumval := float64(0)
	for _, val := range vals {
		if val == nil || val.Nil() || val.Err() {
			// we don't need to evaluate if nil or error
		} else {
			switch v := val.(type) {
			case value.StringValue:
				if fv, ok := value.StringToFloat64(v.Val()); ok && !math.IsNaN(fv) {
					sumval += fv
				}
			case value.StringsValue:
				for _, sv := range v.Val() {
					if fv, ok := value.StringToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					}
				}
			case value.SliceValue:
				for _, sv := range v.Val() {
					if fv, ok := value.ValueToFloat64(sv); ok && !math.IsNaN(fv) {
						sumval += fv
					} else {
						return value.NumberNaNValue, false
					}
				}
			case value.NumericValue:
				fv := v.Float()
				if !math.IsNaN(fv) {
					sumval += fv
				}
			default:
				// Do we silently drop, or fail?
				return value.NumberNaNValue, false
			}
		}
	}
	if sumval == float64(0) {
		return value.NumberNaNValue, false
	}
	return value.NewNumberValue(sumval), true
}

func (m *Sum) GetAggregator() aggr.AggregatorFactory {
	return NewSum
}

type sum struct {
	ct int64
	n  float64
}

func (m *sum) Do(v value.Value) {
	m.ct++
	switch vt := v.(type) {
	case value.IntValue:
		m.n += vt.Float()
	case value.NumberValue:
		m.n += vt.Val()
	}
}
func (m *sum) Result() interface{} {
	return m.n
}
func (m *sum) Reset() { m.n = 0 }
func (m *sum) Merge(a *aggr.AggPartial) {
	m.ct += a.Ct
	m.n += a.N
}
func NewSum() aggr.Aggregator {
	return &sum{}
}
