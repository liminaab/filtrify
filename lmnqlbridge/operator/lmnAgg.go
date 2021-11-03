package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type LiminaAgg struct{}

// Type is NumberType
func (m *LiminaAgg) Type() value.ValueType { return value.UnknownType }
func (m *LiminaAgg) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for liminaagg(arg, arg, ...) but got %s", n)
	}
	return liminaEval, nil
}
func (m *LiminaAgg) IsAgg() bool { return true }

func liminaEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	return vals[0], true
}

func (m *LiminaAgg) GetAggregator() aggr.AggregatorFactory {
	return NewLiminaAgg
}

type liminaAggCalc struct {
	lastVal      interface{}
	isLastValSet bool
}

func (m *liminaAggCalc) Do(v value.Value) {

	if m.isLastValSet && m.lastVal == nil {
		// nothing to do here
		// we will definitely return nil
		return
	}

	switch vt := v.(type) {
	case value.StringValue:
		if !m.isLastValSet {
			m.lastVal = vt.Val()
			m.isLastValSet = true
		} else {
			if m.lastVal.(string) != vt.Val() {
				m.lastVal = nil
			}
		}
		break
	case value.IntValue:
		if !m.isLastValSet {
			m.lastVal = vt.Val()
			m.isLastValSet = true
		} else {
			m.lastVal = m.lastVal.(int64) + vt.Val()
		}
		break
	case value.NumberValue:
		if !m.isLastValSet {
			m.lastVal = vt.Val()
			m.isLastValSet = true
		} else {
			m.lastVal = m.lastVal.(float64) + vt.Val()
		}
		break
	case value.BoolValue:
		if !m.isLastValSet {
			m.lastVal = vt.Val()
			m.isLastValSet = true
		} else {
			if m.lastVal.(bool) != vt.Val() {
				m.lastVal = nil
			}
		}
		break
	case nil, value.NilValue:
		m.lastVal = nil
		m.isLastValSet = true
		break
	default:
	}
}
func (m *liminaAggCalc) Result() interface{} {
	return m.lastVal
}
func (m *liminaAggCalc) Merge(a *aggr.AggPartial) {

}
func (m *liminaAggCalc) Reset() { m.isLastValSet = false; m.lastVal = nil }

func NewLiminaAgg() aggr.Aggregator {
	return &liminaAggCalc{
		isLastValSet: false,
	}
}
