package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type First struct{}

// Type is NumberType
func (m *First) Type() value.ValueType { return value.NumberType }
func (m *First) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for first(arg) but got %s", n)
	}
	return firstEval, nil
}
func (m *First) IsAgg() bool { return true }

func firstEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	return vals[0], true
}

func (m *First) GetAggregator() aggr.AggregatorFactory {
	return NewFirst
}

type first struct {
	val interface{}
}

func (m *first) Do(v value.Value) {
	if m.val == nil {
		m.val = v.Value()
	}
}
func (m *first) Result() interface{} {
	return m.val
}
func (m *first) Merge(a *aggr.AggPartial) {

}
func (m *first) Reset() { m.val = nil }

func NewFirst() aggr.Aggregator {
	return &first{}
}
