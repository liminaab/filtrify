package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Last struct{}

// Type is NumberType
func (m *Last) Type() value.ValueType { return value.NumberType }
func (m *Last) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for last(arg) but got %s", n)
	}
	return lastEval, nil
}
func (m *Last) IsAgg() bool { return true }

func lastEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	return vals[0], true
}

func (m *Last) GetAggregator() aggr.AggregatorFactory {
	return NewLast
}

type last struct {
	val interface{}
}

func (m *last) Do(v value.Value) {
	m.val = v.Value()
}
func (m *last) Result() interface{} {
	return m.val
}
func (m *last) Merge(a *aggr.AggPartial) {

}
func (m *last) Reset() { m.val = nil }

func NewLast() aggr.Aggregator {
	return &last{}
}
