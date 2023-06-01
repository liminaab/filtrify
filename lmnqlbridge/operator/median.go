package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/qlbridge/aggr"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"sort"
	"time"
)

type Median struct{}

// Type is NumberType
func (m *Median) Type() value.ValueType { return value.ValueInterfaceType }
func (m *Median) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("expected 1 arg for MEDIAN(arg) but got %s", n)
	}
	return medianEval, nil
}
func (m *Median) IsAgg() bool { return true }

func medianEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	val := vals[0]
	switch val.(type) {
	case value.StringValue, value.NumericValue, value.IntValue, value.NumberValue, value.DateValue, value.TimeValue, value.TimeOnlyValue:
		return val, true
	default:
		return value.NilValueVal, true
	}
}

func (m *Median) GetAggregator() aggr.AggregatorFactory {
	return NewMedian
}

type median struct {
	vals []value.Value
}

func (m *median) Do(v value.Value) {
	if v.Nil() {
		return
	}
	m.vals = append(m.vals, v)
}

func sortAndPickStrings(vals []value.Value) string {
	data := make([]string, len(vals))
	for i, v := range vals {
		data[i] = v.ToString()
	}
	sort.Strings(data)

	return data[len(data)/2]
}

func sortAndPickInts(vals []value.Value) int64 {
	data := make([]int64, len(vals))
	for i, v := range vals {
		data[i] = v.(value.IntValue).Int()
	}
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	if len(data)%2 == 0 {
		return (data[len(data)/2-1] + data[len(data)/2]) / 2
	}
	return data[len(data)/2]
}

func sortAndPickDoubles(vals []value.Value) float64 {
	data := make([]float64, len(vals))
	for i, v := range vals {
		data[i] = v.(value.NumericValue).Float()
	}
	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
	if len(data)%2 == 0 {
		return (data[len(data)/2-1] + data[len(data)/2]) / 2
	}
	return data[len(data)/2]
}

func sortAndPickTimes(vals []value.Value) time.Time {
	data := make([]time.Time, len(vals))
	for i, v := range vals {
		data[i] = v.(value.TimeValue).Val()
	}
	sort.Slice(data, func(i, j int) bool { return data[i].Before(data[j]) })
	return data[len(data)/2]
}

func sortAndPickDates(vals []value.Value) civil.Date {
	data := make([]civil.Date, len(vals))
	for i, v := range vals {
		data[i] = v.(value.DateValue).Val()
	}
	sort.Slice(data, func(i, j int) bool { return data[i].Before(data[j]) })
	return data[len(data)/2]
}

func isBefore(a, b civil.Time) bool {
	return a.Hour < b.Hour || (a.Hour == b.Hour && a.Minute < b.Minute) || (a.Hour == b.Hour && a.Minute == b.Minute && a.Second < b.Second)
}

func sortAndPickTimeOnly(vals []value.Value) civil.Time {
	data := make([]civil.Time, len(vals))
	for i, v := range vals {
		data[i] = v.(value.TimeOnlyValue).Val()
	}
	sort.Slice(data, func(i, j int) bool { return isBefore(data[i], data[j]) })
	return data[len(data)/2]
}

func (m *median) Result() interface{} {

	// we are assuming all types are same and not nil
	if len(m.vals) == 0 {
		return nil
	}
	valsType := m.vals[0].Type()
	switch valsType {
	case value.StringType:
		return sortAndPickStrings(m.vals)
	case value.IntType:
		return sortAndPickInts(m.vals)
	case value.NumberType:
		return sortAndPickDoubles(m.vals)
	case value.DateType:
		return sortAndPickDates(m.vals)
	case value.TimeType:
		return sortAndPickTimes(m.vals)
	case value.TimeOnlyType:
		return sortAndPickTimeOnly(m.vals)
	default:
		return nil
	}
}
func (m *median) Merge(a *aggr.AggPartial) {
	fmt.Errorf("merge is not supported by median aggregator")
}
func (m *median) Reset() { m.vals = make([]value.Value, 0) }

func NewMedian() aggr.Aggregator {
	return &median{
		vals: make([]value.Value, 0),
	}
}
