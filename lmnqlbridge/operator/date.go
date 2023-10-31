package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"time"
)

type Date struct{}

// Type time
func (m *Date) Type() value.ValueType { return value.DateType }
func (m *Date) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf(`Expected 3 args for DATE(year, month, day) but got %s`, n)
	}
	return dateEval, nil
}
func dateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	years := getNumericTypeValueForTime(args[0])
	months := getNumericTypeValueForTime(args[1])
	days := getNumericTypeValueForTime(args[2])

	selectedDate := civil.Date{
		Year:  years,
		Month: time.Month(months),
		Day:   days,
	}

	return value.NewDateValue(selectedDate), true
}
