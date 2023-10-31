package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"time"
)

type DateTimeUTC struct{}

// Type time
func (m *DateTimeUTC) Type() value.ValueType { return value.DateType }
func (m *DateTimeUTC) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 6 {
		return nil, fmt.Errorf(`Expected 6 args for DATETIMEUTC(year, month, day, hour, minutes, second) but got %s`, n)
	}
	return dateTimeUTCEval, nil
}
func dateTimeUTCEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	years := getNumericTypeValueForTime(args[0])
	months := getNumericTypeValueForTime(args[1])
	days := getNumericTypeValueForTime(args[2])
	hours := getNumericTypeValueForTime(args[3])
	minutes := getNumericTypeValueForTime(args[4])
	seconds := getNumericTypeValueForTime(args[5])
	selectedDateTime := time.Date(years, time.Month(months), days, hours, minutes, seconds, 0, time.UTC)
	return value.NewTimeValue(selectedDateTime), true
}
