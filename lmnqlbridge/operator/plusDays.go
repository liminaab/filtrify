package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/dateparse"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Plusdays struct{}

func (m *Plusdays) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for Plusdays(arg) but got %s", n)
	}
	return plusDaysEval, nil
}

func plusDaysEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	dateTimePart := args[0]
	dayCountPart := args[1]

	if !dayCountPart.Type().IsNumeric() {
		return value.NewTimeValue(time.Time{}), false
	}

	// we can only process these 3 as date types
	if dateTimePart.Type() != value.TimeType && dateTimePart.Type() != value.StringType && dateTimePart.Type() != value.DateType {
		return value.NewTimeValue(time.Time{}), false
	}
	var parsedTime *time.Time = nil
	if dateTimePart.Type() == value.StringType {
		// let's try to parse it
		pTime, err := dateparse.ParseAny(dateTimePart.ToString())
		if err != nil {
			return value.NewTimeValue(time.Time{}), false
		}
		parsedTime = &pTime
	}
	if dateTimePart.Type() == value.TimeType {
		pTime := dateTimePart.Value().(time.Time)
		parsedTime = &pTime
	}
	if dateTimePart.Type() == value.DateType {
		date := dateTimePart.Value().(civil.Date)
		pTime := date.In(time.UTC)
		parsedTime = &pTime
	}
	if parsedTime == nil {
		return value.NewTimeValue(time.Time{}), false
	}

	dayCountNumericVal := dayCountPart.(value.NumericValue)
	parsedDayCount := int(dayCountNumericVal.Int())
	addedTime := parsedTime.AddDate(0, 0, parsedDayCount)

	return value.NewTimeValue(addedTime), true

}

func (m *Plusdays) Type() value.ValueType { return value.TimeType }
