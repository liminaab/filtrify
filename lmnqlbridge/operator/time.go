package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Now Get current time of Message (message time stamp) or else choose current
// server time if none is available in message context
type Time struct{}

// Type time
func (m *Time) Type() value.ValueType { return value.TimeType }

func (m *Time) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf("expected 3 args for time() but got %s", n)
	}
	return timeEval, nil
}

func getNumericTypeValueForTime(arg value.Value) int {
	if !arg.Type().IsNumeric() {
		return 0
	}

	switch val := arg.(type) {
	case value.NumberValue:
		return int(val.Float())
	case value.IntValue:
		return int(val.Int())
	default:
		return 0
	}
}

func timeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	var hours int = 0
	var minutes int = 0
	var seconds int = 0
	if len(args) > 0 {
		hours = getNumericTypeValueForTime(args[0])
	}
	if len(args) > 1 {
		minutes = getNumericTypeValueForTime(args[1])
	}
	if len(args) > 2 {
		seconds = getNumericTypeValueForTime(args[2])
	}

	bod := civil.Time{
		Hour:       hours,
		Minute:     minutes,
		Second:     seconds,
		Nanosecond: 0,
	}
	return value.NewTimeOnlyValue(bod), true
}
