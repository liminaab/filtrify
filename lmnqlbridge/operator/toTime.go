package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"time"
)

// todate:   convert to Date
//
//	// uses lytics/datemath
//	todatetime("now-3m")
//
//	// uses araddon/dateparse util to recognize formats
//	todatetime(field)
//
//	// first parameter is the layout/format
//	todatetime("01/02/2006", field )
type ToTime struct{}

// Type time
func (m *ToTime) Type() value.ValueType { return value.TimeType }
func (m *ToTime) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 1 {
		return nil, fmt.Errorf(`Expected 1 arg for ToTime([format] , field) but got %s`, n)
	}
	return toTimeEval, nil
}

func ParseTime(s string) (civil.Time, error) {
	t, err := time.Parse("15:04:05.999999999", s)
	if err == nil {
		return civil.TimeOf(t), nil
	}
	t, err = time.Parse("15:04:05", s)
	if err == nil {
		return civil.TimeOf(t), nil
	}
	t, err = time.Parse("15:04", s)
	if err == nil {
		return civil.TimeOf(t), nil
	}
	return civil.Time{}, err
}

func toTimeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if len(args) == 1 {
		timeStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeOnlyZeroValue, false
		}
		t, err := ParseTime(timeStr)

		if err != nil {
			return value.TimeOnlyZeroValue, false
		}
		return value.NewTimeOnlyValue(t), true
	}
	return value.TimeOnlyZeroValue, false
}
