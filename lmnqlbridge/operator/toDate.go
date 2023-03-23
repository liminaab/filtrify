package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/lytics/datemath"
	"strings"
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
type ToDate struct{}

// Type time
func (m *ToDate) Type() value.ValueType { return value.DateType }
func (m *ToDate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return nil, fmt.Errorf(`Expected 1 or 2 args for ToDate([format] , field) but got %s`, n)
	}
	return toDateEval, nil
}
func toDateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if len(args) == 1 {
		dateStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.DateZeroValue, false
		}

		if len(dateStr) > 3 && strings.ToLower(dateStr[:3]) == "now" {
			// Is date math
			if t, err := datemath.Eval(dateStr[3:]); err == nil {
				return value.NewDateValue(civil.DateOf(t)), true
			}
		} else {
			if t, err := dateparse.ParseAny(dateStr); err == nil {
				return value.NewDateValue(civil.DateOf(t)), true
			}
		}

	} else if len(args) == 2 {

		formatStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.DateZeroValue, false
		}

		dateStr, ok := value.ValueToString(args[1])
		if !ok {
			return value.DateZeroValue, false
		}

		//u.Infof("hello  layout=%v  time=%v", formatStr, dateStr)
		if t, err := time.Parse(formatStr, dateStr); err == nil {
			return value.NewDateValue(civil.DateOf(t)), true
		}
	}

	return value.DateZeroValue, false
}
