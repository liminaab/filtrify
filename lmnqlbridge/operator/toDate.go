package operator

import (
	"cloud.google.com/go/civil"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
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
	if len(n.Args) == 1 {
		return nil, fmt.Errorf(`Expected 1 arg for ToDate([format] , field) but got %s`, n)
	}
	return toDateEval, nil
}
func toDateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if len(args) == 1 {
		dateText, ok := value.ValueToString(args[0])
		if !ok {
			return value.DateZeroValue, false
		}
		d, err := civil.ParseDate(dateText)
		if err != nil {
			return value.DateZeroValue, false
		}
		return value.NewDateValue(d), true
	}
	return value.DateZeroValue, false
}
