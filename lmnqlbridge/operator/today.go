package operator

import (
	"fmt"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Now Get current time of Message (message time stamp) or else choose current
// server time if none is available in message context
//
type Today struct{}

// Type time
func (m *Today) Type() value.ValueType { return value.TimeType }

func (m *Today) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("expected 0 args for today() but got %s", n)
	}
	return todayEval, nil
}
func todayEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	year, month, day := time.Now().Date()
	bod := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return value.NewTimeValue(bod), true
}
