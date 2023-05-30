package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"strings"
)

type Trim struct{}

// Type is string
func (m *Trim) Type() value.ValueType { return value.StringType }
func (m *Trim) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf(`expected 1 arg for trim(ColumnName) but got %s`, n)
	}
	return trimEval, nil
}

func trimEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(vals[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.TrimSpace(val)), true
}
