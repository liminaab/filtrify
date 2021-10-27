package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Left struct{}

func firstN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (m *Left) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for left(arg) but got %s", n)
	}
	return leftEval, nil
}

func leftEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	charCount, ok := value.ValueToInt(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	return value.NewStringValue(firstN(val, charCount)), true
}

func (m *Left) Type() value.ValueType { return value.StringType }
