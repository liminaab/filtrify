package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Right struct{}

func lastN(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func (m *Right) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("expected 2 arg for right(arg) but got %s", n)
	}
	return rightEval, nil
}

func rightEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	charCount, ok := value.ValueToInt(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	return value.NewStringValue(lastN(val, charCount)), true
}

func (m *Right) Type() value.ValueType { return value.StringType }
