package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"strings"
)

type Replace struct{}

func (m *Replace) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf("expected 3 arg for replace(arg) but got %s", n)
	}
	return replaceEval, nil
}

func replaceEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	patternToReplace, ok := value.ValueToString(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	patternToReplaceWith, ok := value.ValueToString(args[2])
	if !ok {
		return value.EmptyStringValue, false
	}
	result := strings.ReplaceAll(val, patternToReplace, patternToReplaceWith)
	return value.NewStringValue(result), true
}

func (m *Replace) Type() value.ValueType { return value.StringType }
