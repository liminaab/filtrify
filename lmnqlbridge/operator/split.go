package operator

import (
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Split struct{}

func (m *Split) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 3 {
		return nil, fmt.Errorf("expected 3 arg for split(arg) but got %s", n)
	}
	return splitEval, nil
}

func splitEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	seperator, ok := value.ValueToString(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	wordIndex, ok := value.ValueToInt(args[2])
	if !ok {
		return value.EmptyStringValue, false
	}

	splittedList := strings.Split(val, seperator)
	if len(splittedList) < wordIndex {
		return value.NewStringValue(""), true
	}

	return value.NewStringValue(splittedList[wordIndex-1]), true
}

func (m *Split) Type() value.ValueType { return value.StringType }
