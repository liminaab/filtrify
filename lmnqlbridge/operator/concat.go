package operator

import (
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Concat struct{}

// Type is string
func (m *Concat) Type() value.ValueType { return value.StringType }
func (m *Concat) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`expected 2 or more args for concat("apples","ap") but got %s`, n)
	}
	return concatEval, nil
}

func concatEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	sep, ok := value.ValueToString(vals[len(vals)-1])
	if !ok {
		return value.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(vals)-1; i++ {
		switch valTyped := vals[i].(type) {
		case value.SliceValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv.ToString()
			}
			args = append(args, svals...)
		case value.StringsValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv
			}
			args = append(args, svals...)
		case value.StringValue, value.NumberValue, value.IntValue:
			val := valTyped.ToString()
			if val == "" {
				continue
			}
			args = append(args, val)
		case value.BoolValue:
			val := "false"
			if valTyped.Val() {
				val = "true"
			}
			args = append(args, val)
		}
	}
	if len(args) == 0 {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.Join(args, sep)), true
}
