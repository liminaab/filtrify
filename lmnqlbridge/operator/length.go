package operator

import (
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type Length struct{}

// Type is string
func (m *Length) Type() value.ValueType { return value.IntType }
func (m *Length) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf(`expected 1 arg for length(ColumnName) but got %s`, n)
	}
	return lengthEval, nil
}

func lengthEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if vals[0].Nil() {
		return value.NewIntValue(0), true
	}
	if vals[0].Type() == value.NilType {
		return value.NewIntValue(0), true
	}
	val, ok := value.ValueToString(vals[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(int64(len(val))), true
}
