package operator

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type AND struct{}

func (m *AND) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("expected at least 2 arg for AND(arg) but got %s", n)
	}
	return andEval, nil
}

func andEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	for _, arg := range args {
		boolVal := getArgBooleanValue(arg)
		if !boolVal {
			return value.NewBoolValue(false), true
		}
	}

	return value.NewBoolValue(true), true
}

func (m *AND) Type() value.ValueType { return value.BoolType }

func getArgBooleanValue(arg value.Value) bool {
	if arg.Nil() {
		return false
	}
	switch arg.Type() {
	case value.NilType:
		return false
	case value.ErrorType:
		return false
	case value.UnknownType:
		return false
	case value.ValueInterfaceType:
		return arg.Value() != nil
	case value.NumberType:
		return arg.(value.NumberValue).Float() != 0
	case value.IntType:
		return arg.(value.IntValue).Int() != 0
	case value.BoolType:
		return arg.(value.BoolValue).Val()
	case value.TimeType:
		return !arg.(value.TimeValue).Time().IsZero()
	case value.ByteSliceType:
		return len(arg.(value.ByteSliceValue).Val()) > 0
	case value.StringType:
		return arg.(value.StringValue).Val() != ""
	case value.StringsType:
		return len(arg.(value.StringsValue).Val()) > 0
	case value.MapValueType:
		return len(arg.(value.MapValue).Val()) > 0
	case value.MapIntType:
		return len(arg.(value.MapIntValue).Val()) > 0
	case value.MapStringType:
		return len(arg.(value.MapStringValue).Val()) > 0
	case value.MapNumberType:
		return len(arg.(value.MapNumberValue).Val()) > 0
	case value.MapTimeType:
		return len(arg.(value.MapTimeValue).Val()) > 0
	case value.MapBoolType:
		return len(arg.(value.MapBoolValue).Val()) > 0
	case value.SliceValueType:
		return len(arg.(value.SliceValue).Val()) > 0
	case value.StructType:
		return arg.(value.StructValue).Val() != nil
	case value.JsonType:
		return arg.(value.JsonValue).Val() != nil
	case value.TimeOnlyType:
		return arg.(value.TimeOnlyValue).Val().IsValid()
	case value.DateType:
		return arg.(value.DateValue).Val().IsValid()
	default:
		return false
	}
}
