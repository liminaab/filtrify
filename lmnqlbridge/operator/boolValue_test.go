package operator

import (
	"cloud.google.com/go/civil"
	"encoding/json"
	"github.com/araddon/qlbridge/value"
	"testing"
	"time"
)

func TestGetArgBooleanValue(t *testing.T) {
	now := time.Now()
	invalidDate := civil.Date{}
	validDate := civil.Date{Year: 2023, Month: 5, Day: 31}
	invalidTime := civil.Time{
		Hour: 25,
	}
	validTime := civil.Time{Hour: 12, Minute: 0, Second: 0, Nanosecond: 0}

	tests := []struct {
		name string
		arg  value.Value
		want bool
	}{
		{"nilValue", value.NewNilValue(), false},
		{"errorValue", value.NewErrorValue(nil), false},
		{"numberValueZero", value.NewNumberValue(0.0), false},
		{"numberValueNonZero", value.NewNumberValue(1.0), true},
		{"intValueZero", value.NewIntValue(0), false},
		{"intValueNonZero", value.NewIntValue(1), true},
		{"boolValueFalse", value.NewBoolValue(false), false},
		{"boolValueTrue", value.NewBoolValue(true), true},
		{"timeValueZero", value.NewTimeValue(time.Time{}), false},
		{"timeValueNonZero", value.NewTimeValue(now), true},
		{"byteSliceValueEmpty", value.NewByteSliceValue([]byte{}), false},
		{"byteSliceValueNonEmpty", value.NewByteSliceValue([]byte{1, 2, 3}), true},
		{"stringValueEmpty", value.NewStringValue(""), false},
		{"stringValueNonEmpty", value.NewStringValue("hello"), true},
		{"stringsValueEmpty", value.NewStringsValue([]string{}), false},
		{"stringsValueNonEmpty", value.NewStringsValue([]string{"hello"}), true},
		{"mapValueEmpty", value.NewMapValue(map[string]interface{}{}), false},
		{"mapIntValueEmpty", value.NewMapIntValue(make(map[string]int64)), false},
		{"mapNumberValueEmpty", value.NewMapNumberValue(make(map[string]float64)), false},
		{"mapStringValueEmpty", value.NewMapStringValue(make(map[string]string)), false},
		{"mapBoolValueEmpty", value.NewMapBoolValue(make(map[string]bool)), false},
		{"mapTimeValueEmpty", value.NewMapTimeValue(make(map[string]time.Time)), false},
		{"structValueNil", value.NewStructValue(nil), false},
		{"jsonValueEmpty", value.NewJsonValue(json.RawMessage{}), true},
		{"jsonValueNil", value.NewJsonValue(nil), false},
		{"timeOnlyValueInvalid", value.NewTimeOnlyValue(invalidTime), false},
		{"timeOnlyValueValid", value.NewTimeOnlyValue(validTime), true},
		{"dateValueInvalid", value.NewDateValue(invalidDate), false},
		{"dateValueValid", value.NewDateValue(validDate), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getArgBooleanValue(tt.arg); got != tt.want {
				t.Errorf("getArgBooleanValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
