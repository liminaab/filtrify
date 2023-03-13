package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type TransformationOperatorType int64
type CellDataType int64

const (
	Filter TransformationOperatorType = iota
	NewColumn
	Aggregate
	Lookup
	MappedValue
	Sort
	RemoveColumn
	RenameColumn
	ChangeColumnType
	JSON
	Objectify
)

func (t TransformationOperatorType) String() string {
	switch t {
	case Filter:
		return "Filter"
	case NewColumn:
		return "NewColumn"
	case Aggregate:
		return "Aggregate"
	case Lookup:
		return "Lookup"
	case MappedValue:
		return "MappedValue"
	case Sort:
		return "Sort"
	case RemoveColumn:
		return "RemoveColumn"
	case RenameColumn:
		return "RenameColumn"
	case ChangeColumnType:
		return "ChangeColumnType"
	case JSON:
		return "JSON"
	case Objectify:
		return "Objectify"
	}
	return "Unknown"
}

// intValue            int32
// 	longValue           int64
// 	timestampValue      time.Time // used for Timestamp, Date and Time of day
// 	stringValue         string
// 	doubleValue         float64
// 	boolValue           bool
// 	is_original_field   bool

const (
	IntType CellDataType = iota
	LongType
	TimestampType
	StringType
	DoubleType
	BoolType
	NilType
	ObjectType
)

func (e CellDataType) String() string {
	switch e {
	case IntType:
		return "IntType"
	case LongType:
		return "LongType"
	case TimestampType:
		return "TimestampType"
	case StringType:
		return "StringType"
	case DoubleType:
		return "DoubleType"
	case BoolType:
		return "BoolType"
	case NilType:
		return "NilType"
	case ObjectType:
		return "ObjectType"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type InputData struct {
	RawData                  [][]string
	RawDataFirstLineIsHeader bool
}

type TransformationStep struct {
	Operator      TransformationOperatorType
	Configuration string
}

type TransformationOperator interface {
	Transform(dataset *DataSet, config string, otherSets map[string]*DataSet) (*DataSet, error)
	ValidateConfiguration(config string) (bool, error)
}

// type DataSet struct {
// 	RawData                  [][]string
// 	RawDataFirstLineIsHeader bool
// }

type DataSet struct {
	Rows []*DataRow
}

func (t *DataSet) ToRawData() [][]string {
	if len(t.Rows) < 1 {
		return [][]string{}
	}
	// we are adding one more row for headers
	rawData := make([][]string, len(t.Rows)+1)
	// let's use first row to extract headers
	firstRow := t.Rows[0]
	rawData[0] = make([]string, len(firstRow.Columns))
	for i, c := range firstRow.Columns {
		rawData[0][i] = c.ColumnName
	}

	for i, r := range t.Rows {
		rawData[i+1] = make([]string, len(r.Columns))
		for j, c := range r.Columns {
			rawData[i+1][j] = c.CellValue.ToString()
		}
	}

	return rawData
}

type DataRow struct {
	Columns []*DataColumn
}

type DataColumn struct {
	ColumnName string
	CellValue  *CellValue
}
type CellValue struct {
	DataType       CellDataType
	IntValue       int32
	LongValue      int64
	TimestampValue time.Time // used for Timestamp, Date and Time of day
	StringValue    string
	DoubleValue    float64
	BoolValue      bool
	ObjectValue    map[string]interface{}
}

func (c *CellValue) Value() interface{} {
	if c == nil {
		return nil
	}

	if c.DataType == NilType {
		return nil
	}

	switch c.DataType {
	case IntType:
		return c.IntValue
	case LongType:
		return c.LongValue
	case TimestampType:
		return c.TimestampValue
	case StringType:
		return c.StringValue
	case DoubleType:
		return c.DoubleValue
	case BoolType:
		return c.BoolValue
	case ObjectType:
		return c.ObjectValue
	}

	return nil
}

func (c *CellValue) ToString() string {
	if c == nil {
		return ""
	}

	if c.DataType == NilType {
		return ""
	}

	switch c.DataType {
	case IntType:
		return strconv.FormatInt(int64(c.IntValue), 10)
	case LongType:
		return strconv.FormatInt(c.LongValue, 10)
	case TimestampType:
		return c.TimestampValue.Format(time.RFC3339)
	case StringType:
		return c.StringValue
	case DoubleType:
		return strconv.FormatFloat(c.DoubleValue, 'f', -1, 64)
	case BoolType:
		if c.BoolValue {
			return "true"
		}
		return "false"
	case ObjectType:
		b, err := json.Marshal(c.ObjectValue)
		if err != nil {
			return ""
		}
		return string(b)
	}

	return ""
}

func (v *CellValue) IsNumeric() bool {
	switch v.DataType {
	case IntType:
		return true
	case LongType:
		return true
	case TimestampType:
		return false
	case StringType:
		return false
	case DoubleType:
		return true
	case BoolType:
		return false
	case NilType:
		return false
	case ObjectType:
		return false
	}

	return false
}

func (v *CellValue) GetNumericVal() float64 {

	switch v.DataType {
	case IntType:
		return float64(v.IntValue)
	case LongType:
		return float64(v.LongValue)
	case DoubleType:
		return v.DoubleValue
	}
	return -1
}

func (v *CellValue) Equals(other *CellValue) bool {
	if v == nil || other == nil {
		return false
	}

	if v.DataType != other.DataType {
		// there is only one exception here - if these are numeric types we still should check their
		if v.IsNumeric() && other.IsNumeric() {
			return v.GetNumericVal() == other.GetNumericVal()
		}

		return false
	}

	if v.DataType == NilType || other.DataType == NilType {
		return false
	}

	switch v.DataType {
	case IntType:
		return v.IntValue == other.IntValue
	case LongType:
		return v.LongValue == other.LongValue
	case TimestampType:
		return v.TimestampValue.Equal(other.TimestampValue)
	case StringType:
		return v.StringValue == other.StringValue
	case DoubleType:
		return v.DoubleValue == other.DoubleValue
	case BoolType:
		return v.BoolValue == other.BoolValue
	case ObjectType:
		// we don't support object comparison for now
		return false
	}

	return false
}

// func (v CellValue) Add(other CellValue) (CellValue, error)      {}
// func (v CellValue) Subtract(other CellValue) (CellValue, error) {}
// func (v CellValue) Multiply(other CellValue) (CellValue, error) {}
// func (v CellValue) Divide(other CellValue) (CellValue, error)   {}
// func (v CellValue) Equals(other CellValue) (CellValue, error)   {}
