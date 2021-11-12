package types

import (
	"fmt"
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

type DataRow struct {
	Columns []*DataColumn
}
type DataColumn struct {
	ColumnName string
	CellValue  *CellValue
}
type CellValue struct {
	DataType            CellDataType
	IntValue            int32
	LongValue           int64
	TimestampValue      time.Time // used for Timestamp, Date and Time of day
	StringValue         string
	DoubleValue         float64
	BoolValue           bool
	Is_original_field   bool
	Original_field_name string // e.g. “name”
	// original_field_source enum   // e.g. “enums.DataSourcePortfolio”
}

func (v *CellValue) Equals(other *CellValue) bool {
	if v == nil || other == nil {
		return false
	}

	if v.DataType != other.DataType {
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
	}

	return false
}

// func (v CellValue) Add(other CellValue) (CellValue, error)      {}
// func (v CellValue) Subtract(other CellValue) (CellValue, error) {}
// func (v CellValue) Multiply(other CellValue) (CellValue, error) {}
// func (v CellValue) Divide(other CellValue) (CellValue, error)   {}
// func (v CellValue) Equals(other CellValue) (CellValue, error)   {}
