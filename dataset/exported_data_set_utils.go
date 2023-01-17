package dataset

import (
	"github.com/liminaab/filtrify/conversion"
	"time"

	"github.com/liminaab/filtrify/types"
)

// Some helpers to make it easier to use the library from the outside

func New(rows []*types.DataRow) *types.DataSet {
	return &types.DataSet{
		Rows: rows,
	}
}

func DataRow(columns ...*types.DataColumn) *types.DataRow {
	return &types.DataRow{
		Columns: columns,
	}
}

func NewColumn(name string, val string, targetType types.CellDataType) (*types.DataColumn, error) {
	cell, err := conversion.ParseToCell(val, targetType)
	if err != nil {
		return nil, err
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  cell,
	}, nil
}

func LongColumn(name string, val int64) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue: &types.CellValue{
			DataType:  types.LongType,
			LongValue: val,
		},
	}
}

func StringColumn(name string, val string) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue: &types.CellValue{
			DataType:    types.StringType,
			StringValue: val,
		},
	}
}

func IntColumn(name string, val int32) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.IntType, IntValue: val},
	}
}

func TimestampColumn(name string, val time.Time) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.TimestampType, TimestampValue: val},
	}
}

func DoubleColumn(name string, val float64) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.DoubleType, DoubleValue: val},
	}
}

func BoolColumn(name string, val bool) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.BoolType, BoolValue: val},
	}
}

func NilColumn(name string) *types.DataColumn {
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.NilType},
	}
}

func LongOrNilColumn(name string, val *int64) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue: &types.CellValue{
			DataType:  types.LongType,
			LongValue: *val,
		},
	}
}

func StringOrNilColumn(name string, val *string) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue: &types.CellValue{
			DataType:    types.StringType,
			StringValue: *val,
		},
	}
}

func IntOrNilColumn(name string, val *int32) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.IntType, IntValue: *val},
	}
}

func TimestampOrNilColumn(name string, val *time.Time) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.TimestampType, TimestampValue: *val},
	}
}

func DoubleOrNilColumn(name string, val *float64) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.DoubleType, DoubleValue: *val},
	}
}

func BoolOrNilColumn(name string, val *bool) *types.DataColumn {
	if val == nil {
		return NilColumn(name)
	}
	return &types.DataColumn{
		ColumnName: name,
		CellValue:  &types.CellValue{DataType: types.BoolType, BoolValue: *val},
	}
}
