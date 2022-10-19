package dataset

import (
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
