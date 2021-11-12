package operator

import (
	"reflect"
	"time"
	"unsafe"

	"github.com/liminaab/filtrify/types"
)

func convertToCell(d interface{}) *types.CellValue {
	cell := types.CellValue{}
	isPtr := reflect.ValueOf(d).Type().Kind() == reflect.Ptr
	if isPtr {
		d = reflect.Indirect(reflect.ValueOf(d)).Interface()
	}

	switch v := d.(type) {
	case int32:
		cell.DataType = types.IntType
		cell.IntValue = v
		break
	case int64:
		cell.DataType = types.LongType
		cell.LongValue = v
		break
	case int:
		size := unsafe.Sizeof(v)
		if size == 4 {
			cell.DataType = types.IntType
			cell.IntValue = d.(int32)
		} else {
			cell.DataType = types.LongType
			cell.LongValue = d.(int64)
		}
		break
	case time.Time:
		cell.DataType = types.TimestampType
		cell.TimestampValue = v
		break
	case string:
		cell.DataType = types.StringType
		cell.StringValue = v
		break
	case float32:
		cell.DataType = types.DoubleType
		cell.DoubleValue = float64(v)
		break
	case float64:
		cell.DataType = types.DoubleType
		cell.DoubleValue = v
		break
	case bool:
		cell.DataType = types.BoolType
		cell.BoolValue = v
		break
	default:
		cell.DataType = types.NilType
		break
	}
	return &cell
}

func convertToDataSet(data [][]interface{}, headers []string) *types.DataSet {
	dataSet := &types.DataSet{}
	dataSet.Rows = make([]*types.DataRow, len(data))
	for ri, r := range data {
		dataRow := types.DataRow{}
		dataRow.Columns = make([]*types.DataColumn, len(headers))
		for i, c := range r {
			dataRow.Columns[i] = &types.DataColumn{}
			dataRow.Columns[i].ColumnName = headers[i]
			dataRow.Columns[i].CellValue = convertToCell(c)
		}

		dataSet.Rows[ri] = &dataRow
	}

	return dataSet
}
