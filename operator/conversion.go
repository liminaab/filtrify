package operator

import (
	"cloud.google.com/go/civil"
	"github.com/araddon/qlbridge/value"
	"reflect"
	"time"
	"unsafe"

	"github.com/liminaab/filtrify/types"
)

func convertToCell(d interface{}, existingType types.CellDataType) *types.CellValue {
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
	case civil.Date:
		cell.DataType = types.DateType
		cell.TimestampValue = v.In(time.UTC)
	case civil.Time:
		cell.DataType = types.TimeOfDayType
		cell.TimestampValue = time.Date(0, 0, 0, v.Hour, v.Minute, v.Second, v.Nanosecond, time.UTC)
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
		// this might be 3 different types
		// we need to check the existing type
		// if it's nil, we'll just assume it's a timestamp
		if existingType == types.NilType {
			cell.DataType = types.TimestampType
		} else {
			cell.DataType = existingType
		}
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
	case map[string]value.Value:
		// let's convert this to our internal object type
		objectVal := make(map[string]interface{})
		for k, v := range v {
			objectVal[k] = v.Value()
		}
		cell.DataType = types.ObjectType
		cell.ObjectValue = objectVal
	default:
		cell.DataType = types.NilType
		break
	}
	return &cell
}

func convertToDataSet(data [][]interface{}, headers []string, existingColumnTypeMap map[string]types.CellDataType) *types.DataSet {
	dataSet := &types.DataSet{}
	dataSet.Rows = make([]*types.DataRow, len(data))
	for ri, r := range data {
		dataRow := types.DataRow{}
		dataRow.Columns = make([]*types.DataColumn, len(headers))
		for i, c := range r {
			existingType, ok := existingColumnTypeMap[headers[i]]
			if !ok {
				existingType = types.NilType
			}
			dataRow.Columns[i] = &types.DataColumn{}
			dataRow.Columns[i].ColumnName = headers[i]
			dataRow.Columns[i].CellValue = convertToCell(c, existingType)
		}

		dataSet.Rows[ri] = &dataRow
	}

	return dataSet
}
