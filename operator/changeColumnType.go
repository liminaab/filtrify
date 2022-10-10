package operator

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type ChangeColumnTypeOperator struct {
}

type ChangeColumnTypeConfiguration struct {
	Columns map[string]ConversionConfiguration `json:"columns"`
}

type ConversionConfiguration struct {
	TargetType      types.CellDataType               `json:"targetType"`
	StringToNumeric FromStringToNumericConfiguration `json:"stringToNumericConfiguration"`
	StringToDate    FromStringToDateConfiguration    `json:"stringToDateConfiguration"`
}

type FromStringToNumericConfiguration struct {
	DecimalSymbol     string `json:"decimalSymbol"`
	ThousandSeperator string `json:"thousandSeperator"`
}

type FromStringToDateConfiguration struct {
}

type conversionFunc func(I interface{}, config ConversionConfiguration) interface{}

var conversionMap map[types.CellDataType]map[types.CellDataType]conversionFunc = map[types.CellDataType]map[types.CellDataType]conversionFunc{}

func init() {
	// Timestamp conversion functions
	conversionMap[types.TimestampType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.TimestampType][types.TimestampType] = noopConversion
	conversionMap[types.TimestampType][types.StringType] = timeToString
	conversionMap[types.TimestampType][types.IntType] = timeToInt
	conversionMap[types.TimestampType][types.LongType] = timeToLong
	conversionMap[types.TimestampType][types.DoubleType] = timeToDouble
	conversionMap[types.TimestampType][types.BoolType] = timeToBool

	// Integer conversion functions
	conversionMap[types.IntType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.IntType][types.IntType] = noopConversion
	conversionMap[types.IntType][types.StringType] = intToString
	conversionMap[types.IntType][types.TimestampType] = intToTime
	conversionMap[types.IntType][types.LongType] = intToLong
	conversionMap[types.IntType][types.DoubleType] = intToDouble
	conversionMap[types.IntType][types.BoolType] = intToBool

	// Long conversion functions
	conversionMap[types.LongType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.LongType][types.LongType] = noopConversion
	conversionMap[types.LongType][types.StringType] = longToString
	conversionMap[types.LongType][types.TimestampType] = longToTime
	conversionMap[types.LongType][types.IntType] = longToInt
	conversionMap[types.LongType][types.DoubleType] = longToDouble
	conversionMap[types.LongType][types.BoolType] = longToBool

	// Double conversion functions
	conversionMap[types.DoubleType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.DoubleType][types.DoubleType] = noopConversion
	conversionMap[types.DoubleType][types.StringType] = doubleToString
	conversionMap[types.DoubleType][types.TimestampType] = doubleToTime
	conversionMap[types.DoubleType][types.IntType] = doubleToInt
	conversionMap[types.DoubleType][types.LongType] = doubleToLong
	conversionMap[types.DoubleType][types.BoolType] = doubleToBool

	// Bool conversion functions
	conversionMap[types.BoolType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.BoolType][types.BoolType] = noopConversion
	conversionMap[types.BoolType][types.StringType] = boolToString
	conversionMap[types.BoolType][types.TimestampType] = boolToTime
	conversionMap[types.BoolType][types.IntType] = boolToInt
	conversionMap[types.BoolType][types.LongType] = boolToLong
	conversionMap[types.BoolType][types.DoubleType] = boolToDouble

	// String conversion functions
	conversionMap[types.StringType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.StringType][types.StringType] = noopConversion
	conversionMap[types.StringType][types.BoolType] = stringToBool
	conversionMap[types.StringType][types.TimestampType] = stringToTime
	conversionMap[types.StringType][types.IntType] = stringToInt
	conversionMap[types.StringType][types.LongType] = stringToLong
	conversionMap[types.StringType][types.DoubleType] = stringToDouble
}

func (t *ChangeColumnTypeOperator) convertColumn(col *types.DataColumn, config ConversionConfiguration) types.DataColumn {
	nilColumn := types.DataColumn{
		ColumnName: col.ColumnName,
		CellValue: &types.CellValue{
			DataType: types.NilType,
		},
	}
	targetMap, found := conversionMap[col.CellValue.DataType]
	if !found {
		return nilColumn
	}
	conversionF, found := targetMap[config.TargetType]
	if !found {
		return nilColumn
	}
	var sourceData interface{}
	switch col.CellValue.DataType {
	case types.TimestampType:
		sourceData = col.CellValue.TimestampValue
	case types.IntType:
		sourceData = col.CellValue.IntValue
	case types.LongType:
		sourceData = col.CellValue.LongValue
	case types.DoubleType:
		sourceData = col.CellValue.DoubleValue
	case types.BoolType:
		sourceData = col.CellValue.BoolValue
	case types.StringType:
		sourceData = col.CellValue.StringValue
	case types.NilType:
		return nilColumn
	}
	convertedData := conversionF(sourceData, config)
	convertedColumn := types.DataColumn{
		ColumnName: col.ColumnName,
		CellValue: &types.CellValue{
			DataType: config.TargetType,
		},
	}
	switch config.TargetType {
	case types.TimestampType:
		convertedColumn.CellValue.TimestampValue = convertedData.(time.Time)
	case types.IntType:
		convertedColumn.CellValue.IntValue = convertedData.(int32)
	case types.LongType:
		convertedColumn.CellValue.LongValue = convertedData.(int64)
	case types.DoubleType:
		convertedColumn.CellValue.DoubleValue = convertedData.(float64)
	case types.BoolType:
		convertedColumn.CellValue.BoolValue = convertedData.(bool)
	case types.StringType:
		convertedColumn.CellValue.StringValue = convertedData.(string)
	case types.NilType:
		return nilColumn
	}
	return convertedColumn
}

func (t *ChangeColumnTypeOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	newDataset := types.DataSet{
		Rows: make([]*types.DataRow, len(dataset.Rows)),
	}

	for i, row := range dataset.Rows {
		newRow := types.DataRow{
			Columns: make([]*types.DataColumn, 0),
		}
		for _, col := range row.Columns {
			newType, found := typedConfig.Columns[col.ColumnName]
			if !found {
				newRow.Columns = append(newRow.Columns, col)
				continue
			}
			newCol := t.convertColumn(col, newType)
			newRow.Columns = append(newRow.Columns, &newCol)
		}
		newDataset.Rows[i] = &newRow
	}

	return &newDataset, nil
}

func (t *ChangeColumnTypeOperator) buildConfiguration(config string) (*ChangeColumnTypeConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := ChangeColumnTypeConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Columns) < 1 {
		return nil, errors.New("missing columns in changeColumnType configuration")
	}

	return &typedConfig, nil
}

func (t *ChangeColumnTypeOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}

func noopConversion(input interface{}, config ConversionConfiguration) interface{} {
	return input
}

func timeToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	convertedInputText := convertedInput.Format("2006-01-02 15:04:05")
	return convertedInputText
}

func timeToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return int32(convertedInput.Unix())
}

func timeToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return convertedInput.Unix()
}

func timeToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return float64(convertedInput.Unix())
}

func timeToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return !convertedInput.IsZero()
}

func intToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return strconv.Itoa(int(convertedInput))
}

func intToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return time.Unix(int64(convertedInput), 0)
}

func intToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return int64(convertedInput)
}

func intToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return float64(convertedInput)
}

func intToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return convertedInput != 0
}

func longToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return strconv.FormatInt(convertedInput, 10)
}

func longToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return time.Unix(convertedInput, 0)
}

func longToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return int32(convertedInput)
}

func longToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return float64(convertedInput)
}

func longToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return convertedInput != 0
}

func doubleToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return strconv.FormatFloat(convertedInput, 'f', -1, 64)
}

func doubleToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return time.Unix(int64(convertedInput), 0)
}

func doubleToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return int32(convertedInput)
}

func doubleToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return int64(convertedInput)
}

func doubleToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return convertedInput != 0
}

func boolToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(bool)
	if !convertedInput {
		return "False"
	}
	return "True"
}

func boolToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(bool)
	if !convertedInput {
		return time.Time{}
	}
	return time.Now()
}

func boolToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(bool)
	if !convertedInput {
		return int32(0)
	}
	return int32(1)
}

func boolToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(bool)
	if !convertedInput {
		return int64(0)
	}
	return int64(1)
}

func boolToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(bool)
	if !convertedInput {
		return float64(0)
	}
	return float64(1)
}

func stringToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	if strings.EqualFold(convertedInput, "true") {
		return true
	}
	return false
}

func stringToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	parsedTime, err := parseTimestamp(convertedInput)
	if err != nil {
		return time.Time{}
	}
	return parsedTime
}

func stringToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	i, err := strconv.ParseInt(convertedInput, 10, 32)
	if err != nil {
		return int32(0)
	}
	return int32(i)
}

func stringToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	i, err := strconv.ParseInt(convertedInput, 10, 64)
	if err != nil {
		return int64(0)
	}
	return i
}

func stringToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	if len(config.StringToNumeric.ThousandSeperator) > 0 {
		// let's throw away thousand seperator
		convertedInput = strings.Replace(convertedInput, config.StringToNumeric.ThousandSeperator, "", -1)
	}
	if len(config.StringToNumeric.DecimalSymbol) > 0 && config.StringToNumeric.DecimalSymbol != "." {
		convertedInput = strings.Replace(convertedInput, config.StringToNumeric.DecimalSymbol, ".", 1)
	}
	i, err := strconv.ParseFloat(convertedInput, 64)
	if err != nil {
		return float64(0)
	}
	return i
}
