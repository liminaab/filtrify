package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
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
	TargetType    types.CellDataType          `json:"targetType"`
	StringNumeric *StringNumericConfiguration `json:"stringNumericConfiguration"`
	StringDate    *StringDateConfiguration    `json:"stringDateConfiguration"`
	NumericDate   *NumericDateConfiguration   `json:"numericDateConfiguration"`
	DateTimeDate  *DateTimeDateConfiguration  `json:"dateTimeDateConfiguration"`
}

type DateTimeDateConfiguration struct {
	Timezone     string `json:"timezone"`
	SelectedTime string `json:"selectedTime"`
}

type StringNumericConfiguration struct {
	DecimalSymbol     string `json:"decimalSymbol"`
	ThousandSeperator string `json:"thousandSeperator"`
	NumberOfDecimals  int    `json:"numberOfDecimals"`
}

type NumericDateConfiguration struct {
	IsUnixSeconds bool `json:"isUnixSeconds"`
	IsUnixMillis  bool `json:"isUnixMillis"`
	IsExcelDate   bool `json:"isExcelDate"`
}

type StringDateConfiguration struct {
	DateFormat string `json:"dateFormat"`
	Timezone   string `json:"timezone"`
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
	conversionMap[types.TimestampType][types.DateType] = timeToDate
	conversionMap[types.TimestampType][types.TimeOfDayType] = timeToTimeofDay

	// Integer conversion functions
	conversionMap[types.IntType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.IntType][types.IntType] = noopConversion
	conversionMap[types.IntType][types.StringType] = intToString
	conversionMap[types.IntType][types.TimestampType] = intToTime
	conversionMap[types.IntType][types.LongType] = intToLong
	conversionMap[types.IntType][types.DoubleType] = intToDouble
	conversionMap[types.IntType][types.BoolType] = intToBool
	conversionMap[types.IntType][types.DateType] = intToDate
	conversionMap[types.IntType][types.TimeOfDayType] = noopConversion

	// Long conversion functions
	conversionMap[types.LongType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.LongType][types.LongType] = noopConversion
	conversionMap[types.LongType][types.StringType] = longToString
	conversionMap[types.LongType][types.TimestampType] = longToTime
	conversionMap[types.LongType][types.IntType] = longToInt
	conversionMap[types.LongType][types.DoubleType] = longToDouble
	conversionMap[types.LongType][types.BoolType] = longToBool
	conversionMap[types.LongType][types.DateType] = longToDate
	conversionMap[types.LongType][types.TimeOfDayType] = noopConversion

	// Double conversion functions
	conversionMap[types.DoubleType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.DoubleType][types.DoubleType] = noopConversion
	conversionMap[types.DoubleType][types.StringType] = doubleToString
	conversionMap[types.DoubleType][types.TimestampType] = doubleToTime
	conversionMap[types.DoubleType][types.IntType] = doubleToInt
	conversionMap[types.DoubleType][types.LongType] = doubleToLong
	conversionMap[types.DoubleType][types.BoolType] = doubleToBool
	conversionMap[types.DoubleType][types.DateType] = doubleToDate
	conversionMap[types.DoubleType][types.TimeOfDayType] = noopConversion

	// Bool conversion functions
	conversionMap[types.BoolType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.BoolType][types.BoolType] = noopConversion
	conversionMap[types.BoolType][types.StringType] = boolToString
	conversionMap[types.BoolType][types.TimestampType] = boolToTime
	conversionMap[types.BoolType][types.IntType] = boolToInt
	conversionMap[types.BoolType][types.LongType] = boolToLong
	conversionMap[types.BoolType][types.DoubleType] = boolToDouble
	conversionMap[types.BoolType][types.DateType] = noopConversion
	conversionMap[types.BoolType][types.TimeOfDayType] = noopConversion

	// String conversion functions
	conversionMap[types.StringType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.StringType][types.StringType] = noopConversion
	conversionMap[types.StringType][types.BoolType] = stringToBool
	conversionMap[types.StringType][types.TimestampType] = stringToTime
	conversionMap[types.StringType][types.IntType] = stringToInt
	conversionMap[types.StringType][types.LongType] = stringToLong
	conversionMap[types.StringType][types.DoubleType] = stringToDouble
	conversionMap[types.StringType][types.DateType] = stringToDate
	conversionMap[types.StringType][types.TimeOfDayType] = stringToTimeofDay

	// Date conversion functions
	conversionMap[types.DateType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.DateType][types.DateType] = noopConversion
	conversionMap[types.DateType][types.StringType] = dateToString
	conversionMap[types.DateType][types.IntType] = dateToInt
	conversionMap[types.DateType][types.LongType] = dateToLong
	conversionMap[types.DateType][types.DoubleType] = dateToDouble
	conversionMap[types.DateType][types.BoolType] = dateToBool
	conversionMap[types.DateType][types.TimestampType] = dateToTime
	conversionMap[types.DateType][types.TimeOfDayType] = noopConversion

	// Timeofday conversion functions
	conversionMap[types.TimeOfDayType] = make(map[types.CellDataType]conversionFunc)
	conversionMap[types.TimeOfDayType][types.TimeOfDayType] = noopConversion
	conversionMap[types.TimeOfDayType][types.StringType] = timeofDayToString
	conversionMap[types.TimeOfDayType][types.IntType] = noopConversion
	conversionMap[types.TimeOfDayType][types.LongType] = noopConversion
	conversionMap[types.TimeOfDayType][types.DoubleType] = noopConversion
	conversionMap[types.TimeOfDayType][types.BoolType] = noopConversion
	conversionMap[types.TimeOfDayType][types.TimestampType] = timeofDayToTime
	conversionMap[types.TimeOfDayType][types.DateType] = noopConversion
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
	case types.TimestampType, types.DateType, types.TimeOfDayType:
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
	case types.TimestampType, types.DateType, types.TimeOfDayType:
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

	newDataset.Headers = buildHeaders(&newDataset, dataset)
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

func convertTimeToDate(t time.Time, config ConversionConfiguration) time.Time {
	location := time.UTC
	if config.DateTimeDate != nil && len(config.DateTimeDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.DateTimeDate.Timezone)
		if err != nil {
			fmt.Print("Unable to load timezone: " + config.DateTimeDate.Timezone)
			l = time.UTC
		}
		location = l
	}
	convertedInput := t.In(location)
	return time.Date(convertedInput.Year(), convertedInput.Month(), convertedInput.Day(), 0, 0, 0, 0, time.UTC)
}

func convertTimeToTimeofDay(t time.Time, config ConversionConfiguration) time.Time {
	utcInput := t.In(time.UTC)
	location := time.UTC
	if config.DateTimeDate != nil && len(config.DateTimeDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.DateTimeDate.Timezone)
		if err != nil {
			fmt.Print("Unable to load timezone: " + config.DateTimeDate.Timezone)
			l = time.UTC
		}
		location = l
	}
	convertedInput := utcInput.In(location)
	return time.Date(0, 0, 0, convertedInput.Hour(), convertedInput.Minute(), convertedInput.Second(), convertedInput.Nanosecond(), time.UTC)
}

////////////////// Timestamp conversions //////////////////

func commonTimeToInt(t time.Time, config ConversionConfiguration) int64 {
	if config.NumericDate != nil && config.NumericDate.IsUnixMillis {
		return t.UnixMilli()
	}
	if config.NumericDate != nil && config.NumericDate.IsUnixSeconds {
		return t.Unix()
	}
	if config.NumericDate != nil && config.NumericDate.IsExcelDate {
		return (t.Unix() / 86400) + numberOfDaysBetweenUnixEpochAndExcelEpoch
	}
	// Default to Unix timestamp
	return t.Unix()
}

// Converts a Java-style datetime layout string to a Go-style layout string
func convertJavaLayoutToGoLayout(javaLayout string) (string, error) {
	// Define the Java-style layout strings and their Go-style equivalents
	javaLayouts := [][]string{
		{"yyyy", "2006"},
		{"YYYY", "2006"},
		{"yy", "06"},
		{"MM", "01"},
		{"M", "1"},
		{"dd", "02"},
		{"d", "2"},
		{"HH", "15"},
		{"hh", "15"},
		{"H", "3"},
		{"mm", "04"},
		{"m", "4"},
		{"ss", "05"},
		{"s", "5"},
		{"SSS", "000"},
		{"Z", "Z07:00"},
		{"ZZ", "-07:00"},
	}

	// Replace each Java-style layout string with its Go-style equivalent
	goLayout := javaLayout
	for _, layoutMap := range javaLayouts {
		javaStr := layoutMap[0]
		goStr := layoutMap[1]
		if !strings.Contains(javaLayout, javaStr) {
			continue
		}
		goLayout = strings.ReplaceAll(goLayout, javaStr, goStr)
	}
	return goLayout, nil
}

// Converts an ISO 8601 layout string to a Go layout string
func convertISO8601ToGoLayout(layout string) string {
	// Replace ISO 8601 date format strings with their Go equivalents
	layout = strings.ReplaceAll(layout, "YYYY", "2006")
	layout = strings.ReplaceAll(layout, "YY", "06")
	layout = strings.ReplaceAll(layout, "MM", "01")
	layout = strings.ReplaceAll(layout, "DD", "02")

	// Replace ISO 8601 time format strings with their Go equivalents
	layout = strings.ReplaceAll(layout, "hh", "15")
	layout = strings.ReplaceAll(layout, "mm", "04")
	layout = strings.ReplaceAll(layout, "ss", "05")
	layout = strings.ReplaceAll(layout, "SSS", "000")

	// Replace ISO 8601 timezone format strings with their Go equivalents
	layout = strings.ReplaceAll(layout, "ZZ", "-07:00")
	layout = strings.ReplaceAll(layout, "Z", "Z07:00")

	return layout
}

func commonTimeToString(t time.Time, config ConversionConfiguration, defaultFormat string) string {
	format := defaultFormat
	if config.StringDate != nil && len(config.StringDate.DateFormat) > 0 {
		f, err := convertJavaLayoutToGoLayout(config.StringDate.DateFormat)
		if err != nil {
			fmt.Print("Unable to convert date format: " + config.StringDate.DateFormat)
			return ""
		} else {
			format = f
		}
	}
	return t.Format(format)
}

func timeToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "2006-01-02 15:04:05")
}

func timeToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return int32(commonTimeToInt(convertedInput, config))
}

func timeToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return commonTimeToInt(convertedInput, config)
}

func timeToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return float64(commonTimeToInt(convertedInput, config))
}

func timeToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return !convertedInput.IsZero()
}

func timeToDate(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return convertTimeToDate(convertedInput, config)
}

func timeToTimeofDay(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return convertTimeToTimeofDay(convertedInput, config)
}

////////////////// Date conversions //////////////////

func dateToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	selectedTime := time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
	if config.DateTimeDate != nil && len(config.DateTimeDate.SelectedTime) > 0 {
		t, err := ParseTime(config.DateTimeDate.SelectedTime)
		if err != nil {
			fmt.Print("Unable to parse selected time: " + config.DateTimeDate.SelectedTime)
		} else {
			selectedTime = t
		}
	}
	selectedLocation := time.UTC
	if config.DateTimeDate != nil && len(config.DateTimeDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.DateTimeDate.Timezone)
		if err != nil {
			fmt.Println("Unable to load timezone: " + config.DateTimeDate.Timezone)
		} else {
			selectedLocation = l
		}
	}
	computedDateTime := time.Date(convertedInput.Year(), convertedInput.Month(), convertedInput.Day(), selectedTime.Hour(), selectedTime.Minute(), selectedTime.Second(), selectedTime.Nanosecond(), time.UTC)
	return computedDateTime.In(selectedLocation)
}

func dateToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "2006-01-02")
}

func dateToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return int32(commonTimeToInt(convertedInput, config))
}

func dateToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return commonTimeToInt(convertedInput, config)
}

func dateToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return float64(commonTimeToInt(convertedInput, config))
}

func dateToBool(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return !convertedInput.IsZero()
}

////////////////// Timeofday conversions //////////////////

func timeofDayToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return time.Date(0, 0, 0, convertedInput.Hour(), convertedInput.Minute(), convertedInput.Second(), convertedInput.Nanosecond(), time.UTC)
}

func timeofDayToString(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "15:04:05")
}

////////////////// int conversions //////////////////

func intToString(input interface{}, config ConversionConfiguration) interface{} {
	p := message.NewPrinter(language.English)
	convertedNumber := p.Sprintf("%d", input)
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", config.StringNumeric.ThousandSeperator)
	} else {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", "")
	}
	return convertedNumber
}

const numberOfDaysBetweenUnixEpochAndExcelEpoch = 25569

func commonIntToTime(input int64, config ConversionConfiguration) time.Time {
	if config.NumericDate != nil && config.NumericDate.IsUnixMillis {
		return time.UnixMilli(input).In(time.UTC)
	}
	if config.NumericDate != nil && config.NumericDate.IsUnixSeconds {
		return time.Unix(input, 0).In(time.UTC)
	}
	if config.NumericDate != nil && config.NumericDate.IsExcelDate {
		// Convert Excel date value to Unix timestamp
		unixTimestamp := (input - numberOfDaysBetweenUnixEpochAndExcelEpoch) * 86400
		// Convert Unix timestamp to time.Time value
		return time.Unix(unixTimestamp, 0).In(time.UTC)
	}
	// Default to Unix timestamp
	return time.Unix(input, 0).In(time.UTC)
}

func intToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	return commonIntToTime(int64(convertedInput), config)
}

func intToDate(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int32)
	timeResult := commonIntToTime(int64(convertedInput), config)
	return convertTimeToDate(timeResult, config)
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
	p := message.NewPrinter(language.English)
	convertedNumber := p.Sprintf("%d", input)
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", config.StringNumeric.ThousandSeperator)
	} else {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", "")
	}
	return convertedNumber
}

func longToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	return commonIntToTime(convertedInput, config)
}

func longToDate(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(int64)
	timeResult := commonIntToTime(convertedInput, config)
	return convertTimeToDate(timeResult, config)
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

const tempThousandPlaceholder = "__"
const tempDecimalPlaceholder = "**"

func doubleToString(input interface{}, config ConversionConfiguration) interface{} {
	p := message.NewPrinter(language.English)
	decimalPlaces := 2
	if config.StringNumeric != nil {
		decimalPlaces = config.StringNumeric.NumberOfDecimals
	}
	convertedNumber := p.Sprintf("%."+strconv.Itoa(decimalPlaces)+"f", input)
	if config.StringNumeric != nil {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", tempThousandPlaceholder)
	}
	if config.StringNumeric != nil && len(config.StringNumeric.DecimalSymbol) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, ".", tempDecimalPlaceholder)
	}
	// we are doing this in 2 steps - otherwise thousand and decimal seperator might be swapped
	if config.StringNumeric != nil {
		convertedNumber = strings.ReplaceAll(convertedNumber, tempThousandPlaceholder, config.StringNumeric.ThousandSeperator)
	}
	if config.StringNumeric != nil && len(config.StringNumeric.DecimalSymbol) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, tempDecimalPlaceholder, config.StringNumeric.DecimalSymbol)
	}
	return convertedNumber
}

func doubleToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	return commonIntToTime(int64(convertedInput), config)
}

func doubleToDate(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(float64)
	timeResult := commonIntToTime(int64(convertedInput), config)
	return convertTimeToDate(timeResult, config)
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

func commonStringToTime(input string, config ConversionConfiguration, defaultFormat string) time.Time {
	format := defaultFormat
	if config.StringDate != nil && len(config.StringDate.DateFormat) > 0 {
		f, err := convertJavaLayoutToGoLayout(config.StringDate.DateFormat)
		if err != nil {
			fmt.Print("Unable to convert date format: " + config.StringDate.DateFormat)
		} else {
			format = f
		}
	}
	t, err := time.Parse(format, input)
	if err != nil {
		fmt.Printf("error parsing time %v with format %v", input, format)
		return time.Time{}
	}
	if len(config.StringDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.StringDate.Timezone)
		if err != nil {
			fmt.Print("Unable to load timezone: " + config.StringDate.Timezone)
		} else {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l)
		}
	}
	return t
}

func stringToTime(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, time.RFC3339)
}

func stringToDate(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, "2006-01-02")
}

func stringToTimeofDay(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, "15:04:05")
}

func commonStringToNumeric(input string, config ConversionConfiguration) float64 {
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		// let's throw away thousand seperator
		input = strings.Replace(input, config.StringNumeric.ThousandSeperator, "", -1)
	}
	if config.StringNumeric != nil && len(config.StringNumeric.DecimalSymbol) > 0 && config.StringNumeric.DecimalSymbol != "." {
		input = strings.Replace(input, config.StringNumeric.DecimalSymbol, ".", 1)
	}
	i, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return float64(0)
	}
	return i
}

func stringToInt(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return int32(commonStringToNumeric(convertedInput, config))
}

func stringToLong(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return int64(commonStringToNumeric(convertedInput, config))
}

func stringToDouble(input interface{}, config ConversionConfiguration) interface{} {
	convertedInput := input.(string)
	return commonStringToNumeric(convertedInput, config)
}
