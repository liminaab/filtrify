package operatorv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/liminaab/filtrify/conversion"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type ChangeColumnTypeOperator struct {
}

type ChangeColumnTypeConfiguration struct {
	Columns map[string]ConversionConfiguration `json:"columns"`
}

type ConversionConfiguration struct {
	TargetType            types.CellDataType          `json:"targetType"`
	StringNumeric         *StringNumericConfiguration `json:"stringNumericConfiguration"`
	StringDate            *StringDateConfiguration    `json:"stringDateConfiguration"`
	NumericDate           *NumericDateConfiguration   `json:"numericDateConfiguration"`
	DateTimeDate          *DateTimeDateConfiguration  `json:"dateTimeDateConfiguration"`
	SkipConversionIfFails *bool                       `json:"skipConversionIfFails"`
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

type conversionFunc func(I interface{}, config ConversionConfiguration) (interface{}, error)

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

// sourceValue reads slot i of arr as the native Go type the conversionFuncs
// expect. GetOneForMarshal is not usable here - it renders temporal values as
// JSON strings, while the conversionFuncs assert on time.Time.
func sourceValue(arr arrow.Array, i int) (interface{}, error) {
	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return a.Value(i).ToTime(unit), nil
	default:
		return nil, fmt.Errorf("unsupported source array %T", arr)
	}
}

// appendConverted writes a value produced by a conversionFunc into b, encoding
// time.Time the same way the conversion package does when it reads raw data.
func appendConverted(b array.Builder, v interface{}) error {
	switch bb := b.(type) {
	case *array.Int32Builder:
		val, ok := v.(int32)
		if !ok {
			return fmt.Errorf("expected int32, got %T", v)
		}
		bb.Append(val)
	case *array.Int64Builder:
		val, ok := v.(int64)
		if !ok {
			return fmt.Errorf("expected int64, got %T", v)
		}
		bb.Append(val)
	case *array.Float64Builder:
		val, ok := v.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", v)
		}
		bb.Append(val)
	case *array.BooleanBuilder:
		val, ok := v.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", v)
		}
		bb.Append(val)
	case *array.StringBuilder:
		val, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
		bb.Append(val)
	case *array.LargeStringBuilder:
		val, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
		bb.Append(val)
	case *array.TimestampBuilder:
		val, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", v)
		}
		bb.Append(arrow.Timestamp(val.UnixMicro()))
	case *array.Date32Builder:
		val, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", v)
		}
		bb.Append(conversion.ToDate32(val))
	case *array.Time64Builder:
		val, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", v)
		}
		bb.Append(conversion.ToTime64us(val))
	case *array.NullBuilder:
		bb.AppendNull()
	default:
		return fmt.Errorf("unsupported target builder %T", b)
	}
	return nil
}

// convertColumn returns an owned reference: the caller must Release the
// returned column once it has been handed off (or discard it on error).
func (t *ChangeColumnTypeOperator) convertColumn(col *arrow.Column, config ConversionConfiguration) (arrow.Column, error) {
	nullType := (&arrow.NullType{}).ID()
	if col.DataType().ID() == nullType {
		col.Retain() // the caller releases every column it gets back
		return *col, nil
	}

	sourceArrowType := col.DataType()
	targetArrowType := conversion.ArrowType(config.TargetType)
	sourceFiltrifyType := conversion.FiltrifyType(sourceArrowType)
	targetMap, found := conversionMap[sourceFiltrifyType]
	if !found {
		return arrow.Column{}, errors.New("invalid source type")
	}
	conversionF, found := targetMap[config.TargetType]
	if !found {
		return arrow.Column{}, errors.New("invalid target type")
	}

	theBuilder := array.NewBuilder(memory.DefaultAllocator, targetArrowType)
	defer theBuilder.Release()

	var newChunks []arrow.Array
	defer func() {
		for _, c := range newChunks {
			c.Release()
		}
	}()

	for _, chunk := range col.Data().Chunks() {
		theBuilder.Reserve(chunk.Len())
		for i := 0; i < chunk.Len(); i++ {
			if chunk.IsNull(i) {
				theBuilder.AppendNull()
				continue
			}
			sourceData, err := sourceValue(chunk, i)
			if err != nil {
				return *col, err
			}
			convertedData, err := conversionF(sourceData, config)
			if err != nil && (config.SkipConversionIfFails == nil || *config.SkipConversionIfFails) {
				return arrow.Column{}, err
			}
			// insert the convertedData
			if convertedData == nil || err != nil {
				// the conversion failed but we were told to keep going - leave a null behind
				theBuilder.AppendNull()
				continue
			}
			if err := appendConverted(theBuilder, convertedData); err != nil {
				return arrow.Column{}, err
			}
		}
		newChunks = append(newChunks, theBuilder.NewArray()) // NewArray also resets the builder
	}

	oldField := col.Field()
	newField := arrow.Field{
		Name:     oldField.Name,
		Type:     targetArrowType,
		Nullable: true,
		Metadata: oldField.Metadata,
	}

	chunked := arrow.NewChunked(newField.Type, newChunks) // retains newChunks
	defer chunked.Release()
	newCol := arrow.NewColumn(newField, chunked) // retains chunked
	return *newCol, nil
}

func (t *ChangeColumnTypeOperator) TransformInternal(dataset *types.DataSetv2, typedConfig *ChangeColumnTypeConfiguration) (*types.DataSetv2, error) {
	newColumns := make([]arrow.Column, dataset.NumCols())
	// every column in newColumns is an owned reference - NewTable takes its own,
	// so we hand ours back once we are done (or on the way out if we fail)
	built := 0
	defer func() {
		for i := range built {
			newColumns[i].Release()
		}
	}()

	for i := range dataset.NumCols() {
		curCol := dataset.Column(int(i))
		curColName := curCol.Name()
		config, exists := typedConfig.Columns[curColName]
		if !exists {
			curCol.Retain() // we release it below, so take a ref of our own
			newColumns[i] = *curCol
			built++
			continue
		}
		convertedColumn, err := t.convertColumn(curCol, config)
		if err != nil {
			return nil, err
		}
		newColumns[i] = convertedColumn
		built++
	}
	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := dataset.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	newTable := array.NewTable(schema, newColumns, dataset.NumRows()) // retains each column
	return &types.DataSetv2{
		newTable,
	}, nil
}

func (t *ChangeColumnTypeOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}
	return t.TransformInternal(dataset, typedConfig)
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

func noopConversion(input interface{}, config ConversionConfiguration) (interface{}, error) {
	return input, nil
}

func convertTimeToDate(t time.Time, config ConversionConfiguration) (time.Time, error) {
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
	return time.Date(convertedInput.Year(), convertedInput.Month(), convertedInput.Day(), 0, 0, 0, 0, time.UTC), nil
}

func convertTimeToTimeofDay(t time.Time, config ConversionConfiguration) (time.Time, error) {
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
	return time.Date(0, 0, 0, convertedInput.Hour(), convertedInput.Minute(), convertedInput.Second(), convertedInput.Nanosecond(), time.UTC), nil
}

////////////////// Timestamp conversions //////////////////

func commonTimeToInt(t time.Time, config ConversionConfiguration) (int64, error) {
	if config.NumericDate != nil && config.NumericDate.IsUnixMillis {
		return t.UnixMilli(), nil
	}
	if config.NumericDate != nil && config.NumericDate.IsUnixSeconds {
		return t.Unix(), nil
	}
	if config.NumericDate != nil && config.NumericDate.IsExcelDate {
		return (t.Unix() / 86400) + numberOfDaysBetweenUnixEpochAndExcelEpoch, nil
	}
	// Default to Unix timestamp
	return t.Unix(), nil
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

func commonTimeToString(t time.Time, config ConversionConfiguration, defaultFormat string) (string, error) {
	format := defaultFormat
	if config.StringDate != nil && len(config.StringDate.DateFormat) > 0 {
		f, err := convertJavaLayoutToGoLayout(config.StringDate.DateFormat)
		if err != nil {
			fmt.Print("Unable to convert date format: " + config.StringDate.DateFormat)
			return "", errors.New("unable to convert date format: " + config.StringDate.DateFormat)
		} else {
			format = f
		}
	}
	targetTimezone := time.UTC
	if config.StringDate != nil && len(config.StringDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.StringDate.Timezone)
		if err != nil {
			fmt.Print("Unable to load timezone: " + config.StringDate.Timezone)
		} else {
			targetTimezone = l
		}
	}
	return t.In(targetTimezone).Format(format), nil
}

func timeToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "2006-01-02 15:04:05")
}

func timeToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	val, err := commonTimeToInt(convertedInput, config)
	if err != nil {
		return int32(0), err
	}
	return int32(val), nil
}

func timeToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return commonTimeToInt(convertedInput, config)
}

func timeToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	val, err := commonTimeToInt(convertedInput, config)
	if err != nil {
		return float64(0), err
	}
	return float64(val), nil
}

func timeToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return !convertedInput.IsZero(), nil
}

func timeToDate(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return convertTimeToDate(convertedInput, config)
}

func timeToTimeofDay(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return convertTimeToTimeofDay(convertedInput, config)
}

////////////////// Date conversions //////////////////

func dateToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
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
	return computedDateTime.In(selectedLocation), nil
}

func dateToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "2006-01-02")
}

func dateToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	val, err := commonTimeToInt(convertedInput, config)
	if err != nil {
		return int32(0), nil
	}
	return int32(val), nil
}

func dateToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return commonTimeToInt(convertedInput, config)
}

func dateToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	val, err := commonTimeToInt(convertedInput, config)
	if err != nil {
		return float64(0), err
	}
	return float64(val), nil
}

func dateToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return !convertedInput.IsZero(), nil
}

////////////////// Timeofday conversions //////////////////

func timeofDayToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return time.Date(0, 0, 0, convertedInput.Hour(), convertedInput.Minute(), convertedInput.Second(), convertedInput.Nanosecond(), time.UTC), nil
}

func timeofDayToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(time.Time)
	return commonTimeToString(convertedInput, config, "15:04:05")
}

////////////////// int conversions //////////////////

func intToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	p := message.NewPrinter(language.English)
	convertedNumber := p.Sprintf("%d", input)
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", config.StringNumeric.ThousandSeperator)
	} else {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", "")
	}
	return convertedNumber, nil
}

const numberOfDaysBetweenUnixEpochAndExcelEpoch = 25569

func commonIntToTime(input int64, config ConversionConfiguration) (time.Time, error) {
	if config.NumericDate != nil && config.NumericDate.IsUnixMillis {
		return time.UnixMilli(input).In(time.UTC), nil
	}
	if config.NumericDate != nil && config.NumericDate.IsUnixSeconds {
		return time.Unix(input, 0).In(time.UTC), nil
	}
	if config.NumericDate != nil && config.NumericDate.IsExcelDate {
		// Convert Excel date value to Unix timestamp
		unixTimestamp := (input - numberOfDaysBetweenUnixEpochAndExcelEpoch) * 86400
		// Convert Unix timestamp to time.Time value
		return time.Unix(unixTimestamp, 0).In(time.UTC), nil
	}
	// Default to Unix timestamp
	return time.Unix(input, 0).In(time.UTC), nil
}

func intToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int32)
	return commonIntToTime(int64(convertedInput), config)
}

func intToDate(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int32)
	timeResult, err := commonIntToTime(int64(convertedInput), config)
	if err != nil {
		return time.Time{}, err
	}
	return convertTimeToDate(timeResult, config)
}

func intToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int32)
	return int64(convertedInput), nil
}

func intToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int32)
	return float64(convertedInput), nil
}

func intToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int32)
	return convertedInput != 0, nil
}

func longToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	p := message.NewPrinter(language.English)
	convertedNumber := p.Sprintf("%d", input)
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", config.StringNumeric.ThousandSeperator)
	} else {
		convertedNumber = strings.ReplaceAll(convertedNumber, ",", "")
	}
	return convertedNumber, nil
}

func longToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int64)
	return commonIntToTime(convertedInput, config)
}

func longToDate(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int64)
	timeResult, err := commonIntToTime(convertedInput, config)
	if err != nil {
		return time.Time{}, err
	}
	return convertTimeToDate(timeResult, config)
}

func longToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int64)
	return int32(convertedInput), nil
}

func longToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int64)
	return float64(convertedInput), nil
}

func longToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(int64)
	return convertedInput != 0, nil
}

const tempThousandPlaceholder = "__"
const tempDecimalPlaceholder = "**"

func doubleToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
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
	return convertedNumber, nil
}

func doubleToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(float64)
	return commonIntToTime(int64(convertedInput), config)
}

func doubleToDate(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(float64)
	timeResult, err := commonIntToTime(int64(convertedInput), config)
	if err != nil {
		return time.Time{}, err
	}
	return convertTimeToDate(timeResult, config)
}

func doubleToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(float64)
	return int32(convertedInput), nil
}

func doubleToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(float64)
	return int64(convertedInput), nil
}

func doubleToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(float64)
	return convertedInput != 0, nil
}

func boolToString(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(bool)
	if !convertedInput {
		return "False", nil
	}
	return "True", nil
}

func boolToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(bool)
	if !convertedInput {
		return time.Time{}, nil
	}
	return time.Now(), nil
}

func boolToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(bool)
	if !convertedInput {
		return int32(0), nil
	}
	return int32(1), nil
}

func boolToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(bool)
	if !convertedInput {
		return int64(0), nil
	}
	return int64(1), nil
}

func boolToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(bool)
	if !convertedInput {
		return float64(0), nil
	}
	return float64(1), nil
}

func stringToBool(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	if strings.EqualFold(convertedInput, "true") {
		return true, nil
	}
	return false, nil
}

func commonStringToTime(input string, config ConversionConfiguration, defaultFormat string) (time.Time, error) {
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
		return time.Time{}, errors.New("conversion failed")
	}
	if config.StringDate != nil && len(config.StringDate.Timezone) > 0 {
		l, err := time.LoadLocation(config.StringDate.Timezone)
		if err != nil {
			fmt.Print("Unable to load timezone: " + config.StringDate.Timezone)
		} else {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), l)
		}
	}
	return t, nil
}

func stringToTime(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, time.RFC3339)
}

func stringToDate(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, "2006-01-02")
}

func stringToTimeofDay(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	return commonStringToTime(convertedInput, config, "15:04:05")
}

var removeWhitespaceRegex = regexp.MustCompile(`[\s\x{00A0}]+`)

func commonStringToNumeric(input string, config ConversionConfiguration) (float64, error) {
	input = strings.TrimSpace(input)
	if config.StringNumeric != nil && len(config.StringNumeric.ThousandSeperator) > 0 {
		// let's throw away thousand seperator
		// no break space should also be removed if space is the thousand separator
		if config.StringNumeric.ThousandSeperator == " " {
			input = removeWhitespaceRegex.ReplaceAllString(input, "")
		} else {
			input = strings.Replace(input, config.StringNumeric.ThousandSeperator, "", -1)
		}
	}
	if config.StringNumeric != nil && len(config.StringNumeric.DecimalSymbol) > 0 && config.StringNumeric.DecimalSymbol != "." {
		input = strings.Replace(input, config.StringNumeric.DecimalSymbol, ".", 1)
	}
	i, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return float64(0), errors.New("conversion failed")
	}
	return i, nil
}

func stringToInt(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	val, err := commonStringToNumeric(convertedInput, config)
	return int32(val), err
}

func stringToLong(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	val, err := commonStringToNumeric(convertedInput, config)
	return int64(val), err
}

func stringToDouble(input interface{}, config ConversionConfiguration) (interface{}, error) {
	convertedInput := input.(string)
	return commonStringToNumeric(convertedInput, config)
}
