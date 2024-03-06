package conversion

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liminaab/filtrify/types"
)

const defaultColumnName string = "Column"

// 2000-01-01
const minTimestampVal int64 = 946684800

// 2100-01-01
const maxTimestampVal int64 = 4102444800

// 2000-01-01
const minTimestampValMiliseconds int64 = 946684800000

// 2100-01-01
const maxTimestampValMiliseconds int64 = 4102444800000

type ConversionMap map[string]bool

var wellknownFormats = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05-0700",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"January 02, 2006",
	"02 January 2006",
	"02-Jan-2006",
	"Jan-02-06",
	"Jan-02-2006",
}

var dateTimeFormats map[string]types.CellDataType = map[string]types.CellDataType{
	// datetime
	time.RFC3339:                   types.TimestampType,
	"2006-01-02T15:04:05":          types.TimestampType,
	"2006-01-02T15:04:05-0700":     types.TimestampType,
	"2 Jan 2006 15:04:05":          types.TimestampType,
	"2 Jan 2006 15:04":             types.TimestampType,
	"Mon, 2 Jan 2006 15:04:05 MST": types.TimestampType,
	"2006-01-02 15:04:05":          types.TimestampType,
	"02/01/2006 15:04:05":          types.TimestampType,
	"01/02/2006 15:04:05":          types.TimestampType,
	// date
	"2006-01-02":       types.DateType,
	"20060102":         types.DateType,
	"January 02, 2006": types.DateType,
	"02 January 2006":  types.DateType,
	"02-Jan-2006":      types.DateType,
	"02/01/2006":       types.DateType,
	"02/01/06":         types.DateType,
	"01/02/06":         types.DateType,
	"01/02/2006":       types.DateType,
	"010206":           types.DateType,
	"Jan-02-06":        types.DateType,
	"Jan-02-2006":      types.DateType,
	// "06",
	"Mon":     types.DateType,
	"Monday	": types.DateType,
	"Jan-06":  types.DateType,
	// time
	"15:04":       types.TimeOfDayType,
	"15:04:05":    types.TimeOfDayType,
	"3:04 PM":     types.TimeOfDayType,
	"03:04:05 PM": types.TimeOfDayType,
}

func tryParseUnixTimestampSeconds(data string) *time.Time {
	i, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return nil
	}
	// let's check the range
	if i > maxTimestampVal || i < minTimestampVal {
		return nil
	}

	// wow this is a real timestamp
	t := time.Unix(i, 0)
	return &t
}

func tryParseUnixTimestampMiliseconds(data string) *time.Time {
	i, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return nil
	}
	// let's check the range
	if i > maxTimestampValMiliseconds || i < minTimestampValMiliseconds {
		return nil
	}
	sec := i / 1000
	msec := i % 1000
	// wow this is a real timestamp
	t := time.Unix(sec, msec)
	return &t
}

func tryParseDateAndTime(data string) (*time.Time, types.CellDataType, string) {
	for layout, layoutType := range dateTimeFormats {
		t, err := time.Parse(layout, data)
		if err != nil {
			continue
		}
		return &t, layoutType, layout
	}

	return nil, types.NilType, ""
}

func parsePercentage(data string) (float64, error) {
	newData := strings.ReplaceAll(data, " ", "")
	if strings.Contains(newData, "%") {
		newData = strings.ReplaceAll(data, "%", "")
		val, err := strconv.ParseFloat(newData, 64)
		if err != nil {
			return 0, err
		}
		return val / 100, nil
	}
	return 0, errors.New("invalid percentage format")
}

func parseTimeData(data string, parseInfo interface{}) (*time.Time, types.CellDataType, string, error) {

	if parseInfo != nil {
		// we already have parse info
		// let's try to parse it
		layout, ok := parseInfo.(string)
		if ok && len(layout) > 0 {
			t, err := time.Parse(layout, data)
			if err != nil {
				return nil, types.NilType, "", err
			}
			return &t, dateTimeFormats[layout], layout, nil
		}
	}
	// let's start with most restrictive format to least restrictive one
	// let's first check if this is a unix timestamp
	t := tryParseUnixTimestampSeconds(data)
	if t != nil {
		return t, types.TimestampType, "", nil
	}
	t = tryParseUnixTimestampMiliseconds(data)
	if t != nil {
		return t, types.TimestampType, "", nil
	}

	t, layoutType, layout := tryParseDateAndTime(data)
	if t != nil {
		return t, layoutType, layout, nil
	}

	return nil, types.NilType, "", errors.New("invalid time format")
}

func ParseToCell(data string, enforceType types.CellDataType, parseInfo interface{}) (*types.CellValue, interface{}, error) {
	cellValue := &types.CellValue{
		DataType: enforceType,
	}
	var resultParseInfo interface{}
	switch enforceType {
	case types.IntType:
		i, err := strconv.ParseInt(data, 10, 32)
		if err != nil {
			return nil, nil, err
		}
		cellValue.IntValue = int32(i)
		break
	case types.LongType:
		i, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		cellValue.LongValue = i
		break
	case types.TimestampType, types.TimeOfDayType, types.DateType:
		i, dataType, layout, err := parseTimeData(data, parseInfo)
		if err != nil {
			return nil, nil, err
		}
		resultParseInfo = layout
		cellValue.DataType = dataType
		cellValue.TimestampValue = *i
		break
	case types.StringType:
		cellValue.StringValue = data
		break
	case types.DoubleType:
		// how about we try to check if this is percentage?
		i, err := parsePercentage(data)
		if err == nil {
			cellValue.DoubleValue = i
		} else {
			i, err = strconv.ParseFloat(data, 64)
			if err != nil {
				return nil, nil, err
			}
			cellValue.DoubleValue = i
		}
		break
	case types.BoolType:
		data = strings.ToLower(data)
		if data == "true" {
			cellValue.BoolValue = true
		} else if data == "false" {
			cellValue.BoolValue = false
		} else {
			return nil, nil, errors.New("invalid boolean value")
		}

		break
	case types.NilType:
		break

	}
	return cellValue, resultParseInfo, nil
}

// order of parsing will be like this
// timestamp
// int
// long
// float
// bool
// string
func getNextTypeToParse(t types.CellDataType, convertNumbers bool) types.CellDataType {
	// we are no more auto parsing number types
	if convertNumbers {
		switch t {
		case types.TimestampType:
			return types.IntType
		case types.IntType:
			return types.LongType
		case types.LongType:
			return types.DoubleType
		case types.DoubleType:
			return types.BoolType
		case types.BoolType:
			return types.StringType
		case types.StringType:
			return types.StringType
		}
	}
	switch t {
	case types.TimestampType:
		return types.BoolType
	case types.BoolType:
		return types.StringType
	case types.StringType:
		return types.StringType
	}
	return types.StringType
}

const maxFormatChangeCount = 100

func checkIfTimestamp(rawData [][]string, colIndex int) (bool, types.CellDataType, interface{}) {

	currentType := types.TimestampType
	isAllEmpty := true
	anySuccess := false
	var parseInfo interface{}
	numberOfFormatChanges := 0
	for i := 0; i < len(rawData); i++ {
		cellData := rawData[i][colIndex]
		// no need to try this cell
		if len(cellData) == 0 {
			continue
		}
		isAllEmpty = false
		cellVal, info, err := ParseToCell(cellData, types.TimestampType, parseInfo)
		if err != nil {
			if numberOfFormatChanges > maxFormatChangeCount {
				// let's prevent an infinite loop - we can't parse this
				return false, types.StringType, nil
			}
			if anySuccess {
				// we parsed to timestamp earlier - maybe we need to change the format
				// let's check if we can find a formula for this cell
				_, info, err = ParseToCell(cellData, types.TimestampType, nil)
				if err != nil {
					// nope this is hopeless for timestamp
					return false, types.StringType, nil
				}
				// we have a new format
				parseInfo = info
				numberOfFormatChanges++
				// let's start from the beginning
				i = -1
				continue
			} else {
				return false, types.StringType, nil
			}
		} else {
			currentType = cellVal.DataType
			anySuccess = true
		}
		parseInfo = info
	}
	if isAllEmpty {
		return false, types.StringType, nil
	}

	if currentType == types.TimeOfDayType {
		// this is not a timestamp we don't need to predict days for this
		return true, currentType, parseInfo
	}

	textLayout, ok := parseInfo.(string)
	if !ok {
		return false, types.StringType, nil
	}
	// let's check if this is a string date or something like unixtimestamp
	if len(textLayout) == 0 {
		// a timestamp - let's return it
		return true, currentType, parseInfo
	}
	for _, layout := range wellknownFormats {
		if layout == textLayout {
			// at this point we don't need to determine if the format is correct
			// we have a match
			return true, currentType, parseInfo
		}
	}

	// at this point we need to check if our format is correct for sure
	// to do this - we are going to check for a day that is bigger than 12
	// so we can be sure we are not parsing a month as a day
	anyDayBiggerThan12 := false
	for i := 0; i < len(rawData); i++ {
		cellData := rawData[i][colIndex]
		// no need to try this cell
		if len(cellData) == 0 {
			continue
		}
		t, _, _, _ := parseTimeData(cellData, parseInfo)
		if t.Day() > 12 {
			// ok we can trust our format prediction
			anyDayBiggerThan12 = true
			break
		}
	}
	if !anyDayBiggerThan12 {
		// we can't predict this for sure - let's skip this
		return false, types.StringType, nil
	}

	return true, currentType, parseInfo
}

func estimateColumnType(rawData [][]string, colIndex int, convertNumbers bool) (types.CellDataType, interface{}) {
	parsed, colType, timestampParseInfo := checkIfTimestamp(rawData, colIndex)
	if parsed {
		return colType, timestampParseInfo
	}
	currentType := types.BoolType
	if convertNumbers {
		currentType = types.IntType
	}
	isAllEmpty := true
	var parseInfo interface{}
	for i := 0; i < len(rawData); i++ {
		cellData := rawData[i][colIndex]
		// no need to try this cell
		if len(cellData) == 0 {
			continue
		}
		isAllEmpty = false
		_, info, err := ParseToCell(cellData, currentType, parseInfo)
		if err != nil {
			currentType = getNextTypeToParse(currentType, convertNumbers)
			i = -1
		}
		parseInfo = info
	}
	if isAllEmpty {
		return types.StringType, nil
	}
	return currentType, parseInfo
}

func ConvertToTypedData(rawData [][]string, firstLineIsHeader bool, convertDataTypes bool, conversionMap ConversionMap, convertNumbers bool) (*types.DataSet, error) {
	// let's try
	data, headers, err := extractHeaders(rawData, firstLineIsHeader)
	if err != nil {
		return nil, err
	}

	cellTypes := make([]types.CellParsingInfo, len(headers))
	typedHeaders := make(map[string]*types.Header)
	for i := range headers {
		shouldConvert := convertDataTypes
		if shouldConvert && conversionMap != nil {
			convert, found := conversionMap[headers[i]]
			if found {
				shouldConvert = convert
			}
		}

		if shouldConvert {
			cellType, parseInfo := estimateColumnType(data, i, convertNumbers)
			cellTypes[i] = types.CellParsingInfo{
				DataType: cellType,
				Info:     parseInfo,
			}
		} else {
			cellTypes[i] = types.CellParsingInfo{
				DataType: types.StringType,
				Info:     nil,
			}
		}
		typedHeaders[headers[i]] = &types.Header{
			ColumnName: headers[i],
			DataType:   cellTypes[i].DataType,
		}
	}

	dataRows := make([]*types.DataRow, len(data))
	dataSet := types.DataSet{
		Rows:    dataRows,
		Headers: typedHeaders,
	}
	// now we need to iterate over these
	for ri, row := range data {
		typedCols := make([]*types.DataColumn, len(headers))
		typedRow := &types.DataRow{
			Columns: typedCols,
		}
		for ci, _ := range headers {
			typedCols[ci] = &types.DataColumn{}
			typedCols[ci].ColumnName = headers[ci]
			var cell *types.CellValue
			if len(row[ci]) > 0 {
				cell, _, err = ParseToCell(row[ci], cellTypes[ci].DataType, cellTypes[ci].Info)
			} else {
				cell = &types.CellValue{
					DataType: types.NilType,
				}
			}
			if err != nil {
				return nil, err
			}
			typedCols[ci].CellValue = cell
		}

		dataRows[ri] = typedRow
	}

	return &dataSet, nil
}

func extractHeaders(rawData [][]string, firstLineIsHeader bool) ([][]string, []string, error) {
	if firstLineIsHeader {
		if len(rawData) < 1 {
			return nil, nil, errors.New("empty raw data")
		}
		headers, data := rawData[0], rawData[1:]
		return data, headers, nil
	}

	// we need to build a headers slice
	if len(rawData) < 1 || len(rawData[0]) < 1 {
		return nil, nil, errors.New("no data")
	}

	columnLength := len(rawData[0])
	cols := make([]string, columnLength)
	for i := 0; i < columnLength; i++ {
		cols[i] = fmt.Sprintf("%s%d", defaultColumnName, i)
	}

	return rawData, cols, nil
}
