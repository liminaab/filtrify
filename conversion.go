package dyntransformer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"limina.com/dyntransformer/types"
)

const defaultColumnName string = "Column"

// 2000-01-01
const minTimestampVal int64 = 946684800

// 2100-01-01
const maxTimestampVal int64 = 4102444800

var dateTimeFormats []string = []string{
	// datetime
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05-0700",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04",
	"Mon, 2 Jan 2006 15:04:05 MST",
	// date
	"2006-01-02",
	"20060102",
	"January 02, 2006",
	"02 January 2006",
	"02-Jan-2006",
	"01/02/06",
	"01/02/2006",
	"010206",
	"Jan-02-06",
	"Jan-02-2006",
	"06",
	"Mon",
	"Monday	",
	"Jan-06",
	// time
	"15:04",
	"15:04:05",
	"3:04 PM",
	"03:04:05 PM",
}

func tryParseUnixTimestamp(data string) *time.Time {
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

func tryParseDateAndTime(data string) *time.Time {
	for _, layout := range dateTimeFormats {
		t, err := time.Parse(layout, data)
		if err != nil {
			continue
		}
		return &t
	}

	return nil
}

func parseTimestamp(data string) (*time.Time, error) {
	// let's start with most restrictive format to least restrictive one
	// let's first check if this is a unix timestamp
	t := tryParseUnixTimestamp(data)
	if t != nil {
		return t, nil
	}

	t = tryParseDateAndTime(data)
	if t != nil {
		return t, nil
	}

	return nil, errors.New("invalid time format")
}

func parseToCell(data string, enforceType types.CellDataType) (*types.CellValue, error) {
	cellValue := &types.CellValue{
		DataType: enforceType,
	}
	switch enforceType {
	case types.IntType:
		i, err := strconv.ParseInt(data, 10, 32)
		if err != nil {
			return nil, err
		}
		cellValue.IntValue = int32(i)
		break
	case types.LongType:
		i, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return nil, err
		}
		cellValue.LongValue = i
		break
	case types.TimestampType:
		i, err := parseTimestamp(data)
		if err != nil {
			return nil, err
		}
		cellValue.TimestampValue = *i
		break
	case types.StringType:
		cellValue.StringValue = data
		break
	case types.DoubleType:
		i, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return nil, err
		}
		cellValue.DoubleValue = i
		break
	case types.BoolType:
		data = strings.ToLower(data)
		if data == "true" {
			cellValue.BoolValue = true
		} else if data == "false" {
			cellValue.BoolValue = false
		} else {
			return nil, errors.New("invalid boolean value")
		}

		break
	case types.NilType:
		break

	}
	return cellValue, nil
}

// order of parsing will be like this
// timestamp
// int
// long
// float
// bool
// string
func getNextTypeToParse(t types.CellDataType) types.CellDataType {
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
		return types.NilType
	case types.NilType:
		return types.NilType
	}

	return types.NilType
}

func estimateColumnType(rawData [][]string, colIndex int) types.CellDataType {
	currentType := types.TimestampType
	for i := 0; i < len(rawData); i++ {
		cellData := rawData[i][colIndex]
		_, err := parseToCell(cellData, currentType)
		if err != nil {
			currentType = getNextTypeToParse(currentType)
			i = -1
		}
	}
	return currentType
}

func ConvertToTypedData(rawData [][]string, firstLineIsHeader bool, convertDataTypes bool) (*types.DataSet, error) {
	// let's try
	data, headers, err := ExtractHeaders(rawData, firstLineIsHeader)
	if err != nil {
		return nil, err
	}

	cellTypes := make([]types.CellDataType, len(headers))
	for i := range headers {
		if convertDataTypes {
			cellTypes[i] = estimateColumnType(data, i)
		} else {
			cellTypes[i] = types.StringType
		}
	}

	dataRows := make([]types.DataRow, len(data))
	dataSet := types.DataSet{
		Rows: dataRows,
	}
	// now we need to iterate over these
	for ri, row := range data {
		typedCols := make([]types.DataColumn, len(headers))
		typedRow := types.DataRow{
			Columns: typedCols,
		}
		for ci, _ := range headers {
			typedCols[ci].ColumnName = &headers[ci]
			cell, err := parseToCell(row[ci], cellTypes[ci])
			if err != nil {
				return nil, err
			}
			typedCols[ci].CellValue = *cell
		}

		dataRows[ri] = typedRow
	}

	return &dataSet, nil
}

func ExtractHeaders(rawData [][]string, firstLineIsHeader bool) ([][]string, []string, error) {
	if firstLineIsHeader {
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
