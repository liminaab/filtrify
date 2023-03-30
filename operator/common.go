package operator

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/schema"
	"github.com/liminaab/filtrify/lmnqlbridge"
	"github.com/liminaab/filtrify/types"
)

const defaultTableName string = "ext"

func init() {
	builtins.LoadAllBuiltins()
	lmnqlbridge.LoadLiminaOperators()
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyz123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func executeSQLQuery(q string, dataset *types.DataSet, existingColumnTypeMap map[string]types.CellDataType) (*types.DataSet, error) {
	exit := make(chan bool)
	inMemoryDataSource := lmnqlbridge.NewLmnInMemDataSource(exit)

	inMemoryDataSource.AddTable(defaultTableName, dataset)
	schemaName := RandStringBytesMaskImprSrcUnsafe(15)

	err := schema.RegisterSourceAsSchema(schemaName, inMemoryDataSource)
	if err != nil {
		return nil, err
	}
	defer func() {
		schema.DefaultRegistry().SchemaDrop(schemaName, schemaName, lex.TokenSchema)
	}()
	result, columns, err := lmnqlbridge.RunQLQuery(schemaName, q)
	if err != nil {
		return nil, err
	}
	ds := convertToDataSet(result, columns, existingColumnTypeMap)
	return ds, nil
}

func extractHeadersAndTypeMap(dataset *types.DataSet) ([]string, map[string]types.CellDataType) {
	columnTypeMap := make(map[string]types.CellDataType)
	columnLength := len(dataset.Rows[0].Columns)
	cols := make([]string, columnLength)
	for i := 0; i < columnLength; i++ {
		cols[i] = dataset.Rows[0].Columns[i].ColumnName
		columnTypeMap[dataset.Rows[0].Columns[i].ColumnName] = types.NilType
		for j := 0; j < len(dataset.Rows); j++ {
			if dataset.Rows[j].Columns[i].CellValue.DataType != types.NilType {
				columnTypeMap[dataset.Rows[0].Columns[i].ColumnName] = dataset.Rows[j].Columns[i].CellValue.DataType
				break
			}
		}
	}

	return cols, columnTypeMap
}

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func buildSelectStatement(selectFields []string) string {
	var sb strings.Builder
	for i, h := range selectFields {
		sb.WriteString(fmt.Sprintf("`%s`", h))
		if i != len(selectFields)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func buildColumnNotExistsError(column string) error {
	return fmt.Errorf("attempted to operate on column “%s” but no such column available", column)
}

// 2000-01-01
const minTimestampVal int64 = 946684800

// 2100-01-01
const maxTimestampVal int64 = 4102444800

// 2000-01-01
const minTimestampValMiliseconds int64 = 946684800000

// 2100-01-01
const maxTimestampValMiliseconds int64 = 4102444800000

var dateTimeFormats []string = []string{
	// datetime
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05-0700",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"2006-01-02 15:04:05",
	// date
	"2006-01-02",
	"20060102",
	"January 02, 2006",
	"02 January 2006",
	"02-Jan-2006",
	"01/02/06",
	"01/02/2006",
	"01/01/2006",
	"02/01/2006",
	"010206",
	"Jan-02-06",
	"Jan-02-2006",
	// "06",
	"Mon",
	"Monday	",
	"Jan-06",
	// time
	"15:04",
	"15:04:05",
	"3:04 PM",
	"03:04:05 PM",
}

func buildHeaders(newDataset *types.DataSet, oldDataset *types.DataSet) map[string]*types.Header {
	headers := make(map[string]*types.Header)
	if len(newDataset.Rows) > 0 {
		sampleRow := newDataset.Rows[0]
		for ci := range sampleRow.Columns {
			bestColumn := sampleRow.Columns[ci]
			if bestColumn.CellValue.DataType == types.NilType {
				for ri := range newDataset.Rows {
					c := newDataset.Rows[ri].Columns[ci]
					if c.CellValue.DataType != types.NilType {
						// let's try the next row
						bestColumn = c
						break
					}
				}
			}
			// let's find the best column type
			// because we might have some nil values
			// it doesn't mean column type is always nil
			oldHeader, found := oldDataset.Headers[bestColumn.ColumnName]
			if !found {
				oldHeader = &types.Header{}
			}
			newHeader, found := newDataset.Headers[bestColumn.ColumnName]
			if !found {
				newHeader = &types.Header{}
			}
			builtHeader := &types.Header{
				ColumnName: bestColumn.ColumnName,
				DataType:   bestColumn.CellValue.DataType,
				Metadata:   newHeader.Metadata,
			}
			if builtHeader.Metadata == nil {
				builtHeader.Metadata = map[string]interface{}{}
			}
			for k, v := range oldHeader.Metadata {
				if _, ok := builtHeader.Metadata[k]; !ok {
					builtHeader.Metadata[k] = v
				}
			}
			headers[bestColumn.ColumnName] = builtHeader
		}
	}

	return headers
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
	t := tryParseUnixTimestampSeconds(data)
	if t != nil {
		return t, nil
	}
	t = tryParseUnixTimestampMiliseconds(data)
	if t != nil {
		return t, nil
	}

	t = tryParseDateAndTime(data)
	if t != nil {
		return t, nil
	}

	return nil, errors.New("invalid time format")
}

func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse("15:04:05.999999999", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04:05", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04", s)
	if err == nil {
		return t, nil
	}
	return time.Time{}, err
}
