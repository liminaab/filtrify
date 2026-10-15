package conversion

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/liminaab/filtrify/types"
)

var arrowToCell = map[arrow.DataType]types.CellDataType{
	arrow.PrimitiveTypes.Int32:                                     types.IntType,
	arrow.PrimitiveTypes.Int64:                                     types.LongType,
	&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}: types.TimestampType,
	arrow.BinaryTypes.String:                                       types.StringType,
	arrow.PrimitiveTypes.Float64:                                   types.DoubleType,
	arrow.FixedWidthTypes.Boolean:                                  types.BoolType,
	arrow.Null:                                                     types.NilType,
	arrow.BinaryTypes.LargeString:                                  types.ObjectType,
	arrow.FixedWidthTypes.Date32:                                   types.DateType,
	arrow.FixedWidthTypes.Time64us:                                 types.TimeOfDayType,
}

var cellToArrow = map[types.CellDataType]arrow.DataType{
	types.IntType:       arrow.PrimitiveTypes.Int32,
	types.LongType:      arrow.PrimitiveTypes.Int64,
	types.TimestampType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
	types.StringType:    arrow.BinaryTypes.String,
	types.DoubleType:    arrow.PrimitiveTypes.Float64,
	types.BoolType:      arrow.FixedWidthTypes.Boolean,
	types.NilType:       arrow.Null,
	types.ObjectType:    arrow.BinaryTypes.LargeString, // stored as JSON text
	types.DateType:      arrow.FixedWidthTypes.Date32,
	types.TimeOfDayType: arrow.FixedWidthTypes.Time64us,
}

func FiltrifyType(t arrow.DataType) types.CellDataType {
	timestampType := (&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}).ID()
	if t.ID() == timestampType {
		return types.TimestampType
	}
	if dt, ok := arrowToCell[t]; ok {
		return dt
	}
	return types.NilType
}

// ArrowType returns the Arrow type used to store a CellDataType.
func ArrowType(t types.CellDataType) arrow.DataType {
	if dt, ok := cellToArrow[t]; ok {
		return dt
	}
	return arrow.Null
}

// ToTime64us encodes the clock part of t as microseconds since midnight.
func ToTime64us(t time.Time) arrow.Time64 {
	h, m, s := t.Clock()
	us := int64(h)*3_600_000_000 + int64(m)*60_000_000 + int64(s)*1_000_000 + int64(t.Nanosecond())/1_000
	return arrow.Time64(us)
}

// ToDate32 encodes the calendar date of t as days since the Unix epoch.
func ToDate32(t time.Time) arrow.Date32 {
	y, m, d := t.Date()
	return arrow.Date32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

func appendData(b array.Builder, data string, enforceType types.CellDataType, parseInfo interface{}) error {
	if len(data) == 0 {
		b.AppendNull()
		return nil
	}

	switch enforceType {
	case types.IntType:
		i, err := strconv.ParseInt(data, 10, 32)
		if err != nil {
			return err
		}
		bb := b.(*array.Int32Builder)
		bb.Append(int32(i))
		break
	case types.LongType:
		i, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return err
		}
		bb := b.(*array.Int64Builder)
		bb.Append(i)
		break
	case types.TimestampType:
		i, _, _, err := parseTimeData(data, parseInfo)
		if err != nil {
			return err
		}
		bb := b.(*array.TimestampBuilder)
		if i == nil {
			bb.AppendNull()
		} else {
			bb.Append(arrow.Timestamp(i.UnixMicro()))
		}
		break
	case types.TimeOfDayType:
		i, _, _, err := parseTimeData(data, parseInfo)
		if err != nil {
			return err
		}
		bb := b.(*array.Time64Builder)
		if i == nil {
			bb.AppendNull()
		} else {
			bb.Append(ToTime64us(*i))
		}
		break
	case types.DateType:
		i, _, _, err := parseTimeData(data, parseInfo)
		if err != nil {
			return err
		}
		bb := b.(*array.Date32Builder)
		if i == nil {
			bb.AppendNull()
		} else {
			bb.Append(ToDate32(*i))
		}
		break
	case types.StringType:
		bb := b.(*array.StringBuilder)
		if len(data) == 0 {
			bb.AppendNull()
		} else {
			bb.Append(data)
		}
		break
	case types.DoubleType:
		bb := b.(*array.Float64Builder)
		// how about we try to check if this is percentage?
		i, err := parsePercentage(data)

		if err == nil {
			bb.Append(i)
		} else {
			i, err = strconv.ParseFloat(data, 64)
			if err != nil {
				return err
			}
			bb.Append(i)
		}
		break
	case types.BoolType:
		bb := b.(*array.BooleanBuilder)
		data = strings.ToLower(data)
		if data == "true" {
			bb.Append(true)
		} else if data == "false" {
			bb.Append(false)
		} else {
			return errors.New("invalid boolean value")
		}
		break
	case types.NilType:
		bb := b.(*array.NullBuilder)
		bb.AppendNull()
		break

	}
	return nil
}

// MigrateToV2 rebuilds a row-oriented [types.DataSet] as a column-oriented
// [types.DataSetv2] holding the same data. Column order follows the order the
// columns appear in the rows, and each column gets the Arrow type matching the
// cell type its values carry - a column whose cells are all nil falls back to
// the type declared in the dataset headers.
func MigrateToV2(dataset *types.DataSet) (*types.DataSetv2, error) {
	if dataset == nil {
		return nil, errors.New("cannot migrate a nil dataset")
	}

	names := columnOrder(dataset)
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{
			Name:     name,
			Type:     ArrowType(resolveColumnType(dataset, name)),
			Nullable: true,
		}
	}

	dataSchema := arrow.NewSchema(fields, nil)
	rowBuilder := array.NewRecordBuilder(memory.DefaultAllocator, dataSchema)
	defer rowBuilder.Release()
	rowBuilder.Reserve(len(dataset.Rows))

	for _, row := range dataset.Rows {
		for ci, name := range names {
			var cell *types.CellValue
			if row != nil {
				if col := row.GetColumn(name); col != nil {
					cell = col.CellValue
				}
			}
			if err := appendCellValue(rowBuilder.Field(ci), cell); err != nil {
				return nil, fmt.Errorf("column %q: %w", name, err)
			}
		}
	}

	rec := rowBuilder.NewRecordBatch()
	defer rec.Release()
	return &types.DataSetv2{
		Table: array.NewTableFromRecords(dataSchema, []arrow.RecordBatch{rec}),
	}, nil
}

// columnOrder lists every column of dataset once, in the order the rows hold
// them. Columns that only exist in the headers are appended afterwards, ordered
// by their header order then by name so an empty dataset still migrates
// deterministically.
func columnOrder(dataset *types.DataSet) []string {
	seen := make(map[string]bool, len(dataset.Headers))
	names := make([]string, 0, len(dataset.Headers))
	for _, row := range dataset.Rows {
		if row == nil {
			continue
		}
		for _, col := range row.Columns {
			if col == nil || seen[col.ColumnName] {
				continue
			}
			seen[col.ColumnName] = true
			names = append(names, col.ColumnName)
		}
	}

	headerOnly := make([]*types.Header, 0, len(dataset.Headers))
	for name, header := range dataset.Headers {
		if seen[name] || header == nil {
			continue
		}
		headerOnly = append(headerOnly, header)
	}
	sort.Slice(headerOnly, func(i, j int) bool {
		if headerOnly[i].Order != headerOnly[j].Order {
			return headerOnly[i].Order < headerOnly[j].Order
		}
		return headerOnly[i].ColumnName < headerOnly[j].ColumnName
	})
	for _, header := range headerOnly {
		names = append(names, header.ColumnName)
	}

	return names
}

// resolveColumnType picks the cell type that can hold every value the column
// called name carries. Mixed numeric types widen to the widest one, any other
// mix falls back to text, and a column without a single non-nil value takes the
// type from the headers.
func resolveColumnType(dataset *types.DataSet, name string) types.CellDataType {
	resolved := types.NilType
	for _, row := range dataset.Rows {
		if row == nil {
			continue
		}
		col := row.GetColumn(name)
		if col == nil || col.CellValue == nil || col.CellValue.DataType == types.NilType {
			continue
		}
		dataType := col.CellValue.DataType
		if resolved == types.NilType {
			resolved = dataType
			continue
		}
		resolved = mergeCellTypes(resolved, dataType)
		if resolved == types.StringType {
			// text holds anything - no wider type left to look for
			break
		}
	}

	if resolved == types.NilType {
		if header, ok := dataset.Headers[name]; ok && header != nil {
			return header.DataType
		}
	}
	return resolved
}

// mergeCellTypes returns a type able to hold both a and b.
func mergeCellTypes(a types.CellDataType, b types.CellDataType) types.CellDataType {
	if a == b {
		return a
	}
	ra, rb := numericRank(a), numericRank(b)
	if ra > 0 && rb > 0 {
		if ra > rb {
			return a
		}
		return b
	}
	return types.StringType
}

// numericRank orders the numeric cell types from narrowest to widest, and
// returns 0 for everything else.
func numericRank(t types.CellDataType) int {
	switch t {
	case types.IntType:
		return 1
	case types.LongType:
		return 2
	case types.DoubleType:
		return 3
	}
	return 0
}

// appendCellValue writes a single v1 cell into b, encoding each type the same
// way appendData does when it reads raw text. A nil cell - or one carrying no
// value - becomes a null.
func appendCellValue(b array.Builder, cell *types.CellValue) error {
	if cell == nil || cell.DataType == types.NilType {
		b.AppendNull()
		return nil
	}

	switch bb := b.(type) {
	case *array.Int32Builder:
		if !cell.IsNumeric() {
			return fmt.Errorf("expected a numeric value, got %s", cell.DataType)
		}
		bb.Append(int32(cell.GetNumericVal()))
	case *array.Int64Builder:
		if !cell.IsNumeric() {
			return fmt.Errorf("expected a numeric value, got %s", cell.DataType)
		}
		bb.Append(int64(cell.GetNumericVal()))
	case *array.Float64Builder:
		if !cell.IsNumeric() {
			return fmt.Errorf("expected a numeric value, got %s", cell.DataType)
		}
		bb.Append(cell.GetNumericVal())
	case *array.BooleanBuilder:
		if cell.DataType != types.BoolType {
			return fmt.Errorf("expected a bool value, got %s", cell.DataType)
		}
		bb.Append(cell.BoolValue)
	case *array.StringBuilder:
		// every type has a text form, so anything can land in a string column
		bb.Append(cell.ToString())
	case *array.LargeStringBuilder:
		// objects are stored as JSON text - ToString marshals them
		bb.Append(cell.ToString())
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(cell.TimestampValue.UnixMicro()))
	case *array.Date32Builder:
		bb.Append(ToDate32(cell.TimestampValue))
	case *array.Time64Builder:
		bb.Append(ToTime64us(cell.TimestampValue))
	case *array.NullBuilder:
		bb.AppendNull()
	default:
		return fmt.Errorf("unsupported column type %T", b)
	}
	return nil
}

func ConvertToTypedDatav2(rawData [][]string, firstLineIsHeader bool, convertDataTypes bool, conversionMap ConversionMap, convertNumbers bool) (*types.DataSetv2, error) {
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

	fields := make([]arrow.Field, len(headers))
	for i := range headers {
		typedHeader := typedHeaders[headers[i]]
		fields[i] = arrow.Field{
			Name:     typedHeader.ColumnName,
			Type:     ArrowType(typedHeader.DataType),
			Nullable: true,
		}
	}

	dataSchema := arrow.NewSchema(fields, nil)
	rowBuilder := array.NewRecordBuilder(memory.DefaultAllocator, dataSchema)
	defer rowBuilder.Release()
	rowBuilder.Reserve(len(data))

	for _, row := range data {
		for ci, _ := range fields {
			arrayBuilder := rowBuilder.Field(ci)
			theField := fields[ci]
			typedHeader, ok := typedHeaders[theField.Name]
			if !ok {
				return nil, errors.New("could not find field " + theField.Name)
			}
			cellType := cellTypes[ci]
			err := appendData(arrayBuilder, row[ci], typedHeader.DataType, cellType.Info)
			if err != nil {
				return nil, err
			}
		}
	}

	rec := rowBuilder.NewRecordBatch()
	defer rec.Release()
	return &types.DataSetv2{
		array.NewTableFromRecords(dataSchema, []arrow.RecordBatch{rec}),
	}, nil
}
