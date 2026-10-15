package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type DataSetv2 struct {
	arrow.Table
}

// AddColumns returns a new DataSetv2 holding every column of t followed by cols.
// Arrow tables are immutable, so t is left untouched and the caller keeps
// ownership of both t and the columns it passed in - the new table takes its own
// references, so release yours once you are done with them.
func (t *DataSetv2) AddColumns(cols ...*arrow.Column) (*DataSetv2, error) {
	// an empty table carries no row count of its own, so the first new column
	// decides how long the result is
	numRows := t.NumRows()
	if t.NumCols() == 0 && len(cols) > 0 {
		numRows = int64(cols[0].Len())
	}

	existing := make(map[string]bool, t.NumCols())
	for i := range t.NumCols() {
		existing[t.Column(int(i)).Name()] = true
	}

	for _, c := range cols {
		if c == nil {
			return nil, errors.New("cannot add a nil column")
		}
		if int64(c.Len()) != numRows {
			return nil, fmt.Errorf("column %q has %d rows, table has %d", c.Name(), c.Len(), numRows)
		}
		if existing[c.Name()] {
			return nil, fmt.Errorf("column %q already exists", c.Name())
		}
		existing[c.Name()] = true
	}

	newColumns := make([]arrow.Column, 0, int(t.NumCols())+len(cols))
	// every entry of newColumns is a reference we own - NewTable takes its own,
	// so we hand ours back once we are done
	defer func() {
		for i := range newColumns {
			newColumns[i].Release()
		}
	}()

	for i := range t.NumCols() {
		col := t.Column(int(i))
		col.Retain()
		newColumns = append(newColumns, *col)
	}
	for _, c := range cols {
		c.Retain()
		newColumns = append(newColumns, *c)
	}

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := t.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	return &DataSetv2{array.NewTable(schema, newColumns, numRows)}, nil
}

func (t *DataSetv2) GetColumn(name string) *arrow.Column {
	for i := range t.NumCols() {
		if t.Column(int(i)).Name() == name {
			return t.Column(int(i))
		}
	}
	return nil
}

type valuer[T any] interface {
	arrow.Array
	Value(i int) T
}

func (t *DataSetv2) ValuesOf[T any](col *arrow.Column) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for _, chunk := range col.Data().Chunks() {
			arr, ok := chunk.(valuer[T])
			if !ok {
				var zero T
				panic(fmt.Sprintf("arrowiter: column %q is %s, cannot read as %T",
					col.Name(), col.DataType(), zero))
			}
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					if !yield(nil) {
						return
					}
					continue
				}
				theVal := arr.Value(i)
				if !yield(&theVal) {
					return
				}
			}
		}
	}
}

func (t *DataSetv2) ValuesByName[T any](name string) iter.Seq[*T] {
	col := t.GetColumn(name)
	if col == nil {
		return nil
	}
	return t.ValuesOf[T](col)
}

// Values yields a pointer to every row of col across all chunks,
// or nil for null rows. Each pointer is fresh, so it's safe to keep.
// It panics if the column's arrays don't have a Value(int) T method.
func (t *DataSetv2) Values[T any](col *arrow.Column) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for _, chunk := range col.Data().Chunks() {
			arr, ok := chunk.(valuer[T])
			if !ok {
				var zero T
				panic(fmt.Sprintf("arrowiter: column %q is %s, cannot read as %T",
					col.Name(), col.DataType(), zero))
			}
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					if !yield(nil) {
						return
					}
					continue
				}
				v := arr.Value(i)
				if !yield(&v) {
					return
				}
			}
		}
	}
}

// IsNumeric reports whether dt is an integer, floating-point or decimal type.
func (t *DataSetv2) IsNumericCol(name string) bool {
	col := t.GetColumn(name)
	if col == nil {
		return false
	}
	dt := col.DataType()
	return t.IsNumericType(dt)
}

func (t *DataSetv2) IsNumericType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	}
	return false
}

// NumericValues yields every row of a numeric column as a *float64,
// or nil for null rows. It panics if the column isn't numeric.
func (t *DataSetv2) NumericValues(col *arrow.Column) iter.Seq[*float64] {
	return func(yield func(*float64) bool) {
		if !t.IsNumericType(col.DataType()) {
			panic(fmt.Sprintf("arrowiter: column %q is %s, not numeric",
				col.Name(), col.DataType()))
		}
		for _, chunk := range col.Data().Chunks() {
			get := floatGetter(chunk)
			if get == nil {
				panic(fmt.Sprintf("arrowiter: unexpected array type %T", chunk))
			}
			for i := 0; i < chunk.Len(); i++ {
				if chunk.IsNull(i) {
					if !yield(nil) {
						return
					}
					continue
				}
				v := get(i)
				if !yield(&v) {
					return
				}
			}
		}
	}
}

// floatGetter returns a func that reads row i of a numeric array as float64,
// or nil if the array isn't numeric.
func floatGetter(chunk arrow.Array) func(int) float64 {
	switch a := chunk.(type) {
	case *array.Int8:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Int16:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Int32:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Int64:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Uint8:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Uint16:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Uint32:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Uint64:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Float16:
		return func(i int) float64 { return float64(a.Value(i).Float32()) }
	case *array.Float32:
		return func(i int) float64 { return float64(a.Value(i)) }
	case *array.Float64:
		return a.Value
	case *array.Decimal32:
		scale := a.DataType().(*arrow.Decimal32Type).Scale
		return func(i int) float64 { return a.Value(i).ToFloat64(scale) }
	case *array.Decimal64:
		scale := a.DataType().(*arrow.Decimal64Type).Scale
		return func(i int) float64 { return a.Value(i).ToFloat64(scale) }
	case *array.Decimal128:
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		return func(i int) float64 { return a.Value(i).ToFloat64(scale) }
	case *array.Decimal256:
		scale := a.DataType().(*arrow.Decimal256Type).Scale
		return func(i int) float64 { return a.Value(i).ToFloat64(scale) }
	}
	return nil
}

// RemoveColumn returns a new DataSetv2 holding every column of t except the one
// called name, and an error if no such column exists. As with
// [DataSetv2.AddColumns], the receiver is left untouched and the new table takes
// its own references.
func (t *DataSetv2) RemoveColumn(name string) (*DataSetv2, error) {
	found := false
	for i := range t.NumCols() {
		if t.Column(int(i)).Name() == name {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("column %q does not exist", name)
	}

	newColumns := make([]arrow.Column, 0, int(t.NumCols())-1)
	// every entry of newColumns is a reference we own - NewTable takes its own,
	// so we hand ours back once we are done
	defer func() {
		for i := range newColumns {
			newColumns[i].Release()
		}
	}()

	for i := range t.NumCols() {
		col := t.Column(int(i))
		if col.Name() == name {
			continue
		}
		col.Retain()
		newColumns = append(newColumns, *col)
	}

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := t.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	return &DataSetv2{array.NewTable(schema, newColumns, t.NumRows())}, nil
}

// RenameColumn returns a new DataSetv2 where the column called oldName is
// renamed to newName, keeping its position, type and data. It errors if oldName
// does not exist or if newName is already taken by another column. Renaming a
// column to its own name is a no-op copy. As with [DataSetv2.AddColumns], the
// receiver is left untouched and the new table takes its own references.
func (t *DataSetv2) RenameColumn(oldName string, newName string) (*DataSetv2, error) {
	if len(newName) < 1 {
		return nil, errors.New("cannot rename a column to an empty name")
	}

	found := false
	for i := range t.NumCols() {
		name := t.Column(int(i)).Name()
		if name == oldName {
			found = true
			continue
		}
		if name == newName {
			return nil, fmt.Errorf("column %q already exists", newName)
		}
	}
	if !found {
		return nil, fmt.Errorf("column %q does not exist", oldName)
	}

	newColumns := make([]arrow.Column, 0, int(t.NumCols()))
	// every entry of newColumns is a reference we own - NewTable takes its own,
	// so we hand ours back once we are done
	defer func() {
		for i := range newColumns {
			newColumns[i].Release()
		}
	}()

	for i := range t.NumCols() {
		col := t.Column(int(i))
		if col.Name() != oldName {
			col.Retain()
			newColumns = append(newColumns, *col)
			continue
		}
		field := col.Field()
		field.Name = newName
		// NewColumn retains the chunked data, so the renamed column keeps the
		// original buffers alive without copying them
		renamed := arrow.NewColumn(field, col.Data())
		newColumns = append(newColumns, *renamed)
	}

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := t.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	return &DataSetv2{array.NewTable(schema, newColumns, t.NumRows())}, nil
}

// AddConstantColumn returns a new DataSetv2 with a column called name appended,
// holding value of type dt in every row. A nil value fills the column with
// nulls. As with [DataSetv2.AddColumns], the receiver is left untouched.
func (t *DataSetv2) AddConstantColumn(name string, value interface{}, dt arrow.DataType) (*DataSetv2, error) {
	if dt == nil {
		return nil, errors.New("cannot add a column with a nil type")
	}

	builder := array.NewBuilder(memory.DefaultAllocator, dt)
	defer builder.Release()

	numRows := int(t.NumRows())
	builder.Reserve(numRows)
	for range numRows {
		if value == nil {
			builder.AppendNull()
			continue
		}
		if err := appendConstant(builder, value); err != nil {
			return nil, err
		}
	}

	arr := builder.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: name, Type: dt, Nullable: true}
	chunked := arrow.NewChunked(dt, []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	col := arrow.NewColumn(field, chunked) // retains chunked
	defer col.Release()

	return t.AddColumns(col)
}

// appendConstant writes value into b once, mirroring how the conversion package
// encodes each type when it reads raw data.
func appendConstant(b array.Builder, value interface{}) error {
	switch bb := b.(type) {
	case *array.Int32Builder:
		val, ok := value.(int32)
		if !ok {
			return fmt.Errorf("expected int32, got %T", value)
		}
		bb.Append(val)
	case *array.Int64Builder:
		val, ok := value.(int64)
		if !ok {
			return fmt.Errorf("expected int64, got %T", value)
		}
		bb.Append(val)
	case *array.Float64Builder:
		val, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", value)
		}
		bb.Append(val)
	case *array.BooleanBuilder:
		val, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
		bb.Append(val)
	case *array.StringBuilder:
		val, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
		bb.Append(val)
	case *array.LargeStringBuilder:
		val, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
		bb.Append(val)
	case *array.TimestampBuilder:
		val, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", value)
		}
		bb.Append(arrow.Timestamp(val.UnixMicro()))
	case *array.Date32Builder:
		val, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", value)
		}
		y, m, d := val.Date()
		bb.Append(arrow.Date32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400))
	case *array.Time64Builder:
		val, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", value)
		}
		h, m, s := val.Clock()
		us := int64(h)*3_600_000_000 + int64(m)*60_000_000 + int64(s)*1_000_000 + int64(val.Nanosecond())/1_000
		bb.Append(arrow.Time64(us))
	case *array.NullBuilder:
		bb.AppendNull()
	default:
		return fmt.Errorf("unsupported column type %T", b)
	}
	return nil
}

type TransformationOperatorType int64
type CellDataType int64

type CellParsingInfo struct {
	DataType CellDataType
	Info     interface{}
}

const (
	Filter TransformationOperatorType = iota
	NewColumn
	Aggregate
	Lookup
	MappedValue
	Sort
	RemoveColumn
	RenameColumn
	ChangeColumnType
	JSON
	Objectify
	CumulativeSum
	GroupBy = 13
)

func (t TransformationOperatorType) String() string {
	switch t {
	case Filter:
		return "Filter"
	case NewColumn:
		return "NewColumn"
	case Aggregate:
		return "Aggregate"
	case GroupBy:
		return "GroupBy"
	case Lookup:
		return "Lookup"
	case MappedValue:
		return "MappedValue"
	case Sort:
		return "Sort"
	case RemoveColumn:
		return "RemoveColumn"
	case RenameColumn:
		return "RenameColumn"
	case ChangeColumnType:
		return "ChangeColumnType"
	case JSON:
		return "JSON"
	case Objectify:
		return "Objectify"
	case CumulativeSum:
		return "CumulativeSum"
	}
	return "Unknown"
}

const (
	IntType CellDataType = iota
	LongType
	TimestampType
	StringType
	DoubleType
	BoolType
	NilType
	ObjectType
	DateType
	TimeOfDayType
)

func (e CellDataType) String() string {
	switch e {
	case IntType:
		return "IntType"
	case LongType:
		return "LongType"
	case TimestampType:
		return "TimestampType"
	case StringType:
		return "StringType"
	case DoubleType:
		return "DoubleType"
	case BoolType:
		return "BoolType"
	case NilType:
		return "NilType"
	case ObjectType:
		return "ObjectType"
	case DateType:
		return "DateType"
	case TimeOfDayType:
		return "TimeOfDayType"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type InputData struct {
	RawData                  [][]string
	RawDataFirstLineIsHeader bool
}

type TransformationStep struct {
	Operator      TransformationOperatorType
	Configuration string
}

type TransformationOperator interface {
	Transform(dataset *DataSet, config string, otherSets map[string]*DataSet) (*DataSet, error)
	ValidateConfiguration(config string) (bool, error)
}

type TransformationOperatorv2 interface {
	Transform(dataset *DataSetv2, config string, otherSets map[string]*DataSetv2) (*DataSetv2, error)
	ValidateConfiguration(config string) (bool, error)
}

// type DataSet struct {
// 	RawData                  [][]string
// 	RawDataFirstLineIsHeader bool
// }

type Header struct {
	ColumnName string
	DataType   CellDataType
	Order      int64
}

type HeaderMap map[string]*Header

type DataSet struct {
	Rows    []*DataRow
	Headers HeaderMap
}

func (t *DataSet) ToRawData() [][]string {
	if len(t.Rows) < 1 {
		return [][]string{}
	}
	// we are adding one more row for headers
	rawData := make([][]string, len(t.Rows)+1)
	// let's use first row to extract headers
	firstRow := t.Rows[0]
	rawData[0] = make([]string, len(firstRow.Columns))
	for i, c := range firstRow.Columns {
		rawData[0][i] = c.ColumnName
	}

	for i, r := range t.Rows {
		rawData[i+1] = make([]string, len(r.Columns))
		for j, c := range r.Columns {
			rawData[i+1][j] = c.CellValue.ToString()
		}
	}

	return rawData
}

type DataRow struct {
	Key     *string
	Columns []*DataColumn
}

func (t *DataRow) GetColumn(name string) *DataColumn {
	for _, c := range t.Columns {
		if c.ColumnName == name {
			return c
		}
	}
	return nil
}

type DataColumn struct {
	ColumnName string
	CellValue  *CellValue
}

func NewBoolDataColumn(val *bool, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:  BoolType,
			BoolValue: *val,
		},
	}
}

func NewIntDataColumn(val *int32, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType: IntType,
			IntValue: *val,
		},
	}
}

func NewLongDataColumn(val *int64, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:  LongType,
			LongValue: *val,
		},
	}
}

func NewDoubleDataColumn(val *float64, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:    DoubleType,
			DoubleValue: *val,
		},
	}
}

func NewTimestampDataColumn(val *int64, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:       TimestampType,
			TimestampValue: time.Unix(*val, 0),
		},
	}
}

func NewDateDataColumn(val *int64, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:       DateType,
			TimestampValue: time.Unix(*val, 0),
		},
	}
}

func NewStringDataColumn(val *string, name string) *DataColumn {
	if val == nil {
		return &DataColumn{
			ColumnName: name,
			CellValue: &CellValue{
				DataType: NilType,
			},
		}
	}
	return &DataColumn{
		ColumnName: name,
		CellValue: &CellValue{
			DataType:    StringType,
			StringValue: *val,
		},
	}
}

type CellValue struct {
	DataType       CellDataType
	IntValue       int32
	LongValue      int64
	TimestampValue time.Time // used for Timestamp, Date and Time of day
	StringValue    string
	DoubleValue    float64
	BoolValue      bool
	ObjectValue    map[string]interface{}
}

func (c *CellValue) Value() interface{} {
	if c == nil {
		return nil
	}

	if c.DataType == NilType {
		return nil
	}

	switch c.DataType {
	case IntType:
		return c.IntValue
	case LongType:
		return c.LongValue
	case TimestampType, DateType, TimeOfDayType:
		return c.TimestampValue
	case StringType:
		return c.StringValue
	case DoubleType:
		return c.DoubleValue
	case BoolType:
		return c.BoolValue
	case ObjectType:
		return c.ObjectValue
	}

	return nil
}

func (c *CellValue) ToString() string {
	if c == nil {
		return ""
	}

	if c.DataType == NilType {
		return ""
	}

	switch c.DataType {
	case IntType:
		return strconv.FormatInt(int64(c.IntValue), 10)
	case LongType:
		return strconv.FormatInt(c.LongValue, 10)
	case TimestampType:
		return c.TimestampValue.Format(time.RFC3339)
	case TimeOfDayType:
		return c.TimestampValue.Format("15:04:05")
	case DateType:
		return c.TimestampValue.Format("2006-01-02")
	case StringType:
		return c.StringValue
	case DoubleType:
		return strconv.FormatFloat(c.DoubleValue, 'f', -1, 64)
	case BoolType:
		if c.BoolValue {
			return "true"
		}
		return "false"
	case ObjectType:
		b, err := json.Marshal(c.ObjectValue)
		if err != nil {
			return ""
		}
		return string(b)
	}

	return ""
}

func (v *CellValue) IsNumeric() bool {
	switch v.DataType {
	case IntType, LongType, DoubleType:
		return true
	}

	return false
}

func (v *CellValue) GetNumericVal() float64 {

	switch v.DataType {
	case IntType:
		return float64(v.IntValue)
	case LongType:
		return float64(v.LongValue)
	case DoubleType:
		return v.DoubleValue
	}
	return -1
}

func (v *CellValue) Equals(other *CellValue) bool {
	if v == nil || other == nil {
		return false
	}

	if v.DataType != other.DataType {
		// there is only one exception here - if these are numeric types we still should check their
		if v.IsNumeric() && other.IsNumeric() {
			return v.GetNumericVal() == other.GetNumericVal()
		}

		return false
	}

	if v.DataType == NilType || other.DataType == NilType {
		return false
	}

	switch v.DataType {
	case IntType:
		return v.IntValue == other.IntValue
	case LongType:
		return v.LongValue == other.LongValue
	case TimestampType, DateType, TimeOfDayType:
		return v.TimestampValue.Equal(other.TimestampValue)
	case StringType:
		return v.StringValue == other.StringValue
	case DoubleType:
		return v.DoubleValue == other.DoubleValue
	case BoolType:
		return v.BoolValue == other.BoolValue
	case ObjectType:
		// we don't support object comparison for now
		return false
	}

	return false
}

func (v *CellValue) EqualsAsText(other *CellValue) bool {
	if v == nil || other == nil {
		return false
	}

	if v.DataType == other.DataType {
		return v.Equals(other)
	}
	// if they don't have the same data type - we will convert them to string and compare
	return v.ToString() == other.ToString()
}

// func (v CellValue) Add(other CellValue) (CellValue, error)      {}
// func (v CellValue) Subtract(other CellValue) (CellValue, error) {}
// func (v CellValue) Multiply(other CellValue) (CellValue, error) {}
// func (v CellValue) Divide(other CellValue) (CellValue, error)   {}
// func (v CellValue) Equals(other CellValue) (CellValue, error)   {}
