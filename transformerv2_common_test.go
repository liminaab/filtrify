package filtrify_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/liminaab/filtrify/types"
)

// The tests in the transformerv2_*_test.go files are the transformer_*_test.go
// ones run against Transformv2 instead of Transform. They keep the original test
// logic and assertions verbatim; the only thing that changes is how the result is
// read, because a v2 result is a column-oriented arrow table rather than a slice
// of rows.
//
// rowsOf bridges that gap: it renders a *types.DataSetv2 back into the
// row-oriented *types.DataSet shape the assertions already speak, so each v2 test
// body stays a line-for-line copy of its v1 counterpart. It is a view for
// assertions only - no operator ever sees it - so it reads every value out of the
// arrow arrays and never shares memory with the table.
//
// Two places where the two generations genuinely do not represent a value the
// same way, and the affected assertions say so at the call site:
//
//   - A row key. v1 carries one per row (types.DataRow.Key) and the operators
//     thread it through; a v2 table has no such notion at all. The "WithRowKey"
//     tests are still ported - they exercise the same transform over the same
//     data - but their one key assertion has nowhere to point, so it is dropped
//     and noted in the test.
//   - A time of day. v1 holds one as a time.Time whose date half is whatever the
//     producer happened to leave there (time.Parse gives year 0 January 1,
//     qlbridge's civil.Time path gives time.Date(0, 0, 0, ...)); arrow stores
//     microseconds since midnight and no date whatsoever. rowsOf rebuilds the
//     v1-by-way-of-time.Parse form - year 0, January 1 - so a time of day is
//     compared by its clock.
func rowsOf(t *testing.T, ds *types.DataSetv2) *types.DataSet {
	t.Helper()

	if ds == nil || ds.Table == nil {
		t.Fatal("the transformation returned no dataset")
	}

	numCols := int(ds.NumCols())
	flats := make([]*flatTestColumn, numCols)
	headers := make(types.HeaderMap, numCols)
	for i := range numCols {
		col := ds.Column(i)
		flats[i] = newFlatTestColumn(col)
		headers[col.Name()] = &types.Header{
			ColumnName: col.Name(),
			DataType:   cellTypeOf(col.DataType()),
			Order:      int64(i),
		}
	}

	out := &types.DataSet{
		Headers: headers,
		Rows:    make([]*types.DataRow, ds.NumRows()),
	}
	for r := range ds.NumRows() {
		columns := make([]*types.DataColumn, numCols)
		for c := range numCols {
			arr, idx, found := flats[c].at(r)
			if !found {
				t.Fatalf("column %q has no row %d", ds.Column(c).Name(), r)
			}
			columns[c] = &types.DataColumn{
				ColumnName: ds.Column(c).Name(),
				CellValue:  cellOf(t, arr, idx),
			}
		}
		out.Rows[r] = &types.DataRow{Columns: columns}
	}

	return out
}

// flatTestColumn gives a chunked column random access by absolute row number.
type flatTestColumn struct {
	chunks []arrow.Array
	starts []int64
}

func newFlatTestColumn(col *arrow.Column) *flatTestColumn {
	chunks := col.Data().Chunks()
	f := &flatTestColumn{chunks: chunks, starts: make([]int64, len(chunks))}
	var offset int64
	for i, chunk := range chunks {
		f.starts[i] = offset
		offset += int64(chunk.Len())
	}
	return f
}

func (f *flatTestColumn) at(row int64) (arrow.Array, int, bool) {
	for i, chunk := range f.chunks {
		if row < f.starts[i]+int64(chunk.Len()) {
			return chunk, int(row - f.starts[i]), true
		}
	}
	return nil, 0, false
}

// cellTypeOf maps an arrow type onto the filtrify cell type that holds it. It
// matches on the type ID rather than the type value, so a column carrying a
// freshly built type instance - which is what comes back out of a query or an
// arrow compute kernel - maps the same way as one built by the conversion
// package's own singletons.
func cellTypeOf(dt arrow.DataType) types.CellDataType {
	switch dt.ID() {
	case arrow.INT32:
		return types.IntType
	case arrow.INT64:
		return types.LongType
	case arrow.FLOAT64:
		return types.DoubleType
	case arrow.BOOL:
		return types.BoolType
	case arrow.STRING:
		return types.StringType
	case arrow.LARGE_STRING:
		// the conversion package stores an object as JSON text in a large string
		return types.ObjectType
	case arrow.DATE32:
		return types.DateType
	case arrow.TIME64:
		return types.TimeOfDayType
	case arrow.TIMESTAMP:
		return types.TimestampType
	}
	return types.NilType
}

// cellOf reads slot i of arr into the v1 cell holding the same value. A null -
// and every slot of a null-typed column - becomes a NilType cell, which is how
// v1 spells "no value".
func cellOf(t *testing.T, arr arrow.Array, i int) *types.CellValue {
	t.Helper()

	if arr.DataType().ID() == arrow.NULL || arr.IsNull(i) {
		return &types.CellValue{DataType: types.NilType}
	}

	switch a := arr.(type) {
	case *array.Int32:
		return &types.CellValue{DataType: types.IntType, IntValue: a.Value(i)}
	case *array.Int64:
		return &types.CellValue{DataType: types.LongType, LongValue: a.Value(i)}
	case *array.Float64:
		return &types.CellValue{DataType: types.DoubleType, DoubleValue: a.Value(i)}
	case *array.Boolean:
		return &types.CellValue{DataType: types.BoolType, BoolValue: a.Value(i)}
	case *array.String:
		return &types.CellValue{DataType: types.StringType, StringValue: a.Value(i)}
	case *array.LargeString:
		object := make(map[string]interface{})
		if err := json.Unmarshal([]byte(a.Value(i)), &object); err != nil {
			t.Fatalf("object column row %d is not a JSON object: %v", i, err)
		}
		return &types.CellValue{DataType: types.ObjectType, ObjectValue: object}
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return &types.CellValue{
			DataType:       types.TimestampType,
			TimestampValue: a.Value(i).ToTime(unit).UTC(),
		}
	case *array.Date32:
		return &types.CellValue{
			DataType:       types.DateType,
			TimestampValue: a.Value(i).ToTime().UTC(),
		}
	case *array.Time64:
		// arrow keeps microseconds since midnight and no date, so the date half
		// is rebuilt as the one time.Parse leaves behind for a "15:04:05" value
		unit := a.DataType().(*arrow.Time64Type).Unit
		clock := a.Value(i).ToTime(unit).UTC()
		return &types.CellValue{
			DataType: types.TimeOfDayType,
			TimestampValue: time.Date(0, 1, 1,
				clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), time.UTC),
		}
	}

	t.Fatalf("unsupported result column type %T", arr)
	return nil
}
