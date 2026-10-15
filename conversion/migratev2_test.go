package conversion

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/liminaab/filtrify/types"
	assert2 "github.com/stretchr/testify/assert"
)

func TestMigrateToV2(t *testing.T) {
	assert := assert2.New(t)
	raw := [][]string{
		{"Instrument name", "Quantity", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From", "TimeOfDay"},
		{"ERIC B SS Equity", "175000", "0.08", "2021-12-31", "true", "2020-01-01 12:00:00", "12:20:04"},
		{"AMZN US Equity", "1500", "0.25", "2021-12-16", "false", "2020-03-01 12:00:00", "16:45:00"},
		{"USD Cash", "", "", "2022-01-05", "", "2020-11-22 12:00:00", "01:02:03"},
	}

	v1, err := ConvertToTypedData(raw, true, true, nil, true)
	assert.Nil(err)

	v2, err := MigrateToV2(v1)
	assert.Nil(err)
	assert.Equal(int64(3), v2.NumRows())
	assert.Equal(int64(7), v2.NumCols())

	// column order follows the v1 rows
	wantNames := raw[0]
	for i, name := range wantNames {
		assert.Equal(name, v2.Column(i).Name())
	}

	wantTypes := []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Float64,
		arrow.FixedWidthTypes.Date32,
		arrow.FixedWidthTypes.Boolean,
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		arrow.FixedWidthTypes.Time64us,
	}
	for i, want := range wantTypes {
		assert.Equal(want.ID(), v2.Column(i).DataType().ID(), "column %s", v2.Column(i).Name())
	}

	// values survive the migration
	names := v2.Column(0).Data().Chunks()[0].(*array.String)
	assert.Equal("ERIC B SS Equity", names.Value(0))
	assert.Equal("USD Cash", names.Value(2))

	qty := v2.Column(1).Data().Chunks()[0].(*array.Int32)
	assert.Equal(int32(175000), qty.Value(0))
	assert.True(qty.IsNull(2), "empty quantity should be null")

	exposure := v2.Column(2).Data().Chunks()[0].(*array.Float64)
	assert.Equal(0.25, exposure.Value(1))

	maturity := v2.Column(3).Data().Chunks()[0].(*array.Date32)
	assert.Equal("2021-12-31", maturity.Value(0).ToTime().Format("2006-01-02"))

	sanction := v2.Column(4).Data().Chunks()[0].(*array.Boolean)
	assert.True(sanction.Value(0))
	assert.False(sanction.Value(1))
	assert.True(sanction.IsNull(2))

	active := v2.Column(5).Data().Chunks()[0].(*array.Timestamp)
	assert.Equal("2020-03-01 12:00:00", active.Value(1).ToTime(arrow.Microsecond).UTC().Format("2006-01-02 15:04:05"))

	tod := v2.Column(6).Data().Chunks()[0].(*array.Time64)
	assert.Equal("01:02:03", tod.Value(2).ToTime(arrow.Microsecond).Format("15:04:05"))
}

func TestMigrateToV2_TypeResolution(t *testing.T) {
	assert := assert2.New(t)

	_, err := MigrateToV2(nil)
	assert.NotNil(err)

	one, two, half := int32(1), int64(2), 2.5
	txt := "hello"
	mixed := &types.DataSet{
		Headers: types.HeaderMap{
			"widen":    {ColumnName: "widen", DataType: types.IntType},
			"fallback": {ColumnName: "fallback", DataType: types.IntType},
			"allnil":   {ColumnName: "allnil", DataType: types.BoolType},
		},
		Rows: []*types.DataRow{
			{Columns: []*types.DataColumn{
				types.NewIntDataColumn(&one, "widen"),
				types.NewIntDataColumn(&one, "fallback"),
				types.NewBoolDataColumn(nil, "allnil"),
			}},
			{Columns: []*types.DataColumn{
				types.NewLongDataColumn(&two, "widen"),
				types.NewStringDataColumn(&txt, "fallback"),
				types.NewBoolDataColumn(nil, "allnil"),
			}},
			{Columns: []*types.DataColumn{
				types.NewDoubleDataColumn(&half, "widen"),
				types.NewIntDataColumn(&one, "fallback"),
				types.NewBoolDataColumn(nil, "allnil"),
			}},
		},
	}

	v2, err := MigrateToV2(mixed)
	assert.Nil(err)
	assert.Equal(int64(3), v2.NumRows())
	assert.Equal(int64(3), v2.NumCols())
	// int + long + double widens to double
	assert.Equal(arrow.PrimitiveTypes.Float64.ID(), v2.Column(0).DataType().ID())
	assert.Equal(2.5, v2.Column(0).Data().Chunks()[0].(*array.Float64).Value(2))
	// int + string falls back to text
	assert.Equal(arrow.BinaryTypes.String.ID(), v2.Column(1).DataType().ID())
	fallback := v2.Column(1).Data().Chunks()[0].(*array.String)
	assert.Equal("1", fallback.Value(0))
	assert.Equal("hello", fallback.Value(1))
	// an all-nil column takes the header type and is all nulls
	assert.Equal(arrow.FixedWidthTypes.Boolean.ID(), v2.Column(2).DataType().ID())
	assert.Equal(3, v2.Column(2).NullN())

	// a dataset with no rows still migrates, using header order
	empty := &types.DataSet{
		Headers: types.HeaderMap{
			"second": {ColumnName: "second", DataType: types.StringType, Order: 2},
			"first":  {ColumnName: "first", DataType: types.IntType, Order: 1},
		},
	}
	ev2, err := MigrateToV2(empty)
	assert.Nil(err)
	assert.Equal(int64(0), ev2.NumRows())
	assert.Equal(int64(2), ev2.NumCols())
	assert.Equal("first", ev2.Column(0).Name())
	assert.Equal("second", ev2.Column(1).Name())
}
