package conversion

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	assert2 "github.com/stretchr/testify/assert"
)

func TestBasicConversionv2(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedDatav2(sampleData, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(int64(4), result.NumRows())
	assert.Equal(int64(5), result.NumCols())
	for j := 0; j < 5; j++ {
		dataType := result.Column(j).DataType()
		assert.Equal(arrow.PrimitiveTypes.Int32.ID(), dataType.ID())
	}
}

func TestDateConversionv2(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedDatav2(sampleDateTable, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(int64(len(sampleDateTable)-1), result.NumRows())
	assert.Equal(int64(1), result.NumCols())
	stringType := (&arrow.StringType{}).ID()
	// those rows can't be parsed as date - because it is not clear which is month and which is day
	assert.Equal(int64(stringType), int64(result.Column(0).DataType().ID()))
}

func TestDateConversion2v2(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedDatav2(sampleDateTable2, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(int64(len(sampleDateTable)-1), result.NumRows())
	assert.Equal(int64(1), result.NumCols())
	column0 := result.Column(0)
	assert.Equal(int64(arrow.PrimitiveTypes.Date32.ID()), int64(column0.DataType().ID()))
	chunk0 := column0.Data().Chunks()[0]
	dateChunk := chunk0.(*array.Date32)
	theDate := dateChunk.Value(0).ToTime()
	assert.Equal("02/13/2024", theDate.Format("01/02/2006"))
}

func TestSimpleConversionWithoutHeader(t *testing.T) {
	assert := assert2.New(t)
	data := [][]string{
		{"15", "test text", "2008-06-02T15:04:05", "3.25"},
	}
	ds, err := ConvertToTypedDatav2(data, false, true, nil, false)
	assert.True(err == nil, "no error: %v", err)
	assert.True(ds.NumRows() == 1, "invalid row count in conversion")
	assert.True(ds.NumCols() == 4, "invalid column count")

	firstColumn := ds.Column(0)
	stringType := (&arrow.StringType{}).ID()
	timestampType := (&arrow.TimestampType{}).ID()

	assert.True(firstColumn.DataType().ID() == stringType, "invalid conversion type should be string")
	textColumn := ds.Column(1)
	assert.True(textColumn.DataType().ID() == stringType, "invalid conversion type should be string")
	timeColumn := ds.Column(2)
	assert.True(timeColumn.DataType().ID() == timestampType, "invalid conversion type should be timestamp")
	stringCol := ds.Column(3)
	assert.True(stringCol.DataType().ID() == stringType, "invalid conversion type should be string")
}

func TestDateConversion_2(t *testing.T) {
	assert := assert2.New(t)
	data := [][]string{
		{"2020-01-01", "20200101", "01/01/2020", "01/01/2020"},
		{"2020-01-08", "20200108", "01/08/2020", "08/01/2020"},
		{"2020-01-13", "20200113", "01/13/2020", "13/01/2020"},
	}
	ds, err := ConvertToTypedDatav2(data, false, true, nil, true)
	assert.True(err == nil, "no error: %v", err)
	assert.True(ds.NumRows() == 3, "invalid row count in conversion")
	colCount := ds.NumCols()
	for i := range colCount {
		currentCol := ds.Column(int(i))
		dateType := arrow.PrimitiveTypes.Date32.ID()
		assert.Equal(dateType, currentCol.DataType().ID(), "invalid conversion type should be date but it is %s", currentCol.DataType().ID())
	}
}

func TestDateTimeConversion(t *testing.T) {
	assert := assert2.New(t)
	data := [][]string{
		{"2020-01-01 12:20:04", "01/01/2020 12:20:04", "09/01/2020 12:20:04"},
		{"2020-01-08 12:20:04", "01/08/2020 12:20:04", "01/08/2020 12:20:04"},
		{"2020-01-13 12:20:04", "01/13/2020 12:20:04", "02/16/2020 12:20:04"},
	}
	ds, err := ConvertToTypedDatav2(data, false, true, nil, true)
	assert.True(err == nil, "no error: %v", err)
	assert.True(ds.NumRows() == 3, "invalid row count in conversion")
	timestampType := (&arrow.TimestampType{}).ID()
	for i := range ds.NumCols() {
		currentCol := ds.Column(int(i))
		assert.Equal(timestampType, currentCol.DataType().ID(), "invalid conversion type should be timestamp")

	}
}

func TestTimeOnlyConversion(t *testing.T) {
	assert := assert2.New(t)
	data := [][]string{
		{"12:20:04", "12:30:04", "12:20:04", "12:20:04"},
		{"12:20:04", "16:45:04", "12:20:04", "12:20:04"},
		{"12:20:04", "12:20", "12:20:59", "12:20:04"},
	}
	ds, err := ConvertToTypedDatav2(data, false, true, nil, true)
	assert.True(err == nil, "no error: %v", err)
	assert.True(ds.NumRows() == 3, "invalid row count in conversion")
	timeOfDayType := arrow.FixedWidthTypes.Time64us.ID()
	stringType := (&arrow.StringType{}).ID()

	for i := range ds.NumCols() {
		if i == 1 {
			assert.Equal(stringType, ds.Column(int(i)).DataType().ID(), "invalid conversion type should be string")
		} else {
			assert.Equal(timeOfDayType, ds.Column(int(i)).DataType().ID(), "invalid conversion type should be time")
		}
	}
}

func TestConversionUAT1(t *testing.T) {
	assert := assert2.New(t)
	var uat1TestData [][]string = [][]string{
		{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
		{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
		{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
		{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
		{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
		{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
	}
	ds, err := ConvertToTypedDatav2(uat1TestData, true, true, nil, false)
	assert.True(err == nil, "no error: %v", err)
	assert.True(ds.NumRows() == 5, "invalid row count in conversion")

	stringType := (&arrow.StringType{}).ID()
	dateType := (&arrow.Date32Type{}).ID()
	boolType := (&arrow.BooleanType{}).ID()
	timestampType := (&arrow.TimestampType{}).ID()

	col0 := ds.Column(0)
	assert.Equal("Instrument name", col0.Name(), "invalid column name on conversion")

	assert.Equal(stringType, col0.DataType().ID(), "invalid column type on conversion. expected string")
	col1 := ds.Column(1)
	assert.Equal("Instrument Type", col1.Name(), "invalid column name on conversion")
	assert.Equal(stringType, col1.DataType().ID(), "invalid column type on conversion. expected string")
	col2 := ds.Column(2)
	assert.Equal("Quantity", col2.Name(), "invalid column name on conversion")
	assert.Equal(stringType, col2.DataType().ID(), "invalid column type on conversion. expected string")
	col3 := ds.Column(3)
	assert.Equal("Market Value (Base)", col3.Name(), "invalid column name on conversion")
	assert.Equal(stringType, col3.DataType().ID(), "invalid column type on conversion. expected string")
	col4 := ds.Column(4)
	assert.Equal("Exposure %", col4.Name(), "invalid column name on conversion")
	assert.Equal(stringType, col4.DataType().ID(), "invalid column type on conversion. expected string")
	col5 := ds.Column(5)
	assert.Equal("Maturity Date", col5.Name(), "invalid column name on conversion")
	assert.Equal(dateType, col5.DataType().ID(), "invalid column type on conversion. expected date")
	col6 := ds.Column(6)
	assert.Equal("EU Sanction listed", col6.Name(), "invalid column name on conversion")
	assert.Equal(boolType, col6.DataType().ID(), "invalid column type on conversion. expected bool")
	col7 := ds.Column(7)
	assert.Equal("Active From", col7.Name(), "invalid column name on conversion")
	assert.Equal(timestampType, col7.DataType().ID(), "invalid column type on conversion. expected timestamp")
}
