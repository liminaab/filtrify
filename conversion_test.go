package dyntransformer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/test"
	"limina.com/dyntransformer/types"
)

func TestSimpleConversionWithoutHeader(t *testing.T) {
	data := [][]string{
		{"15", "test text", "2008-06-02T15:04:05", "3.25"},
	}
	ds, err := dyntransformer.ConvertToTypedData(data, false, true)
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(ds.Rows) == 1, "invalid row count in conversion")
	firstRow := ds.Rows[0]
	assert.True(t, len(firstRow.Columns) == 4, "invalid column count")

	intCell := firstRow.Columns[0]
	assert.True(t, intCell.CellValue.DataType == types.IntType, "invalid conversion type should be int")
	textCell := firstRow.Columns[1]
	assert.True(t, textCell.CellValue.DataType == types.StringType, "invalid conversion type should be string")
	timeCell := firstRow.Columns[2]
	assert.True(t, timeCell.CellValue.DataType == types.TimestampType, "invalid conversion type should be timestamp")
	doubleCell := firstRow.Columns[3]
	assert.True(t, doubleCell.CellValue.DataType == types.DoubleType, "invalid conversion type should be double")
}

func TestDateConversion(t *testing.T) {
	data := [][]string{
		{"2020-01-01", "20200101", "01/01/2020", "01/01/2020"},
		{"2020-01-08", "20200108", "01/08/2020", "08/01/2020"},
		{"2020-01-13", "20200113", "01/13/2020", "13/01/2020"},
	}
	ds, err := dyntransformer.ConvertToTypedData(data, false, true)
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(ds.Rows) == 3, "invalid row count in conversion")

	for _, r := range ds.Rows {
		for _, c := range r.Columns {
			assert.True(t, c.CellValue.DataType == types.TimestampType, "invalid conversion type should be timestamp but it is %s. val: %s", c.CellValue.DataType.String(), test.CellDataToString(&c.CellValue))
		}
	}
}

func TestConversionUAT1(t *testing.T) {
	var uat1TestData [][]string = [][]string{
		{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
		{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
		{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
		{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
		{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
		{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
	}
	ds, err := dyntransformer.ConvertToTypedData(uat1TestData, true, true)
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(ds.Rows) == 5, "invalid row count in conversion")

	for _, r := range ds.Rows {
		cols := r.Columns
		assert.Equal(t, "Instrument name", *cols[0].ColumnName, "invalid column name on conversion")
		assert.Equal(t, types.StringType, cols[0].CellValue.DataType, "invalid column type on conversion. expected string but it is %s. val: %s", cols[0].CellValue.DataType.String(), test.CellDataToString(&cols[0].CellValue))
		assert.Equal(t, "Instrument Type", *cols[1].ColumnName, "invalid column name on conversion")
		assert.Equal(t, types.StringType, cols[1].CellValue.DataType, "invalid column type on conversion. expected string but it is %s. val: %s", cols[1].CellValue.DataType.String(), test.CellDataToString(&cols[1].CellValue))
		assert.Equal(t, "Quantity", *cols[2].ColumnName, "invalid column name on conversion")
		assert.Equal(t, types.DoubleType, cols[2].CellValue.DataType, "invalid column type on conversion. expected double but it is %s. val: %s", cols[2].CellValue.DataType.String(), test.CellDataToString(&cols[2].CellValue))
		assert.Equal(t, "Market Value (Base)", *cols[3].ColumnName, "invalid column name on conversion")
		assert.Equal(t, types.DoubleType, cols[3].CellValue.DataType, "invalid column type on conversion. expected double but it is %s. val: %s", cols[3].CellValue.DataType.String(), test.CellDataToString(&cols[3].CellValue))
		assert.Equal(t, "Exposure %", *cols[4].ColumnName, "invalid column name on conversion")
		assert.Equal(t, types.DoubleType, cols[4].CellValue.DataType, "invalid column type on conversion. expected string but it is %s. val: %s", cols[4].CellValue.DataType.String(), test.CellDataToString(&cols[4].CellValue))
		assert.Equal(t, "Maturity Date", *cols[5].ColumnName, "invalid column name on conversion")
		if cols[5].CellValue.DataType != types.NilType {
			assert.Equal(t, types.TimestampType, cols[5].CellValue.DataType, "invalid column type on conversion. expected timestamp but it is %s. val: %s", cols[5].CellValue.DataType.String(), test.CellDataToString(&cols[5].CellValue))
		}
		assert.Equal(t, "EU Sanction listed", *cols[6].ColumnName, "invalid column name on conversion")
		if cols[6].CellValue.DataType != types.NilType {
			assert.Equal(t, types.BoolType, cols[6].CellValue.DataType, "invalid column type on conversion. expected bool but it is %s. val: %s", cols[6].CellValue.DataType.String(), test.CellDataToString(&cols[6].CellValue))
		}
		assert.Equal(t, "Active From", *cols[7].ColumnName, "invalid column name on conversion")
		if cols[7].CellValue.DataType != types.NilType {
			assert.Equal(t, types.TimestampType, cols[7].CellValue.DataType, "invalid column type on conversion. expected timestamp but it is %s. val: %s", cols[7].CellValue.DataType.String(), test.CellDataToString(&cols[7].CellValue))
		}
	}
}
