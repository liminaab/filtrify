package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestSortv2(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: "Market Value (Base)",
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastVal *float64 = nil
	firstCol := test.GetColumn(sortedData.Rows[0], "Market Value (Base)")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Market Value (Base)"))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.DoubleValue
	}
	for _, r := range sortedData.Rows {
		marketValColumm := test.GetColumn(r, "Market Value (Base)")
		assert.NotNil(t, marketValColumm, fmt.Sprintf("%s column was not found", "Market Value (Base)"))

		if lastVal == nil && marketValColumm.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			assert.LessOrEqual(t, marketValColumm.CellValue.DoubleValue, *lastVal, "descending order failed")
		}

		if marketValColumm.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &marketValColumm.CellValue.DoubleValue
		}

	}
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestSortWithRowKey is TestSortv2.
func TestSortv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}

func TestSortv2WithTime(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormattedWithTime, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	orderedColumn := "Active From"

	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: orderedColumn,
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "sort column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastVal *time.Time = nil
	firstCol := test.GetColumn(sortedData.Rows[0], orderedColumn)
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", orderedColumn))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.TimestampValue
	}
	for _, r := range sortedData.Rows {
		sortedColValue := test.GetColumn(r, orderedColumn)
		assert.NotNil(t, sortedColValue, fmt.Sprintf("%s column was not found", orderedColumn))

		if lastVal == nil && sortedColValue.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			isAfter := lastVal.After(sortedColValue.CellValue.TimestampValue)
			isSame := lastVal.Equal(sortedColValue.CellValue.TimestampValue)
			assert.True(t, isAfter || isSame, "descending order failed")
		}

		if sortedColValue.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &sortedColValue.CellValue.TimestampValue
		}

	}
}

func TestSortv2WithDate(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormattedWithDate, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	orderedColumn := "Active From"

	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: orderedColumn,
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "sort column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastVal *time.Time = nil
	firstCol := test.GetColumn(sortedData.Rows[0], orderedColumn)
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", orderedColumn))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.TimestampValue
	}
	for _, r := range sortedData.Rows {
		sortedColValue := test.GetColumn(r, orderedColumn)
		assert.NotNil(t, sortedColValue, fmt.Sprintf("%s column was not found", orderedColumn))

		if lastVal == nil && sortedColValue.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			isAfter := lastVal.After(sortedColValue.CellValue.TimestampValue)
			isSame := lastVal.Equal(sortedColValue.CellValue.TimestampValue)
			assert.True(t, isAfter || isSame, "descending order failed")
		}

		if sortedColValue.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &sortedColValue.CellValue.TimestampValue
		}

	}
}

func TestSortv2WithDateTime(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	orderedColumn := "Active From"

	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: orderedColumn,
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "sort column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastVal *time.Time = nil
	firstCol := test.GetColumn(sortedData.Rows[0], orderedColumn)
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", orderedColumn))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.TimestampValue
	}
	for _, r := range sortedData.Rows {
		sortedColValue := test.GetColumn(r, orderedColumn)
		assert.NotNil(t, sortedColValue, fmt.Sprintf("%s column was not found", orderedColumn))

		if lastVal == nil && sortedColValue.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			isAfter := lastVal.After(sortedColValue.CellValue.TimestampValue)
			isSame := lastVal.Equal(sortedColValue.CellValue.TimestampValue)
			assert.True(t, isAfter || isSame, "descending order failed")
		}

		if sortedColValue.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &sortedColValue.CellValue.TimestampValue
		}

	}
}

// v1's TestSortWithDateTimeWantedData and TestSortWithDateTimeOnHardcodedData both
// start from test.UAT1TestDataSet - a hand-built v1 dataset rather than raw text -
// so the v2 pair migrates it with conversion.MigrateToV2 instead of converting raw
// data. Everything after that is the v1 test verbatim.
func TestSortv2WithDateTimeWantedData(t *testing.T) {
	data, err := conversion.MigrateToV2(test.UAT1TestDataSet)
	if err != nil {
		assert.NoError(t, err, "basic data migration failed")
	}
	orderedColumn := "Active From"
	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: orderedColumn,
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "sort column operation failed")
	}

	wantTimes := []time.Time{
		time.Date(2021, 04, 06, 12, 00, 00, 0, time.UTC),
		time.Date(2020, 11, 22, 12, 00, 00, 0, time.UTC),
		time.Date(2020, 03, 01, 12, 00, 00, 0, time.UTC),
		time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC),
		time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC),
	}

	sortedData := rowsOf(t, sortedDatav2)
	for i, r := range sortedData.Rows {
		sortedColValue := test.GetColumn(r, orderedColumn)
		assert.NotNil(t, sortedColValue, fmt.Sprintf("%s column was not found", orderedColumn))
		assert.Equal(t, wantTimes[i].In(time.UTC).Format(time.RFC3339), sortedColValue.CellValue.TimestampValue.In(time.UTC).Format(time.RFC3339), "descending order failed")
	}
}

func TestSortv2WithDateTimeOnHardcodedData(t *testing.T) {
	data, err := conversion.MigrateToV2(test.UAT1TestDataSet)
	if err != nil {
		assert.NoError(t, err, "basic data migration failed")
	}
	orderedColumn := "Active From"
	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: orderedColumn,
				Ascending:  false,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "sort column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastVal *time.Time = nil
	firstCol := test.GetColumn(sortedData.Rows[0], orderedColumn)
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", orderedColumn))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.TimestampValue
	}
	for _, r := range sortedData.Rows {
		sortedColValue := test.GetColumn(r, orderedColumn)
		assert.NotNil(t, sortedColValue, fmt.Sprintf("%s column was not found", orderedColumn))

		if lastVal == nil && sortedColValue.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			isAfter := lastVal.After(sortedColValue.CellValue.TimestampValue)
			isSame := lastVal.Equal(sortedColValue.CellValue.TimestampValue)
			assert.True(t, isAfter || isSame, "descending order failed")
		}

		if sortedColValue.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &sortedColValue.CellValue.TimestampValue
		}

	}
}

func TestMultipleSortv2(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: "Instrument Type",
				Ascending:  true,
			},
			{
				ColumnName: "Market Value (Base)",
				Ascending:  true,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Sort,
		Configuration: string(b1),
	}

	sortedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	var lastInstTypeVal *string = nil
	firstInstTypeCol := test.GetColumn(sortedData.Rows[0], "Instrument Type")
	assert.NotNil(t, firstInstTypeCol, fmt.Sprintf("%s column was not found", "Instrument Type"))
	if firstInstTypeCol.CellValue.DataType != types.NilType {
		lastInstTypeVal = &firstInstTypeCol.CellValue.StringValue
	}

	var lastMarketVal *float64 = nil
	firstMarketValCol := test.GetColumn(sortedData.Rows[0], "Market Value (Base)")
	assert.NotNil(t, firstMarketValCol, fmt.Sprintf("%s column was not found", "Market Value (Base)"))
	if firstMarketValCol.CellValue.DataType != types.NilType {
		lastMarketVal = &firstMarketValCol.CellValue.DoubleValue
	}
	for _, r := range sortedData.Rows {
		marketValColumm := test.GetColumn(r, "Market Value (Base)")
		assert.NotNil(t, marketValColumm, fmt.Sprintf("%s column was not found", "Market Value (Base)"))

		instTypeColumm := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeColumm, fmt.Sprintf("%s column was not found", "Instrument Type"))

		if lastInstTypeVal != nil && instTypeColumm.CellValue.DataType == types.NilType {
			assert.Fail(t, "ascending sort failed. nil can't appear after values")
		}
		if lastInstTypeVal != nil {
			assert.GreaterOrEqual(t, instTypeColumm.CellValue.StringValue, *lastInstTypeVal, "ascending sort failed")
		}

		if *lastInstTypeVal == instTypeColumm.CellValue.StringValue {
			if lastMarketVal != nil && marketValColumm.CellValue.DataType == types.NilType {
				assert.Fail(t, "ascending sort failed. nil can't appear after values")
			}
			if lastMarketVal != nil {
				assert.GreaterOrEqual(t, marketValColumm.CellValue.DoubleValue, *lastMarketVal, "ascending sort failed")
			}
		}

		if instTypeColumm.CellValue.DataType == types.NilType {
			lastInstTypeVal = nil
		} else if lastInstTypeVal == nil {
			lastInstTypeVal = &instTypeColumm.CellValue.StringValue
		}

		if marketValColumm.CellValue.DataType == types.NilType {
			lastMarketVal = nil
		} else if lastMarketVal == nil {
			lastMarketVal = &marketValColumm.CellValue.DoubleValue
		}

	}
}
