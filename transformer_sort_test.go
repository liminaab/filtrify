package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.SortConfiguration{
		OrderBy: []*operator.OrderConfiguration{
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

	sortedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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

func TestMultipleSort(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.SortConfiguration{
		OrderBy: []*operator.OrderConfiguration{
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

	sortedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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
