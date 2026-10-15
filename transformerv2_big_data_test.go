package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

// These are v1's big-data tests over Transformv2. The fixture, the timing budget
// and the assertions are the v1 ones; GetBigSalesData and GetRandomResults are
// reused from transformer_big_data_test.go - GetRandomResults takes the
// *types.DataSet rowsOf returns.

func TestBigDataFilterv2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := filtrify.ConvertToTypedDatav2(plainCSV, true, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Units Sold",
				Operator:  ">",
				Value:     "756",
			},
		},
	}
	filterConfText, err := json.Marshal(filterConf)
	if err != nil {
		panic(err.Error())
	}

	changeColumnTypeUnitsSold := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Units Sold":{"targetType":1}}}`,
	}
	steps := []*types.TransformationStep{
		&changeColumnTypeUnitsSold,
		{
			Operator:      types.Filter,
			Configuration: string(filterConfText),
		},
	}
	start = time.Now()
	resultv2, err := filtrify.Transformv2(plainData, steps, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	opTime := time.Since(start)
	assert.LessOrEqual(t, int64(opTime), int64(3*time.Second), "transform operation took longer than expected")
	t.Log(fmt.Printf("Filtering took %s", opTime))
	rows := GetRandomResults(rowsOf(t, resultv2), 1000)
	for _, r := range rows {
		col := test.GetColumn(r, "Units Sold")
		assert.NotNil(t, col, fmt.Sprintf("%s column was not found", "Units Sold"))
		if col.CellValue.DataType == types.NilType {
			continue
		}
		assert.Greater(t, col.CellValue.LongValue, int64(750), "Units Sold filtering has failed")
	}
}

func TestBigDataSortv2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := filtrify.ConvertToTypedDatav2(plainCSV, true, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")
	sortConf := &operatorv2.SortConfiguration{
		OrderBy: []*operatorv2.OrderConfiguration{
			{
				ColumnName: "Units Sold",
				Ascending:  false,
			},
		},
	}
	sortConfText, err := json.Marshal(sortConf)
	if err != nil {
		panic(err.Error())
	}
	steps := []*types.TransformationStep{
		{
			Operator:      types.Sort,
			Configuration: string(sortConfText),
		},
	}
	start = time.Now()
	resultv2, err := filtrify.Transformv2(plainData, steps, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	assert.NotNil(t, resultv2)
	opTime := time.Since(start)
	assert.LessOrEqual(t, int64(opTime), int64(3*time.Second), "transform operation took longer than expected")
	t.Log(fmt.Printf("Sorting took %s", opTime))

	result := rowsOf(t, resultv2)
	var lastVal *int64 = nil
	firstCol := test.GetColumn(result.Rows[0], "Units Sold")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Units Sold"))
	if firstCol.CellValue.DataType != types.NilType {
		lastVal = &firstCol.CellValue.LongValue
	}

	for i := 0; i < 10000; i++ {
		col := test.GetColumn(result.Rows[i], "Units Sold")
		assert.NotNil(t, col, fmt.Sprintf("%s column was not found", "Units Sold"))

		if lastVal == nil && col.CellValue.DataType != types.NilType {
			assert.Fail(t, "descending sort failed. numbers can't appear after nil values")
		}

		if lastVal != nil {
			assert.LessOrEqual(t, col.CellValue.LongValue, *lastVal, "descending order failed")
		}

		if col.CellValue.DataType == types.NilType {
			lastVal = nil
		} else if lastVal == nil {
			lastVal = &col.CellValue.LongValue
		}
	}
}

func TestBigDataAggAvgNewColumnv2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := filtrify.ConvertToTypedDatav2(plainCSV, true, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")

	s1 := "average(`Units Sold`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	start = time.Now()
	resultv2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{newColStep1}, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	opTime := time.Since(start)
	t.Log(fmt.Printf("New Average Column took %s", opTime))
	assert.LessOrEqual(t, int64(opTime), int64(3*time.Second), "transform operation took longer than expected")

	// one header - 2 for filtered out rows
	assert.Len(t, rowsOf(t, resultv2).Rows, int(plainData.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
}

func TestBigDataAverageAggregatev2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := filtrify.ConvertToTypedDatav2(plainCSV, true, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Units Sold":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConvertedv2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.AggregateConfiguration{
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Units Sold"},
				Method:  "average",
			},
		},
		GroupBy: []string{"Region"},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Aggregate,
		Configuration: string(b1),
	}

	start = time.Now()
	aggregatedDatav2, err := filtrify.Transformv2(plainDataConvertedv2, []*types.TransformationStep{step}, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	opTime := time.Since(start)
	t.Log(fmt.Printf("Average took %s", opTime))

	assert.LessOrEqual(t, int64(opTime), int64(3*time.Second), "transform operation took longer than expected")

	aggregatedData := rowsOf(t, aggregatedDatav2)
	plainDataConverted := rowsOf(t, plainDataConvertedv2)

	fieldsToCheck := []string{
		"Unit Price",
	}

	batchCheckAggFields(t, nil, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{false})
	batchCheckAggFields(t, nil, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{true})
	batchCheckAggFields(t, nil, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{nil})

	expectedFalseAggMarket := calculateAVGValueForAgg(t, plainDataConverted, "Units Sold", []string{"Region"}, []interface{}{"Middle East and North Africa"})
	expectedTrueAggMarket := calculateAVGValueForAgg(t, plainDataConverted, "Units Sold", []string{"Region"}, []interface{}{"Central America and the Caribbean"})

	CheckAggrResults(t, aggregatedData, []string{"Region"}, []interface{}{"Middle East and North Africa"}, map[string]interface{}{"Units Sold": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"Region"}, []interface{}{"Central America and the Caribbean"}, map[string]interface{}{"Units Sold": expectedTrueAggMarket})
}
