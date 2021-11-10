package dyntransformer_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/test"
	"limina.com/dyntransformer/types"
)

var HUNDREDTHOUSANDROWS string = "https://eforexcel.com/wp/wp-content/uploads/2017/07/100000-Sales-Records.zip"
var ONEMILLIONROWS string = "https://eforexcel.com/wp/wp-content/uploads/2017/07/1000000%20Sales%20Records.zip"

func GetBigSalesData(t *testing.T) [][]string {
	err := test.DownloadZipFileIfNotExists(HUNDREDTHOUSANDROWS, "/tmp/salesrecords.zip", "/tmp/salesrecords.csv")
	assert.NoError(t, err, "basic data download failed")
	_, plainCSV, err := test.LoadCSVFileFromTestDataDir("/tmp/salesrecords.csv", false)
	assert.NoError(t, err, "basic data load failed")
	return plainCSV
}

func GetRandomResults(dataset *types.DataSet, number int) []*types.DataRow {
	if len(dataset.Rows) < number {
		return dataset.Rows
	}
	rows := make([]*types.DataRow, number)
	rand.Seed(time.Now().Unix())
	for i := 0; i < number; i++ {
		rows[i] = dataset.Rows[rand.Intn(len(dataset.Rows))]
	}

	return rows
}

func TestBigDataFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := dyntransformer.ConvertToTypedData(plainCSV, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")
	filterConf := &operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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
	steps := []*types.TransformationStep{
		{
			Operator:      types.Filter,
			Configuration: string(filterConfText),
		},
	}
	start = time.Now()
	result, err := dyntransformer.Transform(plainData, steps, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	filterTime := time.Since(start)
	t.Log(fmt.Printf("Filtering took %s", filterTime))
	rows := GetRandomResults(result, 1000)
	for _, r := range rows {
		col := test.GetColumn(r, "Units Sold")
		assert.NotNil(t, col, fmt.Sprintf("%s column was not found", "Units Sold"))
		if col.CellValue.DataType == types.NilType {
			continue
		}
		assert.Greater(t, col.CellValue.LongValue, int64(750), "Units Sold filtering has failed")
	}
}

func TestBigDataSort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := dyntransformer.ConvertToTypedData(plainCSV, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")
	sortConf := &operator.SortConfiguration{
		OrderBy: []*operator.OrderConfiguration{
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
	result, err := dyntransformer.Transform(plainData, steps, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	assert.NotNil(t, result)
	filterTime := time.Since(start)
	t.Log(fmt.Printf("Sorting took %s", filterTime))

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

func TestBigDataAggAvgNewColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := dyntransformer.ConvertToTypedData(plainCSV, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")

	s1 := "average(`Units Sold`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	start = time.Now()
	result, err := dyntransformer.Transform(plainData, []*types.TransformationStep{newColStep1}, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	opTime := time.Since(start)
	t.Log(fmt.Printf("New Average Column took %s", opTime))

	// one header - 2 for filtered out rows
	assert.Len(t, result.Rows, len(plainData.Rows), "Basic new column aggregation operation failed. invalid number of rows")
}

func TestBigDataAverageAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	plainCSV := GetBigSalesData(t)
	start := time.Now()
	plainData, err := dyntransformer.ConvertToTypedData(plainCSV, true, true)
	conversionTime := time.Since(start)
	t.Log(fmt.Printf("Conversion took %s", conversionTime))
	assert.NoError(t, err, "basic data conversion failed")

	conf := &operator.AggregateConfiguration{
		Select: []*operator.AggregateSelect{
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
	aggregatedData, err := dyntransformer.Transform(plainData, []*types.TransformationStep{step}, nil)
	assert.NoError(t, err, "new aggregation column operation failed")
	opTime := time.Since(start)
	t.Log(fmt.Printf("Average took %s", opTime))

	fieldsToCheck := []string{
		"Unit Price",
	}

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Region"}, []interface{}{nil})

	expectedFalseAggMarket := calculateAVGValueForAgg(t, plainData, "Units Sold", []string{"Region"}, []interface{}{"Middle East and North Africa"})
	expectedTrueAggMarket := calculateAVGValueForAgg(t, plainData, "Units Sold", []string{"Region"}, []interface{}{"Central America and the Caribbean"})

	CheckAggrResults(t, aggregatedData, []string{"Region"}, []interface{}{"Middle East and North Africa"}, map[string]interface{}{"Units Sold": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"Region"}, []interface{}{"Central America and the Caribbean"}, map[string]interface{}{"Units Sold": expectedTrueAggMarket})
}