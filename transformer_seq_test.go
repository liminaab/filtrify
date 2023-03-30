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

var SEQTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "TRUE", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "FALSE", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "FALSE", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10", "-495000.00", "17%", "2021-12-16", "FALSE", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var SEQTestDataFormatted2 [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "2022-03-14", "TRUE", "12:35:43"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "2021-11-18", "FALSE", "12:07:11"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "FALSE", "09:13:00"},
	{"ESZ1", "Index Future", "-10", "-495000.00", "17%", "2021-12-16", "FALSE", "23:56:34"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "2020-03-14", "", "21:12:48"},
}

func TestFilterAggregateNewColumnSequence(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(SEQTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
				FieldName: "Instrument Type",
				Operator:  "!=",
				Value:     "Index Future",
			},
		},
	}

	aggregateConf := &operator.AggregateConfiguration{
		GroupBy: []string{"Instrument Type"},
	}

	newColConfig := "{\"statement\": \"IFEL(`Market Value (Base)` > 5000000, 'Large', 'Small') AS `Size` \"}"

	filterConfText, err := json.Marshal(filterConf)
	if err != nil {
		panic(err.Error())
	}
	aggregateConfText, err := json.Marshal(aggregateConf)
	if err != nil {
		panic(err.Error())
	}
	steps := []*types.TransformationStep{
		{
			Operator:      types.Filter,
			Configuration: string(filterConfText),
		},
		{
			Operator:      types.Aggregate,
			Configuration: string(aggregateConfText),
		},
		{
			Operator:      types.NewColumn,
			Configuration: newColConfig,
		},
	}

	result, err := filtrify.Transform(plainData, steps, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	// let's first check filter
	for _, r := range result.Rows {
		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, fmt.Sprintf("%s column was not found", "Instrument Type"))
		if instTypeCol.CellValue.DataType == types.NilType {
			continue
		}
		assert.NotEqual(t, "Index Future", instTypeCol.CellValue.StringValue, "balance filtering has failed")
	}

	// let's check aggregation now
	fieldsToCheck := []string{
		"Instrument name", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From",
	}

	batchCheckAggFields(t, plainData, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Equity"})
	batchCheckAggFields(t, plainData, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Bill"})
	batchCheckAggFields(t, plainData, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Cash Account"})

	// and time to check the new column
	for _, r := range result.Rows {
		sizeCol := test.GetColumn(r, "Size")
		assert.NotNil(t, sizeCol, "Size column was not found")

		marketValCol := test.GetColumn(r, "Market Value (Base)")
		assert.NotNil(t, marketValCol, "Market Value (Base) column was not found")

		if marketValCol.CellValue.DataType == types.NilType || marketValCol.CellValue.DoubleValue <= 5000000 {
			assert.Equal(t, "Small", sizeCol.CellValue.StringValue, "New Column failed in sequence tests")
		} else {
			assert.Equal(t, "Large", sizeCol.CellValue.StringValue, "New Column failed in sequence tests")
		}
	}
}
