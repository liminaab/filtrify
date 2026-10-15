package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

// SEQTestDataFormatted and SEQTestDataFormatted2 are the v1 fixtures, shared from
// transformer_seq_test.go.

func TestFilterAggregateNewColumnSequencev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(SEQTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Instrument Type",
				Operator:  "!=",
				Value:     "Index Future",
			},
		},
	}

	aggregateConf := &operatorv2.AggregateConfiguration{
		GroupBy: []string{"Instrument Type"},
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Market Value (Base)"},
				Method:  "average",
			},
		},
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

	resultv2, err := filtrify.Transformv2(plainData, steps, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	result := rowsOf(t, resultv2)

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
		"Market Value (Base)", "Instrument Type",
	}
	expectedEquityFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(4000000),
		"Instrument Type":     "Equity",
	}
	expectedBillFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(8750000),
		"Instrument Type":     "Bill",
	}
	expectedCashAccountFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(5000000),
		"Instrument Type":     "Cash Account",
	}

	batchCheckAggFields(t, expectedEquityFieldOutputs, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Equity"})
	batchCheckAggFields(t, expectedBillFieldOutputs, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Bill"})
	batchCheckAggFields(t, expectedCashAccountFieldOutputs, result, fieldsToCheck, []string{"Instrument Type"}, []interface{}{"Cash Account"})

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
