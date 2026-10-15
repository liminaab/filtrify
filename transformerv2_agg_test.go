package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

// The aggregate assertions themselves - isRowTargetGroup, calculateAVGValueForAgg,
// calculateWeightedAVGValueForAgg, CheckAggrResults, batchCheckAggFields - are the
// v1 ones from transformer_agg_test.go, reused unchanged: they take a
// *types.DataSet, which is what rowsOf hands back.

func TestBasicAggregatev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.AggregateConfiguration{
		GroupBy: []string{"EU Sanction listed"},
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Instrument name"},
				Method:  "last",
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Aggregate,
		Configuration: string(b1),
	}

	aggregatedDatav2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	aggregatedData := rowsOf(t, aggregatedDatav2)

	fieldsToCheck := []string{
		"Instrument name", "EU Sanction listed",
	}
	expectedFalseFieldOutputs := map[string]interface{}{
		"Instrument name":    "ESZ1",
		"EU Sanction listed": false,
	}
	expectedTrueFieldOutputs := map[string]interface{}{
		"Instrument name":    "AMZN US Equity",
		"EU Sanction listed": true,
	}
	expectedNilFieldOutputs := map[string]interface{}{
		"Instrument name":    "USD Cash",
		"EU Sanction listed": nil,
	}
	batchCheckAggFields(t, expectedFalseFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, expectedTrueFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, expectedNilFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})
	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestAverageAggregatev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConvertedv2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.AggregateConfiguration{
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Market Value (Base)"},
				Method:  "average",
			},
		},
		GroupBy: []string{"EU Sanction listed"},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Aggregate,
		Configuration: string(b1),
	}

	aggregatedDatav2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	aggregatedData := rowsOf(t, aggregatedDatav2)
	plainDataConverted := rowsOf(t, plainDataConvertedv2)

	fieldsToCheck := []string{
		"Market Value (Base)", "EU Sanction listed",
	}
	expectedFalseFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(4127500),
		"EU Sanction listed":  false,
	}
	expectedTrueFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(4000000),
		"EU Sanction listed":  true,
	}
	expectedNilFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(5000000),
		"EU Sanction listed":  nil,
	}

	batchCheckAggFields(t, expectedFalseFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, expectedTrueFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, expectedNilFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})

	expectedFalseAggMarket := calculateAVGValueForAgg(t, plainDataConverted, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{false})
	expectedTrueAggMarket := calculateAVGValueForAgg(t, plainDataConverted, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{true})
	expectedNilAggMarket := calculateAVGValueForAgg(t, plainDataConverted, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{nil})

	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{false}, map[string]interface{}{"Market Value (Base)": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{true}, map[string]interface{}{"Market Value (Base)": expectedTrueAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{nil}, map[string]interface{}{"Market Value (Base)": expectedNilAggMarket})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestWeightedAverageAggregatev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	changeColumnType2 := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConvertedv2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{&changeColumnType, &changeColumnType2}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.AggregateConfiguration{
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Market Value (Base)", "Quantity"},
				Method:  "weighted_average",
			},
		},
		GroupBy: []string{"EU Sanction listed"},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Aggregate,
		Configuration: string(b1),
	}

	aggregatedDatav2, err := filtrify.Transformv2(plainDataConvertedv2, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	aggregatedData := rowsOf(t, aggregatedDatav2)
	plainDataConverted := rowsOf(t, plainDataConvertedv2)

	fieldsToCheck := []string{
		"Instrument name", "EU Sanction listed",
	}
	expectedFalseFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(8750010.27),
		"EU Sanction listed":  false,
	}
	expectedTrueFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(2033994.33),
		"EU Sanction listed":  true,
	}
	expectedNilFieldOutputs := map[string]interface{}{
		"Market Value (Base)": float64(5000000),
		"EU Sanction listed":  nil,
	}

	batchCheckAggFields(t, expectedFalseFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, expectedTrueFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, expectedNilFieldOutputs, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})

	weightedFields := []string{"Market Value (Base)", "Quantity"}
	expectedFalseAggMarket := calculateWeightedAVGValueForAgg(t, plainDataConverted, weightedFields, []string{"EU Sanction listed"}, []interface{}{false})
	expectedTrueAggMarket := calculateWeightedAVGValueForAgg(t, plainDataConverted, weightedFields, []string{"EU Sanction listed"}, []interface{}{true})
	expectedNilAggMarket := calculateWeightedAVGValueForAgg(t, plainDataConverted, weightedFields, []string{"EU Sanction listed"}, []interface{}{nil})

	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{false}, map[string]interface{}{"Market Value (Base)": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{true}, map[string]interface{}{"Market Value (Base)": expectedTrueAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{nil}, map[string]interface{}{"Market Value (Base)": expectedNilAggMarket})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestMultipleGroupByColumnsv2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.AggregateConfiguration{
		GroupBy: []string{"Currency", "EU Sanction listed"},
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"Instrument name"},
				Method:  "last",
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Aggregate,
		Configuration: string(b1),
	}

	aggregatedDatav2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	aggregatedData := rowsOf(t, aggregatedDatav2)

	fieldsToCheck := []string{
		"Instrument name", "EU Sanction listed", "Currency",
	}

	expectedUSDTrueFieldOutputs := map[string]interface{}{
		"Instrument name":    "AMZN US Equity",
		"EU Sanction listed": true,
	}
	expectedUSDNilFieldOutputs := map[string]interface{}{
		"Instrument name":    "USD Cash",
		"EU Sanction listed": nil,
	}
	expectedSEKFalseFieldOutputs := map[string]interface{}{
		"Instrument name":    "ESZ1",
		"EU Sanction listed": false,
	}
	expectedSEKTrueFieldOutputs := map[string]interface{}{
		"Instrument name":    "ERIC B SS Equity",
		"EU Sanction listed": true,
	}

	batchCheckAggFields(t, nil, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", false})
	batchCheckAggFields(t, expectedUSDTrueFieldOutputs, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", true})
	batchCheckAggFields(t, expectedUSDNilFieldOutputs, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", nil})

	batchCheckAggFields(t, expectedSEKFalseFieldOutputs, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", false})
	batchCheckAggFields(t, expectedSEKTrueFieldOutputs, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", true})
	batchCheckAggFields(t, nil, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", nil})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 4, "Aggregate operation failed. invalid number of rows")
}
