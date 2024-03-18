package filtrify_test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func isRowTargetGroup(t *testing.T, r *types.DataRow, fields []string, fieldValues []interface{}) bool {
	if len(fields) != len(fieldValues) {
		panic("invalid test code")
	}

	for i, f := range fields {
		col := test.GetColumn(r, f)
		assert.NotNil(t, col, fmt.Sprintf("%s was not found", f))
		if !test.IsEqualToInterfaceVal(col.CellValue, fieldValues[i]) {
			return false
		}
	}

	return true
}

func calculateFieldValueForLmnAgg(t *testing.T, ds *types.DataSet, fieldToCalculate string, fields []string, fieldValues []interface{}) interface{} {

	var lastVal interface{} = nil
	var isLastValSet bool = false

	for _, r := range ds.Rows {
		if !isRowTargetGroup(t, r, fields, fieldValues) {
			continue
		}

		calcCol := test.GetColumn(r, fieldToCalculate)
		assert.NotNil(t, calcCol, fmt.Sprintf("%s was not found", fieldToCalculate))
		// let's aggregate value of this column
		switch calcCol.CellValue.DataType {
		case types.IntType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.IntValue
				isLastValSet = true
			} else {
				lastVal = lastVal.(int32) + calcCol.CellValue.IntValue
			}
			break
		case types.LongType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.LongValue
				isLastValSet = true
			} else {
				lastVal = lastVal.(int64) + calcCol.CellValue.LongValue
			}
			break
		case types.TimestampType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.TimestampValue
				isLastValSet = true
			} else {
				if !calcCol.CellValue.TimestampValue.Equal(lastVal.(time.Time)) {
					return nil
				}
			}
		case types.StringType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.StringValue
				isLastValSet = true
			} else {
				if lastVal.(string) != calcCol.CellValue.StringValue {
					return nil
				}
			}
			break
		case types.DoubleType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.DoubleValue
				isLastValSet = true
			} else {
				lastVal = lastVal.(float64) + calcCol.CellValue.DoubleValue
			}
			break
		case types.BoolType:
			if !isLastValSet {
				lastVal = calcCol.CellValue.BoolValue
				isLastValSet = true
			} else {
				if lastVal.(bool) != calcCol.CellValue.BoolValue {
					return nil
				}
			}
			break
		case types.NilType:
			return nil

		}
	}
	return lastVal
}

func calculateAVGValueForAgg(t *testing.T, ds *types.DataSet, fieldToCalculate string, fields []string, fieldValues []interface{}) interface{} {

	var total float64 = 0
	var numberOfRows int = 0

	for _, r := range ds.Rows {
		if !isRowTargetGroup(t, r, fields, fieldValues) {
			continue
		}
		calcCol := test.GetColumn(r, fieldToCalculate)
		assert.NotNil(t, calcCol, fmt.Sprintf("%s was not found", fieldToCalculate))
		numberOfRows++
		// let's aggregate value of this column
		switch calcCol.CellValue.DataType {
		case types.IntType:
			total += float64(calcCol.CellValue.IntValue)
			break
		case types.LongType:
			total += float64(calcCol.CellValue.LongValue)
			break
		case types.DoubleType:
			total += calcCol.CellValue.DoubleValue
			break
		default:
			return nil
		}
	}
	return total / float64(numberOfRows)
}

func calculateWeightedAVGValueForAgg(t *testing.T, ds *types.DataSet, fieldToCalculate []string, fields []string, fieldValues []interface{}) interface{} {

	var total float64 = 0
	var weight float64 = 0

	for _, r := range ds.Rows {
		var current float64 = 0
		var currentWeight float64 = 0
		if !isRowTargetGroup(t, r, fields, fieldValues) {
			continue
		}
		calcCol := test.GetColumn(r, fieldToCalculate[0])
		assert.NotNil(t, calcCol, fmt.Sprintf("%s was not found", fieldToCalculate[0]))

		calcCol2 := test.GetColumn(r, fieldToCalculate[1])
		assert.NotNil(t, calcCol2, fmt.Sprintf("%s was not found", fieldToCalculate[1]))

		// let's aggregate value of this column
		switch calcCol.CellValue.DataType {
		case types.IntType:
			current = float64(calcCol.CellValue.IntValue)
			break
		case types.LongType:
			current = float64(calcCol.CellValue.LongValue)
			break
		case types.DoubleType:
			current = calcCol.CellValue.DoubleValue
			break
		default:
			return nil
		}

		switch calcCol2.CellValue.DataType {
		case types.IntType:
			currentWeight = float64(calcCol2.CellValue.IntValue)
			break
		case types.LongType:
			currentWeight = float64(calcCol2.CellValue.LongValue)
			break
		case types.DoubleType:
			currentWeight = calcCol2.CellValue.DoubleValue
			break
		default:
			return nil
		}

		total += current * currentWeight
		weight += currentWeight
	}
	val := total / weight
	return math.Round(val*100) / 100
}

func CheckAggrResults(t *testing.T, ds *types.DataSet, fields []string, fieldValues []interface{}, expectedVals map[string]interface{}) {
	for _, r := range ds.Rows {
		if !isRowTargetGroup(t, r, fields, fieldValues) {
			continue
		}

		for key, val := range expectedVals {
			targetCol := test.GetColumn(r, key)
			assert.NotNil(t, targetCol, fmt.Sprintf("%s column was not found", key))
			if !test.IsEqualToInterfaceVal(targetCol.CellValue, val) {
				assert.Fail(t, fmt.Sprintf("%s column value is not equal to %x", key, val))
			}
		}
	}
}

func batchCheckAggFields(t *testing.T, expectedFieldVals map[string]interface{}, aggDataSet *types.DataSet, fieldsToCheck []string, fields []string, fieldValues []interface{}) {
	CheckAggrResults(t, aggDataSet, fields, fieldValues, expectedFieldVals)
}

func TestBasicAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.AggregateConfiguration{
		GroupBy: []string{"EU Sanction listed"},
		Select: []*operator.AggregateSelect{
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

	aggregatedData, err := filtrify.Transform(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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

func TestAverageAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(plainData, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.AggregateConfiguration{
		Select: []*operator.AggregateSelect{
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

	aggregatedData, err := filtrify.Transform(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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

func TestWeightedAverageAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true, true)
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
	plainDataConverted, err := filtrify.Transform(plainData, []*types.TransformationStep{&changeColumnType, &changeColumnType2}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.AggregateConfiguration{
		Select: []*operator.AggregateSelect{
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

	aggregatedData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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

func TestMultipleGroupByColumns(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.AggregateConfiguration{
		GroupBy: []string{"Currency", "EU Sanction listed"},
		Select: []*operator.AggregateSelect{
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

	aggregatedData, err := filtrify.Transform(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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
