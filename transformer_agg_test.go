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

func batchCheckAggFields(t *testing.T, nonAggDataSet *types.DataSet, aggDataSet *types.DataSet, fieldsToCheck []string, fields []string, fieldValues []interface{}) {
	valsToCheck := make(map[string]interface{})
	for _, f := range fieldsToCheck {
		valsToCheck[f] = calculateFieldValueForLmnAgg(t, nonAggDataSet, f, fields, fieldValues)
	}

	CheckAggrResults(t, aggDataSet, fields, fieldValues, valsToCheck)
}

func TestBasicAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.AggregateConfiguration{
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
		"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Active From", "Currency",
	}

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})
	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestAverageAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true)
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
		"Instrument name", "Instrument Type", "Quantity", "Exposure %", "Active From", "Currency",
	}

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})

	expectedFalseAggMarket := calculateAVGValueForAgg(t, plainData, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{false})
	expectedTrueAggMarket := calculateAVGValueForAgg(t, plainData, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{true})
	expectedNilAggMarket := calculateAVGValueForAgg(t, plainData, "Market Value (Base)", []string{"EU Sanction listed"}, []interface{}{nil})

	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{false}, map[string]interface{}{"Market Value (Base)": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{true}, map[string]interface{}{"Market Value (Base)": expectedTrueAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{nil}, map[string]interface{}{"Market Value (Base)": expectedNilAggMarket})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestWeightedAverageAggregate(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true)
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

	aggregatedData, err := filtrify.Transform(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	fieldsToCheck := []string{
		"Instrument name", "Instrument Type", "Quantity", "Exposure %", "Active From", "Currency",
	}

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"EU Sanction listed"}, []interface{}{nil})

	weightedFields := []string{"Market Value (Base)", "Quantity"}
	expectedFalseAggMarket := calculateWeightedAVGValueForAgg(t, plainData, weightedFields, []string{"EU Sanction listed"}, []interface{}{false})
	expectedTrueAggMarket := calculateWeightedAVGValueForAgg(t, plainData, weightedFields, []string{"EU Sanction listed"}, []interface{}{true})
	expectedNilAggMarket := calculateWeightedAVGValueForAgg(t, plainData, weightedFields, []string{"EU Sanction listed"}, []interface{}{nil})

	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{false}, map[string]interface{}{"Market Value (Base)": expectedFalseAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{true}, map[string]interface{}{"Market Value (Base)": expectedTrueAggMarket})
	CheckAggrResults(t, aggregatedData, []string{"EU Sanction listed"}, []interface{}{nil}, map[string]interface{}{"Market Value (Base)": expectedNilAggMarket})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 3, "Aggregate operation failed. invalid number of rows")
}

func TestMultipleGroupByColumns(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.AggregateConfiguration{
		GroupBy: []string{"Currency", "EU Sanction listed"},
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
		"Instrument name", "Instrument Type", "Quantity", "Exposure %", "Active From", "Market Value (Base)",
	}

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"USD", nil})

	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", false})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", true})
	batchCheckAggFields(t, plainData, aggregatedData, fieldsToCheck, []string{"Currency", "EU Sanction listed"}, []interface{}{"SEK", nil})

	// one header - 2 for filtered out rows
	assert.Len(t, aggregatedData.Rows, 4, "Aggregate operation failed. invalid number of rows")
}
