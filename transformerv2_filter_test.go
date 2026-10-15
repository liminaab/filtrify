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

func TestBasicSingleWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(basicData, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "balance",
				Operator:  ">",
				Value:     "300",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"balance":{"targetType":1}}}`,
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType, filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(basicData)-3, "Basic filtering operation failed")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "balance" {
				continue
			}
			assert.Greater(t, c.CellValue.LongValue, int64(300), "balance filtering has failed")
		}
	}
}

func TestHandleNegativeWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Quantity",
				Operator:  "<",
				Value:     "0",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 1, "Basic filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "Quantity" {
				continue
			}
			assert.Less(t, c.CellValue.DoubleValue, float64(0), "quantity filtering has failed")
		}
	}
}

func TestHandleNumericalPrecisionWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Market Value (Base)",
				Operator:  ">",
				Value:     "2000000.00",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 3, "Basic filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "Market Value (Base)" {
				continue
			}
			assert.Greater(t, c.CellValue.DoubleValue, float64(2000000.00), "market value base filtering has failed")
		}
	}
}

func TestHandlePercentageWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Exposure %":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Exposure %",
				Operator:  "<",
				Value:     "0%",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 0, "Basic filtering operation failed. invalid number of columns")
}

func TestHandleListWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Instrument Type",
				Operator:  "=",
				Value:     "(Equity, Bill)",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 3, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}

func TestHandleListAndNestedWhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			NestedCriterias: []*operatorv2.FilterCriteria{
				{
					Criteria: &operatorv2.Criteria{
						FieldName: "Instrument Type",
						Operator:  "=",
						Value:     "(Equity, Bill)",
					},
				},
				{
					Criteria: &operatorv2.Criteria{
						FieldName: "Active From",
						Operator:  ">",
						Value:     "2020-03-01 00:00:00",
					},
				},
			},
			ChainWith: []string{"AND"},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 2, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}

func TestHandleListAndNested2WhereCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			NestedCriterias: []*operatorv2.FilterCriteria{
				{
					NestedCriterias: []*operatorv2.FilterCriteria{
						{
							Criteria: &operatorv2.Criteria{
								FieldName: "Instrument Type",
								Operator:  "=",
								Value:     "Equity",
							},
						},
						{
							Criteria: &operatorv2.Criteria{
								FieldName: "Instrument name",
								Operator:  "CONTAINS",
								Value:     "AMZN",
							},
						},
					},
					ChainWith: []string{"AND"},
				},
				{
					NestedCriterias: []*operatorv2.FilterCriteria{
						{
							Criteria: &operatorv2.Criteria{
								FieldName: "Instrument Type",
								Operator:  "=",
								Value:     "Bill",
							},
						},
						{
							Criteria: &operatorv2.Criteria{
								FieldName: "EU Sanction listed",
								Operator:  "=",
								Value:     "false",
							},
						},
					},
					ChainWith: []string{"AND"},
				},
			},
			ChainWith: []string{"OR"},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 2, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}

func TestIsNullCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			NestedCriterias: []*operatorv2.FilterCriteria{
				{
					Criteria: &operatorv2.Criteria{
						FieldName: "EU Sanction listed",
						Operator:  "IS EMPTY",
						Value:     "",
					},
				},
			},
			ChainWith: []string{},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 1, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "EU Sanction listed" {
				continue
			}
			assert.True(t, c.CellValue.DataType == types.NilType, "EU Sanction listed base filtering has failed")
		}
	}
}

func TestIsNotNullCriteriav2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			NestedCriterias: []*operatorv2.FilterCriteria{
				{
					Criteria: &operatorv2.Criteria{
						FieldName: "EU Sanction listed",
						Operator:  "IS NOT EMPTY",
						Value:     "",
					},
				},
			},
			ChainWith: []string{},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows())-1, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if c.ColumnName != "EU Sanction listed" {
				continue
			}
			assert.True(t, c.CellValue.DataType != types.NilType, "EU Sanction listed base filtering has failed")
		}
	}
}

func TestFilterInvalidColumnv2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(SEQTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Instrument Class",
				Operator:  "!=",
				Value:     "Index Future",
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

	_, err = filtrify.Transformv2(plainData, steps, nil)

	assert.Error(t, err, "invalid column on filter operation didn't return an error")
	assert.EqualError(t, err, "could not apply transformation: attempted to operate on column “Instrument Class” but no such column available (Filter operator, step 0)")
}

func TestFilterDateTypev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(SEQTestDataFormatted2, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Maturity Date",
				Operator:  "=",
				Value:     "2022-03-14",
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

	newDatav2, err := filtrify.Transformv2(plainData, steps, nil)

	assert.Nil(t, err, "filtering by date failed")
	newData := rowsOf(t, newDatav2)
	assert.Len(t, newData.Rows, 1, "filtering by date failed")
	maturityDate := test.GetColumn(newData.Rows[0], "Maturity Date")
	assert.Equal(t, types.DateType, maturityDate.CellValue.DataType, "filtering by date failed")
}

func TestFilterTimeTypev2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(SEQTestDataFormatted2, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Active From",
				Operator:  "=",
				Value:     "21:12:48",
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

	newDatav2, err := filtrify.Transformv2(plainData, steps, nil)

	assert.Nil(t, err, "filtering by time failed")
	newData := rowsOf(t, newDatav2)
	assert.Len(t, newData.Rows, 1, "filtering by time failed")
	maturityDate := test.GetColumn(newData.Rows[0], "Active From")
	assert.Equal(t, types.TimeOfDayType, maturityDate.CellValue.DataType, "filtering by time failed")
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestBasicWhereCriteriaWithRowKey is
// TestBasicSingleWhereCriteriav2.
func TestBasicWhereCriteriav2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
