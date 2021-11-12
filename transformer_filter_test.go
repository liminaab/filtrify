package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

var basicData [][]string = [][]string{
	{"user_id", "balance", "created_at", "is_active", "weight", "name", "ugly_data"},
	{"9Ip1aKbeZe2njCDM", "5000", "2012-11-02T15:04:05", "True", "0.08", "andreas", "12;20"},
	{"Akp1aKbeZe2njCDM", "7000", "2015-11-02T15:04:05", "False", "0.1", "nisan", "12;20"},
	{"Akp1aKbeHe2njCDM", "7800", "2017-11-02T15:04:05", "True", "1.4", "joakim", "12;20"},
	{"hT2impsOPUREcVPc", "286", "2009-09-02T15:04:05", "true", "1.2", "bahadir", "12;20"},
	{"hT2impsabc345c", "9650", "2008-06-02T15:04:05", "false", "2.5", "ricky", "12;20"},
	{"hT2impsafc355c", "300", "2005-03-02T15:04:05", "False", "0.9", "george", "12;20"},
	{"hT2empsafc355c", "5200", "1997-01-02T15:04:05", "fAlsE", "0.74", "boris", "12;20"},
}

func TestBasicSingleWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(basicData, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestHandleNegativeWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestHandleNumericalPrecisionWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestHandlePercentageWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 0, "Basic filtering operation failed. invalid number of columns")
}

func TestHandleListWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestHandleListAndNestedWhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			NestedCriterias: []*operator.FilterCriteria{
				{
					Criteria: &operator.Criteria{
						FieldName: "Instrument Type",
						Operator:  "=",
						Value:     "(Equity, Bill)",
					},
				},
				{
					Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestHandleListAndNested2WhereCriteria(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			NestedCriterias: []*operator.FilterCriteria{
				{
					NestedCriterias: []*operator.FilterCriteria{
						{
							Criteria: &operator.Criteria{
								FieldName: "Instrument Type",
								Operator:  "=",
								Value:     "Equity",
							},
						},
						{
							Criteria: &operator.Criteria{
								FieldName: "Instrument name",
								Operator:  "CONTAINS",
								Value:     "AMZN",
							},
						},
					},
					ChainWith: []string{"AND"},
				},
				{
					NestedCriterias: []*operator.FilterCriteria{
						{
							Criteria: &operator.Criteria{
								FieldName: "Instrument Type",
								Operator:  "=",
								Value:     "Bill",
							},
						},
						{
							Criteria: &operator.Criteria{
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{filterStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
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

func TestFilterInvalidColumn(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(SEQTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
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

	_, err = filtrify.Transform(plainData, steps, nil)

	assert.Error(t, err, "invalid column on filter operation didn't return an error")
	assert.EqualError(t, err, "could not apply transformation: attempted to operate on column “Instrument Class” but no such column available (Filter operator, step 0)")
}
