package dyntransformer_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/types"
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
	ds, err := dyntransformer.ConvertToTypedData(basicData, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
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

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(basicData)-3, "Basic filtering operation failed")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "balance" {
				continue
			}
			assert.Greater(t, c.CellValue.LongValue, int64(300), "balance filtering has failed")
		}
	}
}

var uat1TestData [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175 000.00", "2 000 000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1 500.00", "6 000 000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9 000 000.00", "8 750 000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495 000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5 000 000.00", "5 000 000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var uat1TestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

func TestHandleNegativeWhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
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

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 1, "Basic filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "Quantity" {
				continue
			}
			assert.Less(t, c.CellValue.DoubleValue, float64(0), "quantity filtering has failed")
		}
	}
}

func TestHandleNumericalPrecisionWhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
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

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 3, "Basic filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "Market Value (Base)" {
				continue
			}
			assert.Greater(t, c.CellValue.DoubleValue, float64(2000000.00), "market value base filtering has failed")
		}
	}
}

func TestHandlePercentageWhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
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

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 0, "Basic filtering operation failed. invalid number of columns")
}

func TestHandleListWhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
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

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 3, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}

func TestHandleListAndNestedWhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
			Statements: []*operator.FilterStatement{
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
			Conditions: []string{"AND"},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 2, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}

func TestHandleListAndNested2WhereCriteria(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(uat1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterStep1 := &types.TransformationStep{
		Step:     0,
		Enabled:  true,
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
			Statements: []*operator.FilterStatement{
				{
					Statements: []*operator.FilterStatement{
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
					Conditions: []string{"AND"},
				},
				{
					Statements: []*operator.FilterStatement{
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
					Conditions: []string{"AND"},
				},
			},
			Conditions: []string{"OR"},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{filterStep1})
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 2, "List filtering operation failed. invalid number of columns")
	for _, r := range newData.Rows {
		for _, c := range r.Columns {
			if *c.ColumnName != "Instrument Type" {
				continue
			}
			assert.True(t, c.CellValue.StringValue == "Equity" || c.CellValue.StringValue == "Bill", "instrument type base filtering has failed")
		}
	}
}
