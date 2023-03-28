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

var TestData [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175,000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1,500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9,000,000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5,000,000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

func TestChangeColumnType(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ChangeColumnTypeConfiguration{
		Columns: map[string]operator.ConversionConfiguration{
			"Quantity": {
				TargetType: types.StringType,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: string(b1),
	}

	firstCol := test.GetColumn(data.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion failed")
	}
}

func TestChangeColumnType2(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(TestData, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ChangeColumnTypeConfiguration{
		Columns: map[string]operator.ConversionConfiguration{
			"Quantity": {
				TargetType: types.DoubleType,
				StringNumeric: &operator.StringNumericConfiguration{
					DecimalSymbol:     ".",
					ThousandSeperator: ",",
				},
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: string(b1),
	}

	firstCol := test.GetColumn(data.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion failed")
	}
}
