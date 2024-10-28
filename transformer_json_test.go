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

func TestJSON(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name"},
		TargetFieldName: "jsonified",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.JSON,
		Configuration: string(b1),
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "json operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "jsonified")
	instrumentTypeCol := test.GetColumn(data.Rows[0], "Instrument Type")
	instrumentNameCol := test.GetColumn(data.Rows[0], "Instrument name")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "jsonified"))
	assert.Equal(t, types.StringType, firstCol.CellValue.DataType, "json operation failed")
	jsonData := make(map[string]interface{})
	err = json.Unmarshal([]byte(firstCol.CellValue.StringValue), &jsonData)
	assert.Nil(t, err, "json operation failed")

	assert.Equal(t, instrumentTypeCol.CellValue.StringValue, jsonData["Instrument Type"], "json operation failed")
	assert.Equal(t, instrumentNameCol.CellValue.StringValue, jsonData["Instrument name"], "json operation failed")

	instrumentTypeColOnObjectified := test.GetColumn(objectifiedData.Rows[0], "Instrument Type")
	assert.Nil(t, instrumentTypeColOnObjectified, "json operation failed")
}

func TestJSONReplacesExistingColumns(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name"},
		TargetFieldName: "Quantity",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.JSON,
		Configuration: string(b1),
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "json operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "jsonified"))
	assert.Equal(t, types.StringType, firstCol.CellValue.DataType, "json operation failed")
}

func TestJSONWithRowKey(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name"},
		TargetFieldName: "jsonified",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.JSON,
		Configuration: string(b1),
	}

	for i := range data.Rows {
		key := fmt.Sprintf("row-%d", i)
		data.Rows[i].Key = &key
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "json operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "jsonified")
	instrumentTypeCol := test.GetColumn(data.Rows[0], "Instrument Type")
	instrumentNameCol := test.GetColumn(data.Rows[0], "Instrument name")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "jsonified"))
	assert.Equal(t, types.StringType, firstCol.CellValue.DataType, "json operation failed")
	jsonData := make(map[string]interface{})
	err = json.Unmarshal([]byte(firstCol.CellValue.StringValue), &jsonData)
	assert.Nil(t, err, "json operation failed")

	assert.Equal(t, instrumentTypeCol.CellValue.StringValue, jsonData["Instrument Type"], "json operation failed")
	assert.Equal(t, instrumentNameCol.CellValue.StringValue, jsonData["Instrument name"], "json operation failed")

	instrumentTypeColOnObjectified := test.GetColumn(objectifiedData.Rows[0], "Instrument Type")
	assert.Nil(t, instrumentTypeColOnObjectified, "json operation failed")

	for _, r := range objectifiedData.Rows {
		assert.NotNil(t, r.Key, "Key assignment failed on cumulative sum operator")
	}
}
