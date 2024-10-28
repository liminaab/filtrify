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

func TestObjectify(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name"},
		TargetFieldName: "objectified",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Objectify,
		Configuration: string(b1),
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "objectify operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "objectified")
	instrumentTypeCol := test.GetColumn(data.Rows[0], "Instrument Type")
	instrumentNameCol := test.GetColumn(data.Rows[0], "Instrument name")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "objectified"))
	assert.Equal(t, types.ObjectType, firstCol.CellValue.DataType, "objectify operation failed")
	assert.Equal(t, instrumentTypeCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument Type"], "objectify operation failed")
	assert.Equal(t, instrumentNameCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument name"], "objectify operation failed")

	instrumentTypeColOnObjectified := test.GetColumn(objectifiedData.Rows[0], "Instrument Type")
	assert.Nil(t, instrumentTypeColOnObjectified, "objectify operation failed")
}

func TestObjectifyWithRowKey(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name"},
		TargetFieldName: "objectified",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Objectify,
		Configuration: string(b1),
	}

	for i := range data.Rows {
		key := fmt.Sprintf("row-%d", i)
		data.Rows[i].Key = &key
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "objectify operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "objectified")
	instrumentTypeCol := test.GetColumn(data.Rows[0], "Instrument Type")
	instrumentNameCol := test.GetColumn(data.Rows[0], "Instrument name")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "objectified"))
	assert.Equal(t, types.ObjectType, firstCol.CellValue.DataType, "objectify operation failed")
	assert.Equal(t, instrumentTypeCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument Type"], "objectify operation failed")
	assert.Equal(t, instrumentNameCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument name"], "objectify operation failed")

	instrumentTypeColOnObjectified := test.GetColumn(objectifiedData.Rows[0], "Instrument Type")
	assert.Nil(t, instrumentTypeColOnObjectified, "objectify operation failed")

	for _, r := range objectifiedData.Rows {
		assert.NotNil(t, r.Key, "Key assignment failed on objectify operator")
	}
}

func TestObjectifySilentlySkipsUnknownColumns(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name", "asdasdasdasd"},
		TargetFieldName: "objectified",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Objectify,
		Configuration: string(b1),
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "objectify operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "objectified")
	instrumentTypeCol := test.GetColumn(data.Rows[0], "Instrument Type")
	instrumentNameCol := test.GetColumn(data.Rows[0], "Instrument name")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "objectified"))
	assert.Equal(t, types.ObjectType, firstCol.CellValue.DataType, "objectify operation failed")
	assert.Equal(t, 2, len(firstCol.CellValue.ObjectValue), "objectify operation failed")
	assert.Equal(t, instrumentTypeCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument Type"], "objectify operation failed")
	assert.Equal(t, instrumentNameCol.CellValue.StringValue, firstCol.CellValue.ObjectValue["Instrument name"], "objectify operation failed")
}

func TestObjectifyReplacesExistingColumns(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ObjectifyConfiguration{
		Fields:          []string{"Instrument Type", "Instrument name", "asdasdasdasd"},
		TargetFieldName: "Quantity",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Objectify,
		Configuration: string(b1),
	}

	objectifiedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "objectify operation failed")
	}

	firstCol := test.GetColumn(objectifiedData.Rows[0], "Quantity")
	assert.Equal(t, types.ObjectType, firstCol.CellValue.DataType, "objectify operation failed")
	assert.Equal(t, 2, len(firstCol.CellValue.ObjectValue), "objectify operation failed")
}
