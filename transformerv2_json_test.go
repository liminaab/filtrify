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

func TestJSONv2(t *testing.T) {
	datav2, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.JSONConfiguration{
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

	objectifiedDatav2, err := filtrify.Transformv2(datav2, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "json operation failed")
	}

	data := rowsOf(t, datav2)
	objectifiedData := rowsOf(t, objectifiedDatav2)

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

func TestJSONv2ReplacesExistingColumns(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.JSONConfiguration{
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

	objectifiedDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "json operation failed")
	}

	objectifiedData := rowsOf(t, objectifiedDatav2)
	firstCol := test.GetColumn(objectifiedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "jsonified"))
	assert.Equal(t, types.StringType, firstCol.CellValue.DataType, "json operation failed")
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestJSONWithRowKey is TestJSONv2.
func TestJSONv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
