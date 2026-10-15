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

func TestBasicMappedValuev2(t *testing.T) {
	datav2, err := filtrify.ConvertToTypedDatav2(test.UATMappedValueTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	mappedSetv2, err := filtrify.ConvertToTypedDatav2(test.UATMappedValueMapTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.MappedValueConfiguration{
		TargetDataset:    "Broker Mapped",
		MappedColumnName: "Broker ID",
		NewColumnName:    "Broker Mapped",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.MappedValue,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{
		"Broker Mapped": mappedSetv2,
	}
	joinedDatav2, err := filtrify.Transformv2(datav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	data := rowsOf(t, datav2)
	mappedSet := rowsOf(t, mappedSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	for _, r := range joinedData.Rows {
		lastCol := r.Columns[len(r.Columns)-1]
		assert.Equal(t, conf.NewColumnName, lastCol.ColumnName, "invalid mapping")
	}
	assert.Len(t, joinedData.Rows, len(data.Rows), "join failed. invalid number of rows")

	// let's verify the join
	for _, r := range joinedData.Rows {
		brokerIDCol := test.GetColumn(r, "Broker ID")
		assert.NotNil(t, brokerIDCol, fmt.Sprintf("%s column was not found", "Broker ID"))
		brokerMappedCol := test.GetColumn(r, "Broker Mapped")
		assert.NotNil(t, brokerMappedCol, fmt.Sprintf("%s column was not found", "Broker Mapped"))

		// now we have to make sure that this pair exists in our mapped value table
		if brokerIDCol.CellValue.DataType == types.NilType {
			assert.Equal(t, types.NilType, brokerIDCol.CellValue.DataType, "invalid nil join")
			continue
		}
		// let's find this pair in right table
		isFound := false
		for _, rr := range mappedSet.Rows {
			keyCol := test.GetColumn(rr, "Key")
			valCol := test.GetColumn(rr, "Value")
			if keyCol.CellValue.Equals(brokerIDCol.CellValue) &&
				valCol.CellValue.Equals(brokerMappedCol.CellValue) {
				isFound = true
				break
			}
		}
		if !isFound {
			assert.Fail(t, "mapped value was not found in mapped table. invalid map join")
		}
	}
}

func TestEmbeddedDataMappedValuev2(t *testing.T) {
	datav2, err := filtrify.ConvertToTypedDatav2(test.UATMappedValueTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	mappedSetv2, err := filtrify.ConvertToTypedDatav2(test.UATMappedValueMapTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.MappedValueConfiguration{
		TargetDataset:    "",
		TargetData:       test.UATMappedValueMapEmbeddedTestDataFormatted,
		MappedColumnName: "Broker ID",
		NewColumnName:    "Broker Mapped",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.MappedValue,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{}
	joinedDatav2, err := filtrify.Transformv2(datav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	data := rowsOf(t, datav2)
	mappedSet := rowsOf(t, mappedSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	for _, r := range joinedData.Rows {
		lastCol := r.Columns[len(r.Columns)-1]
		assert.Equal(t, conf.NewColumnName, lastCol.ColumnName, "invalid mapping")
	}
	assert.Len(t, joinedData.Rows, len(data.Rows), "join failed. invalid number of rows")

	// let's verify the join
	for _, r := range joinedData.Rows {
		brokerIDCol := test.GetColumn(r, "Broker ID")
		assert.NotNil(t, brokerIDCol, fmt.Sprintf("%s column was not found", "Broker ID"))
		brokerMappedCol := test.GetColumn(r, "Broker Mapped")
		assert.NotNil(t, brokerMappedCol, fmt.Sprintf("%s column was not found", "Broker Mapped"))

		// now we have to make sure that this pair exists in our mapped value table
		if brokerIDCol.CellValue.DataType == types.NilType {
			assert.Equal(t, types.NilType, brokerIDCol.CellValue.DataType, "invalid nil join")
			continue
		}
		// let's find this pair in right table
		isFound := false
		for _, rr := range mappedSet.Rows {
			keyCol := test.GetColumn(rr, "Key")
			valCol := test.GetColumn(rr, "Value")
			if keyCol.CellValue.Equals(brokerIDCol.CellValue) &&
				valCol.CellValue.Equals(brokerMappedCol.CellValue) {
				isFound = true
				break
			}
		}
		if !isFound {
			assert.Fail(t, "mapped value was not found in mapped table. invalid map join")
		}
	}
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestBasicMappedValueWithRowKey is
// TestBasicMappedValuev2.
func TestBasicMappedValuev2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
