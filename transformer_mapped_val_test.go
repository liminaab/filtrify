package dyntransformer_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/test"
	"limina.com/dyntransformer/types"
)

func TestBasicMappedValue(t *testing.T) {
	data, err := dyntransformer.ConvertToTypedData(test.UATMappedValueTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	mappedSet, err := dyntransformer.ConvertToTypedData(test.UATMappedValueMapTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.MappedValueConfiguration{
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

	joinSet := map[string]*types.DataSet{
		"Broker Mapped": mappedSet,
	}
	joinedData, err := dyntransformer.Transform(data, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

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
			if test.HasSameValues(keyCol.CellValue, brokerIDCol.CellValue) &&
				test.HasSameValues(valCol.CellValue, brokerMappedCol.CellValue) {
				isFound = true
				break
			}
		}
		if !isFound {
			assert.Fail(t, "mapped value was not found in mapped table. invalid map join")
		}
	}
}
