package dyntransformer_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/types"
)

func TestBasicNewColumn(t *testing.T) {
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
