package dyntransformer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/test"
	"limina.com/dyntransformer/types"
)

func TestBasicAggNewColumn(t *testing.T) {
	ds, err := dyntransformer.ConvertToTypedData(test.UATAggregateTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	newColStep1 := &types.TransformationStep{
		Step:          0,
		Enabled:       true,
		Operator:      types.NewColumn,
		Configuration: "{\"groupby\": \"EU Sanction listed\"}",
	}

	newData, err := dyntransformer.Transform(ds, []*types.TransformationStep{newColStep1})
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, 2, "Basic new column left operation failed. invalid number of rows")
}
