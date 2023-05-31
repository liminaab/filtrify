package filtrify_test

import (
	"encoding/json"
	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasicCumulativeSum(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.CumulativeSumConfiguration{
		Column:        "Quantity",
		NewColumnName: "Test Column",
	}
	b1, err := json.Marshal(conf)

	newColStep1 := &types.TransformationStep{
		Operator:      types.CumulativeSum,
		Configuration: string(b1),
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic cumulative sum operation failed. invalid number of rows")

	expectedSum := 0.0
	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		instTypeCol := test.GetColumn(r, "Quantity")
		assert.NotNil(t, instTypeCol, "column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column wasn't copied")
		expectedSum += instTypeCol.CellValue.DoubleValue
		assert.Equal(t, expectedSum, newCol.CellValue.DoubleValue, "new column wasn't copied properly")
	}

}
