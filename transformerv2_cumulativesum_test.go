package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicCumulativeSumv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.CumulativeSumConfiguration{
		Column:        "Quantity",
		NewColumnName: "Test Column",
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}

	newColStep1 := &types.TransformationStep{
		Operator:      types.CumulativeSum,
		Configuration: string(b1),
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, rowsOf(t, newDatav2).Rows, int(ds.NumRows()), "Basic cumulative sum operation failed. invalid number of rows")

	newData := rowsOf(t, newDatav2)
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

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestBasicCumulativeSumWithRowKey is
// TestBasicCumulativeSumv2.
func TestBasicCumulativeSumv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
