package filtrify_test

import (
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

// TestNewColumnBatchDependencyChain mirrors project 202's pattern: a later NewColumn
// step (Doubled) references a column produced by an earlier NewColumn step in the same
// run (Base). If steps in a batch are (incorrectly) evaluated against the original row
// instead of chained, "Doubled" would come out based on stale/missing data instead of
// twice "Base".
func TestNewColumnBatchDependencyChain(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	steps := []*types.TransformationStep{
		{
			Operator:      types.NewColumn,
			Configuration: "{\"statement\": \"MULTIPLY(`Quantity`, 2) AS `Base`\"}",
		},
		{
			Operator:      types.NewColumn,
			Configuration: "{\"statement\": \"MULTIPLY(`Base`, 2) AS `Doubled`\"}",
		},
		{
			Operator:      types.NewColumn,
			Configuration: "{\"statement\": \"MULTIPLY(`Doubled`, 2) AS `Quadrupled`\"}",
		},
	}

	newData, err := filtrify.Transform(ds, steps, nil)
	assert.NoError(t, err, "batched newcolumn transform failed")
	assert.Len(t, newData.Rows, len(ds.Rows))

	for i, r := range newData.Rows {
		qty := test.GetColumn(r, "Quantity")
		base := test.GetColumn(r, "Base")
		doubled := test.GetColumn(r, "Doubled")
		quad := test.GetColumn(r, "Quadrupled")
		assert.NotNil(t, base, "row %d missing Base", i)
		assert.NotNil(t, doubled, "row %d missing Doubled", i)
		assert.NotNil(t, quad, "row %d missing Quadrupled", i)

		assert.Equal(t, qty.CellValue.DoubleValue*2, base.CellValue.DoubleValue, "row %d Base mismatch", i)
		assert.Equal(t, base.CellValue.DoubleValue*2, doubled.CellValue.DoubleValue, "row %d Doubled must chain off Base, not be stale", i)
		assert.Equal(t, doubled.CellValue.DoubleValue*2, quad.CellValue.DoubleValue, "row %d Quadrupled must chain off Doubled, not be stale", i)
	}
}

// TestNewColumnBatchIndependentColumns verifies plain independent NewColumn steps
// (no cross-references) still all land correctly when merged into a single batch.
func TestNewColumnBatchIndependentColumns(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	steps := []*types.TransformationStep{
		{
			Operator:      types.NewColumn,
			Configuration: "{\"statement\": \"`Instrument Type` AS `Col1`\"}",
		},
		{
			Operator:      types.NewColumn,
			Configuration: "{\"statement\": \"MULTIPLY(`Quantity`, 3) AS `Col2`\"}",
		},
	}

	newData, err := filtrify.Transform(ds, steps, nil)
	assert.NoError(t, err, "batched newcolumn transform failed")

	for i, r := range newData.Rows {
		instType := test.GetColumn(r, "Instrument Type")
		qty := test.GetColumn(r, "Quantity")
		col1 := test.GetColumn(r, "Col1")
		col2 := test.GetColumn(r, "Col2")
		assert.NotNil(t, col1, "row %d missing Col1", i)
		assert.NotNil(t, col2, "row %d missing Col2", i)
		assert.Equal(t, instType.CellValue.StringValue, col1.CellValue.StringValue, "row %d Col1 mismatch", i)
		assert.Equal(t, qty.CellValue.DoubleValue*3, col2.CellValue.DoubleValue, "row %d Col2 mismatch", i)
	}
}
