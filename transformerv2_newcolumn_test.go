package filtrify_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Instrument Type` AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "column was not found")
		assert.Equal(t, instTypeCol.CellValue.DataType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, instTypeCol.CellValue.StringValue, newCol.CellValue.StringValue, "new column wasn't copied properly")
	}
}

func TestBasicNewColumnv2WithInvalidConfiguration(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "CONCAT(COL(\"Account Name (Short)\"),\"_\",COL(\"ISIN\"),\"_\",COL(\"Coupon Rate\"),\"_\",COL(\"Ex-Date\")))"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	_, err = filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	assert.NotNil(t, err, "newColumn operation should have failed")
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestBasicNewColumnWithRowKey is TestBasicNewColumnv2.
func TestBasicNewColumnv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}

func TestMathematicalNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Quantity`+1 AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		quantityCol := test.GetColumn(r, "Quantity")
		assert.NotNil(t, quantityCol, "column was not found")
		assert.Equal(t, quantityCol.CellValue.DataType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, quantityCol.CellValue.DoubleValue+1, newCol.CellValue.DoubleValue, "new column wasn't copied properly")
	}
}

func TestCombiningNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	changeColumnType2 := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Exposure %":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType, &changeColumnType2}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Quantity`*`Exposure %` AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		quantityCol := test.GetColumn(r, "Quantity")
		assert.NotNil(t, quantityCol, "column was not found")
		exposureCol := test.GetColumn(r, "Exposure %")
		assert.NotNil(t, exposureCol, "column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, exposureCol.CellValue.DoubleValue*quantityCol.CellValue.DoubleValue, newCol.CellValue.DoubleValue, "new column wasn't copied properly")
	}

}

func TestAggSumNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "SUMX(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(21255000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestAggAvgNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "average(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(4251000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestAggWeightedAvgNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "weighted_average(`Market Value (Base)`, `Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(7343778.68), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestFirstNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	s1 := "first(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(2000000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestLastNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "last(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(5000000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestMaxColNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MAXCOL(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(8750000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestMinColNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MINCOL(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(-495000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestManualStringConcatNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	// TODO think about this?
	// the interface possibly needs to change
	s1 := "CONCAT(`Instrument name`, '<', `Instrument Type`, '>', '') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "Instrument Type was not found")
		expected := instNameCol.CellValue.StringValue + "<" + instTypeCol.CellValue.StringValue + ">"
		assert.Equal(t, expected, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestManualStringNonTextConcatNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	// TODO think about this?
	// the interface possibly needs to change
	s1 := "CONCAT(`Instrument name`, '<S=', `EU Sanction listed`, '>', '') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		euSancListCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, euSancListCol, "EU Sanction listed was not found")
		boolText := ""
		if euSancListCol.CellValue.DataType == types.BoolType {
			if euSancListCol.CellValue.BoolValue {
				boolText = "true"
			} else {
				boolText = "false"
			}
		}
		expected := instNameCol.CellValue.StringValue + "<S=" + boolText + ">"
		assert.Equal(t, expected, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestLeftNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "LEFT(`Instrument name`, 6) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		expectedTestCol := instNameCol.CellValue.StringValue
		if len(expectedTestCol) > 6 {
			expectedTestCol = expectedTestCol[:6]
		}

		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestRightNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "RIGHT(`Instrument name`, 3) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		expectedTestCol := instNameCol.CellValue.StringValue
		if len(expectedTestCol) > 3 {
			expectedTestCol = expectedTestCol[len(expectedTestCol)-3:]
		}

		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestLeftRightNestedNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "RIGHT(LEFT(`Instrument name`, 6), 4) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")

		expectedTestCol := instNameCol.CellValue.StringValue
		if len(expectedTestCol) > 6 {
			expectedTestCol = expectedTestCol[:6]
		}
		if len(expectedTestCol) > 4 {
			expectedTestCol = expectedTestCol[len(expectedTestCol)-4:]
		}

		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestSplitNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "SPLIT(`Instrument name`, ' ', 1) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		expectedTestCol := strings.Split(instNameCol.CellValue.StringValue, " ")[0]
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestSimpleIFNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity', 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "Instrument Type was not found")
		expectedTestCol := "Yes"
		if instTypeCol.CellValue.StringValue != "Equity" {
			expectedTestCol = "No"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestIFWithConditionNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity' && LEFT(`Instrument name`, 4) == 'ERIC', 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "Instrument Type was not found")
		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")

		instNameLeft := instNameCol.CellValue.StringValue
		if len(instNameLeft) > 4 {
			instNameLeft = instNameLeft[:4]
		}

		expectedTestCol := "No"
		if instTypeCol.CellValue.StringValue == "Equity" && instNameLeft == "ERIC" {
			expectedTestCol = "Yes"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestIFWithMultipleConditionsNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	// TODO think about this eval thing
	s1 := "IFEL(`Instrument Type` == 'Equity' || EVAL(`Active From` < '2021-01-01' && `EU Sanction listed` == false), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "Instrument Type was not found")
		euSanctionListedCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, euSanctionListedCol, "EU Sanction listed was not found")

		activeFromCol := test.GetColumn(r, "Active From")
		assert.NotNil(t, activeFromCol, "Active From was not found")

		targetDate, _ := time.Parse("2006-01-02", "2021-01-01")

		expectedTestCol := "No"
		if instTypeCol.CellValue.StringValue == "Equity" {
			expectedTestCol = "Yes"
		} else if activeFromCol.CellValue.TimestampValue.Before(targetDate) &&
			euSanctionListedCol.CellValue.DataType != types.NilType &&
			euSanctionListedCol.CellValue.BoolValue == false {
			expectedTestCol = "Yes"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestIFContainsNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(CONTAINSX(`Instrument name`, 'ERIC'), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")

		expectedTestCol := "No"
		if strings.Contains(instNameCol.CellValue.StringValue, "ERIC") {
			expectedTestCol = "Yes"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestIFNotContainsNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(NOTCONTAINSX(`Instrument name`, 'ERIC'), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")

		expectedTestCol := "Yes"
		if strings.Contains(instNameCol.CellValue.StringValue, "ERIC") {
			expectedTestCol = "No"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestNestedIFNewColumnv2(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity', 'Yes', IFEL(`Exposure %` > 0.25, 'Yes', 'No')) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "Instrument Type was not found")

		exposureCol := test.GetColumn(r, "Exposure %")
		assert.NotNil(t, exposureCol, "Exposure % was not found")
		expectedTestCol := "No"
		if instTypeCol.CellValue.StringValue == "Equity" || exposureCol.CellValue.DoubleValue > 0.25 {
			expectedTestCol = "Yes"
		}
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestNewColumnv2PlusOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "PLUS(3, 5) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(8), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}

}

func TestNewColumnv2TrimOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "TRIM(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.StringType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.True(t, newCol.CellValue.StringValue[0] != ' ', "new column wasn't processed correctly")
	}
}

func TestNewColumnv2LengthOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "LENGTH(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		instCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.LongType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, int64(len(instCol.CellValue.StringValue)), newCol.CellValue.LongValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2AndOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`EU Sanction listed` AND true AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		origCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.BoolType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, origCol.CellValue.BoolValue && true, newCol.CellValue.BoolValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2OROperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`EU Sanction listed` OR true AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		origCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.BoolType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, origCol.CellValue.BoolValue || true, newCol.CellValue.BoolValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2PlusDaysOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "PLUSDAYS(`Active From`, 3) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		instCol := test.GetColumn(r, "Active From")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimestampType, newCol.CellValue.DataType, "new column processed incorrectly")
		expectedTime := instCol.CellValue.TimestampValue.AddDate(0, 0, 3)
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2DoubleMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(2), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2DoubleWithNilMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(2.30), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2StringMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.StringType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, "c", newCol.CellValue.StringValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2TimeMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Active From`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2020-05-01 12:00:00
		expectedTime := time.Date(2020, 5, 1, 12, 0, 0, 0, time.UTC)
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimestampType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2DateMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Maturity Date`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2021-12-16
		expectedDate, _ := civil.ParseDate("2021-12-16")
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DateType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedDate.In(time.UTC), newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2TimeOnlyMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Hour`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// v1 expects time.Date(0, 0, 0, 10, 0, 0, 0, time.UTC) here: that is the
		// form qlbridge's civil.Time path leaves behind, where a time of day is a
		// time.Time with a zero date. Arrow stores microseconds since midnight and
		// no date at all, so rowsOf rebuilds the date half as year 0 January 1 -
		// the form time.Parse produces - and the clock, which is what the
		// assertion is about, is the same 10:00:00.
		expectedTime := time.Date(0, 1, 1, 10, 0, 0, 0, time.UTC)
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimeOfDayType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnv2LongMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"somedata":{"targetType":1}}}`,
	}
	plainDataConverted, err := filtrify.Transformv2(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	s1 := "MEDIAN(`somedata`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newDatav2, err := filtrify.Transformv2(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	newData := rowsOf(t, newDatav2)
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, int(ds.NumRows()), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2021-12-16
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.LongType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, int64(3), newCol.CellValue.LongValue, "new column wasn't processed correctly")
	}
}

// v1's transformer_newcolumn_batch_test.go covers Transform's NewColumn batching:
// consecutive NewColumn steps are collected and run as a single statement (see
// operator.CollectNewColumnBatch). Transformv2 has no such batching path - it runs
// every step on its own - so these two tests check the same end result, that a
// later step sees the column an earlier one produced, over the v2 pipeline.
func TestNewColumnv2BatchDependencyChain(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
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

	newDatav2, err := filtrify.Transformv2(ds, steps, nil)
	assert.NoError(t, err, "batched newcolumn transform failed")
	newData := rowsOf(t, newDatav2)
	assert.Len(t, newData.Rows, int(ds.NumRows()))

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

func TestNewColumnv2BatchIndependentColumns(t *testing.T) {
	ds, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
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

	newDatav2, err := filtrify.Transformv2(ds, steps, nil)
	assert.NoError(t, err, "batched newcolumn transform failed")

	newData := rowsOf(t, newDatav2)
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

// TestConcurrentNewColumnv2NoSchemaRace is v1's TestConcurrentNewColumnNoSchemaRace
// over Transformv2. The shared state it is about - qlbridge's process-global schema
// registry, which executeSQLQuery registers into and drops from - is the same in
// both generations (operatorv2/common.go), so the race it guards against is too.
// As in v1, this is caught deterministically only under `go test -race`.
func TestConcurrentNewColumnv2NoSchemaRace(t *testing.T) {
	base, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	require.NoError(t, err, "basic data conversion failed")

	step := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"`Instrument Type` AS `Test Column`\"}",
	}

	const goroutines = 64
	const iterations = 30

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*iterations)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Fresh dataset per call so goroutines never share arrow tables —
				// the only shared state under test is the schema registration.
				ds, convErr := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
				if convErr != nil {
					errs <- fmt.Errorf("conversion failed: %w", convErr)
					return
				}
				out, tErr := filtrify.Transformv2(ds, []*types.TransformationStep{step}, nil)
				if tErr != nil {
					errs <- fmt.Errorf("transform failed: %w", tErr)
					return
				}
				if out.NumRows() != base.NumRows() {
					errs <- fmt.Errorf("row count mismatch: got %d want %d", out.NumRows(), base.NumRows())
					return
				}
				derivedCol := out.GetColumn("Test Column")
				sourceCol := out.GetColumn("Instrument Type")
				if derivedCol == nil || sourceCol == nil {
					errs <- fmt.Errorf("derived or source column missing")
					return
				}
				if derivedCol.DataType().ID() != sourceCol.DataType().ID() {
					errs <- fmt.Errorf("derived column wrong type: got %s want %s",
						derivedCol.DataType(), sourceCol.DataType())
					return
				}
				derived := derivedCol.Data().Chunks()
				source := sourceCol.Data().Chunks()
				for c := range derived {
					d := derived[c].(*array.String)
					s := source[c].(*array.String)
					for row := 0; row < d.Len(); row++ {
						if d.IsNull(row) != s.IsNull(row) || d.Value(row) != s.Value(row) {
							errs <- fmt.Errorf("derived column wrong: got %q want %q",
								d.Value(row), s.Value(row))
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		assert.NoError(t, e)
	}
}
