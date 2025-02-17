package filtrify_test

import (
	"cloud.google.com/go/civil"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Instrument Type` AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "column was not found")
		assert.Equal(t, instTypeCol.CellValue.DataType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, instTypeCol.CellValue.StringValue, newCol.CellValue.StringValue, "new column wasn't copied properly")
	}
}

func TestBasicNewColumnWithInvalidConfiguration(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "CONCAT(COL(\"Account Name (Short)\"),\"_\",COL(\"ISIN\"),\"_\",COL(\"Coupon Rate\"),\"_\",COL(\"Ex-Date\")))"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}
	_, err = filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	assert.NotNil(t, err, "newColumn operation should have failed")
}

func TestBasicNewColumnWithRowKey(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Instrument Type` AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	for i := range ds.Rows {
		key := fmt.Sprintf("row-%d", i)
		ds.Rows[i].Key = &key
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		instTypeCol := test.GetColumn(r, "Instrument Type")
		assert.NotNil(t, instTypeCol, "column was not found")
		assert.Equal(t, instTypeCol.CellValue.DataType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, instTypeCol.CellValue.StringValue, newCol.CellValue.StringValue, "new column wasn't copied properly")
	}

	for _, r := range newData.Rows {
		assert.NotNil(t, r.Key, "Key assignment failed on newColumn operator")
	}
}

func TestMathematicalNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Quantity`+1 AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	for i := range plainDataConverted.Rows {
		key := fmt.Sprintf("row-%d", i)
		plainDataConverted.Rows[i].Key = &key
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		quantityCol := test.GetColumn(r, "Quantity")
		assert.NotNil(t, quantityCol, "column was not found")
		assert.Equal(t, quantityCol.CellValue.DataType, newCol.CellValue.DataType, "new column wasn't copied")
		assert.Equal(t, quantityCol.CellValue.DoubleValue+1, newCol.CellValue.DoubleValue, "new column wasn't copied properly")
	}

	for _, r := range newData.Rows {
		assert.NotNil(t, r.Key, "Key assignment failed on newColumn operator")
	}

}

func TestCombiningNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
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
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType, &changeColumnType2}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`Quantity`*`Exposure %` AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "filter operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

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

func TestAggSumNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "SUMX(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(21255000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestAggAvgNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "average(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(4251000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestAggWeightedAvgNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "weighted_average(`Market Value (Base)`, `Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(7343778.68), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestFirstNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	s1 := "first(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(2000000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestLastNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Market Value (Base)":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "last(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(5000000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestMaxColNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MAXCOL(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(8750000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestMinColNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MINCOL(`Market Value (Base)`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		sumTestCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, sumTestCol, "test column was not found")
		assert.Equal(t, float64(-495000.00), sumTestCol.CellValue.DoubleValue, "new column wasn't calculated properly")

	}
}

func TestManualStringConcatNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
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

func TestManualStringNonTextConcatNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
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

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column aggregation operation failed. invalid number of rows")
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

func TestLeftNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "LEFT(`Instrument name`, 6) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestRightNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "RIGHT(`Instrument name`, 3) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestLeftRightNestedNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "RIGHT(LEFT(`Instrument name`, 6), 4) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestSplitNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "SPLIT(`Instrument name`, ' ', 1) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
	for _, r := range newData.Rows {
		testCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, testCol, "test column was not found")

		instNameCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, instNameCol, "Instrument name was not found")
		expectedTestCol := strings.Split(instNameCol.CellValue.StringValue, " ")[0]
		assert.Equal(t, expectedTestCol, testCol.CellValue.StringValue, "new column wasn't calculated properly")
	}
}

func TestSimpleIFNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity', 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestIFWithConditionNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity' && LEFT(`Instrument name`, 4) == 'ERIC', 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestIFWithMultipleConditionsNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	// TODO think about this eval thing
	s1 := "IFEL(`Instrument Type` == 'Equity' || EVAL(`Active From` < '2021-01-01' && `EU Sanction listed` == false), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestIFContainsNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(CONTAINSX(`Instrument name`, 'ERIC'), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestIFNotContainsNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(NOTCONTAINSX(`Instrument name`, 'ERIC'), 'Yes', 'No') AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestNestedIFNewColumn(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "IFEL(`Instrument Type` == 'Equity', 'Yes', IFEL(`Exposure %` > 0.25, 'Yes', 'No')) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column left operation failed. invalid number of rows")
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

func TestNewColumnPlusOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "PLUS(3, 5) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(8), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}

}

func TestNewColumnTrimOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "TRIM(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.StringType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.True(t, newCol.CellValue.StringValue[0] != ' ', "new column wasn't processed correctly")
	}
}

func TestNewColumnLengthOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "LENGTH(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		instCol := test.GetColumn(r, "Instrument name")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.LongType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, int64(len(instCol.CellValue.StringValue)), newCol.CellValue.LongValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnAndOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`EU Sanction listed` AND true AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		origCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.BoolType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, origCol.CellValue.BoolValue && true, newCol.CellValue.BoolValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnOROperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "`EU Sanction listed` OR true AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		origCol := test.GetColumn(r, "EU Sanction listed")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.BoolType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, origCol.CellValue.BoolValue || true, newCol.CellValue.BoolValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnPlusDaysOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "PLUSDAYS(`Active From`, 3) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		instCol := test.GetColumn(r, "Active From")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimestampType, newCol.CellValue.DataType, "new column processed incorrectly")
		expectedTime := instCol.CellValue.TimestampValue.AddDate(0, 0, 3)
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnDoubleMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT2TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(2), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnDoubleWithNilMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Quantity`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DoubleType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, float64(2.30), newCol.CellValue.DoubleValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnStringMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Instrument name`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.StringType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, "c", newCol.CellValue.StringValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnTimeMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Active From`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2020-05-01 12:00:00
		expectedTime := time.Date(2020, 5, 1, 12, 0, 0, 0, time.UTC)
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimestampType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnDateMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Maturity Date`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2021-12-16
		expectedDate, _ := civil.ParseDate("2021-12-16")
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.DateType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedDate.In(time.UTC), newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnTimeOnlyMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	s1 := "MEDIAN(`Hour`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(ds, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2021-12-16
		expectedTime := time.Date(0, 0, 0, 10, 0, 0, 0, time.UTC)
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.TimeOfDayType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, expectedTime, newCol.CellValue.TimestampValue, "new column wasn't processed correctly")
	}
}

func TestNewColumnLongMedianOperator(t *testing.T) {
	ds, err := filtrify.ConvertToTypedData(test.UAT3TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"somedata":{"targetType":1}}}`,
	}
	plainDataConverted, err := filtrify.Transform(ds, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	s1 := "MEDIAN(`somedata`) AS `Test Column`"

	newColStep1 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s1 + "\"}",
	}

	newData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{newColStep1}, nil)
	if err != nil {
		assert.NoError(t, err, "newColumn operation failed")
	}
	// one header - 2 for filtered out rows
	assert.Len(t, newData.Rows, len(ds.Rows), "Basic new column operation failed. invalid number of rows")

	for _, r := range newData.Rows {
		// 2021-12-16
		newCol := test.GetColumn(r, "Test Column")
		assert.NotNil(t, newCol, "test column was not found")
		assert.Equal(t, types.LongType, newCol.CellValue.DataType, "new column processed incorrectly")
		assert.Equal(t, int64(3), newCol.CellValue.LongValue, "new column wasn't processed correctly")
	}
}
