package filtrify_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

// The join verification below is v1's verifyJoin from transformer_lookup_test.go,
// retyped against operatorv2.LookupConfiguration. It still works over
// *types.DataSet - what rowsOf returns - so the body is the v1 one unchanged,
// and so are its helpers findFirstRowWithCriteria and shouldColumnExistInJoin,
// reused from the v1 file.

func isRightMatchColumnv2(colName string, config *operatorv2.LookupConfiguration) bool {
	for _, c := range config.Columns {
		if colName == c.Right {
			return true
		}
	}
	return false
}

func shouldColumnExistInJoinv2(config *operatorv2.LookupConfiguration, columnName string) bool {
	shouldColumnExistOnJoinedTable := true
	// not should this column exist or not?
	if len(config.SelectedColumns) > 0 {
		shouldColumnExistOnJoinedTable = false
		for _, sc := range config.SelectedColumns {
			if strings.EqualFold(sc, columnName) {
				shouldColumnExistOnJoinedTable = true
				break
			}
		}
	}
	return shouldColumnExistOnJoinedTable
}

func verifyJoinv2(t *testing.T, left *types.DataSet, right *types.DataSet, joined *types.DataSet, config *operatorv2.LookupConfiguration) {

	leftRefRow := left.Rows[0]
	rightRefRow := right.Rows[0]
	for _, jr := range joined.Rows {
		// there should exist rows from left exactly
		for _, lc := range leftRefRow.Columns {
			col := test.GetColumn(jr, lc.ColumnName)
			assert.NotNil(t, col, fmt.Sprintf("%s column was not found", lc.ColumnName))
		}

		if !config.RemoveRightMatchColumn {
			// there should exist rows from right with a prefix
			for _, rc := range rightRefRow.Columns {
				expectedColName := fmt.Sprintf("%s.%s", config.TargetDataset, rc.ColumnName)
				if config.RemoveRightDatasetPrefix {
					expectedColName = rc.ColumnName
				}
				col := test.GetColumn(jr, expectedColName)
				shouldColumnExistOnJoinedTable := shouldColumnExistInJoinv2(config, rc.ColumnName)
				if shouldColumnExistOnJoinedTable {
					assert.NotNil(t, col, fmt.Sprintf("%s column was not found", expectedColName))
				} else {
					assert.Nil(t, col, fmt.Sprintf("%s column was not filtered properly", expectedColName))
				}
			}

			// now let's see if merged keys were correct
			for _, col := range config.Columns {
				shouldColumnExistOnJoinedTable := shouldColumnExistInJoinv2(config, col.Right)
				if !shouldColumnExistOnJoinedTable {
					continue
				}

				lCol := test.GetColumn(jr, col.Left)
				assert.NotNil(t, lCol, fmt.Sprintf("%s column was not found", col.Left))

				expectedRightColName := fmt.Sprintf("%s.%s", config.TargetDataset, col.Right)
				if config.RemoveRightDatasetPrefix {
					expectedRightColName = col.Right
				}

				rCol := test.GetColumn(jr, expectedRightColName)
				assert.NotNil(t, rCol, fmt.Sprintf("%s column was not found", expectedRightColName))

				// now we need to make sure they are equal
				if lCol == nil || lCol.CellValue.DataType == types.NilType ||
					rCol == nil || rCol.CellValue.DataType == types.NilType {
					continue
				}

				if !lCol.CellValue.Equals(rCol.CellValue) {
					assert.Fail(t, "joined columns don't have same values")
				}
			}
		}

		// we need to make sure that matched row on right is the first row that fulfills match condition
		colsToSearchOnRight := []*types.DataColumn{}
		for _, col := range config.Columns {
			lCol := test.GetColumn(jr, col.Left)
			newCol := test.CopyColumn(lCol)
			newCol.ColumnName = col.Right
			colsToSearchOnRight = append(colsToSearchOnRight, newCol)
		}
		matchedRowOnRight := findFirstRowWithCriteria(right, colsToSearchOnRight)
		if matchedRowOnRight == nil {
			// no need to verify this
			continue
		}
		// let's make sure that all values are same
		// let's create a temporary copy row
		targetLength := len(matchedRowOnRight.Columns)
		if config.RemoveRightMatchColumn {
			targetLength -= len(config.Columns)
		}
		copyRow := &types.DataRow{
			Columns: make([]*types.DataColumn, targetLength),
		}
		colCounter := 0
		for _, c := range matchedRowOnRight.Columns {
			if config.RemoveRightMatchColumn && isRightMatchColumnv2(c.ColumnName, config) {
				// let's skip this
				continue
			}
			copyRow.Columns[colCounter] = test.CopyColumn(c)
			expectedColName := fmt.Sprintf("%s.%s", config.TargetDataset, c.ColumnName)
			if config.RemoveRightDatasetPrefix {
				expectedColName = c.ColumnName
			}
			copyRow.Columns[colCounter].ColumnName = expectedColName
			colCounter++
		}

		// now we have to make sure that joined row is exactly same as copy row in every value
		for _, c := range copyRow.Columns {
			shouldColumnExistOnJoinedTable := shouldColumnExistInJoinv2(config, c.ColumnName)
			if !shouldColumnExistOnJoinedTable {
				continue
			}
			leftCol := test.GetColumn(jr, c.ColumnName)
			if leftCol.CellValue.DataType == types.NilType || c.CellValue.DataType == types.NilType {
				// no need to verify this
				continue
			}
			if !c.CellValue.Equals(leftCol.CellValue) {
				assert.Fail(t, "joined column values are not same. probably join didn't match to first row")
			}
		}

	}
}

func TestBasicLookupv2(t *testing.T) {
	lookupDatav2, err := filtrify.ConvertToTypedDatav2(test.UATLookupTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	instrumentSetv2, err := filtrify.ConvertToTypedDatav2(test.UATLookupJoinTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operatorv2.JoinColumn{
			{
				Left:  "Instrument ID",
				Right: "Instrument ID",
			},
		},
		RemoveRightMatchColumn:   false,
		RemoveRightDatasetPrefix: false,
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Lookup,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{
		"Instrument Data": instrumentSetv2,
	}
	joinedDatav2, err := filtrify.Transformv2(lookupDatav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	lookupData := rowsOf(t, lookupDatav2)
	instrumentSet := rowsOf(t, instrumentSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	assert.Len(t, joinedData.Rows, len(lookupData.Rows), "join failed. invalid number of rows")

	// now we need to make sure join was successful
	verifyJoinv2(t, lookupData, instrumentSet, joinedData, conf)
}

func TestBasicLookupv2WithSelectedColumns(t *testing.T) {
	lookupDatav2, err := filtrify.ConvertToTypedDatav2(test.UATLookupTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	instrumentSetv2, err := filtrify.ConvertToTypedDatav2(test.UATLookupJoinTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operatorv2.JoinColumn{
			{
				Left:  "Instrument ID",
				Right: "Instrument ID",
			},
		},
		SelectedColumns:          []string{"Instrument name", "Currency"},
		RemoveRightMatchColumn:   false,
		RemoveRightDatasetPrefix: false,
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Lookup,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{
		"Instrument Data": instrumentSetv2,
	}
	joinedDatav2, err := filtrify.Transformv2(lookupDatav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	lookupData := rowsOf(t, lookupDatav2)
	instrumentSet := rowsOf(t, instrumentSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	assert.Len(t, joinedData.Rows, len(lookupData.Rows), "join failed. invalid number of rows")

	// now we need to make sure join was successful
	verifyJoinv2(t, lookupData, instrumentSet, joinedData, conf)
}

func TestMultiMatchLookupv2(t *testing.T) {
	lookupDatav2, err := filtrify.ConvertToTypedDatav2(test.UATLookupTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	instrumentSetv2, err := filtrify.ConvertToTypedDatav2(test.UATLookupJoinTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operatorv2.JoinColumn{
			{
				Left:  "ISIN",
				Right: "ISIN",
			},
		},
		RemoveRightMatchColumn:   false,
		RemoveRightDatasetPrefix: false,
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Lookup,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{
		"Instrument Data": instrumentSetv2,
	}
	joinedDatav2, err := filtrify.Transformv2(lookupDatav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	lookupData := rowsOf(t, lookupDatav2)
	instrumentSet := rowsOf(t, instrumentSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	assert.Len(t, joinedData.Rows, len(lookupData.Rows), "join failed. invalid number of rows")

	// now we need to make sure join was successful
	verifyJoinv2(t, lookupData, instrumentSet, joinedData, conf)

}

func TestMultiConditionsLookupv2(t *testing.T) {
	lookupDatav2, err := filtrify.ConvertToTypedDatav2(test.UATLookupTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	instrumentSetv2, err := filtrify.ConvertToTypedDatav2(test.UATLookupJoinTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operatorv2.JoinColumn{
			{
				Left:  "ISIN",
				Right: "ISIN",
			},
			{
				Left:  "Currency",
				Right: "Currency",
			},
		},
		RemoveRightMatchColumn:   false,
		RemoveRightDatasetPrefix: false,
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Lookup,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSetv2{
		"Instrument Data": instrumentSetv2,
	}
	joinedDatav2, err := filtrify.Transformv2(lookupDatav2, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	lookupData := rowsOf(t, lookupDatav2)
	instrumentSet := rowsOf(t, instrumentSetv2)
	joinedData := rowsOf(t, joinedDatav2)

	assert.Len(t, joinedData.Rows, len(lookupData.Rows), "join failed. invalid number of rows")

	// now we need to make sure join was successful
	verifyJoinv2(t, lookupData, instrumentSet, joinedData, conf)
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestBasicLookupWithRowKey is TestBasicLookupv2.
func TestBasicLookupv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
