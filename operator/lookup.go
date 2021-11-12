package operator

import (
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type LookupOperator struct {
}

type JoinColumn struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type LookupConfiguration struct {
	TargetDataset            string        `json:"targetDataset"`
	Columns                  []*JoinColumn `json:"columns"`
	RemoveRightMatchColumn   bool          `json:"removeRightMatchColumn"`
	RemoveRightDatasetPrefix bool          `json:"removeRightDatasetPrefix"`
}

func (t *LookupOperator) GetColumn(r *types.DataRow, col string) *types.DataColumn {
	for _, c := range r.Columns {
		if c.ColumnName == col {
			return c
		}
	}

	return nil
}

func (t *LookupOperator) createColIndex(ds *types.DataSet) map[*types.DataRow]map[string]*types.DataColumn {
	index := make(map[*types.DataRow]map[string]*types.DataColumn)
	for _, r := range ds.Rows {
		index[r] = make(map[string]*types.DataColumn)
		for _, c := range r.Columns {
			index[r][c.ColumnName] = c
		}
	}

	return index
}

func (t *LookupOperator) isEqual(cell1 *types.CellValue, cell2 *types.CellValue) bool {
	if cell1 == nil || cell2 == nil {
		return false
	}

	if cell1.DataType != cell2.DataType {
		return false
	}

	if cell1.DataType == types.NilType || cell2.DataType == types.NilType {
		return false
	}

	switch cell1.DataType {
	case types.IntType:
		return cell1.IntValue == cell2.IntValue
	case types.LongType:
		return cell1.LongValue == cell2.LongValue
	case types.TimestampType:
		return cell1.TimestampValue.Equal(cell2.TimestampValue)
	case types.StringType:
		return cell1.StringValue == cell2.StringValue
	case types.DoubleType:
		return cell1.DoubleValue == cell2.DoubleValue
	case types.BoolType:
		return cell1.BoolValue == cell2.BoolValue
	}

	return false
}

func (t *LookupOperator) copyColumn(col *types.DataColumn, config *LookupConfiguration) *types.DataColumn {

	cellVal := &types.CellValue{
		DataType: col.CellValue.DataType,
	}
	switch cellVal.DataType {
	case types.IntType:
		cellVal.IntValue = col.CellValue.IntValue
		break
	case types.LongType:
		cellVal.LongValue = col.CellValue.LongValue
		break
	case types.TimestampType:
		cellVal.TimestampValue = col.CellValue.TimestampValue
		break
	case types.StringType:
		cellVal.StringValue = col.CellValue.StringValue
		break
	case types.DoubleType:
		cellVal.DoubleValue = col.CellValue.DoubleValue
		break
	case types.BoolType:
		cellVal.BoolValue = col.CellValue.BoolValue
		break
	}

	newCol := &types.DataColumn{
		ColumnName: t.getRightColumnName(col, config),
		CellValue:  cellVal,
	}

	return newCol
}

func (t *LookupOperator) isRightMatchColumn(col *types.DataColumn, config *LookupConfiguration) bool {
	for _, c := range config.Columns {
		if col.ColumnName == c.Right {
			return true
		}
	}
	return false
}

func (t *LookupOperator) getRightColumnName(col *types.DataColumn, config *LookupConfiguration) string {
	if config.RemoveRightDatasetPrefix {
		return col.ColumnName
	}
	return fmt.Sprintf("%s.%s", config.TargetDataset, col.ColumnName)
}

func (t *LookupOperator) mergeRows(left *types.DataRow, right *types.DataRow, config *LookupConfiguration) *types.DataRow {
	targetLength := len(left.Columns) + len(right.Columns)
	if config.RemoveRightMatchColumn {
		targetLength -= len(config.Columns)
	}
	newRow := &types.DataRow{
		Columns: make([]*types.DataColumn, targetLength),
	}
	for i, c := range left.Columns {
		newRow.Columns[i] = c
	}
	colCounter := 0
	for _, c := range right.Columns {
		if config.RemoveRightMatchColumn && t.isRightMatchColumn(c, config) {
			// let's skip this
			continue
		}
		newRow.Columns[len(left.Columns)+colCounter] = t.copyColumn(c, config)
		colCounter++
	}

	return newRow
}

func (t *LookupOperator) mergeNilRow(left *types.DataRow, rightTemplate *types.DataRow, config *LookupConfiguration) *types.DataRow {
	targetLength := len(left.Columns) + len(rightTemplate.Columns)
	if config.RemoveRightMatchColumn {
		targetLength -= len(config.Columns)
	}
	newRow := &types.DataRow{
		Columns: make([]*types.DataColumn, targetLength),
	}
	for i, c := range left.Columns {
		newRow.Columns[i] = c
	}
	colCounter := 0
	for _, c := range rightTemplate.Columns {
		if config.RemoveRightMatchColumn && t.isRightMatchColumn(c, config) {
			// let's skip this
			continue
		}
		newRow.Columns[len(left.Columns)+colCounter] = &types.DataColumn{
			ColumnName: t.getRightColumnName(c, config),
			CellValue:  &types.CellValue{DataType: types.NilType},
		}
		colCounter++
	}

	return newRow
}

func (t *LookupOperator) mergeSets(left *types.DataSet, right *types.DataSet, config *LookupConfiguration) *types.DataSet {
	mergedSet := &types.DataSet{
		Rows: make([]*types.DataRow, len(left.Rows)),
	}
	rightIndex := t.createColIndex(right)
	leftIndex := t.createColIndex(left)

	refRow := right.Rows[0]
	for li, lr := range left.Rows {
		leftJoinColumns := make([]*types.DataColumn, len(config.Columns))
		for i, jc := range config.Columns {
			leftJoinColumns[i] = leftIndex[lr][jc.Left]
		}
		var matchRow *types.DataRow = nil
		for _, rr := range right.Rows {
			rightJoinColumns := make([]*types.DataColumn, len(config.Columns))
			for i, jc := range config.Columns {
				rightJoinColumns[i] = rightIndex[rr][jc.Right]
			}
			foundMatch := true
			// we need to try find if those values are equal?
			for i := range leftJoinColumns {
				if !t.isEqual(leftJoinColumns[i].CellValue, rightJoinColumns[i].CellValue) {
					foundMatch = false
				}
			}
			if foundMatch {
				// we need to merge these 2 rows
				matchRow = rr
				// let's move to next row on left
				break
			}
		}
		var newRow *types.DataRow = nil
		if matchRow != nil {
			newRow = t.mergeRows(lr, matchRow, config)
		} else {
			// we need to do a nil merge
			newRow = t.mergeNilRow(lr, refRow, config)
		}
		mergedSet.Rows[li] = newRow

	}
	return mergedSet
}

func (t *LookupOperator) Transform(dataset *types.DataSet, config string, otherSets map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	if _, ok := otherSets[typedConfig.TargetDataset]; !ok {
		return nil, errors.New("target dataset not found")
	}

	tds := otherSets[typedConfig.TargetDataset]

	if len(tds.Rows) < 1 || len(dataset.Rows) < 1 {
		return dataset, nil
	}

	firstTargetRow := tds.Rows[0]
	// let's check if columns exist
	for _, col := range typedConfig.Columns {
		realCol := t.GetColumn(firstTargetRow, col.Right)
		if realCol == nil {
			return nil, buildColumnNotExistsError(col.Right)
		}
	}

	firstOriginalRow := dataset.Rows[0]
	for _, col := range typedConfig.Columns {
		realCol := t.GetColumn(firstOriginalRow, col.Left)
		if realCol == nil {
			return nil, buildColumnNotExistsError(col.Left)
		}
	}

	// wow we are ready to join those tables
	mergedSet := t.mergeSets(dataset, tds, typedConfig)
	return mergedSet, nil
}

func (t *LookupOperator) buildConfiguration(config string) (*LookupConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := LookupConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.TargetDataset) < 1 {
		return nil, errors.New("missing targetdataset in lookup configuration")
	}

	if len(typedConfig.Columns) < 1 {
		return nil, errors.New("missing columns in lookup configuration")
	}

	for _, ob := range typedConfig.Columns {
		if len(ob.Left) < 1 {
			return nil, errors.New("missing join left in lookup configuration")
		}
		if len(ob.Right) < 1 {
			return nil, errors.New("missing join right in lookup configuration")
		}
	}

	return &typedConfig, nil
}

func (t *LookupOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
