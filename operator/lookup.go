package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type LookupOperator struct {
}

type JoinColumn struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type LookupFilter struct {
	Value  string `json:"value"`
	Filter string `json:"filter"`
}

type LookupConfiguration struct {
	TargetDataset            string                  `json:"targetDataset"`
	Columns                  []*JoinColumn           `json:"columns"`
	RemoveRightMatchColumn   bool                    `json:"removeRightMatchColumn"`
	RemoveRightDatasetPrefix bool                    `json:"removeRightDatasetPrefix"`
	SelectedColumns          []string                `json:"selectedColumns"`
	TargetDatasetFilters     map[string]LookupFilter `json:"targetDatasetFilters"`
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

func (t *LookupOperator) copyColumn(orgDataset *types.DataSet, col *types.DataColumn, config *LookupConfiguration) *types.DataColumn {

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
	case types.TimestampType, types.DateType, types.TimeOfDayType:
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
		ColumnName: t.getRightColumnName(orgDataset, col, config),
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

func (t *LookupOperator) getRightColumnName(orgDataset *types.DataSet, col *types.DataColumn, config *LookupConfiguration) string {
	var columnName string
	if config.RemoveRightDatasetPrefix {
		columnName = col.ColumnName
	} else {
		columnName = fmt.Sprintf("%s.%s", config.TargetDataset, col.ColumnName)
	}
	counter := 1
	for {
		exists := false
		// let's check if this name already exists
		for _, val := range orgDataset.Headers {
			if val.ColumnName == columnName {
				exists = true
				break
			}
		}
		if exists {
			columnName = fmt.Sprintf("%s_%d", columnName, counter)
			counter++
		} else {
			break
		}
	}
	return columnName
}

func (t *LookupOperator) mergeRows(orgDataset *types.DataSet, left *types.DataRow, right *types.DataRow, config *LookupConfiguration) *types.DataRow {
	targetLength := len(left.Columns) + len(right.Columns)
	if config.RemoveRightMatchColumn {
		targetLength -= len(config.Columns)
	}
	if len(config.SelectedColumns) > 0 {
		targetLength = len(left.Columns) + len(config.SelectedColumns)
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
		if len(config.SelectedColumns) > 0 {
			// let's check if this column is selected
			found := false
			for _, sc := range config.SelectedColumns {
				if strings.EqualFold(sc, c.ColumnName) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		newRow.Columns[len(left.Columns)+colCounter] = t.copyColumn(orgDataset, c, config)
		colCounter++
	}

	return newRow
}

func (t *LookupOperator) mergeNilRow(orgDataset *types.DataSet, left *types.DataRow, rightTemplate *types.DataRow, config *LookupConfiguration) *types.DataRow {
	targetLength := len(left.Columns) + len(rightTemplate.Columns)
	if config.RemoveRightMatchColumn {
		targetLength -= len(config.Columns)
	}
	if len(config.SelectedColumns) > 0 {
		targetLength = len(left.Columns) + len(config.SelectedColumns)
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
		if len(config.SelectedColumns) > 0 {
			// let's check if this column is selected
			found := false
			for _, sc := range config.SelectedColumns {
				if strings.EqualFold(sc, c.ColumnName) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		newRow.Columns[len(left.Columns)+colCounter] = &types.DataColumn{
			ColumnName: t.getRightColumnName(orgDataset, c, config),
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
				if !leftJoinColumns[i].CellValue.Equals(rightJoinColumns[i].CellValue) {
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
			newRow = t.mergeRows(left, lr, matchRow, config)
		} else {
			// we need to do a nil merge
			newRow = t.mergeNilRow(left, lr, refRow, config)
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

	filteredSet := tds
	if len(typedConfig.TargetDatasetFilters) > 0 {
		filterOp := &FilterOperator{}
		// before merging the data - let's do a filter for the target dataset if it exists
		for _, filter := range typedConfig.TargetDatasetFilters {
			if len(filter.Filter) == 0 {
				continue
			}
			filteredSet, err = filterOp.Transform(filteredSet, filter.Filter, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	// wow we are ready to join those tables
	mergedSet := t.mergeSets(dataset, filteredSet, typedConfig)
	mergedSet.Headers = buildHeaders(mergedSet, dataset)
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
