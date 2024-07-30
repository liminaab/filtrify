package operator

import (
	"encoding/json"
	"errors"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type GroupByOperator struct {
}

const groupLevelColName = "Group Level"

type GroupByConfiguration struct {
	Select  []*AggregateSelect `json:"select"`
	GroupBy []string           `json:"groupby"`
}

func (t *GroupByOperator) addGroupLevel(data *types.DataSet, level int32) {
	for _, row := range data.Rows {
		levelColumn := &types.DataColumn{
			ColumnName: groupLevelColName,
			CellValue: &types.CellValue{
				DataType: types.IntType,
				IntValue: level,
			},
		}
		row.Columns = append([]*types.DataColumn{levelColumn}, row.Columns...)
		row.Columns = append(row.Columns)
	}
}

type columnComparison struct {
	col1 *types.DataColumn
	col2 *types.DataColumn
}

func (t *GroupByOperator) hasSameValues(cols []string, t1 *types.DataRow, t2 *types.DataRow) bool {
	comparisons := make([]*columnComparison, len(cols))
	for i, c := range cols {
		comparisons[i] = &columnComparison{}
		// we need to find the related cols in each
		for _, col1 := range t1.Columns {
			if col1.ColumnName == c {
				comparisons[i].col1 = col1
				break
			}
		}

		for _, col2 := range t2.Columns {
			if col2.ColumnName == c {
				comparisons[i].col2 = col2
				break
			}
		}
	}
	hasSameValues := true
	// let's compare these
	for _, comp := range comparisons {
		if comp.col1 == nil || comp.col2 == nil {
			hasSameValues = false
			break
		}
		if !comp.col1.CellValue.Equals(comp.col2.CellValue) {
			hasSameValues = false
			break
		}
	}
	return hasSameValues
}

func (t *GroupByOperator) trimDataset(example *types.DataSet, target *types.DataSet) (*types.DataSet, error) {
	colsToRemove := make([]string, 0)
	// we need to remove all the extra columns compared to the example
	for k := range target.Headers {
		if _, ok := example.Headers[k]; !ok {
			// let's add this one
			colsToRemove = append(colsToRemove, k)
		}
	}
	// let's remove unnecessary columns
	removeColOperator := &RemoveColumnOperator{}
	return removeColOperator.TransformWithConfig(target, &RemoveColumnConfiguration{Columns: colsToRemove}, nil)
}

func (t *GroupByOperator) mergeDatasets(to *datasetToMerge, from *datasetToMerge) *datasetToMerge {
	missingCols := make(map[string]bool)
	// let's first merge headers
	for k := range from.Data.Headers {
		if _, ok := to.Data.Headers[k]; !ok {
			// let's add this one
			missingCols[k] = true
		}
	}
	for mr := range missingCols {
		to.Data.Headers[mr] = from.Data.Headers[mr]
		// we need to create columns for missing headers on the target dataset
		for _, row := range to.Data.Rows {
			row.Columns = append(row.Columns, &types.DataColumn{ColumnName: mr, CellValue: &types.CellValue{
				DataType: types.NilType,
			}})
		}
	}
	// all missing headers are copied to the target
	// now it is time to merge the real data
	// we need to merge the rows according to the grouped by column
	mergedDataset := &types.DataSet{
		Headers: to.Data.Headers,
		Rows:    make([]*types.DataRow, 0),
	}
	for _, r1 := range to.Data.Rows {
		mergedDataset.Rows = append(mergedDataset.Rows, r1)
		// let's find all the rows from the other dataset to merge under this one
		for _, r2 := range from.Data.Rows {
			if t.hasSameValues(to.GroupedBy, r1, r2) {
				// let's add this below this row
				mergedDataset.Rows = append(mergedDataset.Rows, r2)
			}
		}
	}

	// we also need to order the columns according to joined data
	colOrderedDataSet := &types.DataSet{
		Headers: to.Data.Headers,
		Rows:    make([]*types.DataRow, len(mergedDataset.Rows)),
	}

	allHeaders := make([]string, 0)
	for k := range mergedDataset.Headers {
		allHeaders = append(allHeaders, k)
	}

	// let's first process grouped by columns then the rest
	// we should always first insert the group level column
	// let's insert this column to all the rows
	for i, r := range mergedDataset.Rows {
		// we created this row
		colOrderedDataSet.Rows[i] = &types.DataRow{
			Columns: make([]*types.DataColumn, len(r.Columns)),
		}
		processedColumns := make(map[string]bool)
		currentCol := 0
		groupLevelCol := r.GetColumn(groupLevelColName)
		if groupLevelCol == nil {
			return nil
		}
		colOrderedDataSet.Rows[i].Columns[currentCol] = groupLevelCol
		currentCol++
		processedColumns[groupLevelColName] = true
		for _, c := range from.GroupedBy {
			theCol := r.GetColumn(c)
			if theCol == nil {
				return nil
			}
			colOrderedDataSet.Rows[i].Columns[currentCol] = theCol
			currentCol++
			processedColumns[c] = true
		}

		for _, k := range allHeaders {
			if _, ok := processedColumns[k]; ok {
				// this has already been processed
				continue
			}
			theCol := r.GetColumn(k)
			if theCol == nil {
				return nil
			}
			colOrderedDataSet.Rows[i].Columns[currentCol] = theCol
			currentCol++
			processedColumns[k] = true
		}
	}
	return &datasetToMerge{
		Data:      colOrderedDataSet,
		GroupedBy: from.GroupedBy,
	}
}

type datasetToMerge struct {
	Data      *types.DataSet
	GroupedBy []string
}

func (t *GroupByOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	levelsOfData := make([]*datasetToMerge, len(typedConfig.GroupBy)+1)
	aggregateOperator := &AggregateOperator{}
	colsToAggregatePerLevel := make([]string, 0)
	for i, col := range typedConfig.GroupBy {
		colsToAggregatePerLevel = append(colsToAggregatePerLevel, col)
		aggregateConfig := AggregateConfiguration{
			Select:  typedConfig.Select,
			GroupBy: colsToAggregatePerLevel,
		}
		aggregatedSet, err := aggregateOperator.TransformWithConfig(dataset, &aggregateConfig, nil)
		if err != nil {
			return nil, err
		}
		levelsOfData[i] = &datasetToMerge{
			Data:      aggregatedSet,
			GroupedBy: colsToAggregatePerLevel,
		}
	}
	// let's add the original dataset as the last level
	// so we have a tree of data that is expanding
	levelsOfData[len(levelsOfData)-1] = &datasetToMerge{
		Data:      dataset,
		GroupedBy: nil,
	}
	// at this point we have all the aggregations we need
	// we need to merge them into a single dataset
	// let's add a grouping level column to the initial dataset and use it as our base
	// we will then merge the other datasets into this one
	baseDataSet := levelsOfData[0]
	baseDataSet.Data.Headers["Group Level"] = &types.Header{
		DataType:   types.IntType,
		ColumnName: "Group Level",
	}
	// let's append group level to all the rows in base set
	for i, levelDataSet := range levelsOfData {
		t.addGroupLevel(levelDataSet.Data, int32(i+1))
	}
	finalDataset := baseDataSet
	// at this point let's merge all of the datasets - except the original one
	for _, levelDataSet := range levelsOfData[1 : len(levelsOfData)-1] {
		finalDataset = t.mergeDatasets(finalDataset, levelDataSet)
		if finalDataset == nil {
			return nil, errors.New("failed to do group by operation")
		}
	}

	// at this point we need to merge the original dataset to the finalDataset
	// we need to remove any extra columns though - because finalDataset has the final columns
	// original dataset might have extra cols - which need to be removed
	initialTrimmedDataset, err := t.trimDataset(finalDataset.Data, dataset)
	if err != nil {
		return nil, err
	}
	// let's merge them
	finalDataset = t.mergeDatasets(finalDataset, &datasetToMerge{Data: initialTrimmedDataset, GroupedBy: typedConfig.GroupBy})
	if finalDataset == nil {
		return nil, errors.New("failed to do group by operation")
	}
	return finalDataset.Data, nil
}

func (t *GroupByOperator) buildConfiguration(config string) (*GroupByConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := GroupByConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.GroupBy) < 1 {
		return nil, errors.New("missing groupby in configuration")
	}

	return &typedConfig, nil
}

func (t *GroupByOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
