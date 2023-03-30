package operator

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type RenameColumnOperator struct {
}

type RenameColumnConfiguration struct {
	Columns map[string]string `json:"columns"`
}

func (t *RenameColumnOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	newDataset := types.DataSet{
		Rows: make([]*types.DataRow, len(dataset.Rows)),
	}

	for i, row := range dataset.Rows {
		newRow := types.DataRow{
			Columns: make([]*types.DataColumn, 0),
		}
		for _, col := range row.Columns {
			newName, found := typedConfig.Columns[col.ColumnName]
			if !found {
				newRow.Columns = append(newRow.Columns, col)
				continue
			}
			copiedValue := types.CellValue{
				DataType:       col.CellValue.DataType,
				IntValue:       col.CellValue.IntValue,
				LongValue:      col.CellValue.LongValue,
				TimestampValue: col.CellValue.TimestampValue,
				StringValue:    col.CellValue.StringValue,
				DoubleValue:    col.CellValue.DoubleValue,
				BoolValue:      col.CellValue.BoolValue,
				ObjectValue:    col.CellValue.ObjectValue,
			}
			newCol := &types.DataColumn{
				ColumnName: newName,
				CellValue:  &copiedValue,
			}
			newRow.Columns = append(newRow.Columns, newCol)
		}
		newDataset.Rows[i] = &newRow
	}

	newDataset.Headers = buildHeaders(&newDataset, dataset)
	return &newDataset, nil
}

func (t *RenameColumnOperator) buildConfiguration(config string) (*RenameColumnConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := RenameColumnConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Columns) < 1 {
		return nil, errors.New("missing columns in removecolumn configuration")
	}

	return &typedConfig, nil
}

func (t *RenameColumnOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
