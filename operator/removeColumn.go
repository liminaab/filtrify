package operator

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type RemoveColumnOperator struct {
}

type RemoveColumnConfiguration struct {
	Columns []string `json:"columns"`
}

func (t *RemoveColumnOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

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
			shouldBeRemoved := false
			for _, toRemove := range typedConfig.Columns {
				if col.ColumnName == toRemove {
					shouldBeRemoved = true
					break
				}
			}
			if !shouldBeRemoved {
				newRow.Columns = append(newRow.Columns, col)
			}
		}
		newDataset.Rows[i] = &newRow
	}

	return &newDataset, nil
}

func (t *RemoveColumnOperator) buildConfiguration(config string) (*RemoveColumnConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := RemoveColumnConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Columns) < 1 {
		return nil, errors.New("missing columns in removecolumn configuration")
	}

	return &typedConfig, nil
}

func (t *RemoveColumnOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
