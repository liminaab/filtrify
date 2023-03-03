package operator

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type ObjectifyOperator struct {
}

type ObjectifyConfiguration struct {
	Fields          []string `json:"fields"`
	TargetFieldName string   `json:"targetFieldName"`
}

func (t *ObjectifyOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, confError := t.buildConfiguration(config)
	if confError != nil {
		return nil, confError
	}

	newDataset := types.DataSet{
		Rows: make([]*types.DataRow, len(dataset.Rows)),
	}

	for i, row := range dataset.Rows {
		newRow := types.DataRow{
			Columns: make([]*types.DataColumn, 0),
		}
		objectColumnMap := make(map[string]interface{})
		for _, col := range row.Columns {
			shouldBeRemoved := false
			for _, toRemove := range typedConfig.Fields {
				if col.ColumnName == toRemove {
					shouldBeRemoved = true
					break
				}
			}
			if !shouldBeRemoved {
				newRow.Columns = append(newRow.Columns, col)
			} else {
				// we should make this a json column
				objectColumnMap[col.ColumnName] = col.CellValue.Value()
			}
		}

		objectColumn := &types.DataColumn{
			ColumnName: typedConfig.TargetFieldName,
			CellValue: &types.CellValue{
				DataType:    types.ObjectType,
				ObjectValue: objectColumnMap,
			},
		}
		newRow.Columns = append(newRow.Columns, objectColumn)
		newDataset.Rows[i] = &newRow
	}
	return &newDataset, nil
}

func (t *ObjectifyOperator) buildConfiguration(config string) (*ObjectifyConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := ObjectifyConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Fields) < 1 {
		return nil, errors.New("missing json configuration")
	}

	if len(typedConfig.TargetFieldName) < 1 {
		return nil, errors.New("missing json configuration")
	}

	for _, ob := range typedConfig.Fields {
		if len(ob) < 1 {
			return nil, errors.New("missing column name in json configuration")
		}
	}

	return &typedConfig, nil
}

func (t *ObjectifyOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
