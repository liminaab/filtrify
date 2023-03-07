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
	Fields          map[string]bool `json:"fields"`
	TargetFieldName string          `json:"targetFieldName"`
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
			// if this col has the same name with targetFieldName, we should skip it
			if col.ColumnName == typedConfig.TargetFieldName {
				continue
			}
			shouldBeRemoved, found := typedConfig.Fields[col.ColumnName]
			if !found || !shouldBeRemoved {
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
		return nil, errors.New("fields must be specified in objectify configuration")
	}

	if len(typedConfig.TargetFieldName) < 1 {
		return nil, errors.New("target field name must be specified in objectify configuration")
	}

	return &typedConfig, nil
}

func (t *ObjectifyOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
