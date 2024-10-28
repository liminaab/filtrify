package operator

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type JSONOperator struct {
}

type JSONConfiguration struct {
	Fields          []string `json:"fields"`
	TargetFieldName string   `json:"targetFieldName"`
}

func (t *JSONOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, confError := t.buildConfiguration(config)
	if confError != nil {
		return nil, confError
	}

	newDataset := types.DataSet{
		Rows: make([]*types.DataRow, len(dataset.Rows)),
	}
	fieldMap := make(map[string]bool)
	for _, field := range typedConfig.Fields {
		fieldMap[field] = true
	}
	for i, row := range dataset.Rows {
		newRow := types.DataRow{
			Key:     row.Key,
			Columns: make([]*types.DataColumn, 0),
		}
		jsonColumnMap := make(map[string]string)
		for _, col := range row.Columns {
			if col.ColumnName == typedConfig.TargetFieldName {
				continue
			}
			shouldBeRemoved, found := fieldMap[col.ColumnName]
			if !found || !shouldBeRemoved {
				newRow.Columns = append(newRow.Columns, col)
			} else {
				// we should make this a json column
				jsonColumnMap[col.ColumnName] = col.CellValue.ToString()
			}
		}
		jsonColumnString, err := json.Marshal(jsonColumnMap)
		if err != nil {
			return nil, err
		}
		jsonColumn := &types.DataColumn{
			ColumnName: typedConfig.TargetFieldName,
			CellValue: &types.CellValue{
				DataType:    types.StringType,
				StringValue: string(jsonColumnString),
			},
		}
		newRow.Columns = append(newRow.Columns, jsonColumn)
		newDataset.Rows[i] = &newRow
	}
	newDataset.Headers = buildHeaders(&newDataset, dataset)
	return &newDataset, nil
}

func (t *JSONOperator) buildConfiguration(config string) (*JSONConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := JSONConfiguration{}
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

func (t *JSONOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
