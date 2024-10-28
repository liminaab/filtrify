package operator

import (
	"encoding/json"
	"errors"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type CumulativeSumOperator struct {
}

type CumulativeSumConfiguration struct {
	Column        string `json:"column"`
	NewColumnName string `json:"newColumnName"`
}

func (t *CumulativeSumOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	newDataset := &types.DataSet{
		Rows: make([]*types.DataRow, len(dataset.Rows)),
	}

	var cumulativeSum float64 = 0
	for i, row := range dataset.Rows {
		newRow := types.DataRow{
			Key:     row.Key,
			Columns: make([]*types.DataColumn, 0),
		}
		for _, col := range row.Columns {
			if col.ColumnName == typedConfig.Column {
				if !col.CellValue.IsNumeric() {
					newRow.Columns = append(newRow.Columns, col)
					continue
				}
				cumulativeSum += col.CellValue.GetNumericVal()
			}
			newRow.Columns = append(newRow.Columns, col)
		}
		newRow.Columns = append(newRow.Columns, &types.DataColumn{
			ColumnName: typedConfig.NewColumnName,
			CellValue: &types.CellValue{
				DataType:    types.DoubleType,
				DoubleValue: cumulativeSum,
			},
		})
		newDataset.Rows[i] = &newRow
	}

	newDataset.Headers = buildHeaders(newDataset, dataset)
	return newDataset, nil
}

func (t *CumulativeSumOperator) buildConfiguration(config string) (*CumulativeSumConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := CumulativeSumConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Column) < 1 {
		return nil, errors.New("missing column name in CumulativeSum configuration")
	}
	if len(typedConfig.NewColumnName) < 1 {
		return nil, errors.New("missing new column name in CumulativeSum configuration")
	}

	return &typedConfig, nil
}

func (t *CumulativeSumOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
