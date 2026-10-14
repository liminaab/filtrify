package operatorv2

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

func (t *RenameColumnOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}
	for oldColName, newColName := range typedConfig.Columns {
		dataset, err = dataset.RenameColumn(oldColName, newColName)
		if err != nil {
			return nil, err
		}
	}
	return dataset, nil
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
