package operatorv2

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

func (t *RemoveColumnOperator) TransformWithConfig(dataset *types.DataSetv2, typedConfig *RemoveColumnConfiguration, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	var err error
	for _, c := range typedConfig.Columns {
		dataset, err = dataset.RemoveColumn(c)
		if err != nil {
			return nil, err
		}
	}
	return dataset, nil
}

func (t *RemoveColumnOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}
	return t.TransformWithConfig(dataset, typedConfig, nil)
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
