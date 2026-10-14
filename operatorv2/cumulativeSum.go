package operatorv2

import (
	"encoding/json"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type CumulativeSumOperator struct {
}

type CumulativeSumConfiguration struct {
	Column        string `json:"column"`
	NewColumnName string `json:"newColumnName"`
}

func (t *CumulativeSumOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	theColumn := dataset.GetColumn(typedConfig.Column)
	if theColumn == nil {
		return nil, buildColumnNotExistsError(typedConfig.Column)
	}

	if !dataset.IsNumericType(theColumn.DataType()) {
		dataset, err = dataset.AddConstantColumn(typedConfig.NewColumnName, 0, &arrow.Float64Type{})
		return dataset, err
	}

	var cumulativeSum float64 = 0
	for val := range dataset.NumericValues(theColumn) {
		if val == nil {
			continue
		}
		cumulativeSum += *val
	}
	dataset, err = dataset.AddConstantColumn(typedConfig.NewColumnName, cumulativeSum, &arrow.Float64Type{})
	return dataset, err
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
