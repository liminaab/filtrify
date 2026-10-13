package operatorv2

import (
	"encoding/json"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type RemoveColumnOperator struct {
}

type RemoveColumnConfiguration struct {
	Columns []string `json:"columns"`
}

func (t *RemoveColumnOperator) TransformWithConfig(dataset *types.DataSetv2, typedConfig *RemoveColumnConfiguration, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	toRemove := make(map[string]bool, len(typedConfig.Columns))
	for _, c := range typedConfig.Columns {
		toRemove[c] = true
	}

	newColumns := make([]arrow.Column, 0, dataset.NumCols())
	built := 0
	defer func() {
		for i := range built {
			newColumns[i].Release()
		}
	}()

	for i := range dataset.NumCols() {
		col := dataset.Column(int(i))
		if _, skip := toRemove[col.Name()]; skip {
			continue
		}
		col.Retain()
		newColumns = append(newColumns, *col)
		built++
	}

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := dataset.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	newTable := array.NewTable(schema, newColumns, dataset.NumRows())
	return &types.DataSetv2{Table: newTable}, nil
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
