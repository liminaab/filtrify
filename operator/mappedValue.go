package operator

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

type MappedValueOperator struct {
}

type MappedValueConfiguration struct {
	MappedColumnName string     `json:"mappedColumnName"`
	NewColumnName    string     `json:"newColumnName"`
	TargetDataset    string     `json:"targetDataset"`
	TargetData       [][]string `json:"targetData"`
}

func (t *MappedValueOperator) Transform(dataset *types.DataSet, config string, otherSets map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	if _, ok := otherSets[typedConfig.TargetDataset]; !ok {
		// let's not give up so fast
		// data might be embedded in our configuration
		if len(typedConfig.TargetData) == 0 {
			return nil, errors.New("target dataset not found")
		}
	}

	var tds *types.DataSet
	if len(typedConfig.TargetData) > 0 {
		// let's append static headers to our data
		typedConfig.TargetData = append([][]string{{"Key", "Value"}}, typedConfig.TargetData...)
		// let's make sure key is always text type
		conversionMap := conversion.ConversionMap{"Key": false}
		tds, err = conversion.ConvertToTypedData(typedConfig.TargetData, true, true, conversionMap, true)
		if err != nil {
			return nil, err
		}
		typedConfig.TargetDataset = RandStringBytesMaskImprSrcUnsafe(10)
		otherSets[typedConfig.TargetDataset] = tds
	} else {
		tds = otherSets[typedConfig.TargetDataset]
	}

	if len(tds.Rows) < 1 || len(dataset.Rows) < 1 {
		return dataset, nil
	}

	refRow := tds.Rows[0]
	if len(refRow.Columns) != 2 {
		return nil, errors.New("invalid map table")
	}
	if refRow.Columns[0].ColumnName != "Key" || refRow.Columns[1].ColumnName != "Value" {
		return nil, errors.New("invalid map table")
	}

	lookupConf := &LookupConfiguration{
		TargetDataset: typedConfig.TargetDataset,
		Columns: []*JoinColumn{
			{
				Left:  typedConfig.MappedColumnName,
				Right: "Key",
			},
		},
		RemoveRightMatchColumn:   true,
		RemoveRightDatasetPrefix: true,
	}
	textConf, err := json.Marshal(lookupConf)
	if err != nil {
		return nil, err
	}

	lookupOp := &LookupOperator{}
	transformedSet, err := lookupOp.Transform(dataset, string(textConf), otherSets)
	if err != nil {
		return nil, err
	}

	// so is our new data is really string?
	// let's try to guess it's real type
	//estimateColumnDataType(transformedSet, typedConfig.NewColumnName)
	for _, r := range transformedSet.Rows {
		lastCol := r.Columns[len(r.Columns)-1]
		if lastCol.ColumnName != "Value" {
			// wow something fishy going on here
			return nil, errors.New("internal join error")
		}
		lastCol.ColumnName = typedConfig.NewColumnName
	}

	transformedSet.Headers = buildHeaders(transformedSet, dataset)
	return transformedSet, nil
}

func (t *MappedValueOperator) buildConfiguration(config string) (*MappedValueConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := MappedValueConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}
	if len(typedConfig.MappedColumnName) < 1 {
		return nil, errors.New("missing mappedcolumname in mappedvalue configuration")
	}
	if len(typedConfig.NewColumnName) < 1 {
		return nil, errors.New("missing newcolumnname in mappedvalue configuration")
	}
	if len(typedConfig.TargetDataset) < 1 && len(typedConfig.TargetData) < 1 {
		return nil, errors.New("missing targetdataset in mappedvalue configuration")
	}

	return &typedConfig, nil
}

func (t *MappedValueOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
