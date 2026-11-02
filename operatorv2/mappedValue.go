package operatorv2

import (
	"encoding/json"
	"errors"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

type MappedValueOperator struct {
}

// A map table is two columns under these names: the key to match the mapped
// column against, and the value to bring over. Data embedded in the
// configuration carries no header row, so these are prepended to it.
const (
	mapKeyColumn   = "Key"
	mapValueColumn = "Value"
)

type MappedValueConfiguration struct {
	MappedColumnName string     `json:"mappedColumnName"`
	NewColumnName    string     `json:"newColumnName"`
	TargetDataset    string     `json:"targetDataset"`
	TargetData       [][]string `json:"targetData"`
}

func (t *MappedValueOperator) Transform(dataset *types.DataSetv2, config string, otherSets map[string]*types.DataSetv2) (*types.DataSetv2, error) {

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

	var tds *types.DataSetv2
	if len(typedConfig.TargetData) > 0 {
		// let's append static headers to our data
		typedConfig.TargetData = append([][]string{{mapKeyColumn, mapValueColumn}}, typedConfig.TargetData...)
		// let's make sure key is always text type
		conversionMap := conversion.ConversionMap{mapKeyColumn: false}
		tds, err = conversion.ConvertToTypedDatav2(typedConfig.TargetData, true, true, conversionMap, true)
		if err != nil {
			return nil, err
		}
		typedConfig.TargetDataset = RandStringBytesMaskImprSrcUnsafe(10)
		otherSets[typedConfig.TargetDataset] = tds
	} else {
		tds = otherSets[typedConfig.TargetDataset]
	}

	if tds.NumRows() < 1 || dataset.NumRows() < 1 {
		return dataset, nil
	}

	// The map table has to be the two columns named above and nothing else. v1
	// reads the width of its first row to check that; the equivalent of a row's
	// width in a column-oriented table is the number of columns, not the number
	// of rows - a map table has as many rows as there are mappings.
	if tds.NumCols() != 2 {
		return nil, errors.New("invalid map table")
	}
	if tds.Column(0).Name() != mapKeyColumn || tds.Column(1).Name() != mapValueColumn {
		return nil, errors.New("invalid map table")
	}

	lookupConf := &LookupConfiguration{
		TargetDataset: typedConfig.TargetDataset,
		Columns: []*JoinColumn{
			{
				Left:  typedConfig.MappedColumnName,
				Right: mapKeyColumn,
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
	// let's rename the last column
	lastCol := transformedSet.Column(int(transformedSet.NumCols() - 1))
	if lastCol.Name() != mapValueColumn {
		return nil, errors.New("internal join error")
	}
	transformedSet, err = transformedSet.RenameColumn(mapValueColumn, typedConfig.NewColumnName)
	if err != nil {
		return nil, err
	}

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
