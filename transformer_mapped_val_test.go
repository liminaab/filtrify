package dyntransformer_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/test"
	"limina.com/dyntransformer/types"
)

func TestBasicMappedValue(t *testing.T) {
	lookupData, err := dyntransformer.ConvertToTypedData(test.UATLookupTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	instrumentSet, err := dyntransformer.ConvertToTypedData(test.UATLookupJoinTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operator.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operator.JoinColumn{
			{
				Left:  "Instrument ID",
				Right: "Instrument ID",
			},
		},
		RemoveRightMatchColumn:   true,
		RemoveRightDatasetPrefix: true,
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.Lookup,
		Configuration: string(b1),
	}

	joinSet := map[string]*types.DataSet{
		"Instrument Data": instrumentSet,
	}
	joinedData, err := dyntransformer.Transform(lookupData, []*types.TransformationStep{step}, joinSet)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}
	test.PrintDataset(joinedData)
	assert.Len(t, joinedData.Rows, len(lookupData.Rows), "join failed. invalid number of rows")

	// now we need to make sure join was successful
	verifyJoin(t, lookupData, instrumentSet, joinedData, conf)
}
