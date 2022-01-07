package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestRemoveColumn(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.RemoveColumnConfiguration{
		Columns: []string{"Quantity", "Market Value (Base)"},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.RemoveColumn,
		Configuration: string(b1),
	}

	newData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "remove column operation failed")
	}
	assert.NotNil(t, newData)

	for _, row := range newData.Rows {
		for _, col := range row.Columns {
			for _, toRemove := range conf.Columns {
				assert.NotEqual(t, col.ColumnName, toRemove)
			}
		}
	}
}
