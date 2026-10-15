package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestRenameColumnv2(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.RenameColumnConfiguration{
		Columns: map[string]string{"Quantity": "renamedQuantity"},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.RenameColumn,
		Configuration: string(b1),
	}

	newDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "rename column operation failed")
	}
	assert.NotNil(t, newDatav2)

	newData := rowsOf(t, newDatav2)
	for _, row := range newData.Rows {
		for _, col := range row.Columns {
			for oldName := range conf.Columns {
				assert.NotEqual(t, col.ColumnName, oldName)
			}
		}
	}
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so the key half of v1's TestRenameColumnWithRowKey has nothing to assert on and
// the rest of it is TestRenameColumnv2.
func TestRenameColumnv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
