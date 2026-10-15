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

func TestRemoveColumnv2(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.RemoveColumnConfiguration{
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

	newDatav2, err := filtrify.Transformv2(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "remove column operation failed")
	}
	assert.NotNil(t, newDatav2)

	newData := rowsOf(t, newDatav2)
	for _, row := range newData.Rows {
		for _, col := range row.Columns {
			for _, toRemove := range conf.Columns {
				assert.NotEqual(t, col.ColumnName, toRemove)
			}
		}
	}
}

// The v1 TestRemoveColumnWithRowKey assigns a key to every row and checks the
// operator carried it through. A v2 table has no per-row key (see rowsOf), so
// there is nothing to assign and nothing to check; what remains is the same
// remove-column assertion over the same data, which TestRemoveColumnv2 above
// already makes. The test is kept so the pairing with v1 stays visible.
func TestRemoveColumnv2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
