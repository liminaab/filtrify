package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestRenameColumn(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.RenameColumnConfiguration{
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

	newData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "rename column operation failed")
	}
	assert.NotNil(t, newData)

	for _, row := range newData.Rows {
		for _, col := range row.Columns {
			for oldName, _ := range conf.Columns {
				assert.NotEqual(t, col.ColumnName, oldName)
			}
		}
	}
}

func TestRenameColumnWithRowKey(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.RenameColumnConfiguration{
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

	for i := range data.Rows {
		key := fmt.Sprintf("row-%d", i)
		data.Rows[i].Key = &key
	}

	newData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "rename column operation failed")
	}
	assert.NotNil(t, newData)

	for _, row := range newData.Rows {
		for _, col := range row.Columns {
			for oldName, _ := range conf.Columns {
				assert.NotEqual(t, col.ColumnName, oldName)
			}
		}
	}

	for _, r := range newData.Rows {
		assert.NotNil(t, r.Key, "Key assignment failed on renameColumn operator")
	}
}
