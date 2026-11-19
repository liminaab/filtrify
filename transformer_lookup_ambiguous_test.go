package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runAmbiguousLookup(t *testing.T, reject bool) *types.DataSet {
	t.Helper()
	input, err := filtrify.ConvertToTypedData([][]string{
		{"Entity", "Amount"},
		{"unique", "1"},
		{"O'Brien", "2"},
		{"missing", "3"},
	}, true, true, false)
	require.NoError(t, err)
	entities, err := filtrify.ConvertToTypedData([][]string{
		{"id", "external_id"},
		{"10", "unique"},
		{"20", "O'Brien"},
		{"30", "O'Brien"},
	}, true, true, false)
	require.NoError(t, err)

	conf, err := json.Marshal(&operator.LookupConfiguration{
		TargetDataset:          "legal_entities",
		Columns:                []*operator.JoinColumn{{Left: "Entity", Right: "external_id"}},
		SelectedColumns:        []string{"id"},
		RejectAmbiguousMatches: reject,
	})
	require.NoError(t, err)
	step := &types.TransformationStep{Operator: types.Lookup, Configuration: string(conf)}

	out, err := filtrify.Transform(input, []*types.TransformationStep{step}, map[string]*types.DataSet{"legal_entities": entities})
	require.NoError(t, err)
	require.Len(t, out.Rows, 3)
	return out
}

func TestLookupRejectAmbiguousMatches(t *testing.T) {
	t.Parallel()
	out := runAmbiguousLookup(t, true)
	errCol := operator.LookupErrorColumnPrefix + "Entity"

	unique, shared, missing := out.Rows[0], out.Rows[1], out.Rows[2]

	assert.Equal(t, "10", test.GetColumn(unique, "legal_entities.id").CellValue.ToString())
	assert.Equal(t, types.NilType, test.GetColumn(unique, errCol).CellValue.DataType)

	assert.Equal(t, types.NilType, test.GetColumn(shared, "legal_entities.id").CellValue.DataType)
	assert.Equal(t, `"O'Brien" matches 2 rows in legal_entities on external_id`, test.GetColumn(shared, errCol).CellValue.ToString())

	assert.Equal(t, types.NilType, test.GetColumn(missing, "legal_entities.id").CellValue.DataType)
	assert.Equal(t, types.NilType, test.GetColumn(missing, errCol).CellValue.DataType)

	_, hasHeader := out.Headers[errCol]
	assert.True(t, hasHeader)
}

func TestLookupAmbiguousMatchesWithoutFlag(t *testing.T) {
	t.Parallel()
	out := runAmbiguousLookup(t, false)

	assert.Equal(t, "20", test.GetColumn(out.Rows[1], "legal_entities.id").CellValue.ToString())
	for _, r := range out.Rows {
		assert.Nil(t, test.GetColumn(r, operator.LookupErrorColumnPrefix+"Entity"))
	}
}

func TestLookupSelectingMissingColumnThenRename(t *testing.T) {
	t.Parallel()
	input, err := filtrify.ConvertToTypedData([][]string{
		{"Entity"},
		{"unique"},
		{"shared"},
	}, true, true, false)
	require.NoError(t, err)
	entities, err := filtrify.ConvertToTypedData([][]string{
		{"id", "external_id"},
		{"10", "unique"},
		{"20", "shared"},
		{"30", "shared"},
	}, true, true, false)
	require.NoError(t, err)

	lookupConf, err := json.Marshal(&operator.LookupConfiguration{
		TargetDataset:          "legal_entities",
		Columns:                []*operator.JoinColumn{{Left: "Entity", Right: "external_id"}},
		SelectedColumns:        []string{"id", "no_such_column"},
		RejectAmbiguousMatches: true,
	})
	require.NoError(t, err)
	renameConf, err := json.Marshal(&operator.RenameColumnConfiguration{
		Columns: map[string]string{"legal_entities.id": "Id"},
	})
	require.NoError(t, err)
	steps := []*types.TransformationStep{
		{Operator: types.Lookup, Configuration: string(lookupConf)},
		{Operator: types.RenameColumn, Configuration: string(renameConf)},
	}

	out, err := filtrify.Transform(input, steps, map[string]*types.DataSet{"legal_entities": entities})
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)
	errCol := operator.LookupErrorColumnPrefix + "Entity"
	unique, shared := out.Rows[0], out.Rows[1]

	require.NotNil(t, test.GetColumn(unique, "Id"), "one match keeps its id column")
	assert.Equal(t, "10", test.GetColumn(unique, "Id").CellValue.ToString())

	require.NotNil(t, test.GetColumn(shared, "Id"), "ambiguous match keeps its id column")
	assert.Equal(t, types.NilType, test.GetColumn(shared, "Id").CellValue.DataType, "ambiguous match leaves the id unset")
	require.NotNil(t, test.GetColumn(shared, errCol), "ambiguous match keeps its error column")
	assert.Equal(t, `"shared" matches 2 rows in legal_entities on external_id`, test.GetColumn(shared, errCol).CellValue.ToString())
}
