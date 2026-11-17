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
