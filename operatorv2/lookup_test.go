package operatorv2

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
)

// These tests pin the v2 lookup operator to the v1 one: every case runs both
// implementations over the same raw data and the same configuration JSON, then
// compares the results cell by cell.

var lookupLeftData = test.UATLookupTestDataFormatted
var lookupRightData = test.UATLookupJoinTestDataFormatted

// cellText renders a v1 cell the way CellValue.ToString does - which is also
// what v1TextOf/v2TextOf have to agree on.
func v1TextOf(c *types.DataColumn) (string, bool) {
	if c == nil || c.CellValue == nil || c.CellValue.DataType == types.NilType {
		return "", true
	}
	return c.CellValue.ToString(), false
}

// v2TextOf renders slot i of arr exactly like CellValue.ToString would render
// the equivalent v1 cell, and reports whether the slot is null.
func v2TextOf(arr arrow.Array, i int) (string, bool) {
	if arr.IsNull(i) {
		return "", true
	}
	switch a := arr.(type) {
	case *array.Null:
		return "", true
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10), false
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10), false
	case *array.Float64:
		return strconv.FormatFloat(a.Value(i), 'f', -1, 64), false
	case *array.Boolean:
		if a.Value(i) {
			return "true", false
		}
		return "false", false
	case *array.String:
		return a.Value(i), false
	case *array.LargeString:
		return a.Value(i), false
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit).Format(time.RFC3339), false
	case *array.Date32:
		return a.Value(i).ToTime().Format("2006-01-02"), false
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return a.Value(i).ToTime(unit).Format("15:04:05"), false
	default:
		return arr.ValueStr(i), false
	}
}

type lookupCase struct {
	name       string
	left       [][]string
	right      [][]string
	targetName string
	config     *LookupConfiguration
}

// run converts both datasets, runs both operators and returns their outputs
// along with the errors they produced.
func (c lookupCase) run(t *testing.T) (*types.DataSet, *types.DataSetv2, error, error) {
	t.Helper()

	cfgJSON, err := json.Marshal(c.config)
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}

	leftV1, err := conversion.ConvertToTypedData(c.left, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 left conversion failed: %v", err)
	}
	rightV1, err := conversion.ConvertToTypedData(c.right, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 right conversion failed: %v", err)
	}
	leftV2, err := conversion.ConvertToTypedDatav2(c.left, true, true, nil, true)
	if err != nil {
		t.Fatalf("v2 left conversion failed: %v", err)
	}
	rightV2, err := conversion.ConvertToTypedDatav2(c.right, true, true, nil, true)
	if err != nil {
		t.Fatalf("v2 right conversion failed: %v", err)
	}

	targetName := c.targetName
	if len(targetName) == 0 {
		targetName = c.config.TargetDataset
	}

	outV1, errV1 := (&operator.LookupOperator{}).Transform(leftV1, string(cfgJSON),
		map[string]*types.DataSet{targetName: rightV1})
	outV2, errV2 := (&LookupOperator{}).Transform(leftV2, string(cfgJSON),
		map[string]*types.DataSetv2{targetName: rightV2})

	return outV1, outV2, errV1, errV2
}

// assertSameOutput fails unless v1 and v2 produced the same columns, in the
// same order, holding the same values.
func assertSameOutput(t *testing.T, v1 *types.DataSet, v2 *types.DataSetv2) {
	t.Helper()

	if len(v1.Rows) != int(v2.NumRows()) {
		t.Fatalf("row count mismatch: v1 has %d, v2 has %d", len(v1.Rows), v2.NumRows())
	}
	if len(v1.Rows) == 0 {
		return
	}

	v1Names := make([]string, len(v1.Rows[0].Columns))
	for i, c := range v1.Rows[0].Columns {
		v1Names[i] = c.ColumnName
	}
	v2Names := make([]string, v2.NumCols())
	for i := range v2.NumCols() {
		v2Names[i] = v2.Column(int(i)).Name()
	}

	if len(v1Names) != len(v2Names) {
		t.Fatalf("column count mismatch: v1 has %d %v, v2 has %d %v",
			len(v1Names), v1Names, len(v2Names), v2Names)
	}
	for i := range v1Names {
		if v1Names[i] != v2Names[i] {
			t.Fatalf("column %d name mismatch: v1 %q, v2 %q (v1 %v, v2 %v)",
				i, v1Names[i], v2Names[i], v1Names, v2Names)
		}
	}

	for ci := range v2Names {
		flat := newFlatColumn(v2.Column(ci))
		for ri, row := range v1.Rows {
			if len(row.Columns) != len(v1Names) {
				t.Fatalf("v1 row %d has %d columns, row 0 has %d", ri, len(row.Columns), len(v1Names))
			}
			if row.Columns[ci].ColumnName != v1Names[ci] {
				t.Fatalf("v1 row %d column %d is named %q, row 0 calls it %q",
					ri, ci, row.Columns[ci].ColumnName, v1Names[ci])
			}

			wantText, wantNull := v1TextOf(row.Columns[ci])
			arr, idx, found := flat.at(int64(ri))
			if !found {
				t.Fatalf("v2 column %q has no row %d", v2Names[ci], ri)
			}
			gotText, gotNull := v2TextOf(arr, idx)

			if wantNull != gotNull {
				t.Errorf("column %q row %d: v1 null = %v, v2 null = %v (v1 %q, v2 %q)",
					v2Names[ci], ri, wantNull, gotNull, wantText, gotText)
				continue
			}
			if wantText != gotText {
				t.Errorf("column %q row %d: v1 = %q, v2 = %q", v2Names[ci], ri, wantText, gotText)
				continue
			}
			// a non-null cell should also carry the same type on both sides
			if !wantNull {
				v1Type := row.Columns[ci].CellValue.DataType
				v2Type := conversion.FiltrifyType(arr.DataType())
				if v1Type != v2Type {
					t.Errorf("column %q row %d: v1 type %v, v2 type %v",
						v2Names[ci], ri, v1Type, v2Type)
				}
			}
		}
	}
}

func runParityCase(t *testing.T, c lookupCase) (*types.DataSet, *types.DataSetv2) {
	t.Helper()
	outV1, outV2, errV1, errV2 := c.run(t)
	if errV1 != nil {
		t.Fatalf("v1 lookup failed: %v", errV1)
	}
	if errV2 != nil {
		t.Fatalf("v2 lookup failed: %v", errV2)
	}
	assertSameOutput(t, outV1, outV2)
	return outV1, outV2
}

func TestLookupV2MatchesV1(t *testing.T) {
	cases := []lookupCase{
		{
			name: "basic join on a single int column",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
			},
		},
		{
			name: "selected columns only",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				SelectedColumns: []string{"Instrument name", "Currency"},
			},
		},
		{
			name: "selected columns are matched case insensitively",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				SelectedColumns: []string{"instrument NAME", "rEgIoN"},
			},
		},
		{
			name: "multiple right rows share a join key - first one wins",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ISIN", Right: "ISIN"},
				},
			},
		},
		{
			name: "composite join key",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ISIN", Right: "ISIN"},
					{Left: "Currency", Right: "Currency"},
				},
			},
		},
		{
			name: "right match column removed",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				RemoveRightMatchColumn: true,
			},
		},
		{
			name: "right match columns removed on a composite key",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ISIN", Right: "ISIN"},
					{Left: "Currency", Right: "Currency"},
				},
				RemoveRightMatchColumn: true,
			},
		},
		{
			name: "right dataset prefix removed - names collide and get suffixed",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				RemoveRightDatasetPrefix: true,
			},
		},
		{
			name: "prefix removed and match column removed",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				RemoveRightMatchColumn:   true,
				RemoveRightDatasetPrefix: true,
			},
		},
		{
			name: "prefix removed with selected columns",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
				RemoveRightDatasetPrefix: true,
				SelectedColumns:          []string{"Region"},
			},
		},
		{
			name: "a target dataset name that needs no prefix escaping",
			config: &LookupConfiguration{
				TargetDataset: "ds",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
			},
		},
		{
			name: "join on a string column with empty values on both sides",
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Currency", Right: "Currency"},
				},
			},
		},
		{
			name:  "right side has a single row",
			right: lookupRightData[:2],
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
			},
		},
		{
			name:  "nothing on the left matches anything on the right",
			right: [][]string{lookupRightData[0], {"99", "Nothing", "XX0000000000", "XXX", "Nowhere"}},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
			},
		},
		{
			name: "join keys of different types compare as text",
			left: [][]string{
				{"Name", "ID"},
				{"first", "1"},
				{"second", "2"},
				{"third", "3"},
			},
			right: [][]string{
				{"ID", "Label"},
				{"1", "one"},
				{"2.0", "two point zero"},
				{"3", "three"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ID", Right: "ID"},
				},
			},
		},
		{
			name: "null join keys never match, not even each other",
			left: [][]string{
				{"Name", "ID"},
				{"has an id", "1"},
				{"has no id", ""},
			},
			right: [][]string{
				{"ID", "Label"},
				{"", "empty id"},
				{"1", "one"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ID", Right: "ID"},
				},
			},
		},
		{
			name: "a right column that is null in every row",
			left: [][]string{
				{"Name", "ID"},
				{"first", "1"},
				{"second", "2"},
			},
			right: [][]string{
				{"ID", "Label", "Always Empty"},
				{"1", "one", ""},
				{"2", "two", ""},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "ID", Right: "ID"},
				},
			},
		},
		{
			name: "join on a date column",
			left: [][]string{
				{"Name", "Trade Date"},
				{"first", "2021-12-31"},
				{"second", "2022-01-30"},
				{"third", ""},
			},
			right: [][]string{
				{"Trade Date", "Label"},
				{"2021-12-31", "new year's eve"},
				{"2022-01-30", "end of january"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Trade Date", Right: "Trade Date"},
				},
			},
		},
		{
			name: "join on a timestamp column",
			left: [][]string{
				{"Name", "Active From"},
				{"first", "2020-01-01 12:00:00"},
				{"second", "2020-03-01 12:00:00"},
				{"third", "2021-04-06 13:30:00"},
			},
			right: [][]string{
				{"Active From", "Label"},
				{"2020-01-01 12:00:00", "january"},
				{"2021-04-06 13:30:00", "april"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Active From", Right: "Active From"},
				},
			},
		},
		{
			name: "join on a time of day column",
			left: [][]string{
				{"Name", "Opens At"},
				{"first", "09:30:00"},
				{"second", "17:00:00"},
			},
			right: [][]string{
				{"Opens At", "Label"},
				{"09:30:00", "morning"},
				{"23:59:00", "midnight"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Opens At", Right: "Opens At"},
				},
			},
		},
		{
			name: "join on a boolean column",
			left: [][]string{
				{"Name", "Listed"},
				{"first", "true"},
				{"second", "false"},
				{"third", ""},
			},
			right: [][]string{
				{"Listed", "Label"},
				{"true", "on the list"},
				{"false", "not on the list"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Listed", Right: "Listed"},
				},
			},
		},
		{
			name: "join on a double column",
			left: [][]string{
				{"Name", "Weight"},
				{"first", "1.5"},
				{"second", "2.25"},
				{"third", "3.0"},
			},
			right: [][]string{
				{"Weight", "Label"},
				{"1.5", "one and a half"},
				{"2.25", "two and a quarter"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Weight", Right: "Weight"},
				},
			},
		},
		{
			name: "a composite key whose parts could otherwise be confused",
			left: [][]string{
				{"Name", "A", "B"},
				{"first", "ab", "c"},
				{"second", "a", "bc"},
			},
			right: [][]string{
				{"A", "B", "Label"},
				{"ab", "c", "ab + c"},
				{"a", "bc", "a + bc"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "A", Right: "A"},
					{Left: "B", Right: "B"},
				},
			},
		},
		{
			name: "the same left column joined to two right columns",
			left: [][]string{
				{"Name", "Code"},
				{"first", "x"},
				{"second", "y"},
			},
			right: [][]string{
				{"Code", "Other Code", "Label"},
				{"x", "x", "both x"},
				{"y", "z", "y and z"},
			},
			config: &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Code", Right: "Code"},
					{Left: "Code", Right: "Other Code"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.left == nil {
				c.left = lookupLeftData
			}
			if c.right == nil {
				c.right = lookupRightData
			}
			runParityCase(t, c)
		})
	}
}

// A selectedColumns entry naming a column the right dataset doesn't have asks
// for more columns than the join can produce. v1 crashes on it whenever any
// left row fails to match: mergeRows fills the leftover slots with "column_%d"
// placeholders but mergeNilRow doesn't, so buildHeaders dereferences a nil
// column. v2 keeps v1's placeholder behaviour for every row instead, which is
// the only part of v1 that is actually reachable - so this case is deliberately
// out of the parity set above, and pinned here on its own.
func TestLookupV2FillsPlaceholdersForUnknownSelectedColumns(t *testing.T) {
	cfg := `{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID",` +
		`"right":"Instrument ID"}],"selectedColumns":["Region","No Such Column"]}`

	// v1 panics instead of returning, so pin that first - if it ever gets
	// fixed this test should be folded back into the parity set
	v1Panicked := func() (panicked bool) {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		leftV1, err := conversion.ConvertToTypedData(lookupLeftData, true, true, nil, true)
		if err != nil {
			t.Fatalf("v1 left conversion failed: %v", err)
		}
		rightV1, err := conversion.ConvertToTypedData(lookupRightData, true, true, nil, true)
		if err != nil {
			t.Fatalf("v1 right conversion failed: %v", err)
		}
		_, _ = (&operator.LookupOperator{}).Transform(leftV1, cfg,
			map[string]*types.DataSet{"Instrument Data": rightV1})
		return false
	}()
	if !v1Panicked {
		t.Errorf("v1 no longer panics on an unknown selected column - " +
			"move this case back into TestLookupV2MatchesV1")
	}

	left, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}
	right, err := conversion.ConvertToTypedDatav2(lookupRightData, true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}
	out, err := (&LookupOperator{}).Transform(left, cfg,
		map[string]*types.DataSetv2{"Instrument Data": right})
	if err != nil {
		t.Fatalf("v2 lookup failed: %v", err)
	}

	// one real joined column plus one placeholder, exactly as v1's targetLength
	// arithmetic promised
	wantColumns := []string{
		"Instrument name", "Instrument ID", "Quantity", "ISIN", "Currency",
		"Instrument Data.Region", "column_6",
	}
	if int(out.NumCols()) != len(wantColumns) {
		t.Fatalf("expected %d columns, got %d", len(wantColumns), out.NumCols())
	}
	for i, want := range wantColumns {
		if got := out.Column(i).Name(); got != want {
			t.Errorf("column %d: expected %q, got %q", i, want, got)
		}
	}

	// the placeholder is null in every row
	placeholder := out.GetColumn("column_6")
	if placeholder == nil {
		t.Fatalf("the placeholder column is missing")
	}
	if placeholder.NullN() != int(out.NumRows()) {
		t.Errorf("expected the placeholder to be null in all %d rows, got %d nulls",
			out.NumRows(), placeholder.NullN())
	}
}

// The filtered target dataset goes through the filter operator of the matching
// generation, so this checks the whole pipeline rather than just the join.
func TestLookupV2TargetDatasetFiltersMatchV1(t *testing.T) {
	cases := []struct {
		name    string
		filters map[string]LookupFilter
	}{
		{
			name: "a string equality filter",
			filters: map[string]LookupFilter{
				"Region": {
					Value: "Europe",
					Filter: `{"filterCriteria":{"criteria":` +
						`{"field":"Region","operator":"=","value":"Europe"}}}`,
				},
			},
		},
		{
			name: "a filter that keeps nothing",
			filters: map[string]LookupFilter{
				"Region": {
					Value: "Atlantis",
					Filter: `{"filterCriteria":{"criteria":` +
						`{"field":"Region","operator":"=","value":"Atlantis"}}}`,
				},
			},
		},
		{
			name: "a filter that keeps everything",
			filters: map[string]LookupFilter{
				"Region": {
					Value: "any",
					Filter: `{"filterCriteria":{"criteria":` +
						`{"field":"Region","operator":"!=","value":"Atlantis"}}}`,
				},
			},
		},
		{
			name: "an empty filter string is skipped",
			filters: map[string]LookupFilter{
				"Region": {Value: "Europe", Filter: ""},
			},
		},
		{
			name: "a numeric comparison filter",
			filters: map[string]LookupFilter{
				"Instrument ID": {
					Value: "3",
					Filter: `{"filterCriteria":{"criteria":` +
						"{\"field\":\"Instrument ID\",\"operator\":\">\",\"value\":\"3\"}}}",
				},
			},
		},
	}

	for _, fc := range cases {
		t.Run(fc.name, func(t *testing.T) {
			runParityCase(t, lookupCase{
				left:  lookupLeftData,
				right: lookupRightData,
				config: &LookupConfiguration{
					TargetDataset: "Instrument Data",
					Columns: []*JoinColumn{
						{Left: "Instrument ID", Right: "Instrument ID"},
					},
					TargetDatasetFilters: fc.filters,
				},
			})
		})
	}
}

// Both generations short-circuit and hand the left dataset straight back when
// either side is empty.
func TestLookupV2EmptySidesMatchV1(t *testing.T) {
	cases := []lookupCase{
		{
			name:  "no rows on the right",
			right: lookupRightData[:1],
		},
		{
			name: "no rows on the left",
			left: lookupLeftData[:1],
		},
		{
			name:  "no rows on either side",
			left:  lookupLeftData[:1],
			right: lookupRightData[:1],
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.left == nil {
				c.left = lookupLeftData
			}
			if c.right == nil {
				c.right = lookupRightData
			}
			c.config = &LookupConfiguration{
				TargetDataset: "Instrument Data",
				Columns: []*JoinColumn{
					{Left: "Instrument ID", Right: "Instrument ID"},
				},
			}

			outV1, outV2, errV1, errV2 := c.run(t)
			if errV1 != nil || errV2 != nil {
				t.Fatalf("lookup failed: v1 %v, v2 %v", errV1, errV2)
			}
			// the left dataset comes back untouched
			if len(outV1.Rows) > 0 && len(outV1.Rows[0].Columns) != len(c.left[0]) {
				t.Fatalf("v1 returned %d columns, the left dataset has %d",
					len(outV1.Rows[0].Columns), len(c.left[0]))
			}
			if int(outV2.NumCols()) != len(c.left[0]) {
				t.Fatalf("v2 returned %d columns, the left dataset has %d",
					outV2.NumCols(), len(c.left[0]))
			}
			assertSameOutput(t, outV1, outV2)
		})
	}
}

// Configuration and column errors have to come out of both generations alike.
func TestLookupV2ErrorsMatchV1(t *testing.T) {
	cases := []struct {
		name       string
		config     string
		targetName string
	}{
		{
			name:   "an empty configuration",
			config: "",
		},
		{
			name:   "a configuration that is not json",
			config: "not json at all",
		},
		{
			name:   "a missing target dataset name",
			config: `{"columns":[{"left":"Instrument ID","right":"Instrument ID"}]}`,
		},
		{
			name:   "no join columns",
			config: `{"targetDataset":"Instrument Data","columns":[]}`,
		},
		{
			name:   "a join column with no left side",
			config: `{"targetDataset":"Instrument Data","columns":[{"right":"Instrument ID"}]}`,
		},
		{
			name:   "a join column with no right side",
			config: `{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID"}]}`,
		},
		{
			name:       "a target dataset that was never passed in",
			config:     `{"targetDataset":"Missing Data","columns":[{"left":"Instrument ID","right":"Instrument ID"}]}`,
			targetName: "Instrument Data",
		},
		{
			name:   "a left column that does not exist",
			config: `{"targetDataset":"Instrument Data","columns":[{"left":"No Such Column","right":"Instrument ID"}]}`,
		},
		{
			name:   "a right column that does not exist",
			config: `{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID","right":"No Such Column"}]}`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			leftV1, err := conversion.ConvertToTypedData(lookupLeftData, true, true, nil, true)
			if err != nil {
				t.Fatalf("v1 left conversion failed: %v", err)
			}
			rightV1, err := conversion.ConvertToTypedData(lookupRightData, true, true, nil, true)
			if err != nil {
				t.Fatalf("v1 right conversion failed: %v", err)
			}
			leftV2, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
			if err != nil {
				t.Fatalf("v2 left conversion failed: %v", err)
			}
			rightV2, err := conversion.ConvertToTypedDatav2(lookupRightData, true, true, nil, true)
			if err != nil {
				t.Fatalf("v2 right conversion failed: %v", err)
			}

			targetName := c.targetName
			if len(targetName) == 0 {
				targetName = "Instrument Data"
			}

			_, errV1 := (&operator.LookupOperator{}).Transform(leftV1, c.config,
				map[string]*types.DataSet{targetName: rightV1})
			_, errV2 := (&LookupOperator{}).Transform(leftV2, c.config,
				map[string]*types.DataSetv2{targetName: rightV2})

			if errV1 == nil {
				t.Fatalf("v1 accepted the configuration, so this is not an error case")
			}
			if errV2 == nil {
				t.Fatalf("v2 accepted a configuration v1 rejected with: %v", errV1)
			}
			if errV1.Error() != errV2.Error() {
				t.Errorf("error mismatch:\n  v1: %v\n  v2: %v", errV1, errV2)
			}

			// ValidateConfiguration has to agree too
			okV1, valErrV1 := (&operator.LookupOperator{}).ValidateConfiguration(c.config)
			okV2, valErrV2 := (&LookupOperator{}).ValidateConfiguration(c.config)
			if okV1 != okV2 {
				t.Errorf("ValidateConfiguration mismatch: v1 %v, v2 %v", okV1, okV2)
			}
			if (valErrV1 == nil) != (valErrV2 == nil) {
				t.Errorf("ValidateConfiguration error mismatch: v1 %v, v2 %v", valErrV1, valErrV2)
			}
			if valErrV1 != nil && valErrV2 != nil && valErrV1.Error() != valErrV2.Error() {
				t.Errorf("ValidateConfiguration error mismatch:\n  v1: %v\n  v2: %v", valErrV1, valErrV2)
			}
		})
	}
}

func TestLookupV2ValidateConfigurationMatchesV1(t *testing.T) {
	configs := []string{
		`{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID","right":"Instrument ID"}]}`,
		`{"targetDataset":"Instrument Data","columns":[{"left":"a","right":"b"},{"left":"c","right":"d"}]}`,
		`{"targetDataset":"Instrument Data","columns":[{"left":"a","right":"b"}],` +
			`"removeRightMatchColumn":true,"removeRightDatasetPrefix":true,"selectedColumns":["x"]}`,
	}

	for i, cfg := range configs {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			okV1, errV1 := (&operator.LookupOperator{}).ValidateConfiguration(cfg)
			okV2, errV2 := (&LookupOperator{}).ValidateConfiguration(cfg)
			if okV1 != okV2 {
				t.Errorf("ValidateConfiguration mismatch: v1 %v, v2 %v", okV1, okV2)
			}
			if (errV1 == nil) != (errV2 == nil) {
				t.Errorf("ValidateConfiguration error mismatch: v1 %v, v2 %v", errV1, errV2)
			}
		})
	}
}

// The configuration JSON both generations read has to stay interchangeable.
func TestLookupV2ConfigurationIsWireCompatibleWithV1(t *testing.T) {
	v2Config := &LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*JoinColumn{
			{Left: "Instrument ID", Right: "Instrument ID"},
		},
		RemoveRightMatchColumn:   true,
		RemoveRightDatasetPrefix: true,
		SelectedColumns:          []string{"Region"},
		TargetDatasetFilters: map[string]LookupFilter{
			"Region": {Value: "Europe", Filter: "some filter"},
		},
	}
	v1Config := &operator.LookupConfiguration{
		TargetDataset: "Instrument Data",
		Columns: []*operator.JoinColumn{
			{Left: "Instrument ID", Right: "Instrument ID"},
		},
		RemoveRightMatchColumn:   true,
		RemoveRightDatasetPrefix: true,
		SelectedColumns:          []string{"Region"},
		TargetDatasetFilters: map[string]operator.LookupFilter{
			"Region": {Value: "Europe", Filter: "some filter"},
		},
	}

	v2JSON, err := json.Marshal(v2Config)
	if err != nil {
		t.Fatalf("could not marshal the v2 configuration: %v", err)
	}
	v1JSON, err := json.Marshal(v1Config)
	if err != nil {
		t.Fatalf("could not marshal the v1 configuration: %v", err)
	}
	if string(v1JSON) != string(v2JSON) {
		t.Fatalf("configuration JSON differs:\n  v1: %s\n  v2: %s", v1JSON, v2JSON)
	}

	// and v2 has to read back what v1 wrote
	readBack, err := (&LookupOperator{}).buildConfiguration(string(v1JSON))
	if err != nil {
		t.Fatalf("v2 could not read the v1 configuration: %v", err)
	}
	roundTripped, err := json.Marshal(readBack)
	if err != nil {
		t.Fatalf("could not marshal the round tripped configuration: %v", err)
	}
	if string(roundTripped) != string(v1JSON) {
		t.Fatalf("configuration did not survive the round trip:\n  before: %s\n  after:  %s",
			v1JSON, roundTripped)
	}
}

// A spot check with values written out by hand, so the parity tests can't pass
// by both generations being wrong in the same way.
func TestLookupV2BasicJoinValues(t *testing.T) {
	left, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}
	right, err := conversion.ConvertToTypedDatav2(lookupRightData, true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}

	cfg := `{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID","right":"Instrument ID"}]}`
	out, err := (&LookupOperator{}).Transform(left, cfg,
		map[string]*types.DataSetv2{"Instrument Data": right})
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}

	wantColumns := []string{
		"Instrument name", "Instrument ID", "Quantity", "ISIN", "Currency",
		"Instrument Data.Instrument ID", "Instrument Data.Instrument name",
		"Instrument Data.ISIN", "Instrument Data.Currency", "Instrument Data.Region",
	}
	if int(out.NumCols()) != len(wantColumns) {
		t.Fatalf("expected %d columns, got %d", len(wantColumns), out.NumCols())
	}
	for i, want := range wantColumns {
		if got := out.Column(i).Name(); got != want {
			t.Errorf("column %d: expected %q, got %q", i, want, got)
		}
	}
	if out.NumRows() != 6 {
		t.Fatalf("expected 6 rows, got %d", out.NumRows())
	}

	// "" for a cell that should be null
	wantRegions := []string{"Europe", "Americas", "Americas", "Americas", "", "Europe"}
	regionCol := newFlatColumn(out.GetColumn("Instrument Data.Region"))
	for ri, want := range wantRegions {
		arr, idx, found := regionCol.at(int64(ri))
		if !found {
			t.Fatalf("no row %d in the region column", ri)
		}
		got, isNull := v2TextOf(arr, idx)
		if want == "" {
			// the fifth row has no instrument id, so it joins to nothing
			if !isNull {
				t.Errorf("row %d: expected a null region, got %q", ri, got)
			}
			continue
		}
		if isNull || got != want {
			t.Errorf("row %d: expected region %q, got %q (null = %v)", ri, want, got, isNull)
		}
	}

	// the joined instrument names come from the right dataset, matched by id
	wantJoinedNames := []string{
		"ERIC B SS Equity", "AMZN US Equity", "T 0 12/31/21", "ESZ1", "", "ERIC B LN Equity",
	}
	nameCol := newFlatColumn(out.GetColumn("Instrument Data.Instrument name"))
	for ri, want := range wantJoinedNames {
		arr, idx, _ := nameCol.at(int64(ri))
		got, isNull := v2TextOf(arr, idx)
		if want == "" {
			if !isNull {
				t.Errorf("row %d: expected a null instrument name, got %q", ri, got)
			}
			continue
		}
		if isNull || got != want {
			t.Errorf("row %d: expected instrument name %q, got %q (null = %v)", ri, want, got, isNull)
		}
	}
}

// Row counts never change: a lookup is a left join that keeps the first match.
func TestLookupV2KeepsEveryLeftRowOnce(t *testing.T) {
	left, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}
	right, err := conversion.ConvertToTypedDatav2(lookupRightData, true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}

	// "ISIN" repeats on the right, so a naive join would multiply the rows
	cfg := `{"targetDataset":"Instrument Data","columns":[{"left":"ISIN","right":"ISIN"}]}`
	out, err := (&LookupOperator{}).Transform(left, cfg,
		map[string]*types.DataSetv2{"Instrument Data": right})
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if out.NumRows() != left.NumRows() {
		t.Fatalf("expected %d rows, got %d", left.NumRows(), out.NumRows())
	}

	// SE0000108656 appears twice on the right (rows 1 and 5) - the first one wins
	ids := newFlatColumn(out.GetColumn("Instrument Data.Instrument ID"))
	isins := newFlatColumn(out.GetColumn("ISIN"))
	for ri := int64(0); ri < out.NumRows(); ri++ {
		isinArr, isinIdx, _ := isins.at(ri)
		isin, _ := v2TextOf(isinArr, isinIdx)
		if isin != "SE0000108656" {
			continue
		}
		idArr, idIdx, _ := ids.at(ri)
		got, isNull := v2TextOf(idArr, idIdx)
		if isNull || got != "1" {
			t.Errorf("row %d: expected the first matching instrument id (1), got %q (null = %v)",
				ri, got, isNull)
		}
	}
}

// A dataset with more than one chunk has to join just like a single chunk one.
func TestLookupV2HandlesChunkedColumns(t *testing.T) {
	left, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}
	singleChunkRight, err := conversion.ConvertToTypedDatav2(lookupRightData, true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}
	// the same rows split in two: rows 0-1 and rows 2-4
	firstHalf, err := conversion.ConvertToTypedDatav2(lookupRightData[:3], true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}
	secondHalf, err := conversion.ConvertToTypedDatav2(
		append([][]string{lookupRightData[0]}, lookupRightData[3:]...), true, true, nil, true)
	if err != nil {
		t.Fatalf("right conversion failed: %v", err)
	}
	chunkedRight := concatChunks(t, firstHalf, secondHalf)

	for i := range chunkedRight.NumCols() {
		if n := len(chunkedRight.Column(int(i)).Data().Chunks()); n != 2 {
			t.Fatalf("column %d has %d chunks, expected 2", i, n)
		}
	}

	cfg := `{"targetDataset":"Instrument Data","columns":[{"left":"Instrument ID","right":"Instrument ID"}]}`
	want, err := (&LookupOperator{}).Transform(left, cfg,
		map[string]*types.DataSetv2{"Instrument Data": singleChunkRight})
	if err != nil {
		t.Fatalf("lookup over a single chunk failed: %v", err)
	}
	got, err := (&LookupOperator{}).Transform(left, cfg,
		map[string]*types.DataSetv2{"Instrument Data": chunkedRight})
	if err != nil {
		t.Fatalf("lookup over two chunks failed: %v", err)
	}

	if want.NumCols() != got.NumCols() || want.NumRows() != got.NumRows() {
		t.Fatalf("shape mismatch: single chunk %dx%d, two chunks %dx%d",
			want.NumRows(), want.NumCols(), got.NumRows(), got.NumCols())
	}
	for ci := range want.NumCols() {
		wantCol := newFlatColumn(want.Column(int(ci)))
		gotCol := newFlatColumn(got.Column(int(ci)))
		name := want.Column(int(ci)).Name()
		if got.Column(int(ci)).Name() != name {
			t.Fatalf("column %d name mismatch: %q vs %q", ci, name, got.Column(int(ci)).Name())
		}
		for ri := int64(0); ri < want.NumRows(); ri++ {
			wantArr, wantIdx, _ := wantCol.at(ri)
			gotArr, gotIdx, _ := gotCol.at(ri)
			wantText, wantNull := v2TextOf(wantArr, wantIdx)
			gotText, gotNull := v2TextOf(gotArr, gotIdx)
			if wantNull != gotNull || wantText != gotText {
				t.Errorf("column %q row %d: single chunk %q (null %v), two chunks %q (null %v)",
					name, ri, wantText, wantNull, gotText, gotNull)
			}
		}
	}
}

// concatChunks stitches two tables of the same schema into one whose columns
// each hold two chunks.
func concatChunks(t *testing.T, a *types.DataSetv2, b *types.DataSetv2) *types.DataSetv2 {
	t.Helper()
	if a.NumCols() != b.NumCols() {
		t.Fatalf("cannot concatenate tables of %d and %d columns", a.NumCols(), b.NumCols())
	}

	columns := make([]arrow.Column, 0, a.NumCols())
	fields := make([]arrow.Field, 0, a.NumCols())
	for i := range a.NumCols() {
		left, right := a.Column(int(i)), b.Column(int(i))
		if !arrow.TypeEqual(left.DataType(), right.DataType()) {
			t.Fatalf("column %q has type %s on one side and %s on the other",
				left.Name(), left.DataType(), right.DataType())
		}
		chunks := append(append([]arrow.Array{}, left.Data().Chunks()...), right.Data().Chunks()...)
		chunked := arrow.NewChunked(left.DataType(), chunks)
		defer chunked.Release()
		col := arrow.NewColumn(left.Field(), chunked)
		defer col.Release()
		columns = append(columns, *col)
		fields = append(fields, left.Field())
	}

	schema := arrow.NewSchema(fields, nil)
	return &types.DataSetv2{array.NewTable(schema, columns, a.NumRows()+b.NumRows())}
}

// A sanity check that the parity harness notices a difference at all.
func TestLookupParityHarnessDetectsDifferences(t *testing.T) {
	left, err := conversion.ConvertToTypedData(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}
	leftV2, err := conversion.ConvertToTypedDatav2(lookupLeftData, true, true, nil, true)
	if err != nil {
		t.Fatalf("left conversion failed: %v", err)
	}

	fake := &testing.T{}
	assertSameOutput(fake, left, leftV2)
	if fake.Failed() {
		t.Fatalf("the harness reported a difference between two identical datasets")
	}

	// drop a column on one side only
	trimmed, err := leftV2.RemoveColumn("Currency")
	if err != nil {
		t.Fatalf("could not remove a column: %v", err)
	}
	fake = &testing.T{}
	func() {
		// assertSameOutput calls Fatalf, which runs runtime.Goexit on a real
		// *testing.T - run it on its own goroutine so the test survives
		done := make(chan struct{})
		go func() {
			defer close(done)
			assertSameOutput(fake, left, trimmed)
		}()
		<-done
	}()
	if !fake.Failed() {
		t.Fatalf("the harness did not notice a missing column")
	}
}
