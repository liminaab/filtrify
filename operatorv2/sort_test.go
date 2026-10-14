package operatorv2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
)

// These tests pin the v2 sort operator to the v1 one: every case runs both
// implementations over the same raw data and the same configuration JSON, then
// compares the results (assertSameSortOutput).
//
// The one thing the two generations are NOT held to is which of two tied rows
// comes out first. v1 sorts with a comparator that answers "less" to a tie,
// which is not a strict weak ordering - the order tied rows land in is an
// artifact of the path sort.SliceStable happens to take, not a decision v1
// makes. v2 delegates to arrow's sort, whose ties fall out of its own stable
// ordering. So the tests below pin what the configuration actually asks for -
// the sequence of key values, and the rows present - and leave the placement of
// tied rows to the implementation. assertSameSortOutput checks exactly that, and
// is strict wherever the keys are distinct.

type sortCase struct {
	name   string
	data   [][]string
	config *SortConfiguration
}

// run converts the raw data for both generations, runs both operators and
// returns their outputs along with the errors they produced.
func (c sortCase) run(t *testing.T) (*types.DataSet, *types.DataSetv2, error, error) {
	t.Helper()

	cfgJSON, err := json.Marshal(c.config)
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}

	v1In, err := conversion.ConvertToTypedData(c.data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	v2In, err := conversion.ConvertToTypedDatav2(c.data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v2 conversion failed: %v", err)
	}

	outV1, errV1 := (&operator.SortOperator{}).Transform(v1In, string(cfgJSON), nil)
	outV2, errV2 := (&SortOperator{}).Transform(v2In, string(cfgJSON), nil)
	return outV1, outV2, errV1, errV2
}

func runSortParityCase(t *testing.T, c sortCase) (*types.DataSet, *types.DataSetv2) {
	t.Helper()
	outV1, outV2, errV1, errV2 := c.run(t)
	if errV1 != nil {
		t.Fatalf("v1 sort failed: %v", errV1)
	}
	if errV2 != nil {
		t.Fatalf("v2 sort failed: %v", errV2)
	}
	assertSameSortOutput(t, outV1, outV2, c.config)
	return outV1, outV2
}

// v1RowText renders a whole v1 row as text, one entry per column, "" for a null.
func v1RowText(row *types.DataRow) []string {
	out := make([]string, len(row.Columns))
	for i, c := range row.Columns {
		text, isNull := v1TextOf(c)
		if isNull {
			out[i] = ""
			continue
		}
		out[i] = text
	}
	return out
}

// v2RowText renders row ri of a v2 table the same way.
func v2RowText(t *testing.T, ds *types.DataSetv2, flats []*flatColumn, ri int64) []string {
	t.Helper()
	out := make([]string, len(flats))
	for ci, flat := range flats {
		arr, idx, found := flat.at(ri)
		if !found {
			t.Fatalf("v2 column %d has no row %d", ci, ri)
		}
		text, isNull := v2TextOf(arr, idx)
		if isNull {
			out[ci] = ""
			continue
		}
		out[ci] = text
	}
	return out
}

// assertSameSortOutput holds the two generations to the same sort result, up to
// the placement of rows the configuration cannot tell apart. It checks that
//
//   - the shape and the column names match,
//   - the sequence of order-by key values is identical, row for row, which is
//     the whole of what the configuration asks for, and
//   - within every run of rows the keys tie on, both sides hold the same rows -
//     the same multiset of whole rows, so no row was invented, dropped or
//     altered, only moved among its equals.
//
// Where no two rows tie, every group has one row and this is a strict row by row
// comparison.
func assertSameSortOutput(t *testing.T, v1 *types.DataSet, v2 *types.DataSetv2, config *SortConfiguration) {
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
	flats := make([]*flatColumn, v2.NumCols())
	for i := range v2.NumCols() {
		v2Names[i] = v2.Column(int(i)).Name()
		flats[i] = newFlatColumn(v2.Column(int(i)))
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

	// the columns the configuration orders by, as positions into a rendered row.
	// A column the dataset doesn't have orders nothing, so it is left out - every
	// row then ties on it and the whole result is one tie group.
	keyCols := make([]int, 0, len(config.OrderBy))
	for _, ob := range config.OrderBy {
		for ci, name := range v2Names {
			if name == ob.ColumnName {
				keyCols = append(keyCols, ci)
				break
			}
		}
	}

	v1Rows := make([][]string, len(v1.Rows))
	v2Rows := make([][]string, len(v1.Rows))
	for ri := range v1.Rows {
		if len(v1.Rows[ri].Columns) != len(v1Names) {
			t.Fatalf("v1 row %d has %d columns, row 0 has %d",
				ri, len(v1.Rows[ri].Columns), len(v1Names))
		}
		v1Rows[ri] = v1RowText(v1.Rows[ri])
		v2Rows[ri] = v2RowText(t, v2, flats, int64(ri))
	}

	keyOf := func(row []string) string {
		key := ""
		for _, ci := range keyCols {
			key += row[ci] + "\x00"
		}
		return key
	}

	// the key sequence has to agree row for row - this is the ordering itself
	for ri := range v1Rows {
		if got, want := keyOf(v2Rows[ri]), keyOf(v1Rows[ri]); got != want {
			t.Errorf("row %d: ordered by a different key: v1 %q, v2 %q\n  v1 keys: %v\n  v2 keys: %v",
				ri, want, got, keySequence(v1Rows, keyCols), keySequence(v2Rows, keyCols))
			return
		}
	}

	// and within each run of equal keys both sides have to hold the same rows
	for start := 0; start < len(v1Rows); {
		end := start + 1
		for end < len(v1Rows) && keyOf(v1Rows[end]) == keyOf(v1Rows[start]) {
			end++
		}

		counts := make(map[string]int, end-start)
		for ri := start; ri < end; ri++ {
			counts[strings.Join(v1Rows[ri], "\x00")]++
			counts[strings.Join(v2Rows[ri], "\x00")]--
		}
		for row, n := range counts {
			if n != 0 {
				t.Errorf("rows %d..%d (key %q) differ: row %q appears %+d times in v1\n  v1: %v\n  v2: %v",
					start, end-1, keyOf(v1Rows[start]), strings.ReplaceAll(row, "\x00", "|"), n,
					v1Rows[start:end], v2Rows[start:end])
				break
			}
		}
		start = end
	}

	// a non-null cell has to carry the same type on both sides, whichever row it
	// ended up in - the types are a property of the column, so row 0 settles it
	for ci := range v2Names {
		arr, idx, found := flats[ci].at(0)
		if !found {
			t.Fatalf("v2 column %q has no row 0", v2Names[ci])
		}
		if arr.IsNull(idx) {
			continue
		}
		for _, row := range v1.Rows {
			c := row.Columns[ci]
			if c == nil || c.CellValue == nil || c.CellValue.DataType == types.NilType {
				continue
			}
			if v1Type, v2Type := c.CellValue.DataType, conversion.FiltrifyType(arr.DataType()); v1Type != v2Type {
				t.Errorf("column %q: v1 type %v, v2 type %v", v2Names[ci], v1Type, v2Type)
			}
			break
		}
	}
}

// keySequence renders just the key columns of every row, for error messages.
func keySequence(rows [][]string, keyCols []int) []string {
	out := make([]string, len(rows))
	for ri, row := range rows {
		parts := make([]string, len(keyCols))
		for i, ci := range keyCols {
			parts[i] = row[ci]
		}
		out[ri] = strings.Join(parts, "|")
	}
	return out
}

// orderBy is a shorthand for a configuration ordering by a single column.
func orderBy(column string, ascending bool) *SortConfiguration {
	return &SortConfiguration{
		OrderBy: []*OrderConfiguration{{ColumnName: column, Ascending: ascending}},
	}
}

// Every column of the standard test data, in both directions. The data covers
// strings, ints, doubles, booleans, timestamps and columns with empty cells.
func TestSortV2MatchesV1OnEveryColumn(t *testing.T) {
	columns := []string{
		"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)",
		"Exposure %", "Maturity Date", "EU Sanction listed", "Active From",
	}
	datasets := map[string][][]string{
		"timestamps": test.UAT1TestDataFormatted,
		"dates":      test.UAT1TestDataFormattedWithDate,
		"times":      test.UAT1TestDataFormattedWithTime,
	}

	for dataName, data := range datasets {
		for _, col := range columns {
			for _, ascending := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/ascending=%v", dataName, col, ascending)
				t.Run(name, func(t *testing.T) {
					runSortParityCase(t, sortCase{data: data, config: orderBy(col, ascending)})
				})
			}
		}
	}
}

// Ordering by more than one column: the second entry only gets a say where the
// first one ties.
func TestSortV2MatchesV1OnMultipleColumns(t *testing.T) {
	// "Instrument Type" repeats (two equities), so the second key decides
	tieData := [][]string{
		{"Instrument name", "Instrument Type", "Quantity"},
		{"ERIC B SS Equity", "Equity", "175000.00"},
		{"AMZN US Equity", "Equity", "1500.00"},
		{"T 0 12/31/21", "Bill", "9000000.00"},
		{"ESZ1", "Index Future", "-10.00"},
		{"USD Cash", "Cash Account", "5000000.00"},
		{"MSFT US Equity", "Equity", "1500.00"},
	}

	cases := []sortCase{
		{
			name: "type then quantity, both ascending",
			data: tieData,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Instrument Type", Ascending: true},
				{ColumnName: "Quantity", Ascending: true},
			}},
		},
		{
			name: "type ascending, quantity descending",
			data: tieData,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Instrument Type", Ascending: true},
				{ColumnName: "Quantity", Ascending: false},
			}},
		},
		{
			name: "type descending, quantity ascending",
			data: tieData,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Instrument Type", Ascending: false},
				{ColumnName: "Quantity", Ascending: true},
			}},
		},
		{
			name: "three keys, the last one never consulted",
			data: tieData,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Instrument Type", Ascending: true},
				{ColumnName: "Quantity", Ascending: true},
				{ColumnName: "Instrument name", Ascending: false},
			}},
		},
		{
			name: "the same column twice",
			data: tieData,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Quantity", Ascending: true},
				{ColumnName: "Quantity", Ascending: false},
			}},
		},
		{
			name: "every column of the standard data at once",
			data: test.UAT1TestDataFormatted,
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "EU Sanction listed", Ascending: true},
				{ColumnName: "Maturity Date", Ascending: false},
				{ColumnName: "Market Value (Base)", Ascending: true},
			}},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runSortParityCase(t, c)
		})
	}
}

// v1's comparator reports every tie as "less", which makes it something other
// than a strict weak ordering: the permutation ties come out in is whatever
// sort.SliceStable's internal path happens to land on. v2 therefore has to run
// the same comparisons in the same sequence, not merely produce "an" order
// consistent with the keys. These datasets are deliberately long enough and
// tie-heavy enough to push sort.SliceStable past its insertion-sort block size
// (20) and into the symmerge path, where a different comparison sequence would
// show up as a different permutation.
func TestSortV2MatchesV1OnTieHeavyData(t *testing.T) {
	// a unique "Row" column so a reordering of the ties is visible
	build := func(rows int, distinctKeys int) [][]string {
		data := make([][]string, 0, rows+1)
		data = append(data, []string{"Row", "Key", "Flag"})
		for i := range rows {
			data = append(data, []string{
				strconv.Itoa(i),
				strconv.Itoa(i % distinctKeys),
				strconv.FormatBool(i%3 == 0),
			})
		}
		return data
	}

	cases := []struct {
		rows         int
		distinctKeys int
	}{
		{rows: 8, distinctKeys: 1},    // every row ties
		{rows: 25, distinctKeys: 1},   // every row ties, past the block size
		{rows: 25, distinctKeys: 2},   // two big groups
		{rows: 40, distinctKeys: 3},   // several groups, well past the block size
		{rows: 64, distinctKeys: 8},   // many small groups
		{rows: 100, distinctKeys: 10}, // deeper merges
		{rows: 100, distinctKeys: 1},  // one enormous tie group
	}

	for _, c := range cases {
		data := build(c.rows, c.distinctKeys)
		for _, key := range []string{"Key", "Flag"} {
			for _, ascending := range []bool{true, false} {
				name := fmt.Sprintf("rows=%d/keys=%d/%s/ascending=%v",
					c.rows, c.distinctKeys, key, ascending)
				t.Run(name, func(t *testing.T) {
					runSortParityCase(t, sortCase{data: data, config: orderBy(key, ascending)})
				})
			}
		}
		// and with a second key that only breaks some of the ties
		t.Run(fmt.Sprintf("rows=%d/keys=%d/two keys", c.rows, c.distinctKeys), func(t *testing.T) {
			runSortParityCase(t, sortCase{
				data: data,
				config: &SortConfiguration{OrderBy: []*OrderConfiguration{
					{ColumnName: "Flag", Ascending: true},
					{ColumnName: "Key", Ascending: false},
				}},
			})
		})
	}
}

// Null cells sort before every value in ascending order and after every value
// in descending order, and two nulls tie - v1's CompareColumns says so, and v2
// has to agree.
func TestSortV2MatchesV1OnNulls(t *testing.T) {
	data := [][]string{
		{"Name", "Score", "Label", "When", "Flag"},
		{"a", "3", "beta", "2021-12-31", "true"},
		{"b", "", "", "", ""},
		{"c", "1", "alpha", "2020-01-01", "false"},
		{"d", "", "", "", ""},
		{"e", "2", "gamma", "2021-06-15", "true"},
	}

	for _, col := range []string{"Score", "Label", "When", "Flag"} {
		for _, ascending := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/ascending=%v", col, ascending), func(t *testing.T) {
				runSortParityCase(t, sortCase{data: data, config: orderBy(col, ascending)})
			})
		}
	}

	t.Run("a column that is null in every row", func(t *testing.T) {
		allNull := [][]string{
			{"Name", "Nothing"},
			{"a", ""},
			{"b", ""},
			{"c", ""},
		}
		runSortParityCase(t, sortCase{data: allNull, config: orderBy("Nothing", true)})
		runSortParityCase(t, sortCase{data: allNull, config: orderBy("Nothing", false)})
	})
}

// Shapes that have nothing to reorder, or only one way to do it.
func TestSortV2MatchesV1OnDegenerateInputs(t *testing.T) {
	cases := []sortCase{
		{
			name:   "a single row",
			data:   test.UAT1TestDataFormatted[:2],
			config: orderBy("Market Value (Base)", true),
		},
		{
			name:   "two rows",
			data:   test.UAT1TestDataFormatted[:3],
			config: orderBy("Market Value (Base)", false),
		},
		{
			name:   "no rows at all",
			data:   test.UAT1TestDataFormatted[:1],
			config: orderBy("Market Value (Base)", true),
		},
		{
			name: "a single column",
			data: [][]string{
				{"Only"},
				{"3"},
				{"1"},
				{"2"},
			},
			config: orderBy("Only", true),
		},
		{
			name: "every row identical",
			data: [][]string{
				{"Name", "Score"},
				{"same", "1"},
				{"same", "1"},
				{"same", "1"},
			},
			config: orderBy("Score", true),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runSortParityCase(t, c)
		})
	}
}

// Ordering by a column that isn't there is not an error in either generation:
// v1's comparator records one, Transform throws it away and hands back a nil
// error, and v2 reports nothing either. What they do with the rows differs, and
// deliberately so.
//
// v1's comparator, once it has recorded the error, answers "less" to every
// comparison from that point on - including the ones about keys that do exist.
// A valid key paired with a missing one therefore orders nothing in v1: the
// comparator is poisoned and the result is only an artifact of the sort's
// internal path (it comes back reversed). v2 drops the key it cannot resolve and
// orders by the ones it can, which is what the configuration asks for as far as
// it can be honoured. Where the missing key is the only one, nothing orders
// anything and the rows stay as they were.
func TestSortV2OnAMissingColumn(t *testing.T) {
	// a missing key alone: no ordering to do, and no error
	for _, cfg := range []*SortConfiguration{
		orderBy("No Such Column", true),
		orderBy("No Such Column", false),
		orderBy("instrument type", true), // column names are case sensitive
	} {
		t.Run(cfg.OrderBy[0].ColumnName+"/only key", func(t *testing.T) {
			v1In, err := conversion.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, nil, true)
			if err != nil {
				t.Fatalf("v1 conversion failed: %v", err)
			}
			v2In, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
			if err != nil {
				t.Fatalf("v2 conversion failed: %v", err)
			}
			if _, errV1 := (&operator.SortOperator{}).Transform(v1In, string(mustJSON(t, cfg)), nil); errV1 != nil {
				t.Fatalf("v1 reported an error for a missing column: %v", errV1)
			}
			out, errV2 := (&SortOperator{}).Transform(v2In, string(mustJSON(t, cfg)), nil)
			if errV2 != nil {
				t.Fatalf("v2 reported an error v1 swallowed: %v", errV2)
			}
			// nothing could be ordered, so the rows are untouched
			assertColumnText(t, out, "Instrument name", []string{
				"ERIC B SS Equity", "AMZN US Equity", "T 0 12/31/21", "ESZ1", "USD Cash",
			})
		})
	}

	// a missing key alongside a valid one: v2 orders by the valid one, in either
	// position, and the missing one simply has no say
	for _, c := range []struct {
		name   string
		config *SortConfiguration
	}{
		{
			name: "the second order by column is missing",
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "Instrument Type", Ascending: true},
				{ColumnName: "No Such Column", Ascending: true},
			}},
		},
		{
			name: "the first order by column is missing",
			config: &SortConfiguration{OrderBy: []*OrderConfiguration{
				{ColumnName: "No Such Column", Ascending: true},
				{ColumnName: "Instrument Type", Ascending: true},
			}},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			in, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
			if err != nil {
				t.Fatalf("conversion failed: %v", err)
			}
			out, err := (&SortOperator{}).Transform(in, string(mustJSON(t, c.config)), nil)
			if err != nil {
				t.Fatalf("v2 sort failed: %v", err)
			}
			// ordered by "Instrument Type" alone - the two equities tie and keep
			// their relative order
			assertColumnText(t, out, "Instrument Type", []string{
				"Bill", "Cash Account", "Equity", "Equity", "Index Future",
			})

			// and that is the same thing as ordering by the valid key on its own
			alone, err := (&SortOperator{}).Transform(in,
				string(mustJSON(t, orderBy("Instrument Type", true))), nil)
			if err != nil {
				t.Fatalf("v2 sort failed: %v", err)
			}
			assertColumnText(t, out, "Instrument name", columnText(t, alone, "Instrument name"))
		})
	}
}

// sameError compares two error messages. encoding/json names the Go type it
// failed to unmarshal into, so a malformed configuration produces a message
// that differs by the package qualifier alone ("[]*operator.OrderConfiguration"
// vs "[]*operatorv2.OrderConfiguration") - that one difference is unavoidable
// and is normalized away here.
func sameError(a error, b error) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	normalize := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(s, "operatorv2.", "@"), "operator.", "@")
	}
	return normalize(a.Error()) == normalize(b.Error())
}

// Configuration errors have to come out of both generations alike, and
// ValidateConfiguration has to agree too.
func TestSortV2ConfigurationErrorsMatchV1(t *testing.T) {
	configs := []struct {
		name   string
		config string
	}{
		{name: "an empty configuration", config: ""},
		{name: "a configuration that is not json", config: "not json at all"},
		{name: "an empty json object", config: "{}"},
		{name: "an empty order by list", config: `{"orderBy":[]}`},
		{name: "a null order by list", config: `{"orderBy":null}`},
		{name: "an order by entry with no column name", config: `{"orderBy":[{"ascending":true}]}`},
		{name: "an order by entry with an empty column name", config: `{"orderBy":[{"columnName":"","ascending":true}]}`},
		{
			name:   "a second order by entry with no column name",
			config: `{"orderBy":[{"columnName":"Quantity"},{"ascending":true}]}`,
		},
		{name: "order by is not a list", config: `{"orderBy":"Quantity"}`},
		{name: "ascending is not a bool", config: `{"orderBy":[{"columnName":"Quantity","ascending":"yes"}]}`},
	}

	for _, c := range configs {
		t.Run(c.name, func(t *testing.T) {
			v1In, err := conversion.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, nil, true)
			if err != nil {
				t.Fatalf("v1 conversion failed: %v", err)
			}
			v2In, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
			if err != nil {
				t.Fatalf("v2 conversion failed: %v", err)
			}

			_, errV1 := (&operator.SortOperator{}).Transform(v1In, c.config, nil)
			_, errV2 := (&SortOperator{}).Transform(v2In, c.config, nil)

			if errV1 == nil {
				t.Fatalf("v1 accepted the configuration, so this is not an error case")
			}
			if errV2 == nil {
				t.Fatalf("v2 accepted a configuration v1 rejected with: %v", errV1)
			}
			if !sameError(errV1, errV2) {
				t.Errorf("error mismatch:\n  v1: %v\n  v2: %v", errV1, errV2)
			}

			okV1, valErrV1 := (&operator.SortOperator{}).ValidateConfiguration(c.config)
			okV2, valErrV2 := (&SortOperator{}).ValidateConfiguration(c.config)
			if okV1 != okV2 {
				t.Errorf("ValidateConfiguration mismatch: v1 %v, v2 %v", okV1, okV2)
			}
			if !sameError(valErrV1, valErrV2) {
				t.Errorf("ValidateConfiguration error mismatch:\n  v1: %v\n  v2: %v", valErrV1, valErrV2)
			}
		})
	}
}

func TestSortV2ValidateConfigurationMatchesV1(t *testing.T) {
	configs := []string{
		`{"orderBy":[{"columnName":"Quantity","ascending":true}]}`,
		`{"orderBy":[{"columnName":"Quantity","ascending":false}]}`,
		`{"orderBy":[{"columnName":"Quantity"}]}`,
		`{"orderBy":[{"columnName":"a","ascending":true},{"columnName":"b","ascending":false}]}`,
		`{"orderBy":[{"columnName":"No Such Column","ascending":true}]}`,
	}

	for i, cfg := range configs {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			okV1, errV1 := (&operator.SortOperator{}).ValidateConfiguration(cfg)
			okV2, errV2 := (&SortOperator{}).ValidateConfiguration(cfg)
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
func TestSortV2ConfigurationIsWireCompatibleWithV1(t *testing.T) {
	v2Config := &SortConfiguration{
		OrderBy: []*OrderConfiguration{
			{ColumnName: "Instrument Type", Ascending: true},
			{ColumnName: "Market Value (Base)", Ascending: false},
		},
	}
	v1Config := &operator.SortConfiguration{
		OrderBy: []*operator.OrderConfiguration{
			{ColumnName: "Instrument Type", Ascending: true},
			{ColumnName: "Market Value (Base)", Ascending: false},
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
	readBack, err := (&SortOperator{}).buildConfiguration(string(v1JSON))
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

	// the v1 operator has to accept what v2 wrote, too
	if _, err := (&operator.SortOperator{}).ValidateConfiguration(string(v2JSON)); err != nil {
		t.Fatalf("v1 could not read the v2 configuration: %v", err)
	}
}

// columnText renders a whole v2 column as text, "" standing in for a null.
func columnText(t *testing.T, ds *types.DataSetv2, name string) []string {
	t.Helper()
	col := ds.GetColumn(name)
	if col == nil {
		t.Fatalf("column %q is missing", name)
	}
	flat := newFlatColumn(col)
	out := make([]string, ds.NumRows())
	for ri := int64(0); ri < ds.NumRows(); ri++ {
		arr, idx, found := flat.at(ri)
		if !found {
			t.Fatalf("column %q has no row %d", name, ri)
		}
		text, isNull := v2TextOf(arr, idx)
		if isNull {
			out[ri] = ""
			continue
		}
		out[ri] = text
	}
	return out
}

func assertColumnText(t *testing.T, ds *types.DataSetv2, name string, want []string) {
	t.Helper()
	got := columnText(t, ds, name)
	if len(got) != len(want) {
		t.Fatalf("column %q has %d rows, expected %d (%v)", name, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("column %q row %d: expected %q, got %q (full column %v)", name, i, want[i], got[i], got)
		}
	}
}

// Spot checks with the order written out by hand, so the parity tests can't
// pass by both generations being wrong in the same way.
func TestSortV2OrdersValues(t *testing.T) {
	data := [][]string{
		{"Name", "Score", "Weight", "Listed", "Trade Date", "Active From", "Opens At"},
		{"c", "3", "3.5", "true", "2021-12-31", "2020-03-01 12:00:00", "17:00:00"},
		{"a", "1", "1.5", "false", "2020-01-01", "2020-01-01 12:00:00", "09:30:00"},
		{"d", "", "", "", "", "", ""},
		{"b", "2", "2.5", "true", "2021-06-15", "2021-04-06 13:30:00", "12:15:00"},
	}

	in, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}

	cases := []struct {
		column    string
		ascending bool
		want      []string // the Name column, in the expected order
	}{
		{column: "Name", ascending: true, want: []string{"a", "b", "c", "d"}},
		{column: "Name", ascending: false, want: []string{"d", "c", "b", "a"}},
		// the null row goes first ascending, last descending
		{column: "Score", ascending: true, want: []string{"d", "a", "b", "c"}},
		{column: "Score", ascending: false, want: []string{"c", "b", "a", "d"}},
		{column: "Weight", ascending: true, want: []string{"d", "a", "b", "c"}},
		{column: "Weight", ascending: false, want: []string{"c", "b", "a", "d"}},
		{column: "Trade Date", ascending: true, want: []string{"d", "a", "b", "c"}},
		{column: "Trade Date", ascending: false, want: []string{"c", "b", "a", "d"}},
		{column: "Active From", ascending: true, want: []string{"d", "a", "c", "b"}},
		{column: "Active From", ascending: false, want: []string{"b", "c", "a", "d"}},
		{column: "Opens At", ascending: true, want: []string{"d", "a", "b", "c"}},
		{column: "Opens At", ascending: false, want: []string{"c", "b", "a", "d"}},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/ascending=%v", c.column, c.ascending), func(t *testing.T) {
			cfgJSON, err := json.Marshal(orderBy(c.column, c.ascending))
			if err != nil {
				t.Fatalf("could not marshal the configuration: %v", err)
			}
			out, err := (&SortOperator{}).Transform(in, string(cfgJSON), nil)
			if err != nil {
				t.Fatalf("sort failed: %v", err)
			}
			assertColumnText(t, out, "Name", c.want)
		})
	}

	// false sorts before true, with the null first ascending
	t.Run("Listed/ascending=true", func(t *testing.T) {
		out, err := (&SortOperator{}).Transform(in,
			`{"orderBy":[{"columnName":"Listed","ascending":true}]}`, nil)
		if err != nil {
			t.Fatalf("sort failed: %v", err)
		}
		assertColumnText(t, out, "Listed", []string{"", "false", "true", "true"})
	})
	t.Run("Listed/ascending=false", func(t *testing.T) {
		out, err := (&SortOperator{}).Transform(in,
			`{"orderBy":[{"columnName":"Listed","ascending":false}]}`, nil)
		if err != nil {
			t.Fatalf("sort failed: %v", err)
		}
		assertColumnText(t, out, "Listed", []string{"true", "true", "false", ""})
	})

	// a second key only decides where the first one ties
	t.Run("two keys", func(t *testing.T) {
		tieData := [][]string{
			{"Name", "Group", "Score"},
			{"a", "x", "2"},
			{"b", "y", "1"},
			{"c", "x", "1"},
			{"d", "y", "2"},
		}
		tieIn, err := conversion.ConvertToTypedDatav2(tieData, true, true, nil, true)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		out, err := (&SortOperator{}).Transform(tieIn,
			`{"orderBy":[{"columnName":"Group","ascending":true},`+
				`{"columnName":"Score","ascending":false}]}`, nil)
		if err != nil {
			t.Fatalf("sort failed: %v", err)
		}
		assertColumnText(t, out, "Name", []string{"a", "c", "d", "b"})
	})
}

// A sort moves rows around - it never adds, drops or retypes a column, and it
// never changes the row count.
func TestSortV2KeepsTheShapeOfTheDataset(t *testing.T) {
	in, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}

	out, err := (&SortOperator{}).Transform(in,
		`{"orderBy":[{"columnName":"Market Value (Base)","ascending":false}]}`, nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}

	if out.NumRows() != in.NumRows() {
		t.Fatalf("expected %d rows, got %d", in.NumRows(), out.NumRows())
	}
	if out.NumCols() != in.NumCols() {
		t.Fatalf("expected %d columns, got %d", in.NumCols(), out.NumCols())
	}
	for i := range in.NumCols() {
		want, got := in.Column(int(i)), out.Column(int(i))
		if want.Name() != got.Name() {
			t.Errorf("column %d: expected name %q, got %q", i, want.Name(), got.Name())
		}
		if !arrow.TypeEqual(want.DataType(), got.DataType()) {
			t.Errorf("column %q: expected type %s, got %s", want.Name(), want.DataType(), got.DataType())
		}
		if want.NullN() != got.NullN() {
			t.Errorf("column %q: expected %d nulls, got %d", want.Name(), want.NullN(), got.NullN())
		}
	}

	// the input is left untouched - arrow tables are immutable
	assertColumnText(t, in, "Instrument name", []string{
		"ERIC B SS Equity", "AMZN US Equity", "T 0 12/31/21", "ESZ1", "USD Cash",
	})
}

// The same rows split across two chunks have to sort just like a single chunk.
// The split happens after conversion so both tables are guaranteed to carry the
// same types - converting each half on its own would re-estimate them from
// fewer rows and could land somewhere else.
func TestSortV2HandlesChunkedColumns(t *testing.T) {
	singleChunk, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	chunked := splitIntoChunks(t, singleChunk, 2)

	for i := range chunked.NumCols() {
		if n := len(chunked.Column(int(i)).Data().Chunks()); n != 2 {
			t.Fatalf("column %d has %d chunks, expected 2", i, n)
		}
	}

	columns := []string{
		"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)",
		"Exposure %", "Maturity Date", "EU Sanction listed", "Active From",
	}
	for _, col := range columns {
		for _, ascending := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/ascending=%v", col, ascending), func(t *testing.T) {
				cfg := string(mustJSON(t, orderBy(col, ascending)))
				want, err := (&SortOperator{}).Transform(singleChunk, cfg, nil)
				if err != nil {
					t.Fatalf("sort over a single chunk failed: %v", err)
				}
				got, err := (&SortOperator{}).Transform(chunked, cfg, nil)
				if err != nil {
					t.Fatalf("sort over two chunks failed: %v", err)
				}

				if want.NumRows() != got.NumRows() || want.NumCols() != got.NumCols() {
					t.Fatalf("shape mismatch: single chunk %dx%d, two chunks %dx%d",
						want.NumRows(), want.NumCols(), got.NumRows(), got.NumCols())
				}
				for ci := range want.NumCols() {
					name := want.Column(int(ci)).Name()
					assertColumnText(t, got, name, columnText(t, want, name))
				}
			})
		}
	}
}

// A column of the null type holds nothing but nulls, so every row ties on it
// whichever way it is ordered - exactly like an all-null column of a normal
// type. Arrow's sort kernel refuses the null type outright, so the operator has
// to keep such a key away from it rather than pass it down.
func TestSortV2HandlesANullTypedColumn(t *testing.T) {
	// "Empty" is all blanks, which type estimation calls a string column - so
	// it ends up a string column that is null in every row
	in, err := conversion.ConvertToTypedDatav2([][]string{
		{"Name", "Score", "Empty"},
		{"c", "3", ""},
		{"a", "1", ""},
		{"b", "2", ""},
	}, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	if in.GetColumn("Empty").DataType().ID() != arrow.STRING {
		t.Fatalf("expected an all-null string column, got %s", in.GetColumn("Empty").DataType())
	}

	withNulls, err := in.AddConstantColumn("Nothing", nil, arrow.Null)
	if err != nil {
		t.Fatalf("could not add a null typed column: %v", err)
	}
	if withNulls.GetColumn("Nothing").DataType().ID() != arrow.NULL {
		t.Fatalf("expected a null typed column, got %s", withNulls.GetColumn("Nothing").DataType())
	}

	for _, ascending := range []bool{true, false} {
		t.Run(fmt.Sprintf("ascending=%v", ascending), func(t *testing.T) {
			nullTyped, err := (&SortOperator{}).Transform(withNulls,
				string(mustJSON(t, orderBy("Nothing", ascending))), nil)
			if err != nil {
				t.Fatalf("sort by the null typed column failed: %v", err)
			}
			allNull, err := (&SortOperator{}).Transform(withNulls,
				string(mustJSON(t, orderBy("Empty", ascending))), nil)
			if err != nil {
				t.Fatalf("sort by the all-null string column failed: %v", err)
			}
			assertColumnText(t, nullTyped, "Name", columnText(t, allNull, "Name"))
			assertColumnText(t, nullTyped, "Nothing", []string{"", "", ""})
		})
	}

	// a real second key still decides, since the first one ties everywhere
	out, err := (&SortOperator{}).Transform(withNulls,
		`{"orderBy":[{"columnName":"Nothing","ascending":true},`+
			`{"columnName":"Name","ascending":true}]}`, nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	assertColumnText(t, out, "Name", []string{"a", "b", "c"})
}

// v1's comparator has no case for an object, so ordering by an object column
// ties every row and leaves them where they were. Arrow could order the large
// string filtrify stores an object in, so the operator has to keep that key away
// from it - otherwise v2 would reorder rows v1 does not.
func TestSortV2DoesNotOrderByAnObjectColumn(t *testing.T) {
	base, err := conversion.ConvertToTypedDatav2([][]string{
		{"Name"},
		{"c"},
		{"a"},
		{"b"},
	}, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}

	// objects are stored as JSON text in a large string column. Distinct values
	// in an order the sort would visibly change if it ordered by them at all.
	in, err := objectColumn(t, base, "Payload", []string{`{"z":1}`, `{"a":2}`, `{"m":3}`})
	if err != nil {
		t.Fatalf("could not build an object column: %v", err)
	}
	if got := in.GetColumn("Payload").DataType().ID(); got != arrow.LARGE_STRING {
		t.Fatalf("expected the large string objects are stored in, got %s",
			in.GetColumn("Payload").DataType())
	}
	if conversion.FiltrifyType(in.GetColumn("Payload").DataType()) != types.ObjectType {
		t.Fatalf("expected an object column")
	}

	for _, ascending := range []bool{true, false} {
		t.Run(fmt.Sprintf("ascending=%v", ascending), func(t *testing.T) {
			out, err := (&SortOperator{}).Transform(in,
				string(mustJSON(t, orderBy("Payload", ascending))), nil)
			if err != nil {
				t.Fatalf("sort failed: %v", err)
			}
			// untouched - every row ties on an object
			assertColumnText(t, out, "Name", []string{"c", "a", "b"})
		})
	}

	// and it has no say next to a key that can order
	out, err := (&SortOperator{}).Transform(in,
		`{"orderBy":[{"columnName":"Payload","ascending":true},`+
			`{"columnName":"Name","ascending":true}]}`, nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	assertColumnText(t, out, "Name", []string{"a", "b", "c"})
}

// objectColumn appends a large string column called name holding values - the
// shape filtrify stores an ObjectType in. Type estimation never infers it from
// raw text, so it has to be built directly.
func objectColumn(t *testing.T, ds *types.DataSetv2, name string, values []string) (*types.DataSetv2, error) {
	t.Helper()
	if int64(len(values)) != ds.NumRows() {
		t.Fatalf("got %d values for %d rows", len(values), ds.NumRows())
	}

	builder := array.NewLargeStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	defer arr.Release()

	chunked := arrow.NewChunked(arrow.BinaryTypes.LargeString, []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	field := arrow.Field{Name: name, Type: arrow.BinaryTypes.LargeString, Nullable: true}
	col := arrow.NewColumn(field, chunked) // retains chunked
	defer col.Release()

	return ds.AddColumns(col)
}

// A NaN is the one value v1 and arrow order differently: v1's comparator calls
// it neither greater nor smaller, so it ties with everything, while arrow gives
// it a definite place (just inside the nulls). Nothing in filtrify's own
// behaviour depends on where it lands, so this test pins what v2 actually does
// rather than asserting the two agree - a NaN still has to come out exactly
// once, with every other row correctly ordered around it.
func TestSortV2PlacesNaN(t *testing.T) {
	in, err := conversion.ConvertToTypedDatav2([][]string{
		{"Name", "Val"},
		{"a", "2.5"},
		{"nan", "NaN"},
		{"b", "1.5"},
		{"null", ""},
		{"c", "3.5"},
	}, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}

	// ascending: nulls lead, then the NaN, then the values in order
	asc, err := (&SortOperator{}).Transform(in,
		string(mustJSON(t, orderBy("Val", true))), nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	assertColumnText(t, asc, "Name", []string{"null", "nan", "b", "a", "c"})

	// descending mirrors it
	desc, err := (&SortOperator{}).Transform(in,
		string(mustJSON(t, orderBy("Val", false))), nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	assertColumnText(t, desc, "Name", []string{"c", "a", "b", "nan", "null"})
}

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}
	return b
}

// splitIntoChunks returns the same table with every column cut into n chunks of
// (almost) equal length, so the same rows and types come back in a shape the
// operator has to stitch together itself.
func splitIntoChunks(t *testing.T, ds *types.DataSetv2, n int) *types.DataSetv2 {
	t.Helper()
	if n < 1 || int64(n) > ds.NumRows() {
		t.Fatalf("cannot split %d rows into %d chunks", ds.NumRows(), n)
	}

	// bounds[k] .. bounds[k+1] is the k-th chunk
	bounds := make([]int64, n+1)
	for k := range bounds {
		bounds[k] = int64(k) * ds.NumRows() / int64(n)
	}

	columns := make([]arrow.Column, 0, ds.NumCols())
	fields := make([]arrow.Field, 0, ds.NumCols())
	defer func() {
		for i := range columns {
			columns[i].Release()
		}
	}()

	for ci := range ds.NumCols() {
		col := ds.Column(int(ci))
		if len(col.Data().Chunks()) != 1 {
			t.Fatalf("column %q already holds %d chunks", col.Name(), len(col.Data().Chunks()))
		}
		whole := col.Data().Chunk(0)

		chunks := make([]arrow.Array, n)
		for k := range n {
			chunks[k] = array.NewSlice(whole, bounds[k], bounds[k+1])
			defer chunks[k].Release()
		}
		chunked := arrow.NewChunked(col.DataType(), chunks) // retains each chunk
		defer chunked.Release()
		newCol := arrow.NewColumn(col.Field(), chunked) // retains chunked
		columns = append(columns, *newCol)
		fields = append(fields, col.Field())
	}

	schema := arrow.NewSchema(fields, nil)
	return &types.DataSetv2{Table: array.NewTable(schema, columns, ds.NumRows())}
}

// Sorting an already sorted dataset is a no-op, and sorting the result of a
// sort by the same key changes nothing either.
func TestSortV2IsIdempotent(t *testing.T) {
	in, err := conversion.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}

	cfg := `{"orderBy":[{"columnName":"Instrument name","ascending":true}]}`
	once, err := (&SortOperator{}).Transform(in, cfg, nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	twice, err := (&SortOperator{}).Transform(once, cfg, nil)
	if err != nil {
		t.Fatalf("second sort failed: %v", err)
	}

	for ci := range once.NumCols() {
		name := once.Column(int(ci)).Name()
		assertColumnText(t, twice, name, columnText(t, once, name))
	}
}

// The parity harness has to notice when the two generations disagree, otherwise
// every test above would pass for the wrong reason.
func TestSortParityHarnessDetectsDifferences(t *testing.T) {
	c := sortCase{
		data:   test.UAT1TestDataFormatted,
		config: orderBy("Market Value (Base)", true),
	}
	outV1, outV2, errV1, errV2 := c.run(t)
	if errV1 != nil || errV2 != nil {
		t.Fatalf("sort failed: v1 %v, v2 %v", errV1, errV2)
	}

	fake := &testing.T{}
	assertSameSortOutput(fake, outV1, outV2, c.config)
	if fake.Failed() {
		t.Fatalf("the harness reported a difference between two equal results")
	}

	// the other direction has to look different
	reversedV2, err := (&SortOperator{}).Transform(outV2,
		`{"orderBy":[{"columnName":"Market Value (Base)","ascending":false}]}`, nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	fake = &testing.T{}
	runIsolated(func() { assertSameSortOutput(fake, outV1, reversedV2, c.config) })
	if !fake.Failed() {
		t.Fatalf("the harness did not notice a reversed result")
	}

	// A permutation of the tied rows has to be accepted, since that is the one
	// thing the two generations are not held to - but a row that is not merely
	// moved among its equals has to be caught. "Instrument Type" has two
	// equities, so sorting by it leaves a tie group of two.
	tieCfg := orderBy("Instrument Type", true)
	tieV1, tieV2, errV1, errV2 := sortCase{data: test.UAT1TestDataFormatted, config: tieCfg}.run(t)
	if errV1 != nil || errV2 != nil {
		t.Fatalf("sort failed: v1 %v, v2 %v", errV1, errV2)
	}
	fake = &testing.T{}
	assertSameSortOutput(fake, tieV1, tieV2, tieCfg)
	if fake.Failed() {
		t.Errorf("the harness rejected a difference that is only tie placement")
	}

	// and a genuinely wrong row inside a tie group still has to fail: drop one
	// equity's name so the group no longer holds the same rows
	damaged := make([][]string, len(test.UAT1TestDataFormatted))
	for i, row := range test.UAT1TestDataFormatted {
		damaged[i] = append([]string(nil), row...)
	}
	for i, row := range damaged {
		if i > 0 && row[1] == "Equity" {
			row[0] = "Something Else Entirely"
			break
		}
	}
	damagedV1, err := conversion.ConvertToTypedData(damaged, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	damagedOut, err := (&operator.SortOperator{}).Transform(damagedV1,
		string(mustJSON(t, tieCfg)), nil)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	fake = &testing.T{}
	runIsolated(func() { assertSameSortOutput(fake, damagedOut, tieV2, tieCfg) })
	if !fake.Failed() {
		t.Errorf("the harness did not notice a changed row inside a tie group")
	}
}

// runIsolated runs fn on its own goroutine. The assertions call Fatalf, which
// runs runtime.Goexit on a real *testing.T - on its own goroutine that unwinds
// fn instead of the test.
func runIsolated(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}
