//go:build duckdb_arrow

package operatorv2

import (
	"encoding/json"
	"fmt"
	"sort"
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

// These tests pin the v2 group-by operator to the v1 one: every case runs both
// implementations over the same raw data and the same configuration JSON, then
// compares the results cell by cell.
//
// Two things the two generations are deliberately NOT held to, both because v1
// does not actually decide them:
//
//   - The order the top-level groups come out in. v1 emits them in the order
//     qlbridge's hash map happened to iterate, which differs from run to run
//     over the same input - the same configuration can return "b" before "a"
//     twice and the other way round the third time. So the rows are compared as
//     a multiset (assertSameGroupByRows); what v2 fixes the order to is pinned
//     separately, against the tree shape v1 produces when its iteration happens
//     to come out in order (TestGroupByV2RowOrder, TestGroupByV2IsDeterministic).
//   - The order of the columns after the group-by ones. Same cause: v1 builds
//     that order by ranging over a map. Every column has to be present with the
//     right values, which the comparison checks by name.
//
// Values are compared exactly, except that floats are compared to ten
// significant digits: float addition is not associative, so a sum over a group
// depends on the order the engine adds it up, and the two engines add in
// different orders (see groupByCellText).

// groupByMethodsInParity are the aggregation methods where v1's aggregator and
// the v2 query agree. Everything lmnqlbridge exposes as an aggregate is here
// except mincol and maxcol, which v1 gets wrong - see
// TestGroupByMinMaxColAreCorrected.
var groupByMethodsInParity = []string{
	"sumx", "average", "median", "first", "last", "lmnagg", "weighted_average",
}

// groupByCellText renders a value for comparison.
//
// Floats are rounded to ten significant digits so the comparison is about the
// value rather than about which order each engine summed a group in - arrow also
// renders a float64 in scientific notation where v1's CellValue.ToString writes
// it out in full, so the two need putting in one form regardless.
//
// A trailing ".000000" is dropped for the same reason: arrow renders a time64us
// to microsecond precision while v1 formats a time of day as "15:04:05". Only an
// all-zero fraction is removed, so a real sub-second difference still shows.
func groupByCellText(s string) string {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return strconv.FormatFloat(f, 'g', 10, 64)
	}
	return strings.TrimSuffix(s, ".000000")
}

// groupByRowsV1 renders a v1 result as one "col=value" string per row, with the
// columns sorted by name so the column order does not enter into it.
func groupByRowsV1(ds *types.DataSet) []string {
	out := make([]string, 0, len(ds.Rows))
	for _, r := range ds.Rows {
		parts := make([]string, 0, len(r.Columns))
		for _, c := range r.Columns {
			if c == nil {
				parts = append(parts, "<missing column>")
				continue
			}
			if c.CellValue == nil || c.CellValue.DataType == types.NilType {
				parts = append(parts, c.ColumnName+"=<null>")
				continue
			}
			parts = append(parts, c.ColumnName+"="+groupByCellText(c.CellValue.ToString()))
		}
		sort.Strings(parts)
		out = append(out, strings.Join(parts, ","))
	}
	return out
}

// groupByRowsV2 is groupByRowsV1 for an arrow result, rendering each cell the
// way CellValue.ToString would render the equivalent v1 one.
func groupByRowsV2(ds *types.DataSetv2) []string {
	out := make([]string, 0, ds.NumRows())
	for r := range ds.NumRows() {
		parts := make([]string, 0, ds.NumCols())
		for c := range ds.NumCols() {
			col := ds.Column(int(c))
			text, isNull := cellText(col, int(r))
			if isNull {
				parts = append(parts, col.Name()+"=<null>")
				continue
			}
			parts = append(parts, col.Name()+"="+groupByCellText(text))
		}
		sort.Strings(parts)
		out = append(out, strings.Join(parts, ","))
	}
	return out
}

// runGroupByBoth converts the raw data for both generations, runs both
// operators and returns their outputs along with the errors they produced.
func runGroupByBoth(t *testing.T, data [][]string, conf *GroupByConfiguration) (*types.DataSet, *types.DataSetv2, error, error) {
	t.Helper()

	cfgJSON, err := json.Marshal(conf)
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}

	v1In, err := conversion.ConvertToTypedData(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	v2In, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v2 conversion failed: %v", err)
	}
	t.Cleanup(v2In.Release)

	// v1's configuration type is a different Go type with the same JSON shape,
	// which is the point of passing the marshalled form to both.
	outV1, errV1 := (&operator.GroupByOperator{}).Transform(v1In, string(cfgJSON), nil)
	outV2, errV2 := (&GroupByOperator{}).Transform(v2In, string(cfgJSON), nil)
	if outV2 != nil {
		t.Cleanup(outV2.Release)
	}
	return outV1, outV2, errV1, errV2
}

// assertSameGroupByRows checks that both generations produced the same rows,
// compared as a multiset for the reasons at the top of this file.
func assertSameGroupByRows(t *testing.T, data [][]string, conf *GroupByConfiguration) {
	t.Helper()

	outV1, outV2, errV1, errV2 := runGroupByBoth(t, data, conf)
	if errV1 != nil {
		t.Fatalf("v1 failed: %v", errV1)
	}
	if errV2 != nil {
		t.Fatalf("v2 failed: %v", errV2)
	}

	wantRows, gotRows := groupByRowsV1(outV1), groupByRowsV2(outV2)
	want := append([]string{}, wantRows...)
	got := append([]string{}, gotRows...)
	sort.Strings(want)
	sort.Strings(got)

	if len(want) != len(got) {
		t.Fatalf("row count mismatch: v1 produced %d, v2 produced %d\nv1:\n  %s\nv2:\n  %s",
			len(want), len(got), strings.Join(wantRows, "\n  "), strings.Join(gotRows, "\n  "))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("row sets differ\nv1:\n  %s\nv2:\n  %s",
				strings.Join(want, "\n  "), strings.Join(got, "\n  "))
			return
		}
	}
}

// groupByTestData is small enough to reason about by hand: two group keys with
// overlapping values, a column that is never aggregated, and both an integer and
// a float measure.
var groupByTestData = [][]string{
	{"name", "g1", "g2", "qty", "val"},
	{"r0", "a", "x", "1", "1.5"},
	{"r1", "a", "y", "2", "2.5"},
	{"r2", "a", "x", "3", "3.5"},
	{"r3", "b", "x", "4", "4.5"},
}

// TestGroupByParity is the main parity sweep: every aggregation method, over
// one, two and three group-by levels.
func TestGroupByParity(t *testing.T) {
	for _, method := range groupByMethodsInParity {
		columns := []string{"qty"}
		if method == "weighted_average" {
			// the only method that reads a second column
			columns = []string{"qty", "val"}
		}
		for _, groupBy := range [][]string{
			{"g1"},
			{"g2"},
			{"g1", "g2"},
			{"g2", "g1"},
			{"name"}, // every row its own group
		} {
			name := fmt.Sprintf("%s by %s", method, strings.Join(groupBy, "+"))
			t.Run(name, func(t *testing.T) {
				assertSameGroupByRows(t, groupByTestData, &GroupByConfiguration{
					GroupBy: groupBy,
					Select:  []*AggregateSelect{{Columns: columns, Method: method}},
				})
			})
		}
	}

	t.Run("three levels", func(t *testing.T) {
		assertSameGroupByRows(t, [][]string{
			{"name", "g1", "g2", "g3", "qty"},
			{"r0", "a", "x", "p", "1"},
			{"r1", "a", "x", "q", "2"},
			{"r2", "a", "y", "p", "3"},
			{"r3", "b", "x", "p", "4"},
		}, &GroupByConfiguration{
			GroupBy: []string{"g1", "g2", "g3"},
			Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
		})
	})

	t.Run("several aggregates at once", func(t *testing.T) {
		assertSameGroupByRows(t, groupByTestData, &GroupByConfiguration{
			GroupBy: []string{"g1", "g2"},
			Select: []*AggregateSelect{
				{Columns: []string{"qty"}, Method: "sumx"},
				{Columns: []string{"val"}, Method: "average"},
				{Columns: []string{"name"}, Method: "lmnagg"},
			},
		})
	})

	// No select list at all: the result is the group-by columns and the level,
	// with one row per group and per original row.
	//
	// Only one group-by column is in parity here. With two or more v1 builds a
	// malformed query: AggregateOperator writes the separating comma before each
	// lmnagg only when the select list is non-empty (aggregate.go's "if
	// len(typedConfig.Select) > 0"), so an empty one runs the group-by columns
	// together into `lmnagg(g1) AS g1lmnagg(g2) AS g2`. v1 comes back with just
	// the level-1 rows and loses every deeper level. TestGroupByNoAggregates
	// pins what v2 returns for that case.
	t.Run("no aggregates", func(t *testing.T) {
		assertSameGroupByRows(t, groupByTestData, &GroupByConfiguration{
			GroupBy: []string{"g1"},
		})
	})

	t.Run("single row", func(t *testing.T) {
		assertSameGroupByRows(t, [][]string{
			{"g1", "qty"},
			{"a", "1"},
		}, &GroupByConfiguration{
			GroupBy: []string{"g1"},
			Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
		})
	})
}

// TestGroupByColumnTypeParity runs the sweep over the column types a group key
// can have - v1 compares keys with CellValue.Equals, which is type-aware, so
// each type is its own path.
func TestGroupByColumnTypeParity(t *testing.T) {
	data := [][]string{
		{"name", "num", "dbl", "flag", "day", "clock", "stamp", "qty"},
		{"r0", "10", "1.5", "true", "2021-01-02", "10:30:00", "2021-01-02 10:30:00", "1"},
		{"r1", "10", "2.5", "false", "2021-01-03", "11:30:00", "2021-01-03 11:30:00", "2"},
		{"r2", "10", "1.5", "true", "2021-01-02", "10:30:00", "2021-01-02 10:30:00", "3"},
		{"r3", "20", "2.5", "true", "2021-01-03", "11:30:00", "2021-01-03 11:30:00", "4"},
	}

	// "clock" is left out: a time-of-day key is the one type where v1 cannot
	// build the tree at all. v1 keys every level on lmnagg of the group-by
	// column, and lmnagg has no case for qlbridge's TimeOnlyValue (see
	// liminaAggCalc.Do), so it returns nil for such a column however well the
	// group agrees - which makes v1 drop every level below the first, the same
	// way a null key does. TestGroupByTimeOfDayKey pins what v2 returns.
	for _, key := range []string{"num", "dbl", "flag", "day", "stamp"} {
		t.Run("group by "+key, func(t *testing.T) {
			assertSameGroupByRows(t, data, &GroupByConfiguration{
				GroupBy: []string{key},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
			})
		})
		if key == "flag" {
			continue
		}
		// The key is also worth exercising as the second level, where it decides
		// which original rows hang off which subgroup rather than only how the
		// top-level groups come out.
		t.Run("group by flag+"+key, func(t *testing.T) {
			assertSameGroupByRows(t, data, &GroupByConfiguration{
				GroupBy: []string{"flag", key},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
			})
		})
	}

	// Aggregating each of those types, rather than grouping by it. The group key
	// is "num" so that "flag" can be aggregated like any other column, and
	// "clock" is left out of the lmnagg case for the reason above.
	for _, col := range []string{"dbl", "flag", "day", "clock", "stamp"} {
		for _, method := range []string{"lmnagg", "first", "last", "median"} {
			if col == "clock" && method == "lmnagg" {
				continue
			}
			// v1's median has no case for a bool either (medianEval falls through
			// to nil for one), so it answers null where v2 orders false before
			// true like any other sortable column. TestGroupByMedianOfBool pins
			// what v2 returns.
			if col == "flag" && method == "median" {
				continue
			}
			t.Run(method+" of "+col, func(t *testing.T) {
				assertSameGroupByRows(t, data, &GroupByConfiguration{
					GroupBy: []string{"num"},
					Select:  []*AggregateSelect{{Columns: []string{col}, Method: method}},
				})
			})
		}
	}
}

// TestGroupByTimeOfDayKey pins grouping by a time-of-day column, which v1
// cannot do: it keys each level on lmnagg of the group-by column, and lmnagg
// returns nil for a time of day whatever the group holds (liminaAggCalc.Do has
// no TimeOnlyValue case), so v1 loses every level below the first. v2 builds the
// tree the same way it does for any other key type.
func TestGroupByTimeOfDayKey(t *testing.T) {
	data := [][]string{
		{"clock", "qty"},
		{"10:30:00", "1"},
		{"11:45:00", "2"},
		{"10:30:00", "3"},
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	if dt := conversion.FiltrifyType(ds.GetColumn("clock").DataType()); dt != types.TimeOfDayType {
		t.Fatalf("expected clock to be a TimeOfDayType column, got %s", dt)
	}

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"clock"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	want := []string{
		"1|10:30:00.000000|4",
		"2|10:30:00.000000|1",
		"2|10:30:00.000000|3",
		"1|11:45:00.000000|2",
		"2|11:45:00.000000|2",
	}
	if int(got.NumRows()) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
	}
	for r := range got.NumRows() {
		parts := make([]string, 0, got.NumCols())
		for c := range got.NumCols() {
			text, isNull := cellText(got.Column(int(c)), int(r))
			if isNull {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, text)
		}
		if line := strings.Join(parts, "|"); line != want[r] {
			t.Errorf("row %d is %q, want %q", r, line, want[r])
		}
	}
}

// TestGroupByNullParity covers nulls in both roles. A null group key is the
// interesting one: v1 matches a row to its parent with CellValue.Equals, which
// is false for a null on either side, so v1 silently drops the descendants of a
// null key. groupByNullGuard reproduces that, and this is what holds it to it.
func TestGroupByNullParity(t *testing.T) {
	// A null group key is where v2 deliberately keeps what v1 drops, so these
	// are pinned directly rather than against v1. See TestGroupByNullKeyIsAGroup.
	t.Run("nulls in the aggregated columns", func(t *testing.T) {
		data := [][]string{
			{"g1", "qty", "dbl", "txt"},
			{"one", "5", "2.5", "k"},
			{"mixed", "", "", ""},
			{"mixed", "3", "1.5", "j"},
			{"mixed", "7", "4.5", ""},
		}
		for _, method := range groupByMethodsInParity {
			for _, col := range []string{"qty", "dbl", "txt"} {
				if method == "weighted_average" {
					continue // needs two numeric columns
				}
				// A numeric aggregate over the text column is rejected rather
				// than answered with v1's 0 - see TestGroupByNumericMethodOnText.
				if col == "txt" && (method == "sumx" || method == "average") {
					continue
				}
				t.Run(method+" of "+col, func(t *testing.T) {
					assertSameGroupByRows(t, data, &GroupByConfiguration{
						GroupBy: []string{"g1"},
						Select:  []*AggregateSelect{{Columns: []string{col}, Method: method}},
					})
				})
			}
		}
	})

	// A group with an even number of values is where v1's median picks the upper
	// middle for text and averages the two middles for numbers - with integer
	// division for integers. Each of those is a separate branch of
	// groupByMedianExpr.
	t.Run("median over even sized groups", func(t *testing.T) {
		data := [][]string{
			{"g1", "qty", "dbl", "txt", "neg"},
			{"two", "1", "1.0", "a", "-7"},
			{"two", "4", "4.0", "d", "-4"},
			{"four", "1", "1.0", "a", "-1"},
			{"four", "2", "2.0", "b", "-2"},
			{"four", "3", "3.0", "c", "-3"},
			{"four", "4", "4.0", "d", "-4"},
			{"odd", "1", "1.0", "a", "-1"},
			{"odd", "2", "2.0", "b", "-2"},
			{"odd", "8", "8.0", "h", "-8"},
		}
		for _, col := range []string{"qty", "dbl", "txt", "neg"} {
			t.Run(col, func(t *testing.T) {
				assertSameGroupByRows(t, data, &GroupByConfiguration{
					GroupBy: []string{"g1"},
					Select:  []*AggregateSelect{{Columns: []string{col}, Method: "median"}},
				})
			})
		}
	})
}

// assertGroupByRows runs v2 alone and checks the result row for row, in order.
// It is for the cases v1 cannot serve as the expectation for.
func assertGroupByRows(t *testing.T, data [][]string, conf *GroupByConfiguration, want []string) {
	t.Helper()

	cfg, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	if int(got.NumRows()) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
	}
	for r := range got.NumRows() {
		parts := make([]string, 0, got.NumCols())
		for c := range got.NumCols() {
			text, isNull := cellText(got.Column(int(c)), int(r))
			if isNull {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, groupByCellText(text))
		}
		if line := strings.Join(parts, "|"); line != want[r] {
			t.Errorf("row %d is %q, want %q", r, line, want[r])
		}
	}
}

// TestGroupByNullKeyIsAGroup pins what v1 loses: a null group key is a group
// like any other, so the levels below it are present. v1 matches a row to its
// parent with CellValue.Equals, which is false for a null on either side, so a
// null key matches no parent and every level past the first disappears for it.
func TestGroupByNullKeyIsAGroup(t *testing.T) {
	data := [][]string{
		{"g1", "g2", "qty"},
		{"a", "x", "1"},
		{"a", "", "2"},
		{"", "x", "3"},
		{"", "", "4"},
	}

	t.Run("null in the first key", func(t *testing.T) {
		// Group Level, g1, qty - the null g1 group keeps its original rows,
		// where v1 would return only its level-1 row.
		assertGroupByRows(t, data, &GroupByConfiguration{
			GroupBy: []string{"g1"},
			Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
		}, []string{
			"1|a|3",
			"2|a|1",
			"2|a|2",
			"1|<null>|7",
			"2|<null>|3",
			"2|<null>|4",
		})
	})

	t.Run("nulls in both keys", func(t *testing.T) {
		// Group Level, g1, g2, qty. Every one of the four (g1, g2) pairs is its
		// own level-2 group with its own row under it, nulls included.
		assertGroupByRows(t, data, &GroupByConfiguration{
			GroupBy: []string{"g1", "g2"},
			Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
		}, []string{
			"1|a|<null>|3",
			"2|a|x|1",
			"3|a|x|1",
			"2|a|<null>|2",
			"3|a|<null>|2",
			"1|<null>|<null>|7",
			"2|<null>|x|3",
			"3|<null>|x|3",
			"2|<null>|<null>|4",
			"3|<null>|<null>|4",
		})
	})
}

// TestGroupByAllNullGroup pins the one remaining disagreement over values: a
// group with nothing to add up. v1's accumulator starts at zero and reports 0;
// SQL's sum() is NULL, and v2's average divides by it so it is NULL too. The
// null is the better answer - 0 cannot be told apart from a group that really
// does sum to zero - and TestDuckNullHandling makes the same choice for the
// aggregate path.
func TestGroupByAllNullGroup(t *testing.T) {
	data := [][]string{
		{"g1", "qty"},
		{"allnull", ""},
		{"allnull", ""},
		{"real", "4"},
		{"real", "6"},
	}

	for _, tc := range []struct {
		method string
		want   []string
	}{
		{"sumx", []string{"1|allnull|<null>", "2|allnull|<null>", "2|allnull|<null>", "1|real|10", "2|real|4", "2|real|6"}},
		{"average", []string{"1|allnull|<null>", "2|allnull|<null>", "2|allnull|<null>", "1|real|5", "2|real|4", "2|real|6"}},
		{"median", []string{"1|allnull|<null>", "2|allnull|<null>", "2|allnull|<null>", "1|real|5", "2|real|4", "2|real|6"}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			assertGroupByRows(t, data, &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: tc.method}},
			}, tc.want)
		})
	}
}

// TestGroupByNumericMethodOnText: a numeric aggregate over a non-numeric column
// is rejected. v1 answers 0 for it, since each of its numeric aggregators skips
// the values it cannot read as a number and returns its zero seed - and a 0
// cannot be told apart from a column that genuinely adds up to zero. DuckDB's
// own error for it names the generated SQL rather than the configuration, so the
// check happens before the query is built.
func TestGroupByNumericMethodOnText(t *testing.T) {
	data := [][]string{
		{"g1", "txt", "day", "qty"},
		{"a", "p", "2021-01-02", "1"},
		{"a", "q", "2021-01-03", "2"},
	}

	for _, tc := range []struct{ method, column string }{
		{"sumx", "txt"},
		{"average", "txt"},
		{"mincol", "txt"},
		{"maxcol", "txt"},
		{"sumx", "day"},
		{"average", "day"},
	} {
		t.Run(tc.method+" of "+tc.column, func(t *testing.T) {
			cfg, err := json.Marshal(&GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{tc.column}, Method: tc.method}},
			})
			if err != nil {
				t.Fatal(err)
			}
			ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
			if err != nil {
				t.Fatalf("conversion failed: %v", err)
			}
			defer ds.Release()

			got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
			if err == nil {
				if got != nil {
					got.Release()
				}
				t.Errorf("expected an error: %s is not a numeric column", tc.column)
			}
		})
	}

	// The methods that are defined over any orderable type still work on one.
	for _, method := range []string{"lmnagg", "first", "last", "median"} {
		t.Run(method+" of txt is allowed", func(t *testing.T) {
			assertSameGroupByRows(t, data, &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{"txt"}, Method: method}},
			})
		})
	}
}

// TestGroupByMedianOfBool pins the median of a bool column, which v1 answers
// null for - medianEval has no case for a BoolValue and falls through to nil.
// v2 orders it like any other sortable column, false before true.
func TestGroupByMedianOfBool(t *testing.T) {
	data := [][]string{
		{"g1", "flag"},
		{"a", "true"},
		{"a", "false"},
		{"a", "true"},
		{"b", "false"},
	}
	// Group Level, g1, flag. Group a sorted is [false, true, true], and v1's
	// median takes index floor(3/2) = 1 of that, which is true.
	assertGroupByRows(t, data, &GroupByConfiguration{
		GroupBy: []string{"g1"},
		Select:  []*AggregateSelect{{Columns: []string{"flag"}, Method: "median"}},
	}, []string{
		"1|a|true",
		"2|a|true",
		"2|a|false",
		"2|a|true",
		"1|b|false",
		"2|b|false",
	})
}

// TestGroupByRealDataParity runs the sweep over the repo's own fixtures, which
// are wider and messier than the hand-written data above - the aggregate
// fixture has empty cells in its date and bool columns, and the fields fixture
// is a few hundred rows over many groups.
func TestGroupByRealDataParity(t *testing.T) {
	t.Run("UAT aggregate fixture", func(t *testing.T) {
		data := test.UATAggregateTestDataFormatted
		for _, method := range groupByMethodsInParity {
			columns := []string{"Quantity"}
			if method == "weighted_average" {
				columns = []string{"Quantity", "Market Value (Base)"}
			}
			for _, groupBy := range [][]string{
				{"Currency"},
				{"Currency", "Instrument Type"},
				{"Instrument Type", "Currency"},
			} {
				name := fmt.Sprintf("%s by %s", method, strings.Join(groupBy, "+"))
				t.Run(name, func(t *testing.T) {
					assertSameGroupByRows(t, data, &GroupByConfiguration{
						GroupBy: groupBy,
						Select:  []*AggregateSelect{{Columns: columns, Method: method}},
					})
				})
			}
		}

		// Group keys that carry dates and bools in this fixture. The ones whose
		// keys hold empty cells - "EU Sanction listed" and "Maturity Date" - are
		// not here: a null key is a group in v2 and is dropped by v1, so there is
		// nothing to compare. TestGroupByNullKeyIsAGroup covers that directly.
		for _, groupBy := range [][]string{
			{"Active From"},
			{"Instrument Type", "Active From"},
		} {
			t.Run("group by "+strings.Join(groupBy, "+"), func(t *testing.T) {
				assertSameGroupByRows(t, data, &GroupByConfiguration{
					GroupBy: groupBy,
					Select:  []*AggregateSelect{{Columns: []string{"Quantity"}, Method: "sumx"}},
				})
			})
		}

		t.Run("several aggregates at once", func(t *testing.T) {
			assertSameGroupByRows(t, data, &GroupByConfiguration{
				GroupBy: []string{"Currency", "Instrument Type"},
				Select: []*AggregateSelect{
					{Columns: []string{"Quantity"}, Method: "sumx"},
					{Columns: []string{"Market Value (Base)"}, Method: "sumx"},
					{Columns: []string{"Instrument name"}, Method: "lmnagg"},
				},
			})
		})
	})

	t.Run("fields fixture", func(t *testing.T) {
		data := test.TestDataWithFields
		for _, groupBy := range [][]string{
			{"gender"},
			{"gender", "country"},
			{"country", "gender"},
			{"active_status"},
			{"join_date"},
			{"gender", "country", "active_status"},
		} {
			t.Run(strings.Join(groupBy, "+"), func(t *testing.T) {
				assertSameGroupByRows(t, data, &GroupByConfiguration{
					GroupBy: groupBy,
					Select: []*AggregateSelect{
						{Columns: []string{"age"}, Method: "sumx"},
						{Columns: []string{"salary"}, Method: "sumx"},
					},
				})
			})
		}
	})
}

// TestGroupByNoAggregates pins the configuration v1 cannot handle: no select
// list and more than one group-by column. v1's generated query is malformed
// there (see the "no aggregates" case in TestGroupByParity), so it returns only
// the level-1 rows; v2 builds the whole tree, which is what the configuration
// asks for.
func TestGroupByNoAggregates(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2(groupByTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{GroupBy: []string{"g1", "g2"}})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	want := []string{
		"1|a|<null>",
		"2|a|x",
		"3|a|x",
		"3|a|x",
		"2|a|y",
		"3|a|y",
		"1|b|<null>",
		"2|b|x",
		"3|b|x",
	}
	if int(got.NumRows()) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
	}
	for r := range got.NumRows() {
		parts := make([]string, 0, got.NumCols())
		for c := range got.NumCols() {
			text, isNull := cellText(got.Column(int(c)), int(r))
			if isNull {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, text)
		}
		if line := strings.Join(parts, "|"); line != want[r] {
			t.Errorf("row %d is %q, want %q", r, line, want[r])
		}
	}
}

// TestGroupByV2RowOrder pins the order v2 puts its rows in, which v1 does not
// decide (see the top of this file). The expectation is the tree v1 produces
// when its map iteration happens to come out in input order: groups in the order
// they first appear, and each group's own row immediately ahead of the rows that
// expand it.
func TestGroupByV2RowOrder(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2(groupByTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"g1", "g2"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	// Group Level, g1, g2, qty - the level first, then the group-by columns in
	// configuration order, then the aggregates.
	want := []string{
		"1|a|<null>|6",
		"2|a|x|4",
		"3|a|x|1",
		"3|a|x|3",
		"2|a|y|2",
		"3|a|y|2",
		"1|b|<null>|4",
		"2|b|x|4",
		"3|b|x|4",
	}

	if int(got.NumRows()) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
	}
	wantCols := []string{GroupLevelColName, "g1", "g2", "qty"}
	if int(got.NumCols()) != len(wantCols) {
		t.Fatalf("got %d columns, want %d:\n%s", got.NumCols(), len(wantCols), dumpDataSet(got))
	}
	for i, name := range wantCols {
		if got.Column(i).Name() != name {
			t.Errorf("column %d is %q, want %q", i, got.Column(i).Name(), name)
		}
	}

	for r := range got.NumRows() {
		parts := make([]string, 0, got.NumCols())
		for c := range got.NumCols() {
			text, isNull := cellText(got.Column(int(c)), int(r))
			if isNull {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, text)
		}
		if line := strings.Join(parts, "|"); line != want[r] {
			t.Errorf("row %d is %q, want %q", r, line, want[r])
		}
	}
}

// TestGroupByV2IsDeterministic: the same input and configuration have to give
// the same rows in the same order every time. v1 does not manage this - its
// group order comes out of a map - and an operator whose output reorders itself
// between runs is miserable to build anything on top of, so the ordering is part
// of what v2 promises.
func TestGroupByV2IsDeterministic(t *testing.T) {
	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"gender", "country"},
		Select:  []*AggregateSelect{{Columns: []string{"salary"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	var first []string
	for run := range 5 {
		ds, err := conversion.ConvertToTypedDatav2(test.TestDataWithFields, true, true, nil, true)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
		if err != nil {
			ds.Release()
			t.Fatalf("v2 failed: %v", err)
		}

		rows := groupByRowsV2(got)
		if run == 0 {
			first = rows
		} else if strings.Join(rows, "\n") != strings.Join(first, "\n") {
			t.Errorf("run %d produced a different order than run 0", run)
		}

		got.Release()
		ds.Release()
	}
}

// TestGroupByResultTypes pins the types the result columns come back as. The
// level is an IntType column in v1, and the group-by columns keep the type they
// had on the way in rather than whatever DuckDB computed them as.
func TestGroupByResultTypes(t *testing.T) {
	data := [][]string{
		{"num", "txt", "qty"},
		{"10", "a", "1"},
		{"10", "a", "2"},
		{"20", "b", "3"},
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"num", "txt"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	for _, tc := range []struct {
		column string
		want   types.CellDataType
	}{
		{GroupLevelColName, types.IntType},
		{"num", types.IntType},
		{"txt", types.StringType},
	} {
		col := got.GetColumn(tc.column)
		if col == nil {
			t.Errorf("column %q is missing from the result:\n%s", tc.column, dumpDataSet(got))
			continue
		}
		if dt := conversion.FiltrifyType(col.DataType()); dt != tc.want {
			t.Errorf("column %q came back as %s, want %s", tc.column, dt, tc.want)
		}
	}
}

// TestGroupByMinMaxColAreCorrected covers mincol and maxcol, which are left out
// of the parity sweep because v1 gets them wrong: both of its aggregators seed
// the running value at 0 instead of at the first value they see, so mincol over
// all-positive input returns 0 and maxcol over all-negative input returns 0.
// min(100, 300) comes back as 0 rather than 100. v2 leaves them as DuckDB's
// min()/max(), which are correct; this pins the correct answers rather than
// v1's, matching what duckdb_test.go's TestDuckMinColIsCorrect already does for
// the aggregate path.
func TestGroupByMinMaxColAreCorrected(t *testing.T) {
	data := [][]string{
		{"g1", "pos", "neg"},
		{"a", "100", "-5"},
		{"a", "300", "-9"},
		{"b", "200", "-1"},
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	for _, tc := range []struct {
		method string
		column string
		want   []string
	}{
		// level 1 is the group, level 2 the original rows
		{"mincol", "pos", []string{"1|a|100", "2|a|100", "2|a|300", "1|b|200", "2|b|200"}},
		{"maxcol", "neg", []string{"1|a|-5", "2|a|-5", "2|a|-9", "1|b|-1", "2|b|-1"}},
	} {
		t.Run(tc.method+" of "+tc.column, func(t *testing.T) {
			cfg, err := json.Marshal(&GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{tc.column}, Method: tc.method}},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
			if err != nil {
				t.Fatalf("v2 failed: %v", err)
			}
			defer got.Release()

			if int(got.NumRows()) != len(tc.want) {
				t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(tc.want), dumpDataSet(got))
			}
			for r := range got.NumRows() {
				parts := make([]string, 0, got.NumCols())
				for c := range got.NumCols() {
					text, isNull := cellText(got.Column(int(c)), int(r))
					if isNull {
						parts = append(parts, "<null>")
						continue
					}
					parts = append(parts, text)
				}
				if line := strings.Join(parts, "|"); line != tc.want[r] {
					t.Errorf("row %d is %q, want %q", r, line, tc.want[r])
				}
			}
		})
	}
}

// TestGroupByConfigurationErrors covers the configurations v2 rejects. v1
// accepts several of them and then produces something unusable - a panic, a
// result with two columns of the same name, or a SQL parse error naming a
// fragment of its own generated query - so these are the places v2 reports a
// problem instead. The cases v1 already rejects have to keep being rejected.
func TestGroupByConfigurationErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *GroupByConfiguration
		// what v1 does with the same configuration, for the record
		v1 string
	}{
		{
			name:   "no groupby",
			config: &GroupByConfiguration{Select: []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}}},
			v1:     "rejects it too",
		},
		{
			name: "groupby a column that does not exist",
			config: &GroupByConfiguration{
				GroupBy: []string{"nope"},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
			},
			v1: "rejects it too, with the same message",
		},
		{
			name: "the same column twice in groupby",
			config: &GroupByConfiguration{
				GroupBy: []string{"g1", "g1"},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
			},
			v1: "panics: it indexes merged rows by position and walks off the end",
		},
		{
			name: "aggregate a column that does not exist",
			config: &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{"nope"}, Method: "sumx"}},
			},
			v1: "fails with a SQL parse error naming its own generated query",
		},
		{
			name: "unknown aggregation method",
			config: &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "bogus"}},
			},
			v1: "fails with a SQL parse error: the method produces an empty select entry",
		},
		{
			name: "aggregate into a groupby column",
			config: &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{"g1"}, Method: "first"}},
			},
			v1: "returns rows with two columns called g1 and a nil column between them",
		},
		{
			name: "an empty select entry",
			config: &GroupByConfiguration{
				GroupBy: []string{"g1"},
				Select:  []*AggregateSelect{{Columns: []string{}, Method: "sumx"}},
			},
			v1: "rejects it too",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := json.Marshal(tc.config)
			if err != nil {
				t.Fatal(err)
			}
			ds, err := conversion.ConvertToTypedDatav2(groupByTestData, true, true, nil, true)
			if err != nil {
				t.Fatalf("conversion failed: %v", err)
			}
			defer ds.Release()

			got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
			if err == nil {
				if got != nil {
					got.Release()
				}
				t.Errorf("expected an error (v1 %s)", tc.v1)
			}
		})
	}

	// Two selects over one column: v1 returns an empty result and a nil error -
	// its aggregate selects the same column twice and qlbridge yields no rows -
	// so the caller is handed nothing and told nothing. v2 accepts the
	// configuration, names the second aggregate "qty1" and leaves its cell null
	// on the original rows, which have no second value to show under that name.
	t.Run("the same column aggregated twice", func(t *testing.T) {
		cfg, err := json.Marshal(&GroupByConfiguration{
			GroupBy: []string{"g1"},
			Select: []*AggregateSelect{
				{Columns: []string{"qty"}, Method: "sumx"},
				{Columns: []string{"qty"}, Method: "average"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		ds, err := conversion.ConvertToTypedDatav2(groupByTestData, true, true, nil, true)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		defer ds.Release()

		got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
		if err != nil {
			t.Fatalf("v2 failed: %v", err)
		}
		defer got.Release()

		// Group Level, g1, qty (the sum), qty1 (the average)
		want := []string{
			"1|a|6|2",
			"2|a|1|<null>",
			"2|a|2|<null>",
			"2|a|3|<null>",
			"1|b|4|4",
			"2|b|4|<null>",
		}
		if int(got.NumRows()) != len(want) {
			t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
		}
		if got.GetColumn("qty1") == nil {
			t.Fatalf("expected a column called qty1:\n%s", dumpDataSet(got))
		}
		for r := range got.NumRows() {
			parts := make([]string, 0, got.NumCols())
			for c := range got.NumCols() {
				text, isNull := cellText(got.Column(int(c)), int(r))
				if isNull {
					parts = append(parts, "<null>")
					continue
				}
				parts = append(parts, text)
			}
			if line := strings.Join(parts, "|"); line != want[r] {
				t.Errorf("row %d is %q, want %q", r, line, want[r])
			}
		}
	})
}

// TestGroupByReservedColumnName: the level is added under a fixed name, so a
// dataset that already has a column called that cannot be grouped - the two
// would collide in the output. v1 overwrites the input column's values with the
// level instead.
func TestGroupByReservedColumnName(t *testing.T) {
	data := [][]string{
		{"g1", GroupLevelColName, "qty"},
		{"a", "7", "1"},
		{"b", "8", "2"},
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	for _, groupBy := range [][]string{{"g1"}, {GroupLevelColName}} {
		t.Run(strings.Join(groupBy, "+"), func(t *testing.T) {
			cfg, err := json.Marshal(&GroupByConfiguration{
				GroupBy: groupBy,
				Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
			if err == nil {
				if got != nil {
					got.Release()
				}
				t.Errorf("expected an error: the dataset already has a %q column", GroupLevelColName)
			}
		})
	}
}

// TestGroupByColumnNamesAreNotParsed: a column name holding a backtick, a quote
// or something that looks like SQL has to be carried through as a name rather
// than as syntax. The bookkeeping columns the query adds also have to stay clear
// of the caller's own names, which uniqueGroupByPrefix is what handles.
func TestGroupByAwkwardColumnNames(t *testing.T) {
	data := [][]string{
		{"gr`oup", "it's", "__gb_rid", "qty"},
		{"a", "x", "9", "1"},
		{"a", "y", "8", "2"},
		{"b", "x", "7", "3"},
	}

	// A name holding an apostrophe is in parity; one holding a backtick is not,
	// because v1 cannot express it at all. v1 quotes identifiers with backticks
	// and the dialect has no escape for one, so `gr`oup` closes early and
	// qlbridge is handed "lmnagg()" with no argument. v2 quotes straight to
	// DuckDB's double-quoted form, which escapes a quote by doubling it.
	t.Run("parity on a name with an apostrophe", func(t *testing.T) {
		assertSameGroupByRows(t, data, &GroupByConfiguration{
			GroupBy: []string{"it's"},
			Select: []*AggregateSelect{
				{Columns: []string{"qty"}, Method: "sumx"},
				{Columns: []string{"__gb_rid"}, Method: "sumx"},
			},
		})
	})

	t.Run("a name with a backtick", func(t *testing.T) {
		// Group Level, gr`oup, it's, qty
		assertGroupByRows(t, data, &GroupByConfiguration{
			GroupBy: []string{"gr`oup", "it's"},
			Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
		}, []string{
			"1|a|<null>|3",
			"2|a|x|1",
			"3|a|x|1",
			"2|a|y|2",
			"3|a|y|2",
			"1|b|<null>|3",
			"2|b|x|3",
			"3|b|x|3",
		})
	})

	// A column called exactly what the internal ordering column would be called
	// has to survive with its own values rather than being shadowed by it.
	t.Run("a column named like the internal ones keeps its values", func(t *testing.T) {
		cfg, err := json.Marshal(&GroupByConfiguration{
			GroupBy: []string{"gr`oup"},
			Select:  []*AggregateSelect{{Columns: []string{"__gb_rid"}, Method: "sumx"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		defer ds.Release()

		got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
		if err != nil {
			t.Fatalf("v2 failed: %v", err)
		}
		defer got.Release()

		want := []string{"1|a|17", "2|a|9", "2|a|8", "1|b|7", "2|b|7"}
		if int(got.NumRows()) != len(want) {
			t.Fatalf("got %d rows, want %d:\n%s", got.NumRows(), len(want), dumpDataSet(got))
		}
		for r := range got.NumRows() {
			parts := make([]string, 0, got.NumCols())
			for c := range got.NumCols() {
				text, isNull := cellText(got.Column(int(c)), int(r))
				if isNull {
					parts = append(parts, "<null>")
					continue
				}
				parts = append(parts, text)
			}
			if line := strings.Join(parts, "|"); line != want[r] {
				t.Errorf("row %d is %q, want %q", r, line, want[r])
			}
		}
	})
}

// TestGroupByEmptyDataset: a dataset with no rows is handed back untouched. The
// transformer never calls an operator with one - it returns early - so there is
// no v1 behaviour to match, and this only pins that it does not fail.
func TestGroupByEmptyDataset(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2([][]string{{"g1", "qty"}}, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"g1"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	if got.NumRows() != 0 {
		t.Errorf("got %d rows, want 0", got.NumRows())
	}

	t.Run("nil dataset", func(t *testing.T) {
		if _, err := (&GroupByOperator{}).Transform(nil, string(cfg), nil); err == nil {
			t.Error("expected an error for a nil dataset")
		}
	})
}

// TestGroupByValidateConfiguration covers the validation the transformer calls
// before running a step.
func TestGroupByValidateConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  string
		wantErr bool
	}{
		{name: "a valid configuration", config: `{"groupby":["g1"],"select":[{"columns":["qty"],"method":"sumx"}]}`},
		{name: "groupby only", config: `{"groupby":["g1"]}`},
		{name: "empty", config: "", wantErr: true},
		{name: "not json", config: "{", wantErr: true},
		{name: "no groupby", config: `{"select":[{"columns":["qty"],"method":"sumx"}]}`, wantErr: true},
		{name: "an empty groupby", config: `{"groupby":[]}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&GroupByOperator{}).ValidateConfiguration(tc.config)
			if tc.wantErr && err == nil {
				t.Error("expected an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestGroupByLargeDataset pushes enough rows through to span several record
// batches, so the streaming path and the chunked result columns are exercised
// rather than only the single-batch case. The expected shape is arithmetic: one
// level-1 row per g1, one level-2 row per (g1, g2), and one level-3 row per
// input row.
func TestGroupByLargeDataset(t *testing.T) {
	const rows = 20000
	const outerGroups = 8
	const innerGroups = 5

	data := make([][]string, 0, rows+1)
	data = append(data, []string{"g1", "g2", "qty"})
	for i := range rows {
		data = append(data, []string{
			"g" + strconv.Itoa(i%outerGroups),
			"h" + strconv.Itoa(i%innerGroups),
			strconv.Itoa(i),
		})
	}

	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"g1", "g2"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	// 8 and 5 are coprime, so every (g1, g2) combination occurs.
	want := int64(outerGroups + outerGroups*innerGroups + rows)
	if got.NumRows() != want {
		t.Errorf("got %d rows, want %d", got.NumRows(), want)
	}

	// The level-1 sums have to add up to the sum over every row.
	var total, levelOne int64
	for i := range rows {
		total += int64(i)
	}
	levels := got.GetColumn(GroupLevelColName)
	sums := got.GetColumn("qty")
	if levels == nil || sums == nil {
		t.Fatalf("missing columns:\n%s", dumpDataSet(got))
	}
	for r := range got.NumRows() {
		levelText, _ := cellText(levels, int(r))
		if levelText != "1" {
			continue
		}
		sumText, isNull := cellText(sums, int(r))
		if isNull {
			t.Errorf("row %d is a level-1 row with a null sum", r)
			continue
		}
		v, err := strconv.ParseFloat(sumText, 64)
		if err != nil {
			t.Fatalf("could not read the sum %q: %v", sumText, err)
		}
		levelOne += int64(v)
	}
	if levelOne != total {
		t.Errorf("the level-1 sums add up to %d, want %d", levelOne, total)
	}
}

// TestGroupByRowOrderAcrossChunks is the hard check on the ordering. The whole
// tree hangs off rowid matching the order rows appear in the input, and the
// input crosses into DuckDB as a stream of record batches that gets copied into
// a table - so if rowid did not follow the arrival order, every level-N+1 row
// would be shuffled within its group. This builds a table of many chunks with
// two interleaved groups and checks the original rows come back in input order.
func TestGroupByRowOrderAcrossChunks(t *testing.T) {
	const chunks = 40
	const perChunk = 300

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "g1", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	recs := make([]arrow.RecordBatch, 0, chunks)
	seq := 0
	for range chunks {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for range perChunk {
			// two groups, interleaved, so each one's rows are spread over every chunk
			b.Field(0).(*array.StringBuilder).Append("g" + strconv.Itoa(seq%2))
			b.Field(1).(*array.Int32Builder).Append(int32(seq))
			seq++
		}
		rec := b.NewRecordBatch()
		recs = append(recs, rec)
		b.Release()
	}
	table := array.NewTableFromRecords(schema, recs)
	for _, r := range recs {
		r.Release()
	}
	ds := &types.DataSetv2{Table: table}
	defer ds.Release()

	if n := len(ds.Column(0).Data().Chunks()); n < 2 {
		t.Fatalf("expected a multi-chunk column, got %d chunk(s)", n)
	}

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"g1"},
		Select:  []*AggregateSelect{{Columns: []string{"seq"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	// Within each group the original rows have to carry strictly increasing seq,
	// seq being the position the row was written at.
	levels, groups, seqs := got.GetColumn(GroupLevelColName), got.GetColumn("g1"), got.GetColumn("seq")
	if levels == nil || groups == nil || seqs == nil {
		t.Fatalf("missing columns:\n%s", dumpDataSet(got))
	}

	last := map[string]int{}
	rows := 0
	for r := range got.NumRows() {
		if lvl, _ := cellText(levels, int(r)); lvl != "2" {
			continue
		}
		group, _ := cellText(groups, int(r))
		text, _ := cellText(seqs, int(r))
		v, err := strconv.Atoi(text)
		if err != nil {
			t.Fatalf("row %d: could not read seq %q: %v", r, text, err)
		}
		if prev, seen := last[group]; seen && v <= prev {
			t.Fatalf("group %s: seq went %d -> %d at row %d - rowid does not follow the input order",
				group, prev, v, r)
		}
		last[group] = v
		rows++
	}
	if rows != chunks*perChunk {
		t.Errorf("saw %d original rows, want %d", rows, chunks*perChunk)
	}
}

// TestGroupByInternalColumnsDoNotLeak: the query carries bookkeeping columns to
// sort by and the outermost select is supposed to project them away. If one ever
// escaped it would show up as a stray column in every grouped result.
func TestGroupByInternalColumnsDoNotLeak(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2(groupByTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	cfg, err := json.Marshal(&GroupByConfiguration{
		GroupBy: []string{"g1", "g2"},
		Select:  []*AggregateSelect{{Columns: []string{"qty"}, Method: "sumx"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := (&GroupByOperator{}).Transform(ds, string(cfg), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer got.Release()

	// Group Level, g1, g2, qty and nothing else.
	if got.NumCols() != 4 {
		t.Errorf("got %d columns, want 4:\n%s", got.NumCols(), dumpDataSet(got))
	}
	for c := range got.NumCols() {
		if name := got.Column(int(c)).Name(); strings.HasPrefix(name, "__gb") {
			t.Errorf("internal column %q leaked into the result:\n%s", name, dumpDataSet(got))
		}
	}
}
