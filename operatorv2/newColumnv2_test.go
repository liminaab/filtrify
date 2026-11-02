//go:build duckdb_arrow

package operatorv2

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
)

// These tests pin NewColumnOperatorv2 to v1's operator.NewColumnOperator: every
// case runs both implementations over the same raw data and the same
// configuration JSON, then compares the results cell by cell.
//
// Where the two are deliberately NOT held to each other, the case lives in
// TestNewColumnV2Corrections instead, which pins what v2 returns and says what
// v1 returns for the same input. See the note on NewColumnOperatorv2 for the
// list and the reasoning.
//
// Values are compared exactly, except that floats are compared to ten
// significant digits: float addition is not associative, so a sum depends on the
// order the engine adds it up, and the two engines add in different orders.

// newColumnCellText renders a value for comparison, in the same form on both
// sides. Floats are rounded to ten significant digits for the reason above, and
// because arrow renders a float64 in scientific notation where v1's
// CellValue.ToString writes it out in full.
//
// A trailing ".000000" is dropped because arrow renders a time64us to
// microsecond precision while v1 formats a time of day as "15:04:05". Only an
// all-zero fraction is removed, so a real sub-second difference still shows.
func newColumnCellText(s string) string {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return strconv.FormatFloat(f, 'g', 10, 64)
	}
	return strings.TrimSuffix(s, ".000000")
}

// newColumnRowsV1 renders a v1 result as one "col=value" string per row, with
// the columns sorted by name so column order does not enter into it.
func newColumnRowsV1(ds *types.DataSet) []string {
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
			parts = append(parts, c.ColumnName+"="+newColumnCellText(c.CellValue.ToString()))
		}
		sort.Strings(parts)
		out = append(out, strings.Join(parts, ","))
	}
	return out
}

// newColumnRowsV2 is newColumnRowsV1 for an arrow result.
func newColumnRowsV2(ds *types.DataSetv2) []string {
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
			parts = append(parts, col.Name()+"="+newColumnCellText(text))
		}
		sort.Strings(parts)
		out = append(out, strings.Join(parts, ","))
	}
	return out
}

// runNewColumnBoth converts the raw data for both generations, runs both
// operators over the same configuration and returns their outputs and errors.
func runNewColumnBoth(t *testing.T, data [][]string, statement string, groupBy string) (*types.DataSet, *types.DataSetv2, error, error) {
	t.Helper()

	cfgJSON, err := json.Marshal(&NewColumnConfiguration{Statement: statement, GroupBy: groupBy})
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

	outV1, errV1 := (&operator.NewColumnOperator{}).Transform(v1In, string(cfgJSON), nil)
	outV2, errV2 := (&NewColumnOperatorv2{}).Transform(v2In, string(cfgJSON), nil)
	if outV2 != nil {
		t.Cleanup(outV2.Release)
	}
	return outV1, outV2, errV1, errV2
}

// assertSameNewColumnRows checks that both generations produced the same rows in
// the same order - unlike group-by, a NewColumn statement never reorders rows,
// so the order is part of the contract here.
func assertSameNewColumnRows(t *testing.T, data [][]string, statement string) {
	t.Helper()

	outV1, outV2, errV1, errV2 := runNewColumnBoth(t, data, statement, "")
	if errV1 != nil {
		t.Fatalf("v1 failed: %v", errV1)
	}
	if errV2 != nil {
		t.Fatalf("v2 failed: %v", errV2)
	}

	want, got := newColumnRowsV1(outV1), newColumnRowsV2(outV2)
	if len(want) != len(got) {
		t.Fatalf("row count mismatch: v1 produced %d, v2 produced %d\nv1:\n  %s\nv2:\n  %s",
			len(want), len(got), strings.Join(want, "\n  "), strings.Join(got, "\n  "))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("row %d differs\n  v1: %s\n  v2: %s", i, want[i], got[i])
		}
	}
}

// newColumnTestData is UAT3 shaped: a text column with an empty value, a
// nullable float, a timestamp, a date with a gap, a time of day with a gap, a
// nullable bool and a nullable integer. Between them they cover every cell type
// the operators distinguish, each with and without a null.
var newColumnTestData = test.UAT3TestDataFormatted

// TestNewColumnV2Parity is the main sweep: every function in the lmnqlbridge
// vocabulary that v1 and v2 agree on, over data that exercises its null and
// edge cases.
func TestNewColumnV2Parity(t *testing.T) {
	for _, statement := range []string{
		// --- a bare column reference and a literal ---------------------
		"`Instrument name` AS `c`",
		"`Quantity` AS `c`",
		"`Active From` AS `c`",
		"`Maturity Date` AS `c`",
		"`Hour` AS `c`",
		"`EU Sanction listed` AS `c`",
		"3.5 AS `c`",
		"42 AS `c`",
		"'text' AS `c`",

		// --- arithmetic -----------------------------------------------
		"`Quantity`+1 AS `c`",
		"`Quantity`-1 AS `c`",
		"`Quantity`*2 AS `c`",
		"PLUS(3, 5) AS `c`",
		"PLUS(`Quantity`, 1) AS `c`",
		"MINUS(`Quantity`, 1) AS `c`",
		"MULTIPLY(`Quantity`, 3) AS `c`",
		"DIVIDE(`Quantity`, 2) AS `c`",
		"DIVIDE(`Quantity`, 0) AS `c`",
		"ABS(`Quantity`) AS `c`",
		"ROUND(`Quantity`, 1) AS `c`",
		"FLOORX(`Quantity`, 1) AS `c`",
		"MINX(`Quantity`, 2) AS `c`",
		"MAXX(`Quantity`, 2) AS `c`",

		// --- text -----------------------------------------------------
		"LEFT(`Instrument name`, 6) AS `c`",
		"RIGHT(`Instrument name`, 3) AS `c`",
		"RIGHT(LEFT(`Instrument name`, 6), 4) AS `c`",
		"LENGTH(`Instrument name`) AS `c`",
		"LENGTH(`Quantity`) AS `c`",
		"TRIM(`Instrument name`) AS `c`",
		"SPLIT(`Instrument name`, ' ', 1) AS `c`",
		"SPLIT(`Instrument Type`, ' ', 2) AS `c`",
		"REPLACE(`Instrument Type`, 'Equity', 'EQ') AS `c`",
		"CONCAT(`Instrument name`, '<', `Instrument Type`, '>', '') AS `c`",
		"CONCAT(`Instrument name`, '<S=', `EU Sanction listed`, '>', '') AS `c`",
		"CONCAT(`Instrument name`, '_', `Quantity`) AS `c`",
		"ISBLANK(`Instrument name`) AS `c`",
		"ISBLANK(`Quantity`) AS `c`",
		"ISBLANK(`EU Sanction listed`) AS `c`",

		// --- conditional ----------------------------------------------
		"IFEL(`Instrument Type` == 'Equity', 'Yes', 'No') AS `c`",
		"IFEL(`Quantity` > 2, 'Yes', 'No') AS `c`",
		"IFEL(`Instrument Type` == 'Equity' && LEFT(`Instrument name`, 1) == 'a', 'Yes', 'No') AS `c`",
		"IFEL(`Instrument Type` == 'Equity' || `Quantity` > 3, 'Yes', 'No') AS `c`",
		"IFEL(`Instrument Type` == 'Equity', 'Yes', IFEL(`Quantity` > 3, 'Yes', 'No')) AS `c`",
		"IFEL(CONTAINSX(`Instrument name`, 'a'), 'Yes', 'No') AS `c`",
		"IFEL(NOTCONTAINSX(`Instrument name`, 'a'), 'Yes', 'No') AS `c`",
		"IFEL(EVAL(`Quantity` > 1 && `EU Sanction listed` == false), 'Yes', 'No') AS `c`",
		"IFEL(`EU Sanction listed`, 'Yes', 'No') AS `c`",

		// --- boolean operators ----------------------------------------
		"`EU Sanction listed` AND true AS `c`",
		"`EU Sanction listed` OR true AS `c`",

		// --- temporal -------------------------------------------------
		"PLUSDAYS(`Active From`, 3) AS `c`",
		"PLUSDAYS(`Maturity Date`, 3) AS `c`",
		"TODATE('2021-12-31') AS `c`",
		"TOTIME('10:30:00') AS `c`",
		"DATE(2021, 12, 31) AS `c`",
		"TIME(10, 30, 0) AS `c`",
		"DATETIMEUTC(2021, 12, 31, 10, 30, 0) AS `c`",

		// --- whole-table aggregates -----------------------------------
		"SUMX(`Quantity`) AS `c`",
		"AVERAGE(`Quantity`) AS `c`",
		"WEIGHTED_AVERAGE(`Quantity`, `Market Value (Base)`) AS `c`",
		"FIRST(`Quantity`) AS `c`",
		"LAST(`Quantity`) AS `c`",
		"MEDIAN(`Quantity`) AS `c`",
		"MEDIAN(`Instrument name`) AS `c`",
		"MEDIAN(`Active From`) AS `c`",
		"MEDIAN(`Maturity Date`) AS `c`",
		"MEDIAN(`Hour`) AS `c`",
	} {
		t.Run(statement, func(t *testing.T) {
			assertSameNewColumnRows(t, newColumnTestData, statement)
		})
	}
}

// TestNewColumnV2ParityOverTypedData runs the parity sweep again over data whose
// columns have been retyped, so the same statements are exercised against an
// integer and a long column rather than only the inferred float and string ones.
func TestNewColumnV2ParityOverTypedData(t *testing.T) {
	// somedata is 1..5 with a gap, which the conversion infers as an integer
	// column - the one input type the float-heavy UAT3 columns do not cover.
	for _, statement := range []string{
		"`somedata` AS `c`",
		"`somedata`+1 AS `c`",
		"PLUS(`somedata`, 1) AS `c`",
		"DIVIDE(`somedata`, 2) AS `c`",
		"ABS(`somedata`) AS `c`",
		"ROUND(`somedata`, 1) AS `c`",
		"SUMX(`somedata`) AS `c`",
		"AVERAGE(`somedata`) AS `c`",
		"FIRST(`somedata`) AS `c`",
		"LAST(`somedata`) AS `c`",
		"MEDIAN(`somedata`) AS `c`",
		"LENGTH(`somedata`) AS `c`",
		"CONCAT(`Instrument name`, '_', `somedata`) AS `c`",
	} {
		t.Run(statement, func(t *testing.T) {
			assertSameNewColumnRows(t, newColumnTestData, statement)
		})
	}
}

// TestNewColumnV2ParityOverUAT1 and the two below run the whole sweep over the
// other corpora the v1 tests use, so a statement is not only checked against one
// shape of input: UAT1 has no nulls in its numeric columns, UAT2 has leading
// whitespace and a missing Quantity, UAT1WithDate/WithTime make "Active From" a
// date and a time of day rather than a timestamp.
func TestNewColumnV2ParityAcrossCorpora(t *testing.T) {
	corpora := map[string][][]string{
		"UAT1":         test.UAT1TestDataFormatted,
		"UAT2":         test.UAT2TestDataFormatted,
		"UAT1WithDate": test.UAT1TestDataFormattedWithDate,
		"UAT1WithTime": test.UAT1TestDataFormattedWithTime,
	}
	statements := []string{
		"`Instrument Type` AS `c`",
		"`Quantity`+1 AS `c`",
		"`Quantity`*`Market Value (Base)` AS `c`",
		"CONCAT(`Instrument name`, '<', `Instrument Type`, '>', '') AS `c`",
		"CONCAT(`Instrument name`, '<S=', `EU Sanction listed`, '>', '') AS `c`",
		"LEFT(`Instrument name`, 6) AS `c`",
		"TRIM(`Instrument name`) AS `c`",
		"LENGTH(`Instrument name`) AS `c`",
		"SPLIT(`Instrument name`, ' ', 1) AS `c`",
		"IFEL(`Instrument Type` == 'Equity', 'Yes', 'No') AS `c`",
		"IFEL(CONTAINSX(`Instrument name`, 'ERIC'), 'Yes', 'No') AS `c`",
		"PLUSDAYS(`Active From`, 3) AS `c`",
		"SUMX(`Market Value (Base)`) AS `c`",
		"AVERAGE(`Market Value (Base)`) AS `c`",
		"WEIGHTED_AVERAGE(`Market Value (Base)`, `Quantity`) AS `c`",
		"FIRST(`Market Value (Base)`) AS `c`",
		"LAST(`Market Value (Base)`) AS `c`",
		"MEDIAN(`Market Value (Base)`) AS `c`",
		"MEDIAN(`Active From`) AS `c`",
	}
	for name, data := range corpora {
		for _, statement := range statements {
			t.Run(name+" "+statement, func(t *testing.T) {
				assertSameNewColumnRows(t, data, statement)
			})
		}
	}
}

// TestNewColumnV2GroupByParity covers the configuration's other field. With a
// groupby set, v1 selects only what the statement names - the original columns
// are not carried through - and groups by the named column, so the result is one
// row per group.
func TestNewColumnV2GroupByParity(t *testing.T) {
	for _, tc := range []struct {
		statement string
		groupBy   string
	}{
		{"lmnagg(`Instrument Type`) AS `c`", "Instrument Type"},
		{"SUMX(`Quantity`) AS `c`", "Instrument Type"},
		{"AVERAGE(`Quantity`) AS `c`", "Instrument Type"},
		{"MEDIAN(`Quantity`) AS `c`", "Instrument Type"},
		{"FIRST(`Quantity`) AS `c`", "Instrument Type"},
		{"LAST(`Quantity`) AS `c`", "Instrument Type"},
		{"lmnagg(`Quantity`) AS `c`", "Instrument Type"},
		{"lmnagg(`Instrument name`) AS `c`", "Instrument Type"},
	} {
		t.Run(tc.statement+" by "+tc.groupBy, func(t *testing.T) {
			outV1, outV2, errV1, errV2 := runNewColumnBoth(t, newColumnTestData, tc.statement, tc.groupBy)
			if errV1 != nil {
				t.Fatalf("v1 failed: %v", errV1)
			}
			if errV2 != nil {
				t.Fatalf("v2 failed: %v", errV2)
			}
			// A GROUP BY result has no inherent row order in either engine -
			// v1's comes out of a qlbridge hash map - so these are compared as
			// a multiset, as the group-by parity tests do.
			want, got := newColumnRowsV1(outV1), newColumnRowsV2(outV2)
			sort.Strings(want)
			sort.Strings(got)
			if len(want) != len(got) {
				t.Fatalf("row count mismatch: v1 produced %d, v2 produced %d\nv1:\n  %s\nv2:\n  %s",
					len(want), len(got), strings.Join(want, "\n  "), strings.Join(got, "\n  "))
			}
			for i := range want {
				if want[i] != got[i] {
					t.Errorf("row sets differ\nv1:\n  %s\nv2:\n  %s",
						strings.Join(want, "\n  "), strings.Join(got, "\n  "))
					return
				}
			}
		})
	}
}

// TestNewColumnV2Errors checks the configuration errors, which are v1's
// verbatim: the same conditions produce the same messages, because both come
// from the same buildConfiguration and the same "column already exists" check.
func TestNewColumnV2Errors(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2(newColumnTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	for _, tc := range []struct {
		name   string
		config string
		want   string
	}{
		{"empty configuration", "", "invalid configuration"},
		{"malformed json", "{", "unexpected end of JSON input"},
		{"no statement", `{"groupby": "Instrument Type"}`, "missing statement in newcolumn configuration"},
		{
			"column already exists",
			`{"statement": "` + "`Quantity` AS `Instrument name`" + `"}`,
			"column already exists",
		},
		{
			"column already exists, different case",
			`{"statement": "` + "`Quantity` AS `INSTRUMENT NAME`" + `"}`,
			"column already exists",
		},
		{
			"more than one statement",
			`{"statement": "` + "`Quantity` AS `a`, `Quantity` AS `b`" + `"}`,
			"new column operator only supports one statement",
		},
		{
			"groupby column does not exist",
			`{"statement": "` + "SUMX(`Quantity`) AS `c`" + `", "groupby": "nope"}`,
			"attempted to operate on column “nope” but no such column available",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&NewColumnOperatorv2{}).Transform(ds, tc.config, nil)
			if err == nil {
				t.Fatalf("expected an error, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error mismatch\n  got:  %v\n  want to contain: %s", err, tc.want)
			}
		})
	}

	// And the same configurations have to fail in v1 too - an error v2 raises
	// that v1 accepts would be a behaviour change even though both return an
	// error-free path for nothing.
	v1In, err := conversion.ConvertToTypedData(newColumnTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	for _, config := range []string{
		"",
		"{",
		`{"groupby": "Instrument Type"}`,
		`{"statement": "` + "`Quantity` AS `Instrument name`" + `"}`,
		`{"statement": "` + "`Quantity` AS `a`, `Quantity` AS `b`" + `"}`,
	} {
		if _, err := (&operator.NewColumnOperator{}).Transform(v1In, config, nil); err == nil {
			t.Errorf("v1 accepted a configuration v2 rejects: %s", config)
		}
	}
}

// TestNewColumnV2InvalidStatement is v1's TestBasicNewColumnWithInvalidConfiguration:
// a statement that does not parse has to be an error rather than a column of
// nulls. The exact message differs - it comes from whichever engine parsed it -
// so only the failure is pinned.
func TestNewColumnV2InvalidStatement(t *testing.T) {
	statement := `CONCAT(COL("Account Name (Short)"),"_",COL("ISIN")))`
	_, outV2, errV1, errV2 := runNewColumnBoth(t, newColumnTestData, statement, "")
	if errV1 == nil {
		t.Error("v1 accepted an invalid statement")
	}
	if errV2 == nil {
		t.Fatalf("v2 accepted an invalid statement, returning:\n%s", dumpDataSet(outV2))
	}
}

// TestNewColumnV2Corrections pins the cases where v2 deliberately differs from
// v1, each with what v1 returns for the same input and why that is a bug rather
// than a semantic. See the note on NewColumnOperatorv2.
func TestNewColumnV2Corrections(t *testing.T) {
	// mincol/maxcol: v1's aggregators seed their accumulator at 0 instead of at
	// the first value, so mincol over all-positive input and maxcol over
	// all-negative input both return 0.
	t.Run("mincol over all-positive input", func(t *testing.T) {
		data := [][]string{
			{"qty"},
			{"100"},
			{"300"},
		}
		assertNewColumnValue(t, data, "MINCOL(`qty`) AS `c`", "c", "100")
		// what v1 answers for the same input, for the record
		assertV1Value(t, data, "MINCOL(`qty`) AS `c`", "c", "0")
	})
	t.Run("maxcol over all-negative input", func(t *testing.T) {
		data := [][]string{
			{"qty"},
			{"-100"},
			{"-300"},
		}
		assertNewColumnValue(t, data, "MAXCOL(`qty`) AS `c`", "c", "-100")
		assertV1Value(t, data, "MAXCOL(`qty`) AS `c`", "c", "0")
	})

	// concat over a temporal value: v1's concat has a case for strings, numbers
	// and bools and silently drops everything else, so the timestamp vanishes
	// and every row comes back as just the first argument and the separator.
	//
	// The data has to be a corpus the conversion infers a real timestamp column
	// from - a single row of one is read as text, which concat does handle, and
	// then there is no difference to show.
	t.Run("concat over a timestamp", func(t *testing.T) {
		statement := "CONCAT(`Instrument Type`, '_', `Active From`) AS `c`"
		assertNewColumnColumn(t, newColumnTestData, statement, "c", []string{
			"Equity_2020-01-01T12:00:00Z",
			"Equity_2020-03-01T12:00:00Z",
			"Bill_2020-11-22T12:00:00Z",
			"Index Future_2021-04-06T12:00:00Z",
			"Cash Account2_2020-02-01T12:00:00Z",
			"Cash Account_2020-05-01T12:00:00Z",
		})
		// v1 drops the timestamp, leaving the separator dangling
		assertV1Column(t, newColumnTestData, statement, "c", []string{
			"Equity_", "Equity_", "Bill_", "Index Future_", "Cash Account2_", "Cash Account_",
		})
	})

	// sumx over a group with nothing to add up: v1's accumulator starts at zero
	// and cannot tell that apart from a column that really sums to zero.
	t.Run("sumx over an all-null column", func(t *testing.T) {
		data := [][]string{
			{"name", "qty"},
			{"a", ""},
			{"b", ""},
		}
		assertNewColumnValue(t, data, "SUMX(`qty`) AS `c`", "c", "<null>")
		assertV1Value(t, data, "SUMX(`qty`) AS `c`", "c", "0")
	})
}

// assertNewColumnValue runs v2 and checks that every row's named column holds
// want, rendered as newColumnCellText would render it.
func assertNewColumnValue(t *testing.T, data [][]string, statement string, column string, want string) {
	t.Helper()

	cfgJSON, err := json.Marshal(&NewColumnConfiguration{Statement: statement})
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	out, err := (&NewColumnOperatorv2{}).Transform(ds, string(cfgJSON), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer out.Release()

	col := out.GetColumn(column)
	if col == nil {
		t.Fatalf("column %q is missing from the result:\n%s", column, dumpDataSet(out))
	}
	for r := range out.NumRows() {
		text, isNull := cellText(col, int(r))
		got := "<null>"
		if !isNull {
			got = newColumnCellText(text)
		}
		if got != want {
			t.Errorf("row %d: got %s, want %s", r, got, want)
		}
	}
}

// assertNewColumnColumn is assertNewColumnValue for a statement whose result
// differs per row, checking the named column against want row by row.
func assertNewColumnColumn(t *testing.T, data [][]string, statement string, column string, want []string) {
	t.Helper()

	cfgJSON, err := json.Marshal(&NewColumnConfiguration{Statement: statement})
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	out, err := (&NewColumnOperatorv2{}).Transform(ds, string(cfgJSON), nil)
	if err != nil {
		t.Fatalf("v2 failed: %v", err)
	}
	defer out.Release()

	col := out.GetColumn(column)
	if col == nil {
		t.Fatalf("column %q is missing from the result:\n%s", column, dumpDataSet(out))
	}
	if int(out.NumRows()) != len(want) {
		t.Fatalf("row count: got %d, want %d", out.NumRows(), len(want))
	}
	for r := range out.NumRows() {
		text, isNull := cellText(col, int(r))
		got := "<null>"
		if !isNull {
			got = newColumnCellText(text)
		}
		if got != want[r] {
			t.Errorf("row %d: got %q, want %q", r, got, want[r])
		}
	}
}

// assertV1Column is assertNewColumnColumn for v1, recording what v1 answers for
// a corrected case so the difference is visible here rather than only described
// in a comment.
func assertV1Column(t *testing.T, data [][]string, statement string, column string, want []string) {
	t.Helper()

	cfgJSON, err := json.Marshal(&NewColumnConfiguration{Statement: statement})
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}
	ds, err := conversion.ConvertToTypedData(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	out, err := (&operator.NewColumnOperator{}).Transform(ds, string(cfgJSON), nil)
	if err != nil {
		t.Fatalf("v1 failed: %v", err)
	}
	if len(out.Rows) != len(want) {
		t.Fatalf("v1 row count: got %d, want %d", len(out.Rows), len(want))
	}
	for i, r := range out.Rows {
		c := r.GetColumn(column)
		if c == nil {
			t.Fatalf("v1 row %d has no column %q", i, column)
		}
		got := "<null>"
		if c.CellValue != nil && c.CellValue.DataType != types.NilType {
			got = newColumnCellText(c.CellValue.ToString())
		}
		if got != want[i] {
			t.Errorf("v1 row %d: got %q, want %q (this test records v1's behaviour)", i, got, want[i])
		}
	}
}

// assertV1Value is assertNewColumnValue for v1, used to record what v1 answers
// for a corrected case so the difference is visible here rather than only
// described in a comment.
func assertV1Value(t *testing.T, data [][]string, statement string, column string, want string) {
	t.Helper()

	cfgJSON, err := json.Marshal(&NewColumnConfiguration{Statement: statement})
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}
	ds, err := conversion.ConvertToTypedData(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	out, err := (&operator.NewColumnOperator{}).Transform(ds, string(cfgJSON), nil)
	if err != nil {
		t.Fatalf("v1 failed: %v", err)
	}
	for i, r := range out.Rows {
		c := r.GetColumn(column)
		if c == nil {
			t.Fatalf("v1 row %d has no column %q", i, column)
		}
		got := "<null>"
		if c.CellValue != nil && c.CellValue.DataType != types.NilType {
			got = newColumnCellText(c.CellValue.ToString())
		}
		if got != want {
			t.Errorf("v1 row %d: got %s, want %s (this test records v1's behaviour)", i, got, want)
		}
	}
}

// TestNewColumnV2PreservesRowOrder checks the one structural guarantee a
// NewColumn statement carries: the rows come back in the order they went in,
// including when the statement is a whole-table aggregate (which this operator
// evaluates through a scalar subquery precisely so it cannot reorder them).
func TestNewColumnV2PreservesRowOrder(t *testing.T) {
	data := [][]string{
		{"name", "qty"},
		{"e", "5"},
		{"a", "1"},
		{"d", "4"},
		{"b", "2"},
		{"c", "3"},
	}
	for _, statement := range []string{
		"`qty` AS `c`",
		"SUMX(`qty`) AS `c`",
		"MEDIAN(`qty`) AS `c`",
	} {
		t.Run(statement, func(t *testing.T) {
			cfgJSON, _ := json.Marshal(&NewColumnConfiguration{Statement: statement})
			ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
			if err != nil {
				t.Fatalf("conversion failed: %v", err)
			}
			defer ds.Release()

			out, err := (&NewColumnOperatorv2{}).Transform(ds, string(cfgJSON), nil)
			if err != nil {
				t.Fatalf("v2 failed: %v", err)
			}
			defer out.Release()

			col := out.GetColumn("name")
			if col == nil {
				t.Fatalf("the name column is missing:\n%s", dumpDataSet(out))
			}
			want := []string{"e", "a", "d", "b", "c"}
			for r := range out.NumRows() {
				got, _ := cellText(col, int(r))
				if got != want[r] {
					t.Errorf("row %d: got %q, want %q - the input order was not kept", r, got, want[r])
				}
			}
		})
	}
}

// TestNewColumnV2EmptyDataset covers a dataset with columns but no rows. The
// column names come from the schema rather than from a sample row, so the
// statement still builds - reading them off the first row would leave the
// operator emitting `SELECT , <statement>`.
//
// Transformv2 returns an empty dataset before reaching any operator, so this
// only arises for a caller using the operator directly.
func TestNewColumnV2EmptyDataset(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2([][]string{{"name", "qty"}}, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()
	if ds.NumRows() != 0 || ds.NumCols() != 2 {
		t.Fatalf("expected a 0-row, 2-column dataset, got %d rows and %d columns", ds.NumRows(), ds.NumCols())
	}

	out, err := (&NewColumnOperatorv2{}).Transform(ds, `{"statement": "`+"`qty` AS `c`"+`"}`, nil)
	if err != nil {
		t.Fatalf("v2 failed over an empty dataset: %v", err)
	}
	defer out.Release()

	if out.NumRows() != 0 {
		t.Errorf("row count: got %d, want 0", out.NumRows())
	}
	if out.GetColumn("c") == nil {
		t.Errorf("the derived column is missing:\n%s", dumpDataSet(out))
	}
}

// TestNewColumnV2DependencyChain is v1's batch test over this operator: a
// statement must see the columns earlier steps produced, not a stale copy of the
// input. Transformv2 runs every step on its own, so this is about the operator
// reading the dataset it was handed.
func TestNewColumnV2DependencyChain(t *testing.T) {
	ds, err := conversion.ConvertToTypedDatav2(newColumnTestData, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	defer ds.Release()

	op := &NewColumnOperatorv2{}
	cur := ds
	for _, statement := range []string{
		"MULTIPLY(`Quantity`, 2) AS `Base`",
		"MULTIPLY(`Base`, 2) AS `Doubled`",
		"MULTIPLY(`Doubled`, 2) AS `Quadrupled`",
	} {
		cfgJSON, _ := json.Marshal(&NewColumnConfiguration{Statement: statement})
		next, err := op.Transform(cur, string(cfgJSON), nil)
		if err != nil {
			t.Fatalf("%s failed: %v", statement, err)
		}
		t.Cleanup(next.Release)
		cur = next
	}

	qty := cur.GetColumn("Quantity")
	quad := cur.GetColumn("Quadrupled")
	if qty == nil || quad == nil {
		t.Fatalf("expected both Quantity and Quadrupled:\n%s", dumpDataSet(cur))
	}
	for r := range cur.NumRows() {
		qtyText, qtyNull := cellText(qty, int(r))
		quadText, quadNull := cellText(quad, int(r))
		if qtyNull != quadNull {
			t.Errorf("row %d: nullness differs, Quantity null=%v but Quadrupled null=%v", r, qtyNull, quadNull)
			continue
		}
		if qtyNull {
			continue
		}
		q, err := strconv.ParseFloat(qtyText, 64)
		if err != nil {
			t.Fatalf("row %d: could not read Quantity %q: %v", r, qtyText, err)
		}
		got, err := strconv.ParseFloat(quadText, 64)
		if err != nil {
			t.Fatalf("row %d: could not read Quadrupled %q: %v", r, quadText, err)
		}
		if got != q*8 {
			t.Errorf("row %d: Quadrupled is %v, want %v - the chain did not see the earlier columns", r, got, q*8)
		}
	}
}
