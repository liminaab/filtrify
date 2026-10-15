//go:build duckdb_arrow

package operatorv2

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

// These tests hold executeSQLQueryDuck to executeSQLQuery: both run the same
// query over the same dataset and the results have to agree. The queries are
// the ones the operators in this package actually build, so a parity failure
// here is a behaviour change a caller would see.

// Trade Date carries a time component so it is inferred as a real timestamp
// column - the filter builder only emits todate()/todatetime() against temporal
// columns, so a date-typed column is what exercises that path.
var duckTestData = [][]string{
	{"Instrument name", "Quantity", "Market Value", "Currency", "Active", "Trade Date", "Settle Time"},
	{"ERIC B", "100", "1000.50", "SEK", "true", "2021-01-02T00:00:00Z", "10:30:00"},
	{"AMZN", "20", "2000.75", "USD", "false", "2021-02-03T00:00:00Z", "11:45:00"},
	{"ERIC B", "300", "3000.25", "SEK", "true", "2021-03-04T00:00:00Z", "12:00:00"},
	{"TSLA", "40", "4000.00", "USD", "true", "2021-04-05T00:00:00Z", "13:15:00"},
}

func duckTestDataset(t *testing.T, data [][]string) *types.DataSetv2 {
	t.Helper()
	ds, err := conversion.ConvertToTypedDatav2(data, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	return ds
}

// cellText renders row i of an arrow column as text, with "" for a null, so two
// results can be compared without caring which concrete array type each side
// chose.
func cellText(col *arrow.Column, row int) (string, bool) {
	// A null-typed column holds nothing but nulls and carries no validity
	// bitmap, so IsNull does not answer for it.
	if col.DataType().ID() == arrow.NULL {
		return "", true
	}
	idx := row
	for _, chunk := range col.Data().Chunks() {
		if idx < chunk.Len() {
			if chunk.IsNull(idx) {
				return "", true
			}
			return chunk.ValueStr(idx), false
		}
		idx -= chunk.Len()
	}
	return "", true
}

func dumpDataSet(ds *types.DataSetv2) string {
	var sb strings.Builder
	for c := range ds.NumCols() {
		col := ds.Column(int(c))
		sb.WriteString(fmt.Sprintf("  %s (%s):", col.Name(), col.DataType()))
		for r := range ds.NumRows() {
			text, isNull := cellText(col, int(r))
			if isNull {
				sb.WriteString(" <null>")
				continue
			}
			sb.WriteString(" " + text)
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// typesAgree reports whether a qlbridge result column type and a DuckDB one are
// the same up to the places the DuckDB path deliberately does NOT reproduce
// qlbridge's behaviour, because in each of them qlbridge loses information:
//
//   - qlbridge infers the result type from the Go values its row scan produced,
//     so an IntType (int32) column comes back int64. The DuckDB path maps the
//     result back onto the input column's own type, so int32 stays int32.
//   - when a query matches no rows qlbridge has no values left to infer from and
//     types every column null, losing the schema. The DuckDB path keeps the
//     column types regardless of how many rows survived.
//   - qlbridge's aggregators accumulate in float64, so summing an integer column
//     yields a float. DuckDB sums exactly, in DECIMAL(38,0), and the result
//     stays an integer.
//
// Everywhere else the types have to match exactly. Values are always compared
// strictly - these allowances are about the declared type, not the data.
func typesAgree(want, got arrow.DataType, wantAllNull bool) bool {
	if want.ID() == got.ID() {
		return true
	}
	// qlbridge widens int32 to int64
	if want.ID() == arrow.INT64 && got.ID() == arrow.INT32 {
		return true
	}
	// qlbridge aggregates integers into a float
	if want.ID() == arrow.FLOAT64 && (got.ID() == arrow.INT32 || got.ID() == arrow.INT64) {
		return true
	}
	// qlbridge infers a column's type from the values it scanned, so with
	// nothing but nulls left - an empty result, or a column null in every
	// surviving row - it has nothing to infer from and falls back to null.
	if wantAllNull && want.ID() == arrow.NULL {
		return true
	}
	return false
}

// allNull reports whether every row of col is null.
func allNull(col *arrow.Column, rows int64) bool {
	for r := range rows {
		if _, isNull := cellText(col, int(r)); !isNull {
			return false
		}
	}
	return true
}

// assertSameResult compares two query results column for column: same shape,
// same column names, same values, and types that agree per typesAgree.
func assertSameResult(t *testing.T, want, got *types.DataSetv2, query string) {
	t.Helper()

	if want.NumCols() != got.NumCols() {
		t.Errorf("%s\ncolumn count mismatch: qlbridge has %d, duckdb has %d\nqlbridge:\n%sduckdb:\n%s",
			query, want.NumCols(), got.NumCols(), dumpDataSet(want), dumpDataSet(got))
		return
	}
	if want.NumRows() != got.NumRows() {
		t.Errorf("%s\nrow count mismatch: qlbridge has %d, duckdb has %d\nqlbridge:\n%sduckdb:\n%s",
			query, want.NumRows(), got.NumRows(), dumpDataSet(want), dumpDataSet(got))
		return
	}

	for c := range want.NumCols() {
		wantCol, gotCol := want.Column(int(c)), got.Column(int(c))
		if wantCol.Name() != gotCol.Name() {
			t.Errorf("%s\ncolumn %d name mismatch: qlbridge %q, duckdb %q",
				query, c, wantCol.Name(), gotCol.Name())
			continue
		}
		if !typesAgree(wantCol.DataType(), gotCol.DataType(), allNull(wantCol, want.NumRows())) {
			t.Errorf("%s\ncolumn %q type mismatch: qlbridge %s, duckdb %s",
				query, wantCol.Name(), wantCol.DataType(), gotCol.DataType())
			continue
		}
		for r := range want.NumRows() {
			wantText, wantNull := cellText(wantCol, int(r))
			gotText, gotNull := cellText(gotCol, int(r))
			if wantNull != gotNull || wantText != gotText {
				t.Errorf("%s\ncolumn %q row %d mismatch: qlbridge %q (null=%v), duckdb %q (null=%v)",
					query, wantCol.Name(), r, wantText, wantNull, gotText, gotNull)
			}
		}
	}
}

// rowTexts renders a whole result as one string per row, so results can be
// compared as a multiset of rows where the row order carries no meaning.
func rowTexts(ds *types.DataSetv2) []string {
	out := make([]string, 0, ds.NumRows())
	for r := range ds.NumRows() {
		parts := make([]string, 0, ds.NumCols())
		for c := range ds.NumCols() {
			text, isNull := cellText(ds.Column(int(c)), int(r))
			if isNull {
				parts = append(parts, "<null>")
				continue
			}
			parts = append(parts, text)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}

// assertSameRowsAnyOrder is assertSameResult for queries with a GROUP BY.
// Neither engine promises an order for grouped output - qlbridge emits groups in
// its hash map's iteration order and DuckDB in its hash aggregate's - so the
// rows are compared as a multiset. Column names and types are still checked in
// order, and the values still have to match exactly, just not row-position for
// row-position.
func assertSameRowsAnyOrder(t *testing.T, want, got *types.DataSetv2, query string) {
	t.Helper()

	if want.NumCols() != got.NumCols() || want.NumRows() != got.NumRows() {
		t.Errorf("%s\nshape mismatch: qlbridge %dx%d, duckdb %dx%d\nqlbridge:\n%sduckdb:\n%s",
			query, want.NumRows(), want.NumCols(), got.NumRows(), got.NumCols(),
			dumpDataSet(want), dumpDataSet(got))
		return
	}

	for c := range want.NumCols() {
		wantCol, gotCol := want.Column(int(c)), got.Column(int(c))
		if wantCol.Name() != gotCol.Name() {
			t.Errorf("%s\ncolumn %d name mismatch: qlbridge %q, duckdb %q",
				query, c, wantCol.Name(), gotCol.Name())
		}
		if !typesAgree(wantCol.DataType(), gotCol.DataType(), allNull(wantCol, want.NumRows())) {
			t.Errorf("%s\ncolumn %q type mismatch: qlbridge %s, duckdb %s",
				query, wantCol.Name(), wantCol.DataType(), gotCol.DataType())
		}
	}

	wantRows, gotRows := rowTexts(want), rowTexts(got)
	for i := range wantRows {
		if wantRows[i] != gotRows[i] {
			t.Errorf("%s\nrow sets differ:\nqlbridge: %v\nduckdb:   %v", query, wantRows, gotRows)
			return
		}
	}
}

func runParity(t *testing.T, query string, data [][]string) {
	t.Helper()
	ds := duckTestDataset(t, data)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	want, errQL := executeSQLQuery(query, ds, colTypes)
	got, errDuck := executeSQLQueryDuck(query, ds, colTypes)

	if errQL != nil {
		t.Fatalf("qlbridge failed on %s: %v", query, errQL)
	}
	if errDuck != nil {
		t.Fatalf("duckdb failed on %s: %v", query, errDuck)
	}
	defer want.Release()
	defer got.Release()

	// Grouped output has no defined row order in either engine.
	if strings.Contains(strings.ToUpper(query), "GROUP BY") {
		assertSameRowsAnyOrder(t, want, got, query)
		return
	}
	assertSameResult(t, want, got, query)
}

// TestDuckFilterParity covers the queries FilterOperator builds.
func TestDuckFilterParity(t *testing.T) {
	all := "`Instrument name`,`Quantity`,`Market Value`,`Currency`,`Active`,`Trade Date`,`Settle Time`"
	for _, q := range []string{
		"SELECT " + all + " FROM ext WHERE ( `Quantity` > 50 )",
		"SELECT " + all + " FROM ext WHERE ( `Quantity` <= 100 )",
		"SELECT " + all + " FROM ext WHERE ( `Currency` = 'SEK' )",
		"SELECT " + all + " FROM ext WHERE ( `Currency` != 'SEK' )",
		"SELECT " + all + " FROM ext WHERE ( `Market Value` > 1500.000000 )",
		"SELECT " + all + " FROM ext WHERE ( `Active` = true )",
		"SELECT " + all + " FROM ext WHERE ( `Instrument name` CONTAINS 'ERIC' )",
		"SELECT " + all + " FROM ext WHERE ( `Instrument name` NOT CONTAINS 'ERIC' )",
		"SELECT " + all + " FROM ext WHERE ( `Currency` = 'SEK' ) OR ( `Quantity` > 30 )",
		"SELECT " + all + " FROM ext WHERE ( `Currency` = 'SEK' ) AND ( `Quantity` > 200 )",
		"SELECT " + all + " FROM ext WHERE ( (`Currency` = NULL OR `Currency` = '') )",
		"SELECT " + all + " FROM ext WHERE ( NOT (`Currency` = NULL OR `Currency` = '') )",
		"SELECT " + all + " FROM ext WHERE ( `Trade Date` >= todatetime('2021-03-01 00:00:00') )",
		"SELECT " + all + " FROM ext WHERE ( `Settle Time` = totime('12:00:00') )",
		"SELECT " + all + " FROM ext WHERE ( `Quantity` > 1000 )",
	} {
		t.Run(q, func(t *testing.T) { runParity(t, q, duckTestData) })
	}
}

// TestDuckNewColumnParity covers the queries NewColumnOperator builds.
func TestDuckNewColumnParity(t *testing.T) {
	all := "`Instrument name`,`Quantity`,`Market Value`,`Currency`,`Active`,`Trade Date`,`Settle Time`"
	for _, q := range []string{
		"SELECT " + all + ", `Quantity` * `Market Value` AS `Total` FROM ext",
		"SELECT " + all + ", `Quantity` + 1 AS `QtyPlus` FROM ext",
		"SELECT " + all + ", `Currency` = 'SEK' AS `IsSEK` FROM ext",
		"SELECT " + all + ", concat(`Instrument name`, `Currency`) AS `Label` FROM ext",
		"SELECT " + all + ", length(`Instrument name`) AS `NameLen` FROM ext",
		"SELECT " + all + ", abs(`Market Value`) AS `AbsVal` FROM ext",
		"SELECT " + all + ", round(`Market Value`, 1) AS `Rounded` FROM ext",
		"SELECT " + all + ", ifel(`Quantity` > 50, 'big', 'small') AS `Size` FROM ext",
		"SELECT sumx(`Quantity`) AS `TotalQty` FROM ext",
		"SELECT average(`Market Value`) AS `AvgVal` FROM ext",
	} {
		t.Run(q, func(t *testing.T) { runParity(t, q, duckTestData) })
	}
}

// TestDuckAggregateParity covers the queries AggregateOperator builds, including
// the lmnagg wrapper it puts around every group-by column.
func TestDuckAggregateParity(t *testing.T) {
	for _, q := range []string{
		"SELECT sumx(`Quantity`) AS `Quantity`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
		"SELECT average(`Market Value`) AS `Market Value`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
		"SELECT sumx(`Quantity`) AS `Quantity`,lmnagg(`Instrument name`) AS `Instrument name` FROM ext GROUP BY `Instrument name`",
		"SELECT lmnagg(`Quantity`) AS `Quantity`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
		"SELECT lmnagg(`Instrument name`) AS `Instrument name`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
		"SELECT maxcol(`Quantity`) AS `Quantity`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
	} {
		t.Run(q, func(t *testing.T) { runParity(t, q, duckTestData) })
	}
}

// TestDuckMinColIsCorrect covers mincol, which is left out of the parity set
// above because qlbridge gets it wrong. Its aggregator seeds the running minimum
// at 0 instead of at the first value it sees and only ever replaces it on
// `realVal < m.val`, so over all-positive input it returns 0 - min(100, 300)
// comes back as 0 rather than 100. DuckDB's min() is correct, and this test pins
// the correct answer rather than qlbridge's.
func TestDuckMinColIsCorrect(t *testing.T) {
	ds := duckTestDataset(t, duckTestData)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	q := "SELECT mincol(`Quantity`) AS `Quantity`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`"
	got, err := executeSQLQueryDuck(q, ds, colTypes)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()

	want := []string{"100|SEK", "20|USD"}
	gotRows := rowTexts(got)
	if len(gotRows) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", len(gotRows), len(want), dumpDataSet(got))
	}
	for i := range want {
		if gotRows[i] != want[i] {
			t.Errorf("row %d is %q, want %q", i, gotRows[i], want[i])
		}
	}
}

// TestDuckNullHandling checks a dataset with holes in it - the type a column
// comes back as depends on the input type, not on which rows happen to be null.
func TestDuckNullHandling(t *testing.T) {
	data := [][]string{
		{"Name", "Qty", "Val", "Cur"},
		{"A", "10", "1.5", "SEK"},
		{"B", "", "", ""},
		{"C", "30", "3.5", "USD"},
	}
	all := "`Name`,`Qty`,`Val`,`Cur`"
	for _, q := range []string{
		"SELECT " + all + " FROM ext WHERE ( (`Cur` = NULL OR `Cur` = '') )",
		"SELECT " + all + " FROM ext WHERE ( NOT (`Cur` = NULL OR `Cur` = '') )",
		"SELECT " + all + " FROM ext WHERE ( `Qty` > 5 )",
		"SELECT lmnagg(`Qty`) AS `Qty`,lmnagg(`Cur`) AS `Cur` FROM ext GROUP BY `Cur`",
	} {
		t.Run(q, func(t *testing.T) { runParity(t, q, data) })
	}

	// Summing a group whose every value is null is the one case where the two
	// engines genuinely disagree rather than one being wrong: SQL defines
	// sum(NULL) as NULL, which is what DuckDB returns, while qlbridge's
	// accumulator starts at zero and reports 0. The null is the better answer -
	// it keeps "nothing to add up" distinct from "adds up to zero" - so this
	// pins DuckDB's result.
	t.Run("summing an all-null group yields null", func(t *testing.T) {
		ds := duckTestDataset(t, data)
		defer ds.Release()
		_, colTypes := extractHeadersAndTypeMap(ds)

		q := "SELECT sumx(`Qty`) AS `Qty`,lmnagg(`Cur`) AS `Cur` FROM ext GROUP BY `Cur`"
		got, err := executeSQLQueryDuck(q, ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()

		want := []string{"10|SEK", "30|USD", "<null>|<null>"}
		gotRows := rowTexts(got)
		sort.Strings(want)
		if len(gotRows) != len(want) {
			t.Fatalf("got %d rows, want %d:\n%s", len(gotRows), len(want), dumpDataSet(got))
		}
		for i := range want {
			if gotRows[i] != want[i] {
				t.Errorf("row %d is %q, want %q", i, gotRows[i], want[i])
			}
		}
	})
}

// TestDuckKeepsColumnTypes pins the two places the DuckDB path intentionally
// differs from qlbridge, both of which are qlbridge losing type information
// (see typesAgree). These are the reason executeSQLQueryDuck takes
// existingColumnTypeMap at all.
func TestDuckKeepsColumnTypes(t *testing.T) {
	ds := duckTestDataset(t, duckTestData)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)
	if colTypes["Quantity"] != types.IntType {
		t.Fatalf("expected Quantity to be an IntType column, got %s", colTypes["Quantity"])
	}

	t.Run("an int column stays int32", func(t *testing.T) {
		got, err := executeSQLQueryDuck("SELECT `Quantity` FROM ext WHERE ( `Quantity` > 50 )", ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		if got.Column(0).DataType().ID() != arrow.INT32 {
			t.Errorf("Quantity came back as %s, want int32", got.Column(0).DataType())
		}
	})

	t.Run("an empty result keeps its schema", func(t *testing.T) {
		got, err := executeSQLQueryDuck("SELECT `Quantity`,`Currency` FROM ext WHERE ( `Quantity` > 99999 )", ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		if got.NumRows() != 0 {
			t.Fatalf("expected no rows, got %d", got.NumRows())
		}
		if got.Column(0).DataType().ID() != arrow.INT32 {
			t.Errorf("Quantity came back as %s, want int32", got.Column(0).DataType())
		}
		if got.Column(1).DataType().ID() != arrow.STRING {
			t.Errorf("Currency came back as %s, want utf8", got.Column(1).DataType())
		}
	})

	t.Run("an integer aggregate stays an exact integer", func(t *testing.T) {
		// DuckDB sums integers into DECIMAL(38,0). A sum can outgrow the input
		// width, so it widens to int64 rather than being forced back to int32 -
		// but it stays an integer instead of going through float64 the way
		// qlbridge's aggregators do.
		got, err := executeSQLQueryDuck("SELECT sumx(`Quantity`) AS `Quantity` FROM ext", ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		if got.Column(0).DataType().ID() != arrow.INT64 {
			t.Errorf("Quantity came back as %s, want int64", got.Column(0).DataType())
		}
		text, _ := cellText(got.Column(0), 0)
		if text != "460" {
			t.Errorf("sum is %q, want 460", text)
		}
	})
}

// TestDuckEmptyDataset: a dataset with no rows still has to produce the right
// schema, and one with no columns is an error rather than a crash.
func TestDuckEmptyDataset(t *testing.T) {
	t.Run("no rows", func(t *testing.T) {
		// A header-only dataset types every column as text, so the filter has to
		// be a text one to be valid against it.
		runParity(t, "SELECT `Name`,`Qty` FROM ext WHERE ( `Name` = 'nothing' )", [][]string{
			{"Name", "Qty"},
		})
	})

	t.Run("no columns", func(t *testing.T) {
		// ConvertToTypedDatav2 rejects empty raw data, so build the zero-column
		// table directly.
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		rec := builder.NewRecordBatch()
		defer rec.Release()
		ds := &types.DataSetv2{Table: array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})}
		defer ds.Release()

		if _, err := executeSQLQueryDuck("SELECT 1", ds, nil); err == nil {
			t.Error("expected an error for a dataset with no columns")
		}
	})

	t.Run("nil dataset", func(t *testing.T) {
		if _, err := executeSQLQueryDuck("SELECT 1", nil, nil); err == nil {
			t.Error("expected an error for a nil dataset")
		}
	})
}

// TestDuckQueryError: a query DuckDB rejects has to surface as an error.
func TestDuckQueryError(t *testing.T) {
	ds := duckTestDataset(t, duckTestData)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	for _, q := range []string{
		"SELECT `No Such Column` FROM ext",
		"SELECT FROM WHERE",
		"SELECT `Quantity` FROM no_such_table",
	} {
		if _, err := executeSQLQueryDuck(q, ds, colTypes); err == nil {
			t.Errorf("expected an error for %q", q)
		}
	}
}

// TestDuckProjectionIsNotCorrupted is a regression guard for the arrow_scan
// projection pushdown bug that made executeSQLQueryDuck copy the view into a
// real table: selecting a subset of columns used to return the data of the
// leading columns under the requested names.
func TestDuckProjectionIsNotCorrupted(t *testing.T) {
	data := [][]string{
		{"a", "b", "c"},
		{"100", "200", "300"},
		{"101", "201", "301"},
	}
	ds := duckTestDataset(t, data)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	for col, want := range map[string][]string{
		"a": {"100", "101"},
		"b": {"200", "201"},
		"c": {"300", "301"},
	} {
		q := fmt.Sprintf("SELECT `%s` FROM ext", col)
		got, err := executeSQLQueryDuck(q, ds, colTypes)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got.NumCols() != 1 || got.Column(0).Name() != col {
			t.Fatalf("%s: unexpected result shape:\n%s", q, dumpDataSet(got))
		}
		for i, wantText := range want {
			gotText, _ := cellText(got.Column(0), i)
			if gotText != wantText {
				t.Errorf("%s: row %d is %q, want %q (projection pushdown corrupted the result)",
					q, i, gotText, wantText)
			}
		}
		got.Release()
	}
}

// TestDuckLargeDataset pushes enough rows through to span several record
// batches, so the streaming path and the chunked result columns both get
// exercised rather than only the single-batch case.
func TestDuckLargeDataset(t *testing.T) {
	const rows = 50000
	const groups = 7

	data := make([][]string, 0, rows+1)
	data = append(data, []string{"id", "grp", "val"})
	for i := range rows {
		data = append(data, []string{
			strconv.Itoa(i),
			"g" + strconv.Itoa(i%groups),
			strconv.FormatFloat(float64(i)*1.5, 'f', 2, 64),
		})
	}

	ds := duckTestDataset(t, data)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	t.Run("filter", func(t *testing.T) {
		got, err := executeSQLQueryDuck("SELECT `id`,`grp`,`val` FROM ext WHERE ( `id` > 25000 )", ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		if want := int64(rows - 25001); got.NumRows() != want {
			t.Errorf("got %d rows, want %d", got.NumRows(), want)
		}
	})

	t.Run("group by", func(t *testing.T) {
		got, err := executeSQLQueryDuck("SELECT sumx(`val`) AS `val`,lmnagg(`grp`) AS `grp` FROM ext GROUP BY `grp`", ds, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		defer got.Release()
		if got.NumRows() != groups {
			t.Errorf("got %d groups, want %d", got.NumRows(), groups)
		}
	})
}

// TestDuckConcurrent runs queries from several goroutines at once. The qlbridge
// path needs process-wide locks around its global schema registry; this one
// gives every call its own in-memory database and should need none, so a shared
// mutable bit of state would show up here (run with -race).
func TestDuckConcurrent(t *testing.T) {
	ds := duckTestDataset(t, duckTestData)
	defer ds.Release()
	_, colTypes := extractHeadersAndTypeMap(ds)

	queries := []string{
		"SELECT `Instrument name`,`Quantity` FROM ext WHERE ( `Quantity` > 50 )",
		"SELECT sumx(`Quantity`) AS `Quantity` FROM ext",
		"SELECT `Currency` FROM ext WHERE ( `Currency` = 'SEK' )",
		"SELECT sumx(`Quantity`) AS `Quantity`,lmnagg(`Currency`) AS `Currency` FROM ext GROUP BY `Currency`",
	}

	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := queries[i%len(queries)]
			got, err := executeSQLQueryDuck(q, ds, colTypes)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				return
			}
			defer got.Release()
			if got.NumCols() == 0 {
				t.Errorf("%s: empty result", q)
			}
		}(i)
	}
	wg.Wait()
}

// TestBuildRequiresDuckArrowTag checks that the package really does refuse to
// build without -tags=duckdb_arrow. duckdb_disabled.go is what enforces that,
// and it is easy to delete by accident - nothing else would notice, and the
// result would be a build that silently lacks query execution. This compiles
// the package in a subprocess with no tags and expects a failure naming the tag.
func TestBuildRequiresDuckArrowTag(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the go tool is not on PATH")
	}

	cmd := exec.Command("go", "build", ".")
	// Build into a scratch dir so this never writes into the module.
	cmd.Env = append(os.Environ(), "GOFLAGS=", "GOBIN="+t.TempDir())
	out, err := cmd.CombinedOutput()

	if err == nil {
		t.Fatalf("the package built without -tags=duckdb_arrow, but the tag is mandatory.\n"+
			"operatorv2/duckdb_disabled.go is supposed to prevent this.\noutput:\n%s", out)
	}
	if !strings.Contains(string(out), "duckdb_arrow") {
		t.Errorf("the tagless build failed, but not with the guard's error - the message "+
			"should name the duckdb_arrow tag so the fix is obvious.\ngot:\n%s", out)
	}
}

// TestTranslateToDuckSQL pins the dialect rewrites, including the ones that
// must not fire inside a string literal.
func TestTranslateToDuckSQL(t *testing.T) {
	colTypes := map[string]types.CellDataType{
		"Qty": types.LongType,
		"Cur": types.StringType,
	}
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{
			name: "backticks become double quotes",
			in:   "SELECT `Qty` FROM ext",
			want: `SELECT "Qty" FROM ext`,
		},
		{
			name: "contains becomes a function call",
			in:   "SELECT * FROM ext WHERE `Cur` CONTAINS 'SE'",
			want: `SELECT * FROM ext WHERE contains(CAST("Cur" AS VARCHAR), CAST('SE' AS VARCHAR))`,
		},
		{
			name: "not contains keeps its negation",
			in:   "SELECT * FROM ext WHERE `Cur` NOT CONTAINS 'SE'",
			want: `SELECT * FROM ext WHERE NOT contains(CAST("Cur" AS VARCHAR), CAST('SE' AS VARCHAR))`,
		},
		{
			name: "equals null becomes is null",
			in:   "SELECT * FROM ext WHERE `Cur` = NULL",
			want: `SELECT * FROM ext WHERE "Cur" IS NULL`,
		},
		{
			name: "not equals null becomes is not null",
			in:   "SELECT * FROM ext WHERE `Cur` != NULL",
			want: `SELECT * FROM ext WHERE "Cur" IS NOT NULL`,
		},
		{
			name: "empty string comparison casts to text",
			in:   "SELECT * FROM ext WHERE `Qty` = ''",
			want: `SELECT * FROM ext WHERE CAST("Qty" AS VARCHAR) = ''`,
		},
		{
			name: "a real value comparison is left alone",
			in:   "SELECT * FROM ext WHERE `Cur` = 'SEK'",
			want: `SELECT * FROM ext WHERE "Cur" = 'SEK'`,
		},
		{
			name: "numeric lmnagg sums the group",
			in:   "SELECT lmnagg(`Qty`) AS `Qty` FROM ext",
			want: `SELECT CASE WHEN count(*) <> count("Qty") THEN NULL ELSE sum("Qty") END AS "Qty" FROM ext`,
		},
		{
			name: "text lmnagg collapses an agreeing group",
			in:   "SELECT lmnagg(`Cur`) AS `Cur` FROM ext",
			want: `SELECT CASE WHEN count(*) <> count("Cur") THEN NULL WHEN count(DISTINCT "Cur") = 1 THEN any_value("Cur") ELSE NULL END AS "Cur" FROM ext`,
		},
		{
			name: "a rewrite does not fire inside a literal",
			in:   "SELECT * FROM ext WHERE `Cur` = 'a CONTAINS b'",
			want: `SELECT * FROM ext WHERE "Cur" = 'a CONTAINS b'`,
		},
		{
			name: "an escaped quote inside a literal survives",
			in:   "SELECT * FROM ext WHERE `Cur` = 'it''s'",
			want: `SELECT * FROM ext WHERE "Cur" = 'it''s'`,
		},
		{
			name: "NULL inside a literal is not rewritten",
			in:   "SELECT * FROM ext WHERE `Cur` = 'x = NULL'",
			want: `SELECT * FROM ext WHERE "Cur" = 'x = NULL'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := translateToDuckSQL(tc.in, colTypes)
			if err != nil {
				t.Fatalf("translate failed: %v", err)
			}
			if got != tc.want {
				t.Errorf("translate(%q)\n  got  %s\n  want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestTranslateToDuckSQLErrors(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM ext WHERE `Cur` = 'unterminated",
		"SELECT `Unterminated FROM ext",
	} {
		if _, err := translateToDuckSQL(q, nil); err == nil {
			t.Errorf("expected an error for %q", q)
		}
	}
}
