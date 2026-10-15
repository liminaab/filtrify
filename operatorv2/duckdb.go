//go:build duckdb_arrow

package operatorv2

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/araddon/dateparse"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

// The whole package requires the duckdb_arrow build tag (duckdb_disabled.go
// enforces that), so the tag on this file is not what makes it mandatory. It is
// here to keep the failure legible: without it, a tagless build would compile
// this file against a duckdb-go that has no Arrow API and bury the real problem
// under a pile of "undefined: duckdb.NewArrowFromConn" errors. Excluding the
// file instead leaves duckdb_disabled.go's single explanatory error.

// duckArrowViewName is the name the input dataset is registered under as an
// Arrow view. It is deliberately not defaultTableName: the view is only ever a
// staging area that gets copied into a real table called defaultTableName, so
// the caller's query still reads from "ext". See executeSQLQueryDuck for why
// the copy is not optional.
const duckArrowViewName = "filtrify_arrow_src"

// duckArrowBatchSize is the record batch size the input table is streamed into
// DuckDB with. DuckDB's own vector size is 2048, so anything in that ballpark
// keeps the scan from being chopped up more finely than it processes.
const duckArrowBatchSize = 8192

// duckCompatMacros teaches DuckDB the lmnqlbridge function vocabulary the
// query builders in this package emit. Only the functions DuckDB does not
// already provide are listed - the deliberately suffixed lmnqlbridge names
// (sumx, floorx, minx, containsx, ...) exist precisely because the unsuffixed
// ones are meant to be the engine's own builtins, so concat/length/trim/abs/
// round/replace/left/right/median/first/last resolve to DuckDB natively.
//
// lmnagg is NOT here: it is type-dependent and is rewritten per call site by
// rewriteLmnAgg instead.
var duckCompatMacros = []string{
	`CREATE MACRO todate(x) AS CAST(x AS DATE)`,
	`CREATE MACRO todatetime(x) AS CAST(x AS TIMESTAMP)`,
	`CREATE MACRO datetimeutc(x) AS CAST(x AS TIMESTAMP)`,
	`CREATE MACRO totime(x) AS CAST(x AS TIME)`,
	`CREATE MACRO today() AS current_date`,
	`CREATE MACRO plusdays(d, n) AS CAST(d AS DATE) + CAST(n AS INTEGER)`,
	`CREATE MACRO average(x) AS avg(x)`,
	`CREATE MACRO sumx(x) AS sum(x)`,
	`CREATE MACRO mincol(x) AS min(x)`,
	`CREATE MACRO maxcol(x) AS max(x)`,
	// lmnqlbridge rounds the weighted average to two decimals
	`CREATE MACRO weighted_average(v, w) AS round(sum(v * w) / sum(w), 2)`,
	`CREATE MACRO ifel(c, a, b) AS CASE WHEN c THEN a ELSE b END`,
	`CREATE MACRO isblank(x) AS x IS NULL OR trim(CAST(x AS VARCHAR)) = ''`,
	`CREATE MACRO containsx(a, b) AS contains(CAST(a AS VARCHAR), CAST(b AS VARCHAR))`,
	`CREATE MACRO notcontainsx(a, b) AS NOT contains(CAST(a AS VARCHAR), CAST(b AS VARCHAR))`,
	`CREATE MACRO floorx(x) AS floor(x)`,
	`CREATE MACRO minx(a, b) AS least(a, b)`,
	`CREATE MACRO maxx(a, b) AS greatest(a, b)`,
	`CREATE MACRO plus(a, b) AS a + b`,
	`CREATE MACRO minus(a, b) AS a - b`,
	`CREATE MACRO multiply(a, b) AS a * b`,
	`CREATE MACRO divide(a, b) AS a / b`,
}

// executeSQLQueryDuck runs q against dataset in an in-process DuckDB instance
// and returns the result as a new DataSetv2. It is the DuckDB counterpart of
// executeSQLQuery: same contract, same query dialect (see translateToDuckSQL),
// but the work happens in DuckDB rather than qlbridge.
//
// existingColumnTypeMap carries the input column types. A result column that
// keeps its original name and that DuckDB computed in the same kind of type is
// handed back as that original type, so a filter over an IntType column still
// yields IntType rather than whatever width DuckDB happened to use. See
// canonicalDuckResultType for the exact rule.
//
// Unlike executeSQLQuery this needs no global locking: every call gets its own
// private in-memory database, so there is no shared schema registry to serialise
// against.
func executeSQLQueryDuck(q string, dataset *types.DataSetv2, existingColumnTypeMap map[string]types.CellDataType) (*types.DataSetv2, error) {
	if dataset == nil || dataset.Table == nil {
		return nil, errors.New("cannot execute a query against a nil dataset")
	}
	// DuckDB cannot represent a zero-column relation, so arrow_scan rejects the
	// view outright. Fail with something more useful than its internal error.
	if dataset.NumCols() == 0 {
		return nil, errors.New("cannot execute a query against a dataset with no columns")
	}

	translated, err := translateToDuckSQL(q, existingColumnTypeMap)
	if err != nil {
		return nil, err
	}

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, err
	}
	defer connector.Close()

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	driverConn, ok := conn.(driver.Conn)
	if !ok {
		return nil, errors.New("duckdb connection does not implement driver.Conn")
	}
	ar, err := duckdb.NewArrowFromConn(driverConn)
	if err != nil {
		return nil, err
	}

	reader := array.NewTableReader(dataset.Table, duckArrowBatchSize)
	defer reader.Release()
	releaseView, err := ar.RegisterView(reader, duckArrowViewName)
	if err != nil {
		return nil, err
	}
	defer releaseView()

	for _, macro := range duckCompatMacros {
		if err := duckExec(ctx, ar, macro); err != nil {
			return nil, fmt.Errorf("could not register compatibility macro %q: %w", macro, err)
		}
	}

	// Copy the Arrow view into a real table before querying it. This is not an
	// optimisation, it is a correctness requirement: DuckDB's arrow_scan ignores
	// the projection it is handed, so reading a subset of the view's columns
	// returns the data of the *first* n columns under the requested names. On a
	// `SELECT "c" FROM view` that silently yields column "a"'s values, and when
	// the two columns have different widths it segfaults. `SELECT *` projects
	// every column, so the copy below is the one shape of read that is safe, and
	// everything downstream then runs against a normal table.
	copyQuery := fmt.Sprintf(`CREATE TEMP TABLE %s AS SELECT * FROM %s`,
		quoteDuckIdentifier(defaultTableName), quoteDuckIdentifier(duckArrowViewName))
	if err := duckExec(ctx, ar, copyQuery); err != nil {
		return nil, err
	}

	result, err := ar.QueryContext(ctx, translated)
	if err != nil {
		return nil, err
	}
	defer result.Release()

	return duckResultToDataSet(result, existingColumnTypeMap)
}

// duckExec runs a statement whose result is not needed, draining and releasing
// the reader it produces.
func duckExec(ctx context.Context, ar *duckdb.Arrow, stmt string) error {
	reader, err := ar.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
		// DDL returns a single "Count" row we have no use for.
	}
	return reader.Err()
}

func quoteDuckIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// --- query translation -------------------------------------------------------

// duckIdentPattern matches either a double-quoted identifier or a bare word, so
// the rewrites below can anchor on the left-hand side of an operator.
const duckIdentPattern = `(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)`

// literalPlaceholder wraps a string literal's index in NUL bytes. NUL cannot
// occur in a SQL query we were handed as a Go string from JSON config, which
// makes the placeholder unambiguous and keeps it clear of regex metacharacters.
const literalPlaceholderFmt = "\x00L%d\x00"

var (
	duckNotContainsRe = regexp.MustCompile(`(?i)(` + duckIdentPattern + `)\s+NOT\s+CONTAINS\s+(\x00L\d+\x00|` + duckIdentPattern + `)`)
	duckContainsRe    = regexp.MustCompile(`(?i)(` + duckIdentPattern + `)\s+CONTAINS\s+(\x00L\d+\x00|` + duckIdentPattern + `)`)
	duckEqNullRe      = regexp.MustCompile(`(?i)(` + duckIdentPattern + `)\s*=\s*NULL\b`)
	duckNotEqNullRe   = regexp.MustCompile(`(?i)(` + duckIdentPattern + `)\s*!=\s*NULL\b`)
	duckEqLiteralRe   = regexp.MustCompile(`(` + duckIdentPattern + `)\s*(=|!=)\s*(\x00L\d+\x00)`)
	duckLmnAggRe      = regexp.MustCompile(`(?i)\blmnagg\(\s*(` + duckIdentPattern + `)\s*\)`)
	duckPlaceholderRe = regexp.MustCompile(`\x00L(\d+)\x00`)
)

// translateToDuckSQL rewrites a query written in the lmnqlbridge dialect that
// the operators in this package build into one DuckDB accepts. The differences
// that matter in practice:
//
//   - identifiers are backtick-quoted by the builders; DuckDB wants double quotes
//   - CONTAINS / NOT CONTAINS are infix operators in qlbridge, functions in DuckDB
//   - `col = NULL` means IS NULL in qlbridge; in DuckDB it is always NULL
//   - comparing a non-text column against a string literal is implicit in
//     qlbridge but a cast error in DuckDB, which the IS EMPTY filter relies on
//   - lmnagg has no DuckDB equivalent and is expanded per column type
//
// Function-name differences are handled by duckCompatMacros rather than here.
func translateToDuckSQL(q string, columnTypeMap map[string]types.CellDataType) (string, error) {
	skeleton, literals, err := extractSQLLiterals(q)
	if err != nil {
		return "", err
	}

	// NOT CONTAINS has to go first, otherwise the CONTAINS rule claims the tail
	// of it and leaves a dangling NOT.
	skeleton = duckNotContainsRe.ReplaceAllString(skeleton, `NOT contains(CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`)
	skeleton = duckContainsRe.ReplaceAllString(skeleton, `contains(CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`)

	skeleton = duckEqNullRe.ReplaceAllString(skeleton, `$1 IS NULL`)
	skeleton = duckNotEqNullRe.ReplaceAllString(skeleton, `$1 IS NOT NULL`)

	// `col = ''` is how the IS EMPTY / IS NOT EMPTY filters test for blankness,
	// and they emit it for every column type. DuckDB would try to cast '' to the
	// column's type and error out, so compare as text instead. Only the
	// empty-string literal needs this - a real value comparison should keep
	// DuckDB's type checking.
	skeleton = duckEqLiteralRe.ReplaceAllStringFunc(skeleton, func(match string) string {
		groups := duckEqLiteralRe.FindStringSubmatch(match)
		lit, ok := lookupSQLLiteral(groups[3], literals)
		if !ok || lit != `''` {
			return match
		}
		return fmt.Sprintf(`CAST(%s AS VARCHAR) %s %s`, groups[1], groups[2], groups[3])
	})

	skeleton = rewriteLmnAgg(skeleton, columnTypeMap)

	return restoreSQLLiterals(skeleton, literals), nil
}

// extractSQLLiterals replaces every single-quoted string literal with an opaque
// placeholder and converts backtick-quoted identifiers to double-quoted ones.
// Pulling the literals out first means the regex rewrites above cannot mistake
// the contents of a string for SQL - a filter value of "NOT CONTAINS" or "x =
// NULL" is data, not syntax.
func extractSQLLiterals(q string) (string, []string, error) {
	var out strings.Builder
	literals := make([]string, 0, 8)
	runes := []rune(q)

	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\'':
			start := i
			i++
			for i < len(runes) {
				if runes[i] != '\'' {
					i++
					continue
				}
				// '' inside a literal is an escaped quote, not the end of it.
				if i+1 < len(runes) && runes[i+1] == '\'' {
					i += 2
					continue
				}
				break
			}
			if i >= len(runes) {
				return "", nil, fmt.Errorf("unterminated string literal in query: %s", q)
			}
			literals = append(literals, string(runes[start:i+1]))
			out.WriteString(fmt.Sprintf(literalPlaceholderFmt, len(literals)-1))
		case '`':
			start := i + 1
			i++
			for i < len(runes) && runes[i] != '`' {
				i++
			}
			if i >= len(runes) {
				return "", nil, fmt.Errorf("unterminated quoted identifier in query: %s", q)
			}
			out.WriteString(quoteDuckIdentifier(string(runes[start:i])))
		case '"':
			// Already DuckDB-style quoting - copy it through untouched so it is
			// not mistaken for anything else.
			start := i
			i++
			for i < len(runes) && runes[i] != '"' {
				i++
			}
			if i >= len(runes) {
				return "", nil, fmt.Errorf("unterminated quoted identifier in query: %s", q)
			}
			out.WriteString(string(runes[start : i+1]))
		default:
			out.WriteRune(runes[i])
		}
	}

	return out.String(), literals, nil
}

func lookupSQLLiteral(placeholder string, literals []string) (string, bool) {
	groups := duckPlaceholderRe.FindStringSubmatch(placeholder)
	if groups == nil {
		return "", false
	}
	idx, err := strconv.Atoi(groups[1])
	if err != nil || idx < 0 || idx >= len(literals) {
		return "", false
	}
	return literals[idx], true
}

func restoreSQLLiterals(skeleton string, literals []string) string {
	return duckPlaceholderRe.ReplaceAllStringFunc(skeleton, func(match string) string {
		if lit, ok := lookupSQLLiteral(match, literals); ok {
			return lit
		}
		return match
	})
}

// rewriteLmnAgg expands lmnagg() calls, which DuckDB has no equivalent for.
// lmnqlbridge's aggregator collapses a group to a single value: numbers are
// summed, anything else survives only if every row in the group agrees, and a
// NULL anywhere in the group poisons the result. The numeric and non-numeric
// forms cannot share one expression - sum() does not bind to VARCHAR - so the
// column's type picks the expansion.
func rewriteLmnAgg(skeleton string, columnTypeMap map[string]types.CellDataType) string {
	return duckLmnAggRe.ReplaceAllStringFunc(skeleton, func(match string) string {
		groups := duckLmnAggRe.FindStringSubmatch(match)
		ref := groups[1]
		colType, known := columnTypeMap[unquoteDuckIdentifier(ref)]
		// A NULL in the group means a NULL result regardless of type.
		nullGuard := fmt.Sprintf(`count(*) <> count(%s)`, ref)
		if known && isNumericCellType(colType) {
			return fmt.Sprintf(`CASE WHEN %s THEN NULL ELSE sum(%s) END`, nullGuard, ref)
		}
		return fmt.Sprintf(
			`CASE WHEN %s THEN NULL WHEN count(DISTINCT %s) = 1 THEN any_value(%s) ELSE NULL END`,
			nullGuard, ref, ref)
	})
}

func unquoteDuckIdentifier(ref string) string {
	if len(ref) >= 2 && strings.HasPrefix(ref, `"`) && strings.HasSuffix(ref, `"`) {
		return strings.ReplaceAll(ref[1:len(ref)-1], `""`, `"`)
	}
	return ref
}

func isNumericCellType(t types.CellDataType) bool {
	switch t {
	case types.IntType, types.LongType, types.DoubleType:
		return true
	}
	return false
}

// --- result conversion -------------------------------------------------------

// duckTypeKind groups the Arrow types DuckDB can return into the families this
// package distinguishes, so a result column can be matched against the type its
// input column had.
type duckTypeKind int

const (
	duckKindUnsupported duckTypeKind = iota
	duckKindInt
	duckKindFloat
	duckKindBool
	duckKindString
	duckKindTimestamp
	duckKindDate
	duckKindTime
	duckKindNull
)

func duckKindOfArrowType(dt arrow.DataType) duckTypeKind {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return duckKindInt
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		return duckKindFloat
	case arrow.BOOL:
		return duckKindBool
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return duckKindString
	case arrow.TIMESTAMP:
		return duckKindTimestamp
	case arrow.DATE32, arrow.DATE64:
		return duckKindDate
	case arrow.TIME32, arrow.TIME64:
		return duckKindTime
	case arrow.NULL:
		return duckKindNull
	}
	return duckKindUnsupported
}

// decimalScale reports the scale of a decimal type, and whether dt was one.
func decimalScale(dt arrow.DataType) (int32, bool) {
	switch d := dt.(type) {
	case *arrow.Decimal32Type:
		return d.Scale, true
	case *arrow.Decimal64Type:
		return d.Scale, true
	case *arrow.Decimal128Type:
		return d.Scale, true
	case *arrow.Decimal256Type:
		return d.Scale, true
	}
	return 0, false
}

func duckKindOfCellType(t types.CellDataType) duckTypeKind {
	switch t {
	case types.IntType, types.LongType:
		return duckKindInt
	case types.DoubleType:
		return duckKindFloat
	case types.BoolType:
		return duckKindBool
	case types.StringType, types.ObjectType:
		return duckKindString
	case types.TimestampType:
		return duckKindTimestamp
	case types.DateType:
		return duckKindDate
	case types.TimeOfDayType:
		return duckKindTime
	case types.NilType:
		return duckKindNull
	}
	return duckKindUnsupported
}

// canonicalDuckResultType picks the Arrow type a result column is materialised
// as. DuckDB computes in types this package does not store - sum() over
// integers comes back DECIMAL(38,0), count() as INT64, a float cast as FLOAT32 -
// so every column is pinned to one of the types in conversion's type map.
//
// When the column kept the name of an input column and the result is in the same
// family, the input's type wins. That keeps a filter or a group-by from quietly
// widening IntType to LongType, or an ObjectType column from decaying to plain
// text, just because of how DuckDB chose to compute it.
func canonicalDuckResultType(dt arrow.DataType, existing types.CellDataType, hasExisting bool) arrow.DataType {
	kind := duckKindOfArrowType(dt)

	// The input type only wins when DuckDB computed in the same family. It is
	// deliberately not stretched across int/float: DuckDB widens a sum over
	// integers to DECIMAL(38,0) and a division to DOUBLE precisely because the
	// result no longer fits the input, so forcing such a column back to a narrow
	// IntType would overflow it. Same family means the value round-trips.
	if hasExisting && duckKindOfCellType(existing) == kind {
		return conversion.ArrowType(existing)
	}

	switch kind {
	case duckKindInt:
		return arrow.PrimitiveTypes.Int64
	case duckKindFloat:
		// A decimal with no fractional digits is how DuckDB returns an exact
		// integer that outgrew its input width - sum() over integers is
		// DECIMAL(38,0). Keep it an integer so the value stays exact instead of
		// going through float64.
		if scale, ok := decimalScale(dt); ok && scale == 0 {
			return arrow.PrimitiveTypes.Int64
		}
		return arrow.PrimitiveTypes.Float64
	case duckKindBool:
		return arrow.FixedWidthTypes.Boolean
	case duckKindTimestamp:
		return conversion.ArrowType(types.TimestampType)
	case duckKindDate:
		return arrow.FixedWidthTypes.Date32
	case duckKindTime:
		return arrow.FixedWidthTypes.Time64us
	case duckKindNull:
		return arrow.Null
	}
	// Anything else - intervals, lists, structs - is kept as its text rendering
	// rather than dropped, which is the closest this package's type set gets.
	return arrow.BinaryTypes.String
}

func duckResultToDataSet(reader array.RecordReader, existingColumnTypeMap map[string]types.CellDataType) (*types.DataSetv2, error) {
	srcFields := reader.Schema().Fields()
	fields := make([]arrow.Field, len(srcFields))
	for i, f := range srcFields {
		existing, ok := existingColumnTypeMap[f.Name]
		fields[i] = arrow.Field{
			Name:     f.Name,
			Type:     canonicalDuckResultType(f.Type, existing, ok),
			Nullable: true,
		}
	}

	dataSchema := arrow.NewSchema(fields, nil)
	rowBuilder := array.NewRecordBuilder(memory.DefaultAllocator, dataSchema)
	defer rowBuilder.Release()

	for reader.Next() {
		rec := reader.RecordBatch()
		for i := range fields {
			arrayBuilder := rowBuilder.Field(i)
			col := rec.Column(i)
			for row := 0; row < col.Len(); row++ {
				if col.IsNull(row) {
					arrayBuilder.AppendNull()
					continue
				}
				if err := appendDuckValue(arrayBuilder, col, row); err != nil {
					return nil, fmt.Errorf("could not read column %q: %w", fields[i].Name, err)
				}
			}
		}
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	rec := rowBuilder.NewRecordBatch()
	defer rec.Release()
	return &types.DataSetv2{
		Table: array.NewTableFromRecords(dataSchema, []arrow.RecordBatch{rec}),
	}, nil
}

// appendDuckValue moves row i of a DuckDB result array into the builder for the
// canonical column type chosen above, converting between representations where
// the two differ.
func appendDuckValue(b array.Builder, arr arrow.Array, i int) error {
	switch bb := b.(type) {
	case *array.Int32Builder:
		v, err := duckValueAsInt64(arr, i)
		if err != nil {
			return err
		}
		bb.Append(int32(v))
	case *array.Int64Builder:
		v, err := duckValueAsInt64(arr, i)
		if err != nil {
			return err
		}
		bb.Append(v)
	case *array.Float64Builder:
		v, err := duckValueAsFloat64(arr, i)
		if err != nil {
			return err
		}
		bb.Append(v)
	case *array.BooleanBuilder:
		v, ok := arr.(*array.Boolean)
		if !ok {
			return fmt.Errorf("expected a boolean array, got %T", arr)
		}
		bb.Append(v.Value(i))
	case *array.StringBuilder:
		bb.Append(arr.ValueStr(i))
	case *array.LargeStringBuilder:
		bb.Append(arr.ValueStr(i))
	case *array.TimestampBuilder:
		v, err := duckValueAsTime(arr, i)
		if err != nil {
			return err
		}
		bb.Append(arrow.Timestamp(v.UnixMicro()))
	case *array.Date32Builder:
		v, err := duckValueAsTime(arr, i)
		if err != nil {
			return err
		}
		bb.Append(conversion.ToDate32(v))
	case *array.Time64Builder:
		v, err := duckValueAsTime(arr, i)
		if err != nil {
			return err
		}
		bb.Append(conversion.ToTime64us(v))
	case *array.NullBuilder:
		bb.AppendNull()
	default:
		return fmt.Errorf("unsupported target builder %T", b)
	}
	return nil
}

func duckValueAsInt64(arr arrow.Array, i int) (int64, error) {
	switch a := arr.(type) {
	case *array.Int8:
		return int64(a.Value(i)), nil
	case *array.Int16:
		return int64(a.Value(i)), nil
	case *array.Int32:
		return int64(a.Value(i)), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return int64(a.Value(i)), nil
	case *array.Uint16:
		return int64(a.Value(i)), nil
	case *array.Uint32:
		return int64(a.Value(i)), nil
	case *array.Uint64:
		return int64(a.Value(i)), nil
	}
	// A DuckDB aggregate over integers yields a decimal, and a division yields a
	// float; both still belong in an integer column when that is what the input
	// column was.
	f, err := duckValueAsFloat64(arr, i)
	if err != nil {
		return 0, err
	}
	return int64(f), nil
}

func duckValueAsFloat64(arr arrow.Array, i int) (float64, error) {
	switch a := arr.(type) {
	case *array.Float16:
		return float64(a.Value(i).Float32()), nil
	case *array.Float32:
		return float64(a.Value(i)), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.Decimal32:
		return a.Value(i).ToFloat64(a.DataType().(*arrow.Decimal32Type).Scale), nil
	case *array.Decimal64:
		return a.Value(i).ToFloat64(a.DataType().(*arrow.Decimal64Type).Scale), nil
	case *array.Decimal128:
		return a.Value(i).ToFloat64(a.DataType().(*arrow.Decimal128Type).Scale), nil
	case *array.Decimal256:
		return a.Value(i).ToFloat64(a.DataType().(*arrow.Decimal256Type).Scale), nil
	case *array.Int8:
		return float64(a.Value(i)), nil
	case *array.Int16:
		return float64(a.Value(i)), nil
	case *array.Int32:
		return float64(a.Value(i)), nil
	case *array.Int64:
		return float64(a.Value(i)), nil
	case *array.Uint8:
		return float64(a.Value(i)), nil
	case *array.Uint16:
		return float64(a.Value(i)), nil
	case *array.Uint32:
		return float64(a.Value(i)), nil
	case *array.Uint64:
		return float64(a.Value(i)), nil
	case *array.Boolean:
		if a.Value(i) {
			return 1, nil
		}
		return 0, nil
	}
	// Text that happens to hold a number - a string column aggregated, say -
	// should still land in a numeric column rather than fail the whole query.
	if f, err := strconv.ParseFloat(strings.TrimSpace(arr.ValueStr(i)), 64); err == nil {
		return f, nil
	}
	return 0, fmt.Errorf("cannot read %T as a number", arr)
}

func duckValueAsTime(arr arrow.Array, i int) (time.Time, error) {
	switch a := arr.(type) {
	case *array.Timestamp:
		return a.Value(i).ToTime(a.DataType().(*arrow.TimestampType).Unit).UTC(), nil
	case *array.Date32:
		return a.Value(i).ToTime().UTC(), nil
	case *array.Date64:
		return a.Value(i).ToTime().UTC(), nil
	case *array.Time32:
		return a.Value(i).ToTime(a.DataType().(*arrow.Time32Type).Unit).UTC(), nil
	case *array.Time64:
		return a.Value(i).ToTime(a.DataType().(*arrow.Time64Type).Unit).UTC(), nil
	}
	// DuckDB renders temporal values as ISO-8601 text, which dateparse - already
	// the repo's date parser elsewhere - understands.
	if t, err := dateparse.ParseIn(arr.ValueStr(i), time.UTC); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot read %T as a time", arr)
}
