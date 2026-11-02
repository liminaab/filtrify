//go:build duckdb_arrow

package operatorv2

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/liminaab/filtrify/types"
)

// This file is the lmnqlbridge-to-DuckDB translation for a NewColumn statement.
//
// It is separate from translateToDuckSQL because the two translate different
// things. That one handles queries this package *builds* - the filter and
// group-by builders emit a small, known set of constructs - so it only needs to
// cover what those builders write. A NewColumn statement is written by whoever
// configured the transformation, so it can name any function in the lmnqlbridge
// vocabulary and combine them freely. What follows therefore covers the whole
// vocabulary (lmnqlbridge/common.go's operators map), and reproduces what each
// of those Go implementations computes rather than what its name suggests.
//
// Two mechanisms, chosen per function:
//
//   - A macro in newColumnv2Macros, when the semantics fit one expression. This
//     is the default; the macros carry a lmnfn_ prefix for the reason given
//     there.
//   - A rewrite below, when they do not - when the argument count varies
//     (concat), the expansion depends on a column's type (lmnagg), or DuckDB's
//     parser will not accept the form at all (the infix operators).

// newColumnFuncPrefix namespaces every compatibility macro.
//
// Prefixing is not cosmetic, it is what makes the macros work at all. A DuckDB
// macro cannot shadow a builtin it calls: `CREATE MACRO round(x, n) AS
// round(CAST(x AS DOUBLE), ...)` binds the inner round to itself and the query
// fails with "Max expression depth limit of 1000 exceeded". Several lmnqlbridge
// names (round, abs, trim, length, median, first, last, concat) are also DuckDB
// builtins whose behaviour differs, and some (left, right, date, time) are
// reserved words a macro cannot be named after at all. Renaming every function
// call to a prefixed macro sidesteps both problems uniformly, so no function
// needs to be special-cased for merely sharing a name with a builtin.
const newColumnFuncPrefix = "lmnfn_"

// duckValueText renders a value as text the way v1's CellValue.ToString does.
// It is the basis of concat and length, both of which are defined in terms of
// a value's text form, so the two disagreements with DuckDB's own CAST have to
// be fixed in one place:
//
//   - Floats. v1 uses strconv.FormatFloat(v, 'f', -1, 64), which writes a whole
//     number as "175000"; DuckDB's CAST writes "175000.0". A float with no
//     fractional part therefore goes through HUGEINT. The 1e15 bound keeps that
//     path to values where every digit is exact; past it strconv switches to
//     scientific notation anyway, so CAST already matches.
//   - Timestamps. v1 formats one with time.RFC3339; DuckDB's CAST writes
//     "2020-01-01 12:00:00+00". Date and time of day already agree
//     ("2006-01-02" and "15:04:05").
//
// Every branch is written to bind for any input type, not only the one the
// typeof() guard selects: DuckDB inlines a macro and binds all of its branches,
// so a strftime(x, ...) that only runs for a timestamp still has to type-check
// when x is a double. Hence the try_cast inside the timestamp branch - a plain
// cast there fails to bind the whole query. Same reason the numeric comparison
// is reached through CAST rather than being applied to x directly.
const duckValueText = `CREATE MACRO ` + newColumnFuncPrefix + `text(x) AS CASE
	WHEN x IS NULL THEN NULL
	WHEN typeof(x) IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
		THEN strftime(try_cast(x AS TIMESTAMP), '%Y-%m-%dT%H:%M:%SZ')
	WHEN typeof(x) IN ('DOUBLE', 'FLOAT')
		AND CAST(x AS DOUBLE) = floor(CAST(x AS DOUBLE))
		AND abs(CAST(x AS DOUBLE)) < 1e15
		THEN CAST(CAST(x AS HUGEINT) AS VARCHAR)
	ELSE CAST(x AS VARCHAR) END`

// duckTruthy reproduces qlbridge's getArgBooleanValue, which is how v1 reads a
// non-boolean as a condition: a null is false, text is true when non-empty, a
// number when non-zero, and anything else - a timestamp, a date - is true. SQL
// has no such coercion (a null operand makes AND yield null rather than false),
// so the boolean operators are routed through this.
const duckTruthy = `CREATE MACRO ` + newColumnFuncPrefix + `truthy(x) AS CASE
	WHEN x IS NULL THEN false
	WHEN typeof(x) = 'BOOLEAN' THEN try_cast(x AS BOOLEAN)
	WHEN typeof(x) = 'VARCHAR' THEN CAST(x AS VARCHAR) <> ''
	WHEN try_cast(x AS DOUBLE) IS NOT NULL THEN try_cast(x AS DOUBLE) <> 0
	ELSE true END`

// duckEmptyToNull is the last thing applied to a statement's result.
//
// v1 stores a cell by scanning it into a qlbridge value and converting that, and
// an empty string arrives as a NilType cell rather than a StringType one holding
// it - so SPLIT past the end of its input, or a bare empty-string literal, is a
// null in v1's output. It is also how the conversion package reads an empty cell
// (appendData appends a null for a zero-length string), so a null is what the
// rest of the library means by an empty text value. DuckDB keeps the empty
// string, so it is mapped here.
//
// The typeof() guard is what keeps this from touching a non-text result: only a
// VARCHAR can be the empty string, and a numeric 0 must stay 0.
const duckEmptyToNull = `CREATE MACRO ` + newColumnFuncPrefix + `result(x) AS CASE
	WHEN typeof(x) = 'VARCHAR' AND CAST(x AS VARCHAR) = '' THEN NULL
	ELSE x END`

// newColumnv2Macros teaches DuckDB the lmnqlbridge function vocabulary, one
// macro per function, measured against the Go implementation in
// lmnqlbridge/operator rather than against the name.
//
// The SET comes first and is not a macro: DuckDB's session timezone defaults to
// the machine's, and a TIMESTAMP WITH TIME ZONE cast through TIMESTAMP (which
// plusdays does) would then shift the clock - a 12:00:00 value came back
// 13:00:00 on a UTC+1 machine. Every timestamp in this package is UTC, so the
// session is pinned to UTC and the cast becomes the identity it is meant to be.
var newColumnv2Macros = append([]string{
	`SET TimeZone='UTC'`,
	duckValueText,
	duckTruthy,
	duckEmptyToNull,
}, buildNewColumnMacros()...)

// newColumnMacroBodies is each lmnqlbridge function's DuckDB expansion, keyed by
// the name it is called by. The comment on a line says what v1 does that the
// obvious spelling would not.
var newColumnMacroBodies = map[string]string{
	// --- arithmetic ------------------------------------------------------
	// v1's PLUS/MINUS/MULTIPLY/DIVIDE all compute in float64 and return a
	// NumberValue whatever they are given, so PLUS(3, 5) is 8.0 rather than the
	// integer 8 DuckDB's + would produce. Hence the casts.
	"plus(a, b)":     `CAST(a AS DOUBLE) + CAST(b AS DOUBLE)`,
	"minus(a, b)":    `CAST(a AS DOUBLE) - CAST(b AS DOUBLE)`,
	"multiply(a, b)": `CAST(a AS DOUBLE) * CAST(b AS DOUBLE)`,
	// v1 returns nil for a zero divisor rather than an error or an infinity,
	// which DuckDB's / would give (1/0 is +Inf there).
	"divide(a, b)": `CASE WHEN CAST(b AS DOUBLE) = 0 THEN NULL
		ELSE CAST(a AS DOUBLE) / CAST(b AS DOUBLE) END`,
	// v1's ABS returns an int for an int and a float for a float; both are
	// stored in the same arrow column, so the float form covers it.
	"abs(x)": `abs(CAST(x AS DOUBLE))`,
	// round and floorx take a digit count, which v1 applies as
	// math.Round(x*10^n)/10^n. DuckDB's round(x, n) is the same thing; floor has
	// no two-argument form, so it is written out.
	"round(x, n)":  `round(CAST(x AS DOUBLE), CAST(n AS INTEGER))`,
	"floorx(x, u)": `floor(CAST(x AS DOUBLE) * pow(10, CAST(u AS DOUBLE))) / pow(10, CAST(u AS DOUBLE))`,
	// minx/maxx are the two-argument forms. least/greatest return NULL if either
	// argument is NULL, which is also what v1 does (a non-numeric argument makes
	// it return NumberNil).
	"minx(a, b)": `least(CAST(a AS DOUBLE), CAST(b AS DOUBLE))`,
	"maxx(a, b)": `greatest(CAST(a AS DOUBLE), CAST(b AS DOUBLE))`,

	// --- text ------------------------------------------------------------
	// v1's length is len(string) in Go - a byte count, not a character count -
	// and returns 0 rather than nil for a null input. encode() gives the byte
	// count; strlen() would count characters and disagree on any non-ASCII text.
	// It measures the value's v1 text form, not DuckDB's, so LENGTH over a float
	// column counts the digits v1 would have rendered.
	"length(x)": `COALESCE(octet_length(encode(` + newColumnFuncPrefix + `text(x))), 0)`,
	// v1 trims with strings.TrimSpace, whose set is wider than the single space
	// DuckDB's trim() defaults to.
	"trim(x)": `trim(CAST(x AS VARCHAR), chr(32) || chr(9) || chr(10) || chr(13) || chr(11) || chr(12))`,
	// left/right are byte-exact in DuckDB for ASCII and character-based beyond
	// it, same as v1's s[:n] on a byte slice for ASCII. The cast is what lets
	// them apply to a non-text column, which v1 allows via ValueToString.
	"left(s, n)":  `left(CAST(s AS VARCHAR), CAST(n AS BIGINT))`,
	"right(s, n)": `right(CAST(s AS VARCHAR), CAST(n AS BIGINT))`,
	// v1's split is 1-indexed and returns "" when the index is past the end.
	// DuckDB's list indexing is also 1-based but yields NULL out of range, and a
	// 0 or negative index means something else entirely there, so both are
	// folded to the empty string.
	"split(s, sep, n)": `CASE WHEN CAST(n AS BIGINT) < 1 THEN ''
		ELSE COALESCE(str_split(CAST(s AS VARCHAR), CAST(sep AS VARCHAR))[CAST(n AS BIGINT)], '') END`,
	"replace(s, a, b)": `replace(CAST(s AS VARCHAR), CAST(a AS VARCHAR), CAST(b AS VARCHAR))`,
	// containsx/notcontainsx answer false/true for an empty needle or haystack
	// rather than letting contains() decide (it calls every string a substring
	// of itself, so '' would match everything).
	"containsx(a, b)": `CASE WHEN a IS NULL OR b IS NULL
		OR CAST(a AS VARCHAR) = '' OR CAST(b AS VARCHAR) = '' THEN false
		ELSE contains(CAST(a AS VARCHAR), CAST(b AS VARCHAR)) END`,
	"notcontainsx(a, b)": `CASE WHEN a IS NULL OR b IS NULL
		OR CAST(a AS VARCHAR) = '' OR CAST(b AS VARCHAR) = '' THEN true
		ELSE NOT contains(CAST(a AS VARCHAR), CAST(b AS VARCHAR)) END`,
	// v1's isblank is true for a null, and for text that is empty after
	// TrimSpace; anything non-text and non-null is false. The typeof guard is
	// what keeps a numeric 0 from being called blank.
	"isblank(x)": `CASE WHEN x IS NULL THEN true
		WHEN typeof(x) IN ('VARCHAR', 'BLOB') THEN trim(CAST(x AS VARCHAR)) = ''
		ELSE false END`,

	// --- conditional -----------------------------------------------------
	// v1's IFEL takes the else branch for a nil condition as well as a false
	// one, which COALESCE is what expresses - a bare CASE WHEN c would also
	// treat NULL as not-true, but only because SQL happens to agree here, and
	// saying it explicitly keeps the two in step.
	"ifel(c, a, b)": `CASE WHEN COALESCE(CAST(c AS BOOLEAN), false) THEN a ELSE b END`,
	// v1's EVAL returns its argument untouched; it exists only to group a
	// condition in the qlbridge parser.
	"eval(x)": `x`,

	// --- temporal --------------------------------------------------------
	// todate/totime parse text into a date / time of day, matching v1's
	// civil.ParseDate and its ParseTime (which accepts 15:04:05.999999999,
	// 15:04:05 and 15:04 - all three of which DuckDB's TIME cast also takes).
	"todate(x)": `CAST(CAST(x AS VARCHAR) AS DATE)`,
	"totime(x)": `CAST(CAST(x AS VARCHAR) AS TIME)`,
	// date/time/datetimeutc build a value from numeric parts.
	"date(y, m, d)":                    `make_date(CAST(y AS BIGINT), CAST(m AS BIGINT), CAST(d AS BIGINT))`,
	"time(h, m, s)":                    `make_time(CAST(h AS BIGINT), CAST(m AS BIGINT), CAST(s AS DOUBLE))`,
	"datetimeutc(y, mo, d, h, mi, se)": `make_timestamp(CAST(y AS BIGINT), CAST(mo AS BIGINT), CAST(d AS BIGINT), CAST(h AS BIGINT), CAST(mi AS BIGINT), CAST(se AS DOUBLE))`,
	"today()":                          `current_date`,
	// v1's plusdays returns a TimeValue - a timestamp - whatever it was handed,
	// including for a date column, so the cast to TIMESTAMP is deliberate and
	// not a widening of what DuckDB would do with date + integer. It is a
	// try_cast because v1 rejects a time of day here (plusDaysEval only accepts
	// TimeType, StringType and DateType) and returns nil for it; a plain cast
	// would instead fail to bind, since DuckDB has no TIME -> TIMESTAMP cast.
	"plusdays(d, n)": `try_cast(d AS TIMESTAMP) + to_days(CAST(n AS INTEGER))`,

	// --- aggregates ------------------------------------------------------
	// Every numeric aggregate reads its argument through try_cast rather than
	// relying on DuckDB's implicit conversion. v1's aggregators take any column
	// and ignore the values they cannot read as a number - sumx over a text
	// column is 0 there, not an error - whereas sum(VARCHAR) does not bind in
	// DuckDB and would fail the whole query. try_cast keeps the statement valid
	// and yields NULL for the values that are not numbers, which is this
	// package's reading of "nothing to add up" (see the note on
	// NewColumnOperatorv2).
	"sumx(x)": `sum(try_cast(x AS DOUBLE))`,
	// v1's average counts every row it is handed and only adds the ones that
	// carry a number, so [null, 5, 2] averages to 7/3. SQL's avg() divides by
	// the non-null count instead.
	"average(x)": `( sum(try_cast(x AS DOUBLE)) / count(*) )`,
	// lmnqlbridge rounds the weighted average to two decimals.
	"weighted_average(v, w)": `round(sum(try_cast(v AS DOUBLE) * try_cast(w AS DOUBLE)) / sum(try_cast(w AS DOUBLE)), 2)`,
	// mincol/maxcol are DuckDB's min/max, NOT v1's - see the note on
	// NewColumnOperatorv2 for why this one difference is deliberate. They are
	// still restricted to numbers, which is what v1's accumulators read.
	"mincol(x)": `min(try_cast(x AS DOUBLE))`,
	"maxcol(x)": `max(try_cast(x AS DOUBLE))`,
}

// buildNewColumnMacros renders newColumnMacroBodies as CREATE MACRO statements.
func buildNewColumnMacros() []string {
	out := make([]string, 0, len(newColumnMacroBodies)+len(newColumnOrderedAggs))
	for signature, body := range newColumnMacroBodies {
		out = append(out, fmt.Sprintf("CREATE MACRO %s%s AS %s", newColumnFuncPrefix, signature, body))
	}
	return append(out, newColumnOrderedAggs...)
}

// newColumnOrderedAggs are the aggregates whose result depends on the order the
// rows arrive in, which a macro body cannot express - an ORDER BY inside an
// aggregate call is syntax, not an argument, so these are written as macros over
// an explicit rowid rather than built by the loop above. rewriteOrderedAggs
// supplies that argument at the call site.
//
// Why each needs pinning (all measured against v1's aggregators):
//
//	first / last  DuckDB's take whichever row the hash aggregate reached first
//	              and the plan is free to vary between queries. v1 is fed rows
//	              in table order, so first is the first NON-null value ("if
//	              m.val == nil" only assigns once something non-null arrives)
//	              and last is the last value, null included.
//
//	median        v1 sorts the non-nulls and takes index floor(n/2) - the UPPER
//	              of the two middles on an even count - except for numbers,
//	              where it averages the two middles, and integers, where that
//	              average is a truncating integer division. DuckDB's median()
//	              interpolates and so disagrees on every even-sized group. The
//	              three forms are separate macros because the expansion depends
//	              on the column's type; rewriteOrderedAggs picks one.
var newColumnOrderedAggs = []string{
	`CREATE MACRO ` + newColumnFuncPrefix + `first_ord(x, rid) AS first(x ORDER BY ( x IS NULL ), rid)`,
	`CREATE MACRO ` + newColumnFuncPrefix + `last_ord(x, rid) AS last(x ORDER BY rid)`,
	// The index arithmetic uses // rather than /: DuckDB's / is float division,
	// so count/2 + 1 over a one-element group is 1.5, which a cast rounds up to
	// 2 - one past the end of the list, yielding NULL for the median of a single
	// value. // is integer division and floors, which is what the 0-based
	// len(data)/2 in v1 does.
	//
	// array_agg keeps nulls and v1 drops them before sorting (median.Do returns
	// early on a nil), so they are filtered out rather than sorted to one end.
	// count(x) then counts exactly the elements of the sorted list.
	`CREATE MACRO ` + newColumnFuncPrefix + `median_other(x) AS
		CASE WHEN count(x) = 0 THEN NULL
		ELSE list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2 + 1] END`,
	`CREATE MACRO ` + newColumnFuncPrefix + `median_double(x) AS
		CASE WHEN count(x) = 0 THEN NULL
		WHEN count(x) % 2 = 1
			THEN list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2 + 1]
		ELSE ( ( list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2]
			+ list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2 + 1] ) / 2.0 ) END`,
	// v1 averages the two middles with Go integer division on int64, which
	// truncates toward zero - (-7 + -4)/2 is -5, not -6.
	`CREATE MACRO ` + newColumnFuncPrefix + `median_int(x) AS
		CASE WHEN count(x) = 0 THEN NULL
		WHEN count(x) % 2 = 1
			THEN list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2 + 1]
		ELSE CAST(trunc( ( list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2]
			+ list_sort(array_agg(x) FILTER ( WHERE x IS NOT NULL ))[count(x) // 2 + 1] ) / 2.0 ) AS BIGINT) END`,
}

// newColumnFuncs is every function name the macros above define, so the rewrite
// can tell a call it must rename from one it must leave alone. Built from the
// same table that builds the macros, so the two cannot drift apart.
var newColumnFuncs = buildNewColumnFuncSet()

func buildNewColumnFuncSet() map[string]bool {
	out := make(map[string]bool, len(newColumnMacroBodies)+8)
	for signature := range newColumnMacroBodies {
		name := signature
		if i := strings.IndexByte(signature, '('); i >= 0 {
			name = signature[:i]
		}
		out[name] = true
	}
	// The functions handled by a rewrite rather than a macro body still have to
	// be recognised as lmnqlbridge calls.
	for _, name := range []string{"concat", "lmnagg", "first", "last", "median"} {
		out[name] = true
	}
	return out
}

var (
	// duckFuncCallRe matches a function call's name and opening parenthesis.
	// Anchoring on the parenthesis is what distinguishes a call from a bare
	// identifier, and the leading guard keeps it from matching the tail of a
	// qualified name or a longer identifier.
	duckFuncCallRe = regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

	// duckInfixAndRe / duckInfixOrRe rewrite qlbridge's C-style boolean
	// operators, which DuckDB's parser rejects outright.
	duckInfixAndRe = regexp.MustCompile(`&&`)
	duckInfixOrRe  = regexp.MustCompile(`\|\|`)

	// duckDoubleEqRe rewrites `==`, which qlbridge accepts as equality. DuckDB
	// happens to accept it too, but normalising it keeps the emitted SQL in one
	// dialect.
	duckDoubleEqRe = regexp.MustCompile(`==`)

	// duckQuotedIdentRe matches a double-quoted identifier, which after
	// extractSQLLiterals is what every column reference in the statement looks
	// like.
	duckQuotedIdentRe = regexp.MustCompile(`"(?:[^"]|"")*"`)
)

// translateNewColumnStatement rewrites one NewColumn statement from the
// lmnqlbridge dialect into DuckDB's.
//
// The order of the steps matters: literals are pulled out first so that none of
// the rewrites can mistake the contents of a string for syntax - a statement
// comparing a column against the text '&&' or 'contains' is data, not operators -
// and are put back last.
func translateNewColumnStatement(statement string, columnTypeMap map[string]types.CellDataType) (string, error) {
	skeleton, literals, err := extractSQLLiterals(statement)
	if err != nil {
		return "", err
	}

	// `||` is string concatenation in DuckDB and logical OR in qlbridge, so this
	// has to happen before anything else looks at the operators.
	skeleton = duckInfixOrRe.ReplaceAllString(skeleton, " OR ")
	skeleton = duckInfixAndRe.ReplaceAllString(skeleton, " AND ")
	skeleton = duckDoubleEqRe.ReplaceAllString(skeleton, "=")

	// CONTAINS / NOT CONTAINS as infix operators, and `x = NULL` as a null test,
	// are the same in a NewColumn statement as in a filter, so the shared rules
	// apply. NOT CONTAINS goes first or the CONTAINS rule claims the tail of it
	// and leaves a dangling NOT.
	skeleton = duckNotContainsRe.ReplaceAllString(skeleton, `NOT contains(CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`)
	skeleton = duckContainsRe.ReplaceAllString(skeleton, `contains(CAST($1 AS VARCHAR), CAST($2 AS VARCHAR))`)
	skeleton = duckEqNullRe.ReplaceAllString(skeleton, `$1 IS NULL`)
	skeleton = duckNotEqNullRe.ReplaceAllString(skeleton, `$1 IS NOT NULL`)

	skeleton = rewriteLmnAgg(skeleton, columnTypeMap)
	skeleton = rewriteNewColumnConcat(skeleton)
	skeleton = rewriteOrderedAggs(skeleton, columnTypeMap)
	skeleton = rewriteBoolOperands(skeleton)
	skeleton = rewriteNewColumnFuncNames(skeleton)

	return restoreSQLLiterals(skeleton, literals), nil
}

// wrapNewColumnResult puts a translated statement's value through the result
// macro, which maps an empty text result to a null (see duckEmptyToNull).
//
// The wrapper has to go inside the alias, so the alias is split off first: a
// translated statement is "<expr> AS <name>" whenever the configuration supplied
// a name, and the column has to keep that name rather than be called after the
// macro. aliasSplitRe finds the trailing AS, which is unambiguous because the
// alias is always a quoted identifier at the very end.
func wrapNewColumnResult(translated string) string {
	expr, alias := splitNewColumnAlias(translated)
	wrapped := fmt.Sprintf("%sresult(%s)", newColumnFuncPrefix, expr)
	if alias == "" {
		return wrapped
	}
	return wrapped + " AS " + alias
}

// aliasSplitRe matches the " AS <quoted identifier>" a translated statement ends
// with. Anchored at the end so a nested alias - inside a subquery, say - is not
// mistaken for the statement's own.
var aliasSplitRe = regexp.MustCompile(`(?i)\s+AS\s+("(?:[^"]|"")*")\s*$`)

func splitNewColumnAlias(translated string) (expr string, alias string) {
	m := aliasSplitRe.FindStringSubmatchIndex(translated)
	if m == nil {
		return translated, ""
	}
	return translated[:m[0]], translated[m[2]:m[3]]
}

// rewriteBoolOperands routes a column used directly as an AND/OR operand through
// the truthy macro, so a null behaves as false the way qlbridge's
// getArgBooleanValue makes it - `col AND true` over a null col is false in v1
// and would be null in plain SQL. See duckTruthy.
// A column is an operand only when it is a *whole* side of the operator, which
// means looking at what sits on the other side of it as well: in
// `"a" OR "b" > 3` the "b" is the left side of a comparison, not an operand of
// the OR, and wrapping it would turn the comparison into truthy("b") > 3. So
// each identifier is examined with its immediate neighbours, and wrapped only
// when an AND/OR is adjacent on one side and nothing that binds tighter is
// adjacent on the other.
func rewriteBoolOperands(skeleton string) string {
	var out strings.Builder
	last := 0
	for _, m := range duckQuotedIdentRe.FindAllStringIndex(skeleton, -1) {
		before := strings.TrimRight(skeleton[last:m[0]], " ")
		after := strings.TrimLeft(skeleton[m[1]:], " ")

		// An operand has a boolean operator on at least one side...
		touchesBool := hasBoolKeywordSuffix(before) || hasBoolKeywordPrefix(after)
		// ...and an operator that binds tighter on neither, since that one
		// consumes the column and yields the boolean itself.
		isOperand := touchesBool &&
			!endsWithTighterOperator(before) && !startsWithTighterOperator(after)

		out.WriteString(skeleton[last:m[0]])
		if isOperand {
			out.WriteString(fmt.Sprintf("%struthy(%s)", newColumnFuncPrefix, skeleton[m[0]:m[1]]))
		} else {
			out.WriteString(skeleton[m[0]:m[1]])
		}
		last = m[1]
	}
	out.WriteString(skeleton[last:])
	return out.String()
}

// boolKeywords are the operators whose operands need the truthy coercion. NOT is
// included for the same reason: qlbridge reads `NOT col` through the same
// getArgBooleanValue.
var boolKeywords = []string{"AND", "OR", "NOT"}

func hasBoolKeywordSuffix(before string) bool {
	upper := strings.ToUpper(before)
	for _, kw := range boolKeywords {
		if strings.HasSuffix(upper, " "+kw) || upper == kw {
			return true
		}
	}
	return false
}

func hasBoolKeywordPrefix(after string) bool {
	upper := strings.ToUpper(after)
	for _, kw := range boolKeywords {
		if strings.HasPrefix(upper, kw+" ") || upper == kw {
			return true
		}
	}
	return false
}

// tighterOperators bind more tightly than AND/OR, so a column next to one of
// them belongs to that operator rather than being a boolean operand itself.
// "IS" covers `col IS NULL`, which the null-test rewrite produces.
var tighterOperators = []string{"=", "!=", "<>", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "||"}

func endsWithTighterOperator(before string) bool {
	for _, op := range tighterOperators {
		if strings.HasSuffix(before, op) {
			return true
		}
	}
	// A column directly inside a call - concat("a", "b") - is an argument, not
	// an operand; the comma and the open parenthesis are what say so.
	return strings.HasSuffix(before, "(") || strings.HasSuffix(before, ",")
}

func startsWithTighterOperator(after string) bool {
	for _, op := range tighterOperators {
		if strings.HasPrefix(after, op) {
			return true
		}
	}
	upper := strings.ToUpper(after)
	if strings.HasPrefix(upper, "IS ") {
		return true
	}
	return strings.HasPrefix(after, ")") || strings.HasPrefix(after, ",")
}

// rewriteNewColumnFuncNames points every lmnqlbridge function call at its
// prefixed macro, leaving calls to anything else - DuckDB's own builtins, and
// the expansions the rewrites above have already produced - alone.
func rewriteNewColumnFuncNames(skeleton string) string {
	return duckFuncCallRe.ReplaceAllStringFunc(skeleton, func(match string) string {
		groups := duckFuncCallRe.FindStringSubmatch(match)
		name := strings.ToLower(groups[1])
		if !newColumnFuncs[name] {
			return match
		}
		return newColumnFuncPrefix + name + "("
	})
}

// rewriteOrderedAggs expands first, last and median, whose macros need either a
// row ordering or the argument's type - see newColumnOrderedAggs.
func rewriteOrderedAggs(skeleton string, columnTypeMap map[string]types.CellDataType) string {
	for _, name := range []string{"first", "last", "median"} {
		skeleton = replaceFuncCall(skeleton, name, func(arg string) string {
			switch name {
			case "first", "last":
				// rowid is the table's physical row number, which is the order
				// v1's aggregator is fed rows in.
				return fmt.Sprintf("%s%s_ord(%s, rowid)", newColumnFuncPrefix, name, arg)
			default:
				suffix := "other"
				switch columnTypeMap[unquoteDuckIdentifier(strings.TrimSpace(arg))] {
				case types.IntType, types.LongType:
					suffix = "int"
				case types.DoubleType:
					suffix = "double"
				}
				return fmt.Sprintf("%smedian_%s(%s)", newColumnFuncPrefix, suffix, arg)
			}
		})
	}
	return skeleton
}

// rewriteNewColumnConcat expands concat, whose argument count varies and so
// cannot be a macro - DuckDB has no variadic macros.
//
// DuckDB's own concat would be close but not equal: v1's drops an argument that
// renders as the empty string, which is why callers write a trailing ” as a
// no-op, and renders a float the way strconv does rather than with a trailing
// ".0". list_filter removes the empty arguments and lmnfn_text fixes the
// rendering; a null is dropped by both implementations.
func rewriteNewColumnConcat(skeleton string) string {
	return replaceFuncCall(skeleton, "concat", func(args string) string {
		parts := splitStatements(args)
		rendered := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			rendered = append(rendered, fmt.Sprintf("%stext(%s)", newColumnFuncPrefix, p))
		}
		if len(rendered) == 0 {
			return "''"
		}
		return fmt.Sprintf(
			`list_aggregate(list_filter([%s], y -> y IS NOT NULL AND y <> ''), 'string_agg', '')`,
			strings.Join(rendered, ", "))
	})
}

// replaceFuncCall rewrites every call to name through expand, which is handed
// the call's argument text. It is a scan rather than a regex because a call's
// arguments can contain nested calls and parenthesised expressions, which a
// regex cannot match to the right closing parenthesis.
//
// A match is only a call when the name is a whole word followed by '(' - so a
// column called `median value` is not one - and a name inside a string literal
// never reaches here, those having been replaced by placeholders upstream.
func replaceFuncCall(skeleton string, name string, expand func(args string) string) string {
	var out strings.Builder
	lowered := strings.ToLower(skeleton)
	i := 0
	for i < len(skeleton) {
		idx := strings.Index(lowered[i:], name)
		if idx < 0 {
			out.WriteString(skeleton[i:])
			break
		}
		start := i + idx
		end := start + len(name)

		// The name has to stand alone: neither preceded nor followed by a
		// character that would make it part of a longer identifier. An
		// underscore counts, so lmnfn_median is not a median call - which is
		// what keeps this from rewriting its own output.
		prevOk := start == 0 || !isIdentByte(skeleton[start-1])
		argStart := end
		for argStart < len(skeleton) && skeleton[argStart] == ' ' {
			argStart++
		}
		if !prevOk || argStart >= len(skeleton) || skeleton[argStart] != '(' {
			out.WriteString(skeleton[i:end])
			i = end
			continue
		}

		closing, ok := matchParen(skeleton, argStart)
		if !ok {
			out.WriteString(skeleton[i:])
			break
		}
		out.WriteString(skeleton[i:start])
		out.WriteString(expand(skeleton[argStart+1 : closing]))
		i = closing + 1
	}
	return out.String()
}

// matchParen returns the index of the parenthesis closing the one at open,
// skipping over nested pairs, and whether there was one.
func matchParen(s string, open int) (int, bool) {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

func isIdentByte(b byte) bool {
	return b == '_' ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}
