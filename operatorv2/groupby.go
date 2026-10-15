//go:build duckdb_arrow

package operatorv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

// The duckdb_arrow tag is on this file because the query below runs through
// executeSQLQueryDuck, which only exists under that tag. The whole package
// requires it anyway (duckdb_disabled.go enforces that); excluding this file
// keeps a tagless build reporting the one explanatory error rather than burying
// it under "undefined: executeSQLQueryDuck".

type GroupByOperator struct {
}

const GroupLevelColName = "Group Level"

type GroupByConfiguration struct {
	Select  []*AggregateSelect `json:"select"`
	GroupBy []string           `json:"groupby"`
}

// The shape this operator produces, which is v1's shape (see operator/groupby.go):
//
// Grouping by [g1, g2] yields one expanding tree of rows rather than a flat
// aggregate. Every row carries a "Group Level" column saying how deep it sits:
//
//	Group Level | g1 | g2 | <aggregates>
//	1           | a  | ∅  | aggregate of every a row
//	2           | a  | x  | aggregate of the (a, x) rows
//	3           | a  | x  | an original (a, x) row
//	3           | a  | x  | an original (a, x) row
//	2           | a  | y  | aggregate of the (a, y) rows
//	3           | a  | y  | an original (a, y) row
//	1           | b  | ∅  | ...
//
// So level L for 1 <= L <= N is the input aggregated by g1..gL, with the
// remaining group-by columns null, and level N+1 is the original rows. v1 builds
// that by running N separate aggregations and merging them in Go, one level at a
// time; this builds a single query - N aggregating branches plus one for the
// original rows, UNION ALLed - and lets DuckDB do all of it.
//
// Where this deliberately does NOT follow v1, in every case because v1 is losing
// data rather than deciding something:
//
//   - A null group key. v1 matches a row to its parent with CellValue.Equals,
//     which answers false for a null on either side, so a null key matches no
//     parent and v1 drops every level below the first for it - grouping by
//     [g1, g2] over a row whose g1 is null keeps that row's level-1 aggregate
//     and silently loses its level-2 aggregate and the row itself. Here a null
//     is a group like any other value, so the tree under it is complete.
//   - A time-of-day group key. v1 keys each level on lmnagg of the group-by
//     column and lmnagg has no case for qlbridge's TimeOnlyValue, so it returns
//     nil whatever the group holds and the key behaves like the null one above -
//     no level past the first survives. Here it is an ordinary key.
//   - mincol / maxcol. Both of v1's aggregators seed the running value at 0
//     instead of at the first value they see, so mincol over all-positive input
//     and maxcol over all-negative input both return 0: min(100, 300) comes back
//     as 0. These stay DuckDB's min()/max(), following what
//     duckdb_test.go's TestDuckMinColIsCorrect already pins for the aggregate
//     path.
//   - An all-null group under a summing aggregate. v1's accumulator starts at
//     zero and reports 0 for a group with nothing to add up; SQL's sum() is NULL,
//     which keeps "nothing to add up" distinct from "adds up to zero". Same
//     choice, and for the same reason, as TestDuckNullHandling.
//   - An empty select list with more than one group-by column. v1 writes the
//     separating comma before each lmnagg only when the select list is non-empty
//     (aggregate.go), so an empty one runs the group-by columns together into
//     malformed SQL and v1 returns level 1 alone. Here the tree is built whether
//     or not anything is being aggregated.
//   - Group and column ordering, which v1 does not fix at all: its group order
//     is whatever qlbridge's hash map iterated in and so differs between runs
//     over the same input, as does the order of the columns after the group-by
//     ones. This orders groups by where they first appear in the input and
//     columns by the input's own column order - both stable, and both what v1
//     produces when its iteration happens to come out in order.
//
// Configurations v1 accepts and then produces something unusable from - a
// repeated group-by column (it panics), an aggregate named after a group-by
// column (two columns of the same name) - are rejected here instead, with an
// error naming the problem. See buildGroupByQuery.
func (t *GroupByOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	if dataset == nil || dataset.Table == nil {
		return nil, errors.New("cannot group by over a nil dataset")
	}
	// Nothing to group, and no rows to read a type map off either. v1 is never
	// called with an empty dataset - the transformer returns it untouched before
	// reaching any operator - so there is no v1 behaviour to match here; handing
	// the input back is what the other v2 operators do (see SortOperator).
	if dataset.NumRows() == 0 {
		return dataset, nil
	}

	headers, columnTypeMap := extractHeadersAndTypeMap(dataset)
	query, err := buildGroupByQuery(typedConfig, headers, columnTypeMap)
	if err != nil {
		return nil, err
	}

	// "Group Level" is not an input column, so without this the result column
	// would be typed from whatever DuckDB computed in - int64 for an INTEGER
	// literal. v1's level is an IntType cell, so declare it one and let
	// canonicalDuckResultType pin it to int32.
	resultTypeMap := make(map[string]types.CellDataType, len(columnTypeMap)+1)
	for k, v := range columnTypeMap {
		resultTypeMap[k] = v
	}
	resultTypeMap[GroupLevelColName] = types.IntType

	return executeSQLQueryDuck(query, dataset, resultTypeMap)
}

// groupByAggregate is one output column that comes from the configuration's
// select list: the aggregate expression the levels compute, and the input column
// the original rows show in its place.
type groupByAggregate struct {
	// alias is the output column name. v1 names an aggregate after the first
	// column it reads, so two selects over the same column are told apart by a
	// suffix - sumx(qty) is "qty" and an average(qty) after it is "qty1".
	alias string
	// expr is the aggregate expression, already aliased, as the AggregateOperator
	// would have written it. Sharing buildLiminaAggSelectStatement is what keeps
	// the two operators agreeing on what a method means.
	expr string
	// source is the input column the level N+1 rows show under alias, or "" when
	// the alias is not an input column - which happens exactly for the suffixed
	// aliases above, since no input column is called "qty1". v1 returns an empty
	// result and a nil error for that configuration (its aggregate selects the
	// same column twice and qlbridge yields no rows), which is the worst of the
	// options: the caller is handed nothing and told nothing. A null is the
	// nearest thing to an answer - the original row has no second value to show
	// under the suffixed name - so the rest of the result survives.
	source string
}

// buildGroupByQuery writes the whole operation as one query, in the lmnqlbridge
// dialect the other builders in this package emit - aggregate names like sumx,
// `x = NULL` for a null test - which executeSQLQueryDuck translates into
// DuckDB's (see translateToDuckSQL). Identifiers are the one exception, quoted
// straight to the target dialect; quoteGroupByIdent says why.
func buildGroupByQuery(cfg *GroupByConfiguration, headers []string, columnTypeMap map[string]types.CellDataType) (string, error) {
	groupBy := cfg.GroupBy

	seenGroupBy := make(map[string]bool, len(groupBy))
	for _, gb := range groupBy {
		if _, exists := columnTypeMap[gb]; !exists {
			return "", buildColumnNotExistsError(gb)
		}
		// v1 indexes its merged rows by position and assumes every group-by
		// column is a distinct one, so a repeat walks off the end of the row and
		// panics. Say what is wrong instead.
		if seenGroupBy[gb] {
			return "", fmt.Errorf("column “%s” is listed more than once in groupby", gb)
		}
		seenGroupBy[gb] = true
		if gb == GroupLevelColName {
			return "", fmt.Errorf("cannot group by column “%s”: the name is reserved for the group level", gb)
		}
	}
	if _, taken := columnTypeMap[GroupLevelColName]; taken {
		return "", fmt.Errorf("the dataset already has a column called “%s”, which the group level needs", GroupLevelColName)
	}

	// Internal columns the ORDER BY needs. They are projected away by the
	// outermost select, so they never reach the caller; the prefix is only
	// widened far enough that it cannot collide with a column of the input.
	prefix := uniqueGroupByPrefix(headers)
	ridCol := prefix + "_rid"
	ordCols := make([]string, len(groupBy))
	for i := range groupBy {
		ordCols[i] = prefix + "_ord" + strconv.Itoa(i+1)
	}
	srcName := prefix + "_src"

	aggregates, err := buildGroupByAggregates(cfg, headers, columnTypeMap, seenGroupBy, ridCol)
	if err != nil {
		return "", err
	}

	var sb strings.Builder

	// The source: every input row, plus its position and, for each group-by
	// prefix, the position of the first row of the group it belongs to. Those
	// first-appearance positions are what orders the groups further down.
	sb.WriteString("WITH ")
	sb.WriteString(quoteGroupByIdent(srcName))
	sb.WriteString(" AS ( SELECT *, rowid AS ")
	sb.WriteString(quoteGroupByIdent(ridCol))
	for i := range groupBy {
		sb.WriteString(", min(rowid) OVER ( PARTITION BY ")
		sb.WriteString(quotedGroupByList(groupBy[:i+1]))
		sb.WriteString(" ) AS ")
		sb.WriteString(quoteGroupByIdent(ordCols[i]))
	}
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	sb.WriteString(" ) SELECT ")

	// The output columns, in v1's order: the level, then the group-by columns,
	// then the aggregates.
	outputCols := make([]string, 0, 1+len(groupBy)+len(aggregates))
	outputCols = append(outputCols, GroupLevelColName)
	outputCols = append(outputCols, groupBy...)
	for _, agg := range aggregates {
		outputCols = append(outputCols, agg.alias)
	}
	sb.WriteString(quotedGroupByList(outputCols))
	sb.WriteString(" FROM ( ")

	// One branch per level. Levels 1..N aggregate by a growing prefix of the
	// group-by columns; the columns past the prefix are null, which is how v1
	// leaves them too.
	for level := 1; level <= len(groupBy); level++ {
		if level > 1 {
			sb.WriteString(" UNION ALL ")
		}
		sb.WriteString(fmt.Sprintf("SELECT CAST(%d AS INTEGER) AS %s", level, quoteGroupByIdent(GroupLevelColName)))
		for i, gb := range groupBy {
			if i < level {
				sb.WriteString(", ")
				sb.WriteString(quoteGroupByIdent(gb))
				continue
			}
			sb.WriteString(fmt.Sprintf(", NULL AS %s", quoteGroupByIdent(gb)))
		}
		for _, agg := range aggregates {
			sb.WriteString(", ")
			sb.WriteString(agg.expr)
		}
		// The group a level row belongs to spans several input groups once the
		// prefix is shorter than the full key, so its position is the earliest of
		// theirs.
		for _, ord := range ordCols {
			sb.WriteString(fmt.Sprintf(", min(%s) AS %s", quoteGroupByIdent(ord), quoteGroupByIdent(ord)))
		}
		sb.WriteString(fmt.Sprintf(", min(%s) AS %s", quoteGroupByIdent(ridCol), quoteGroupByIdent(ridCol)))
		sb.WriteString(" FROM ")
		sb.WriteString(quoteGroupByIdent(srcName))
		sb.WriteString(" GROUP BY ")
		sb.WriteString(quotedGroupByList(groupBy[:level]))
	}

	// The last level: the original rows, trimmed to the columns the levels above
	// produce. v1 trims them the same way, dropping any input column the
	// configuration does not account for.
	sb.WriteString(" UNION ALL ")
	sb.WriteString(fmt.Sprintf("SELECT CAST(%d AS INTEGER) AS %s", len(groupBy)+1, quoteGroupByIdent(GroupLevelColName)))
	for _, gb := range groupBy {
		sb.WriteString(", ")
		sb.WriteString(quoteGroupByIdent(gb))
	}
	for _, agg := range aggregates {
		if agg.source == "" {
			sb.WriteString(fmt.Sprintf(", NULL AS %s", quoteGroupByIdent(agg.alias)))
			continue
		}
		sb.WriteString(fmt.Sprintf(", %s AS %s", quoteGroupByIdent(agg.source), quoteGroupByIdent(agg.alias)))
	}
	for _, ord := range ordCols {
		sb.WriteString(", ")
		sb.WriteString(quoteGroupByIdent(ord))
	}
	sb.WriteString(", ")
	sb.WriteString(quoteGroupByIdent(ridCol))
	sb.WriteString(" FROM ")
	sb.WriteString(quoteGroupByIdent(srcName))

	sb.WriteString(" ) AS ")
	sb.WriteString(quoteGroupByIdent(prefix + "_all"))

	// The ordering is what turns a pile of levels into the tree above: groups in
	// the order they first appear, and inside each group its own row ahead of the
	// rows that expand it. Alternating the group position with "is this row
	// deeper than level j" is what interleaves them - for a given g1 the level-1
	// row sorts first, then each level-2 subgroup followed immediately by its own
	// original rows, rather than all level-2 rows and then all level-3 rows.
	sb.WriteString(" ORDER BY ")
	for i, ord := range ordCols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteGroupByIdent(ord))
		sb.WriteString(fmt.Sprintf(", ( %s > %d )", quoteGroupByIdent(GroupLevelColName), i+1))
	}
	sb.WriteString(", ")
	sb.WriteString(quoteGroupByIdent(ridCol))

	return sb.String(), nil
}

// groupByNumericMethods are the aggregation methods that only mean anything
// over a number. They are rejected against a column of any other type rather
// than let through to DuckDB, whose error names the generated SQL instead of the
// configuration - see buildGroupByAggregates.
//
// lmnagg is not here even though it sums a numeric column: over a non-numeric
// one it collapses an agreeing group instead, which is well defined. Nor are
// first, last and median, which work on any orderable type.
var groupByNumericMethods = map[string]bool{
	"sumx":             true,
	"average":          true,
	"mincol":           true,
	"maxcol":           true,
	"weighted_average": true,
}

// buildGroupByAggregates turns the configuration's select list into one output
// column each, in v1's order: the input's column order, and within a column the
// order the selects were configured in.
func buildGroupByAggregates(cfg *GroupByConfiguration, headers []string, columnTypeMap map[string]types.CellDataType, isGroupBy map[string]bool, ridCol string) ([]*groupByAggregate, error) {
	headerSelectMap := make(map[string][]*AggregateSelect, len(cfg.Select))
	for _, sel := range cfg.Select {
		if sel == nil || len(sel.Columns) < 1 {
			return nil, errors.New("invalid configuration")
		}
		for _, col := range sel.Columns {
			colType, exists := columnTypeMap[col]
			if !exists {
				return nil, buildColumnNotExistsError(col)
			}
			// A numeric aggregate over a text column is a configuration mistake,
			// and DuckDB reports it as a binder error naming the generated SQL
			// ("No function matches the given name and argument types
			// 'sum(VARCHAR)'"), which says nothing to whoever wrote the config.
			// v1 answers 0 for it - every one of its numeric aggregators ignores
			// the values it cannot read as a number and returns its zero seed -
			// which is worse than an error, because 0 is indistinguishable from a
			// column that really does add up to zero.
			if groupByNumericMethods[strings.ToLower(sel.Method)] && !isNumericCellType(colType) {
				return nil, fmt.Errorf("cannot apply “%s” to column “%s”: it is %s, not a number",
					sel.Method, col, colType)
			}
		}
		// v1 names the output after the first column, so that is the one that
		// decides where the aggregate lands in the column order.
		headerSelectMap[sel.Columns[0]] = append(headerSelectMap[sel.Columns[0]], sel)
	}

	aggregates := make([]*groupByAggregate, 0, len(cfg.Select))
	for _, h := range headers {
		for i, sel := range headerSelectMap[h] {
			expr, err := buildLiminaAggSelectStatement(sel, i)
			if err != nil {
				return nil, err
			}
			// buildLiminaAggSelectStatement writes nothing when it does not
			// recognise the method, which leaves v1 with a hole in its select list
			// and a SQL parse error to report. Name the real problem.
			if len(expr) == 0 {
				return nil, fmt.Errorf("unknown aggregation method “%s”", sel.Method)
			}
			alias := h
			if i > 0 {
				alias = h + strconv.Itoa(i)
			}
			expr = rewriteGroupByAgg(expr, sel, alias, ridCol, columnTypeMap)
			// An aggregate named after a group-by column would collide with that
			// column in the output. v1 emits both and ends up with two columns of
			// the same name and nil cells between them.
			if isGroupBy[alias] {
				return nil, fmt.Errorf("cannot aggregate into column “%s”: it is a groupby column", alias)
			}
			if alias == GroupLevelColName {
				return nil, fmt.Errorf("cannot aggregate into column “%s”: the name is reserved for the group level", alias)
			}

			source := ""
			if _, exists := columnTypeMap[alias]; exists {
				source = alias
			}
			aggregates = append(aggregates, &groupByAggregate{alias: alias, expr: expr, source: source})
		}
	}

	return aggregates, nil
}

// rewriteGroupByAgg replaces the aggregate expressions whose DuckDB spelling
// does not compute what v1's aggregator computes. Everything not listed here is
// left exactly as the AggregateOperator built it, and reaches DuckDB through the
// macros in duckCompatMacros - sharing that builder is what keeps the two
// operators agreeing on what a method means.
//
// The four rewritten methods, and why each needs one (all measured against v1's
// aggregators in lmnqlbridge/operator):
//
//	first / last  DuckDB's take whichever row the hash aggregate happened to
//	              reach first, and the plan is free to vary - over one dataset
//	              they returned a group's null on one query and a real value on
//	              another. v1 is fed rows in table order, so first is the first
//	              NON-null value ("if m.val == nil" only assigns once something
//	              non-null arrives) and last is the last value, null included
//	              (it assigns every time). An ORDER BY inside the aggregate pins
//	              both; the (x IS NULL) term is what makes first skip over
//	              leading nulls.
//
//	average       v1 counts every row it is handed and only adds up the ones
//	              that carry a number, so a group of [null, 5, 2] averages to
//	              7/3, not 7/2. SQL's avg() divides by the non-null count, so
//	              the division is written out.
//
//	median        v1 sorts the non-nulls and takes index floor(n/2) - the UPPER
//	              of the two middles on an even count - except for numbers,
//	              where it averages the two middles, and integers, where that
//	              average is a truncating integer division. DuckDB's median()
//	              interpolates and so disagrees on every even-sized group.
//
// mincol and maxcol are deliberately NOT rewritten, which is the one place this
// operator does not reproduce v1. Both of v1's seed their accumulator at 0
// instead of at the first value, so over all-negative input maxcol returns 0 and
// over all-positive input mincol returns 0 - min(100,300) comes back as 0. That
// is a bug, not a semantic, and duckdb_test.go's TestDuckMinColIsCorrect already
// pins the corrected answer for the aggregate path; this follows it.
func rewriteGroupByAgg(expr string, sel *AggregateSelect, alias string, ridCol string, columnTypeMap map[string]types.CellDataType) string {
	arg := quoteGroupByIdent(sel.Columns[0])
	out := quoteGroupByIdent(alias)
	rid := quoteGroupByIdent(ridCol)

	switch strings.ToLower(sel.Method) {
	case "first":
		return fmt.Sprintf("first(%s ORDER BY ( %s IS NULL ), %s) AS %s", arg, arg, rid, out)
	case "last":
		return fmt.Sprintf("last(%s ORDER BY %s) AS %s", arg, rid, out)
	case "average":
		// count(*) rather than count(arg): v1 divides by every row in the group.
		return fmt.Sprintf("( sum(%s) / count(*) ) AS %s", arg, out)
	case "median":
		return fmt.Sprintf("%s AS %s", groupByMedianExpr(arg, columnTypeMap[sel.Columns[0]]), out)
	}
	return expr
}

// groupByMedianExpr writes v1's median for a column of type colType.
//
// The shape is the same in all three cases - sort the non-nulls, index into
// them - and only the even-count branch differs. n/2 and n/2+1 are the lower and
// upper middle of a 1-based list of n elements; v1 takes the upper one, which is
// what data[len(data)/2] is after its 0-based sort.
func groupByMedianExpr(arg string, colType types.CellDataType) string {
	// array_agg keeps nulls, and v1 drops them before sorting (median.Do returns
	// early on a nil), so they are filtered out here rather than sorted to one
	// end. count(arg) then counts exactly the elements of the sorted list.
	//
	// The index arithmetic uses // rather than /: DuckDB's / is float division,
	// so count/2 + 1 over a one-element group is 1.5, which a cast rounds up to
	// 2 - one past the end of the list, yielding NULL for the median of a single
	// value. // is integer division and floors, which is what the 0-based
	// len(data)/2 in v1 does.
	sorted := fmt.Sprintf("list_sort(array_agg(%s) FILTER ( WHERE NOT ( %s = NULL ) ))", arg, arg)
	n := fmt.Sprintf("count(%s)", arg)
	lower := fmt.Sprintf("%s[%s // 2]", sorted, n)
	upper := fmt.Sprintf("%s[%s // 2 + 1]", sorted, n)

	// An empty group has no middle. v1 returns nil for one (median.Result gives
	// up on an empty slice), and indexing past the end of a DuckDB list yields
	// NULL anyway - the guard says so explicitly rather than relying on that.
	even := upper
	switch colType {
	case types.IntType, types.LongType:
		// v1 averages the two middles with Go integer division on int64, which
		// truncates toward zero - (-7 + -4)/2 is -5, not -6.
		even = fmt.Sprintf("CAST(trunc( ( %s + %s ) / 2.0 ) AS BIGINT)", lower, upper)
	case types.DoubleType:
		even = fmt.Sprintf("( ( %s + %s ) / 2.0 )", lower, upper)
	}

	return fmt.Sprintf("CASE WHEN %s = 0 THEN NULL WHEN %s %% 2 = 1 THEN %s ELSE %s END",
		n, n, upper, even)
}

// uniqueGroupByPrefix returns a prefix no input column name starts with, so the
// bookkeeping columns built from it cannot collide with the caller's data.
func uniqueGroupByPrefix(headers []string) string {
	prefix := "__gb"
	for {
		clash := false
		for _, h := range headers {
			if strings.HasPrefix(h, prefix) {
				clash = true
				break
			}
		}
		if !clash {
			return prefix
		}
		prefix += "_"
	}
}

// quoteGroupByIdent quotes an identifier for the query this operator builds.
//
// The other builders in this package write backticks and let extractSQLLiterals
// convert them, but a backtick has no escape in that dialect - it scans to the
// next one - so a column whose own name contains one cannot be written at all.
// DuckDB's double quoting escapes a quote by doubling it, and extractSQLLiterals
// copies an already-double-quoted identifier through verbatim, so the escaped
// form reaches DuckDB intact. Hence quoting straight to the target dialect here.
func quoteGroupByIdent(name string) string {
	return quoteDuckIdentifier(name)
}

func quotedGroupByList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteGroupByIdent(n)
	}
	return strings.Join(quoted, ",")
}

func (t *GroupByOperator) buildConfiguration(config string) (*GroupByConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := GroupByConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.GroupBy) < 1 {
		return nil, errors.New("missing groupby in configuration")
	}

	return &typedConfig, nil
}

func (t *GroupByOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
