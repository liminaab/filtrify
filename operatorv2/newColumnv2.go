//go:build duckdb_arrow

package operatorv2

import (
	"errors"
	"fmt"
	"strings"

	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

// The duckdb_arrow tag is on this file because the queries below run through
// executeSQLQueryDuckWith, which only exists under that tag. The whole package
// requires it anyway (duckdb_disabled.go enforces that); excluding this file
// keeps a tagless build reporting the one explanatory error rather than burying
// it under "undefined: executeSQLQueryDuckWith".

// NewColumnOperatorv2 adds a derived column to a dataset, computing it in DuckDB
// rather than in qlbridge. It is the DuckDB counterpart of
// operator.NewColumnOperator in v1: same configuration,
// same statement dialect, same output.
//
// The statement is the one part of this package's SQL that this package does not
// write - it comes from whoever configured the transformation - so unlike the
// filter and group-by builders it can name any lmnqlbridge function. Reproducing
// what each of those functions computes is most of the work here, and
// translateNewColumnStatement plus newColumnv2Macros is where it happens.
//
// Where this deliberately does NOT follow v1, in every case because v1 is losing
// data or answering nonsense rather than deciding something:
//
//   - mincol / maxcol. Both of v1's aggregators seed their accumulator at 0
//     instead of at the first value they see, so mincol over all-positive input
//     and maxcol over all-negative input both return 0 - min(100, 300) comes
//     back as 0. These stay DuckDB's min()/max(), which is what
//     duckdb_test.go's TestDuckMinColIsCorrect and the v2 group-by operator
//     already settled for the aggregate path.
//   - concat over a temporal value. v1's concat has a case for strings, numbers
//     and bools and silently drops everything else, so CONCAT(`name`, `Active
//     From`) returns the name alone and the timestamp vanishes. Here every
//     argument is rendered as text, so the value appears.
//   - sumx over a group with nothing to add up. v1's accumulator starts at zero
//     and reports 0; SQL's sum() is NULL, which keeps "nothing to add up"
//     distinct from "adds up to zero". Same choice, and for the same reason, as
//     the group-by operator and TestDuckNullHandling.
//
// Two further differences are not choices but consequences of the engine:
//
//   - v1 wraps its query in a workaround for a qlbridge bug: when the query
//     fails and the statement is a bare float literal, it adds that float to
//     every row by hand, because qlbridge's driver cannot select a float64.
//     DuckDB selects one fine, so `SELECT 3.5 AS c` just works and the
//     workaround has nothing to do. It is not reproduced.
//   - A statement may also name a function neither lmnqlbridge nor this file
//     knows, which in v1 resolved against qlbridge's own builtins and here
//     resolves against DuckDB's. The two sets overlap but are not the same, so a
//     statement built on a qlbridge builtin outside the lmnqlbridge vocabulary
//     is the one shape of statement that is not carried over.
type NewColumnOperatorv2 struct {
}

// newColumnAggAlias is the alias the whole-table aggregate subquery is given
// when the statement carries no "AS" of its own. It is only ever an internal
// name - with an alias present the caller's one is used instead.
const newColumnAggAlias = "__nc_agg"

func (t *NewColumnOperatorv2) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	if dataset == nil || dataset.Table == nil {
		return nil, errors.New("cannot add a column to a nil dataset")
	}

	headers, columnTypeMap := newColumnHeadersAndTypeMap(dataset)

	// let's check what is the name of the final result column
	// we need to make sure it doesn't exist in our dataset
	// if it does we need to return error
	selectedColName := findSelectedColumnName(typedConfig)
	if selectedColName != nil {
		for _, h := range headers {
			if strings.EqualFold(h, *selectedColName) {
				return nil, errors.New("column already exists")
			}
		}
	}

	query, err := t.buildQuery(typedConfig, headers, columnTypeMap)
	if err != nil {
		return nil, err
	}

	// The statement has already been translated by buildQuery, so the shared
	// rewrites must not run over it a second time - see
	// translateNewColumnStatement for what it does differently from
	// translateToDuckSQL.
	return executeSQLQueryDuckWith(query, dataset, columnTypeMap, duckQueryOptions{
		extraMacros:     newColumnv2Macros,
		skipTranslation: true,
	})
}

// buildQuery writes the whole operation as one DuckDB query. The three shapes it
// produces are v1's three (see operator/newColumn.go), differing only in how the
// aggregating one is expressed - v1 runs the aggregate as a second query and
// copies the single value onto every row in Go, where this leaves it to a scalar
// subquery.
func (t *NewColumnOperatorv2) buildQuery(typedConfig *NewColumnConfiguration, headers []string, columnTypeMap map[string]types.CellDataType) (string, error) {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	// we can't select original columns if there is a group by statement
	if typedConfig.GroupBy == "" {
		// we need to execute multiple queries here
		// first do we have any aggregations in statement?
		plainStatement, aggs, err := splitAggs(typedConfig.Statement)
		if err != nil {
			return "", err
		}

		sb.WriteString(quotedNewColumnList(headers))

		// splitAggs allows exactly one statement and routes that one whole
		// statement into either bucket, so these two branches are exclusive -
		// a statement either aggregates or it does not.
		if len(plainStatement) > 0 {
			translated, err := translateNewColumnStatement(plainStatement, columnTypeMap)
			if err != nil {
				return "", err
			}
			sb.WriteString(", ")
			sb.WriteString(wrapNewColumnResult(translated))
		}
		for _, agg := range aggs {
			// An aggregate with no GROUP BY is one value for the whole table,
			// shown against every row. v1 runs it as a separate query and
			// appends the result column to each row; a scalar subquery says the
			// same thing in one query and, unlike a window function, does not
			// reorder the rows it is selected alongside.
			//
			// The alias has to come out of the subquery for the output column to
			// carry the caller's name, so it is split off and reattached outside.
			cfg := &NewColumnConfiguration{Statement: agg}
			expr := agg
			alias := newColumnAggAlias
			if inner := getSelectedStatement(cfg); inner != nil {
				expr = *inner
				alias = *findSelectedColumnName(cfg)
			}
			translated, err := translateNewColumnStatement(expr, columnTypeMap)
			if err != nil {
				return "", err
			}
			sb.WriteString(fmt.Sprintf(", ( SELECT %s FROM %s ) AS %s",
				wrapNewColumnResult(translated), defaultTableName, quoteDuckIdentifier(alias)))
		}
	} else {
		if len(typedConfig.Statement) > 0 {
			translated, err := translateNewColumnStatement(typedConfig.Statement, columnTypeMap)
			if err != nil {
				return "", err
			}
			sb.WriteString(wrapNewColumnResult(translated))
		} else {
			sb.WriteString(quotedNewColumnList(headers))
		}
	}

	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	if typedConfig.GroupBy != "" {
		if _, exists := columnTypeMap[typedConfig.GroupBy]; !exists {
			return "", buildColumnNotExistsError(typedConfig.GroupBy)
		}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(quoteDuckIdentifier(typedConfig.GroupBy))
	}

	return sb.String(), nil
}

// newColumnHeadersAndTypeMap reads the dataset's columns and their types.
//
// It is extractHeadersAndTypeMap except for an empty table: that one returns no
// headers at all when there are no rows, which would leave this operator
// building `SELECT , <statement> FROM ext` - a syntax error rather than an empty
// result. The column names and types live in the schema and are just as
// available with zero rows, so they are read from there.
//
// Transformv2 returns an empty dataset before reaching any operator, so this
// only matters to a caller invoking the operator directly.
func newColumnHeadersAndTypeMap(dataset *types.DataSetv2) ([]string, map[string]types.CellDataType) {
	cols := make([]string, dataset.NumCols())
	columnTypeMap := make(map[string]types.CellDataType, dataset.NumCols())
	for i := range dataset.NumCols() {
		col := dataset.Column(int(i))
		cols[i] = col.Name()
		columnTypeMap[col.Name()] = conversion.FiltrifyType(col.DataType())
	}
	return cols, columnTypeMap
}

func quotedNewColumnList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteDuckIdentifier(n)
	}
	return strings.Join(quoted, ",")
}

func (t *NewColumnOperatorv2) buildConfiguration(config string) (*NewColumnConfiguration, error) {
	return buildNewColumnConfiguration(config)
}

func (t *NewColumnOperatorv2) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
