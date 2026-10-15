//go:build !duckdb_arrow

package operatorv2

// This file is the guard that makes the duckdb_arrow build tag mandatory for the
// whole package. It is compiled only when the tag is ABSENT, and all it does is
// name an identifier that does not exist, so the build stops with:
//
//	operatorv2/duckdb_disabled.go:29:11: undefined: filtrify_requires_the_duckdb_arrow_build_tag
//
// The identifier is spelled as a sentence because the compiler error is the
// whole user interface here - whoever hits it has usually just cloned the repo
// or opened it in an editor with no tags configured, and the message has to say
// what to do without their having to find this file. See the README.
//
// Why the tag is mandatory: every operatorv2 operator executes its SQL through
// DuckDB's Apache Arrow interface (executeSQLQueryDuck in duckdb.go), and Arrow
// is also how datasets cross into DuckDB and back. That interface is an opt-in
// dependency of duckdb-go, available only under this tag - so a tagless build
// would be a library that silently cannot run a query. Hence this file.
//
// duckdb.go and groupby.go carry the positive form of the tag for a different
// reason: not to make it mandatory - that is this file's job - but to keep the
// failure legible, since compiling them without the tag would bury the error
// below under "undefined: duckdb.NewArrowFromConn" and friends.
//
// TestBuildRequiresDuckArrowTag (duckdb_test.go) builds the package with no tags
// in a subprocess and fails if it succeeds, so deleting this file is caught.
const _ = filtrify_requires_the_duckdb_arrow_build_tag
