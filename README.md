# Filtrify

## Building

This module **must** be built with the `duckdb_arrow` build tag:

```sh
go build -tags=duckdb_arrow ./...
go test  -tags=duckdb_arrow ./...
```

The `operatorv2` operators execute their SQL through DuckDB's Apache Arrow
interface, and Arrow is also how datasets cross into DuckDB and back. That
interface is an opt-in dependency of `duckdb-go`, available only under this tag.

Building without the tag is a deliberate compile error rather than a build that
silently lacks query execution:

```
operatorv2/duckdb_disabled.go:29:11: undefined: filtrify_requires_the_duckdb_arrow_build_tag
```

Every `Makefile` target already passes the tag, so `make build` and `make test`
work as-is.

### Editor setup

Without the tag your editor will report the `operatorv2` package as broken.

- **VS Code / gopls** — set `"gopls": {"build.buildFlags": ["-tags=duckdb_arrow"]}`
- **GoLand** — Settings > Go > Build Tags & Vendoring > Custom tags: `duckdb_arrow`

### Consuming this library

A project that imports `filtrify` needs the tag in its own build too; build tags
are not inherited from a dependency.
