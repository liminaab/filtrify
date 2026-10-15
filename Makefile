# The duckdb_arrow tag turns on DuckDB's Arrow interface in duckdb-go, which is
# an opt-in dependency there. executeSQLQueryDuck needs it; without the tag that
# function is a stub that returns an error (see operatorv2/duckdb_disabled.go).
TAGS := duckdb_arrow

build:
	go build -tags=$(TAGS) -o bin/main main/main.go

.PHONY: test
test:
	go test -tags=$(TAGS) -short -v ./

.PHONY: testbigdata
testbigdata:
	go test -tags=$(TAGS) -v ./ -run ^TestBigData

.PHONY: testall
testall:
	go test -tags=$(TAGS) -v ./

.PHONY: testduck
testduck:
	go test -tags=$(TAGS) -v ./operatorv2/ -run TestDuck
