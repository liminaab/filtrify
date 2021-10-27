build:
	go build -o bin/main main/main.go

.PHONY: test
test:
	go test -v ./