build:
	go build -o bin/main main/main.go

.PHONY: test
test:
	go test -short -v ./

.PHONY: testbigdata
testbigdata:
	go test -v ./ -run ^TestBigData

.PHONY: testall
testall:
	go test -v ./
