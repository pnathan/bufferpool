
report.xml: $(shell find  src/ -type f -name '*.go')
	gotestsum --junitfile report.xml --format standard-verbose

bin/cmd:
	mkdir -p bin
	go build -o bin ./...
