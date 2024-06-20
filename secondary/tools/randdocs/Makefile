all: clean comp run

export GOPATH=$(PWD)/../../../../../../../../goproj:$(PWD)/../../../../../../../../godeps
export GO111MODULE=auto
export CGO_CFLAGS=-I$(PWD)/../../../../../../../../sigar/include -I$(PWD)/../../../../../../../../build/tlm/deps/zstd-cpp.exploded/include -I$(PWD)/../../../../../../../../build/tlm/deps/jemalloc.exploded/include -I$(PWD)/../../../../../../../../forestdb/include/ -DJEMALLOC=1
export CGO_LDFLAGS=-L $(PWD)/../../../../../../../../install/lib -Wl,-rpath $(PWD)/../../../../../../../../install/lib

clean:
	rm -rf ./randdocs

comp:
	go build -o randdocs cmd/main.go

run:
	./randdocs -config ./cmd/config.json
