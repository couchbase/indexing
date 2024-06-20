all: clean comp run

export GOPATH=$(PWD)/../../../../../../../../goproj:$(PWD)/../../../../../../../../godeps
export GO111MODULE=auto
export CGO_CFLAGS=-I$(PWD)/../../../../../../../../sigar/include -I$(PWD)/../../../../../../../../build/tlm/deps/zstd-cpp.exploded/include -I$(PWD)/../../../../../../../../build/tlm/deps/jemalloc.exploded/include -I$(PWD)/../../../../../../../../forestdb/include/ -DJEMALLOC=1
export CGO_LDFLAGS=-L $(PWD)/../../../../../../../../install/lib -Wl,-rpath $(PWD)/../../../../../../../../install/lib

clean:
	rm -rf ./cluster_ops

comp:
	go build -o cluster_ops main.go init_cluster.go

run:
# Performs a rebalance on the cluster without adding or removing
# nodes from the cluster. See Readme.md for other options to compile
# and run
	./cluster_ops
