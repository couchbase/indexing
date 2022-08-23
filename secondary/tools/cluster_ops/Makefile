all: clean comp run

export GOPATH=$(PWD)/../../../../../../../../goproj:$(PWD)/../../../../../../../../godeps
export GO111MODULE=off

clean:
	rm -rf ./cluster_ops

comp:
	go build -o cluster_ops main.go init_cluster.go

run:
# Performs a rebalance on the cluster without adding or removing
# nodes from the cluster. See Readme.md for other options to compile
# and run
	./cluster_ops
