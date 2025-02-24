#!/bin/bash

export WORKSPACE="$(PWD)/../../../../../../../../"
export GOPATH="$WORKSPACE/goproj":"$WORKSPACE/godeps"
export GO111MODULE=auto
export CGO_CFLAGS="-I$WORKSPACE/sigar/include -I$WORKSPACE/build/tlm/deps/zstd-cpp.exploded/include -I$WORKSPACE/build/tlm/deps/jemalloc.exploded/include -I$WORKSPACE/forestdb/include/ -DJEMALLOC=1"
export CGO_LDFLAGS="-L $WORKSPACE/install/lib -Wl,-rpath $WORKSPACE/install/lib"
export CBAUTH_REVRPC_URL="http://Administrator:asdasd@127.0.0.1:9000/query"
