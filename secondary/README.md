Secondary Index Repository
==========================

All production secondary index related components (code, design, documentation) live here.

###Setup and Build Instructions

    $ mkdir -p $GOPATH/src/github.com/couchbase
    $ cd $GOPATH/src/github.com/couchbase
    $ git clone git@github.com:couchbase/indexing.git
    $ cd indexing/secondary
    $ go get -d -v ./...
    $ cd $GOPATH/src/github.com/couchbase/query/parser/n1ql
    $ go tool yacc n1ql.y
    $ cd $GOPATH/src/github.com/couchbase/indexing/secondary
    $ ./build.sh

Note:
Following dependencies need to be installed beforehand:
- Protobuf: https://code.google.com/p/protobuf/
- ForestDB: https://github.com/couchbaselabs/forestdb

If build is successful, indexing/secondary/bin will have the binaries for projector and indexer.

####Starting Projector

Projector can be started with below command line options:

    bin > ./projector
    Usage : ./projector [OPTIONS] <cluster-addr> 
      -adminport="localhost:9999": adminport address
      -loglevel=debug: choose logging level
      -kvaddrs="127.0.0.1:12000": comma separated list of kvaddrs
      
E.g. projector can be started in trace mode against Couchbase server running with 
cluster_run mode with the command:

    ./projector -trace localhost:9000
    

Projector has a sample test program which can be run using:

    go run tools/datapath.go

####Starting Indexer

Projector can be started with below command line options:

    bin > ./indexer -h
    Usage of ./indexer:
      -loglevel=info|debug|trace
      -projector="http://localhost:9999": Projector Admin Port Address
      -vbuckets=1024: Number of vbuckets configured in Couchbase
    
    
E,g. indexer can be started for 8 vbuckets and log level of Info with the below command:

    ./indexer -vbuckets 8 -log 1
    
Currently the only entry point for Indexer is tuqtng command line shell. 
Follow the instructions for setting up the tuqtng project:

https://github.com/couchbaselabs/tuqtng#developers

Instead of the master branch, index-preview branch needs to be built. 
Rest of the instructions remain the same. 

And on the cbq shell, creating an index of type "forestdb" will allocate a new index
in the Indexer:

    cbq> create index idx1 on default(field1) using forestdb;
    





