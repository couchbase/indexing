Usage:

Do "make comp" to compile the tool

./cluster_ops -> Performs only rebalance on the existing cluster. If cluster is not initialised, initialises the cluster and adds nodes 127.0.0.1:9000 (kv+n1ql), 127.0.0.1:9001 (index), 127.0.0.1:9002 (index) services to cluster
./cluster_ops --Nodes 127.0.0.1:9003 --Nodes 127.0.0.1:9004 -> Initialises cluster with nodes 127.0.0.1:9003 & 127.0.0.1:9004 to the cluster
./cluster_ops --addNodes 127.0.0.1:9003 --addNodes 127.0.0.1:9004 -> Adds nodes 127.0.0.1:9003 & 127.0.0.1:9004 to the cluster
              and performs rebalance (Unless -rebalance=false is set explicitly)
./cluster_ops --ejectNodes 127.0.0.1:9003 --ejectNodes 127.0.0.1:9004 -> Ejects nodes 127.0.0.1:9003 & 127.0.0.1:9004 to the cluster
              and performs rebalance (Unless -rebalance=false is set explicitly)
