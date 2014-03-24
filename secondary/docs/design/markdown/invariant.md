A list of invariants in the system.

* gauranteed transmission of kv mutations across the system.
* kv mutations within a vbucket is always ordered.
* the tip of indexer node across the index-cluster is always at a stable
  timestamp.
* query is always based on a stable timestamp.
* ensure that at any given time there is only one unique path for each
  mutations from kv-vbucket to index-slice.
* indexer/coordinator is responsible for recovery.

* all components will have a server thread listening on admin port, and strictly
  follow request and response protocol with its clients.
* indexer/coordinator can detect if a projector is disconnected and
  re-establish the connection.
* indexer/coordinator can call projector but it does not interact with KV
  directly.
* indexer/coordinator does not infer `{projector -> VB}` mapping by itself.
* projector can call the coordinator.
* projector can respond to indexer request.
* projector cannot call indexer directly since projector does not know about
  topology.
* router can send key-versions and control-messages to indexers through the
  endpoints provided in the topology.
* rollback happens when there is data loss in kv.
* rollback is stopping the whole system from functioning.

* an index can be split into N number of logical partition [1...N] uniquely
  identified by Partitionid.
* a logical partition could span across multiple indexer nodes.
* a logical partition is replicated 1 or more times within the secondary index
  cluster.
* a replica can decide how to shard the data within a logical partition.
* each shard is hosted by an indexer node.
* a shard is further divided into slices and each replica can decide how to
  slice data within the shard.
* a slice is the fundamental unit of index management like replication,
  rebalance etc.
* a logical partition on each replica should convey same subset of index.
* during rebalance a slice will move between shards within same logical
  parition.
