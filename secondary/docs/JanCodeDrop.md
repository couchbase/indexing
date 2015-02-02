##2i Jan Code Drop Features
  
###Indexer

####QE Ready

- Create/Drop/List/Scan On Single Indexer Node
- Multi Indexer Nodes
- Rebalance KV Node In/Out
- Failover KV Node(Graceful)
- Bucket Delete
- Projector Process Failure Recovery
- KV Failure(Memcached Crash/Node Restart) Recovery
- Indexer Process Failure Recovery
- Deferred Index Build
- Auto-discovery of Cluster Services
- CbAuth Integration
- Configurable Indexer Settings
- Indexer Statistics

###Implemented

- Failover KV Node(Hard) [MB-13239](https://issues.couchbase.com/browse/MB-13239)
- Bucket Flush [MB-13239](https://issues.couchbase.com/browse/MB-13239)
- Index Compaction [MB-13111](https://issues.couchbase.com/browse/MB-13239)
- In-memory Snapshots
- Multi Buckets
