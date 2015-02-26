##Couchbase Secondary Indexes

Secondary Index Project aims to provide an alternative to the existing map-reduce based 
*Hash Partitioned Secondary Indexes*(known as Views) available in Couchbase.
This project is being built from ground up to improve the existing solution and
support new use-cases e.g. N1QL.

###Goals and Motivation

- **Performance** – Support index clustering to avoid scatter/gather and index working set management
- **Independent Scaling** – Enable secondary index to scale independent of Key-value workload
- **Partitioning Scheme Flexibility** – Can support for different partitioning schemes for index data (hash, range, key)
- **Indexing Structure Flexibility** – Can support for different indexing structure (b-tree/trie, hash, R+ tree)
- **Scan Consistency** – Using timestamp to ensure query stability and consistency


###Version1 Features

#####Indexing
- Key Based Partitioning Support
- Replicated Index Data to support Index Node Failure
- Horizontal Scalability(Rebalance) 
- Cluster Management using ns_server(master election)
- Distributed Index Metadata Management
- ForestDB Integration as backend for Persistence/Query
  - Support for Crash Recovery and Compaction
- Error Management (Recovery for all Indexing component failures)
- Administration UI (Management And Statistics)

#####KV Related
- Independent Scaling from KV Cluster
- Mutation Stream via DCP
- Support KV Failover(data loss), Rebalance

#####Query Related
- Consistency/Stability Options
- Active Replica for Query


Please see [Design Documentation](overview.md) for more details.

