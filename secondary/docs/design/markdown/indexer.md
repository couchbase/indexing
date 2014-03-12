## Indexer

Indexer is a node in secondary-index cluster that will host secondary-index
components like IndexManager, Index-Coordinator, Index-Coordinator-Replica and
Local-Indexer process.

### Scope of local indexer

1. run one or more instance of storage engine to persist projected key-versions.
2. manage index partitions and slices for each index.
3. independent restart path
4. Manage Storage Engine
   * Control compaction frequency
   * Snapshot Creation/Deletion
