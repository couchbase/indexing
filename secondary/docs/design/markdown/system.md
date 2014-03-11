##System Diagram


###KV-Index System Diagram With Multiple Bucket and Indexes

The below diagram shows the system with Multiple Buckets and Indexes:

**KV Cluster(2 nodes)** <br>
2 Buckets - B1 and B2 (Each with 4 vbuckets)
<br>

**Index Cluster(2 nodes)** <br>
2 Indexes per bucket:<br>
B1 - I1, I2 <br>
B2 - I3, I4

**BLACK** arrows represent flow of Data Messages(key versions)<br>
**RED** arrows represent Meta Messages

![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/SystemDiagramMultipleBuckets.svg)

####Annotations

*HWT* - [High-Watermark Timestamp](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md)<br>
*ST* - [Stability Timestamp](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md)<br>
*FI* - [Forward Index](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md)<br>
*BI* - [Back Index](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md)<br>

1. Data Mutations via [UPR](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) are sent to [Projector](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) which subscribes for the active vbuckets on a node. Projector runs map functions based on Index Definitions and outputs secondary key versions.
2. Secondary Key versions are sent to [Router](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) component. Based on index distribution/partitioning topology it determines which [Indexer](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) node should receive the key version. 
3. Router has a guaranted delivery component called Transporter which handles the actual network transport. A single mutation can result in multiple messages to be sent to multiple index nodes. [More Details](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/mutation.md).
4. Router can occasionally send SYNC messages to [Index Manager](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) which enables it to calculate next [Stability Timestamp](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) for all Indexers.
5. For normal workflow, Indexer will accept and store the mutations in [Mutation Queue](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md). These mutations get processed once Index Manager decides to generate a Stability Timestamp.
6. For Rollback scenarios, Indexer will store the mutations in [CatchUp Queue](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md). For complete details of rollback workflow, see [Recovery](Add Link Here).
7. Index Manager master synchronizes with its replica to synchronously replicate Index Definition metadata, Recovery context etc.
8. Index Manager communicates with all Indexers to announce new Stability Timestamp, Rollback Mode Init, collect HW timestamps for recovery etc.
9. Local Persistence for Index Manager. 
10. Local Persistence for Indexer for all secondary key versions. All Persistent Snapshots are stored locally.

####Highlights
- Indexer maintains HWT and ST at per bucket level on each node. In this case I1 and I2 share HWT+ST as these are from same bucket. I3 has its own copy. 
- Index Manager maintains ST for each bucket. 
- There is a single Mutation Queue(Catchup Queue) per node. 
- Router sends periodic SYNC messages to Index Manager (based on snapshot markers?)

####Open Questions
- How does Index Manager get the "Sync" messages to decide on the next Stability Timestamp
- How does Indexer get the updated topology information to service Scan request? Index Manager exposes an API or from the replicated metadata file directly?
- Does the Mutation/Catchup Queue needs to be per bucket as well?


###KV-Index System Diagram (Single Bucket/Index)
![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/SystemDiagram.svg)
