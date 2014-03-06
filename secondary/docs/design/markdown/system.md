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

*HWT* - High-Watermark Timestamp<br>
*ST* - Stability Timestamp<br>
*FI* - Forward Index<br>
*BI* - Back Index<br>

####Highlights
- Indexer maintains HWT and ST at per bucket level on each node. In this case I1 and I2 share HWT+ST as these are from same bucket. I3 has its own copy. 
- Index Manager maintains ST for each bucket. 
- There is a single Mutation Queue(Catchup Queue) per node. 
- Router sends periodic SYNC messages to Index Manager (based on snapshot markers?)

####Open Questions
- How does Index Manager get the "Sync" messages to decide on the next Stability Timestamp
- How does Indexer get the update topology information to service Scan request? Index Manager exposes an API or from the replicated metadata file directly?
- Does the Mutation/Catchup Queue needs to be per bucket as well?


###KV-Index System Diagram (Single Bucket/Index)
![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/SystemDiagram.svg)
