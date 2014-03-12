## System Diagram Index-Query

###Index-Query System Diagram For Scan Request

![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/SystemDiagramScan.svg)

__Annotations__

1. Index Scan Request from Query Server to Index Client.
2. Index Client sends the Scan request to __ANY__ Indexer(which gets promoted to scan co-ordinator).
3. Scan Co-ordinator requests required Indexers(local and remote) to run the Scan.
4. Scan Co-ordinator gathers the results.
5. Local Indexer runs a Scan on persisted index snapshots to formulate the results.
6. Scan Co-ordinator returns the results to requesting Index Client.
7. Scan Results are returned to Query Server.

###Index-Query System Diagram For DDL Request


![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/SystemDiagramDDL.svg)

__Annotations__

1. Index DDL Request from Query Server to Index Client.
2. Index Client sends the DDL request to Master Index Manager.
3. Index Manager decides the topology and informs participating Local Indexers to process DDL.
4. Local Indexers allocates/deallocates storage for new DDL request.
5. Index Manager replicates the updated metadata.
6. Metadata is persisted at both master and replica Index Manager.
7. Index Manager notifies __ALL__ KV projectors for the new DDL request.
8. Index Manager returns status of DDL request to Index Client.
9. DDL Request Status is returned to Query Server.
