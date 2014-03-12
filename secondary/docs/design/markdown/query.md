##Query Execution Flow

This document describes the flow of execution of a query engine request.

####Scan Request 

![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/ScanWorkflow.svg)

**Description**

1. Index Manager will periodically notify the Indexer about the latest changes in index toplogy. This enables Indexer to maintain a local copy of latest index topologies.
2. Index Client(query catalog implementation which resides on query server) receives index scan request from the Query Server Component. 
3. Index Client will choose a Indexer node to send this request to. Index Client will have a list of available indexer nodes.
4. Index Client will choose(or will be provided by query engine) a Consistency/Stability option for the index scan. This is based on the consistency/latency requirements of the application issuing the query.

  __Consistency Options__
  - `Any Consistency` : The indexer would return the most current data available at the moment.  
  - `Session Consistency` : The indexer would query the latest timestamp from each KV node.   It will ensure that the scan   result is at least as recent as the KV timestamp.  In other words, this option ensures the query result is at least as recent as what the user session has observed so far.   

  __Stability Options__
  - `No Stability` : The indexer does not ensure any stability of data within each scan.  This is the only stability option for consistency-Any.
  - `Scan Stability` : The indexer would ensure stability of data within each scan.
  - `Query Stability` : The indexer would ensure stability of data between multiple scans within a query.

5. Scan Request is sent to the Indexer identified in Step 2 along with the consistency/stability options.
6. The Indexer receiving the Scan request will become the scan co-ordinator. It checks the local topology information to decide which Indexer nodes will participate in this scan.
  - If index is not found, scan coordinator will return an error "INDEX_NOT_FOUND"
  - If index exists but is in rollback mode, scan coordinator will return an error "INDEX_IN_ROLLBACK"
7. Scan co-ordinator decides on the Scan Timestamp for this scan. Based on consistency/stability options, it would either: 
  - Poll all KV nodes for the latest timestamp(`Session Consistency`) and choose a Stability Timestamp later than that. Otherwise wait for such Stability Timestamp to be available.
  - Choose latest Stability Timestamp as the Scan Timestamp for the Scan (`Any Consistency` + `Scan/Query Stability`).
  - Choose a nil Scan Timestamp for scanning the tip(`Any Consistency` + `No Stability`).
8. Scan coordinator will send all participating indexers(identified in Step 6) the Scan request with Scan Timestamp.
9. Each local indexer node would use its topology metadata for finding the slices that are required for this scan.
  - If the slices is not active or cannot be found, the local indexer should return an error to the scan coordinator.
10. Local indexer will:
  - Wait for its local stability timestamp to get past Scan Timestamp(`Session Consistency`) and run the scan.
  - Run scan against the snapshot matching Scan Timestamp(`Any Consistency` + `Scan/Query Stability`).
  - Run the scan on the tip(`Any Consistency` + `No Stability`). <br>
If snapshot is deleted, then the indexer will return an error “SNAPSHOT_TOO_OLD”
11. Scan results are returned to Scan co-ordinator.
12. Results are consolidated/aggregated as required. 
13. Results are returned to index client. Local indexer may start streaming the results to index client before all scans are finished for efficiency.
