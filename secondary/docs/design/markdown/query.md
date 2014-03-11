##Query Execution Flow

This document describes the flow of execution of a query engine request.

####Create/Drop DDL Request

####Scan Request 

![](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/ScanWorkflow.svg)

**Description**

1. Index Client(query catalog implementation which resides on query server) receives index scan request from the Query Server Component. 
2. Index Client will choose a Indexer node to send this request to. Index Client will have a list of available indexer nodes.
3. Index Client will choose(or will be provided by query engine) a Consistency/Stability option for the index scan. This is based on the consistency/latency requirements of the application issuing the query.

  __Consistency Options__
  - Any Consistency : The indexer would return the most current data available at the moment.  
  - Session Consistency : The indexer would query the latest timestamp from each KV node.   It will ensure that the scan   result is at least as recent as the KV timestamp.  In other words, this option ensures the query result is at least as recent as what the user session has observed so far.   

  __Stability Options__
  - No Stability.   The indexer does not ensure any stability of data within each scan.  This is the only stability option for consistency-Any.
  - Scan Stability : The indexer would ensure stability of data within each scan.
  - Query Stability.  The indexer would ensure stability of data between multiple scans within a query.

4. Scan Request is sent to the Indexer identified in Step 2 along with the consistency/stability options.
5. The Indexer receiving the Scan request will become the scan co-ordinator. It requests the local Index Manager for latest index topology information for the index to be scanned.
6. Index Manager responds with the index topology information for the requested index.
  - If index is not found, scan coordinator can return an error "INDEX_NOT_FOUND"
  - If index exists but is in rollback mode, scan coordinator can return an error "INDEX_IN_ROLLBACK"
7. Scan coordinator requests latest Stability Timestamp from the Index Manager(except for No Stability option).
8. Index Manager responds with the latest Stability Timestamp.
9. Based on consistency/stability options for the scan, Scan coordinator would either poll all KV nodes for the latest timestamp(Session Consistency) and mark that as the Scan Timestamp or choose Stability Timestamp for the Scan (Scan/Query Stability) or a nil scan timestamp for scanning the tip(Any Consistency/No Stability).
10. Scan coordinator will send all participating indexers(identified in Step 5) the Scan request with Scan Timestamp.
11. Each local indexer node would use its topology metadata for finding the slices that are required for this scan.
  - If the slices is not active or cannot be found, the local indexer should return an error to the scan coordinator.
12. Local indexer will wait for its local stability timestamp to get past Scan Timestamp(Session Consistency) or run scan against the snapshot matching Scan Timestamp(Scan/Query Stability) or run the scan on the tip(Any Consistency/No Stability).
  - If snapshot is deleted, then the indexer can return an error “SNAPSHOT_TOO_OLD”
13. Scan results are returned to Scan co-ordinator.
14. Results are consolidated/aggregated as required. 
15. Results are returned to index client. Local indexer may start streaming the results to index client before all scans are finished for efficiency.
