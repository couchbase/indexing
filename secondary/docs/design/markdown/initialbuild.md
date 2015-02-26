##Execution flow of CREATE INDEX(Initial Index Build) 

Initial Index Build can be divided into 2 phases:
- **Prepare Phase** - Index metadata (id, topology) is generated and storage gets allocated for the new index.
- **Load Phase** - DCP mutation stream is initiated for the new index and indexers process/store secondary keys.

*After Prepare phase, it will require the administrator to initiate index building from UI.
The administrator will have to select the new index definitions for initial loading.
This will allow same DCP connection to be shared for multiple indexes' initial build.*

####Prepare Phase

![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/InitialBuild_Prepare.svg)

#####Description


1. The Index Coordinator receives CREATE INDEX request from client.
2. A unique index id is generated, the state of index is marked as **INIT**, StateContext is updated with proposed topology information and CAS is incremented for StateContext.
3. StateContext is persisted locally so it can be recovered if Index Coordinator restarts.
4. StateContext is replicated to *ALL* Index Coordinator Replicas synchronously i.e. Index coordinator waits for each replica to ACK the replication request.
  -  If Replica doesn't reply with ACK, Index Coordinator will retry
  -  If retry doesn't succeed, Coordinator can check with ns_server if replica is down.
5. Index Coordinator Replica will locally persist the StateContext and send ACK back to the master. 
  - If Master dies and this replica gets promoted to master, it can resume the index building or do cleanup depending on the state of the system.
  - The new master must consolidate the StateContext from other replicas. It will pick the highest version/CAS of the StateContext among the replica. The new master is responsible for synchronize the state again among the replica.
6. Same as Step 4(Step 4 and 5 will be repeated for all index coordinator replicas).
7. Same as Step 5.
8. The Index Coordinator sends Create Index request to *ALL* Indexers which are part of index topology. 
9. Local Indexer will allocate storage for the new index, update its local metadata and sends ACK to coordinator.
10. Same as Step 8(Step 8 and 9 will be repeated for all indexers hosting this index).
11. Same as Step 9.
12. Once Index Coordinator has received ACKs from all Indexers, Index State will be marked as **PREPARED**, StateContext will be updated and CAS gets incremented. The new StateContext is persisted locally.
13. Index Coordinator will replicate new StateContext to *ALL* Index Coordinator Replicas synchronously.
14. Index Coordinator Replica will locally perist the StateContext and send ACK back to the master.
15. Same as Step 13(Step 13 and 14 will be repeated for all index coordinator replicas).
16. Same as Step 14.
17. Index Coordinator returns SUCCESS to client.


####Load Phase

![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/InitialBuild_Load.svg)

#####Description

1. Receive Index Build request as a result of administrator action on UI.
2. Index coordinator will ask *ALL* Projectors to provide the current KV timestamp. This timestamp becomes the `Initial Build Timestamp`.
3. Projector will get the current timestamp from KV and reply to Index coordinator.
4. Index coordinator will notify all Routers about new index topology. 
5. Router will create topic/subscribers for the new index and ACK to index coordinator.
6. Index Coordinator will notify all Projectors about the new index definitions. 
7. Projector will start DCP mutation stream for new topic. This new stream will be in addition to the existing DCP index maintenance stream.
8. For each incoming mutation on the stream, projector would run the map functions for the indexes being built and publish the secondary key versions to Router on the new topic.
9. Router will forward the secondary key versions to local indexers based on the subscribers for the new topic.
10. For the new index, the local indexer would now have two incoming DCP streams, one for indexing building and the other one for index maintenance. The mutations from index building stream will be put in the `Initial Build Queue` and index status marked as **INITIAL_BUILD**.
11. Local Indexer will persist mutations from `Initial Build Queue` on a regular basis and create snapshots which can be used to resume the index build if there is a crash.
12. Once thetimestamp of mutations with highest seq no in `Initial Build Queue` matches the `Initial Build Timestamp`, the local indexer will mark the index initial build as complete.
13. Indexer will send "Initial Build Complete" message to index coordinator.
14. Once index coordinator has all "Initial Build Complete" message from all the indexers, it will mark the index as **READY** (available for query) in State Context. This StateContext is then replicated.
15. All Indexers are informed about the new Index Status. Indexers will now change index status to **READY** and accept any Scan requests for this index.


After this point, the index will still be behind the main `Mutation Queue`. It will continue to use the `Initial Build Queue` to accept mutation till:
- High Seq numbers for all vbuckets in `Initial Build Queue` is equal to the low seq numbers of `Mutation Queue` 

Once the index status gets changed to READY, indexer will persist the mutations from `Initial Build Queue` at Stability Timestamps(and create stability snapshots) so the new index can be used for scan request and can also participate in recovery.  

####Notes

- Detailed description and recovery cases in Initial Build can be found in [John's Inital Index Build Document.](https://docs.google.com/document/d/18B_PtgpbI413NcVUjyN0PmfRIwX2umYnr4Tp1L4PX1w/edit)

- If there is network partitioning before the replication is completed,
the algorithm would allow the master to continue to replicate the
StateContext to the other replica once the network is healed.  This is
expecting that there is no split brain (automatic failover will be off if
ns_server detects more than one node is down).  There is still a corner
case that split brain may still happen (the master looses connectivity).
But this algorithm should work in this case since there is at least one
replica has the replicated StateContext.

- Let's say the master has replicated the state to a set of replica and
then the rack fails.  Let's also say that for some strange reason, the
master and those replica are in the same rack.  In this case if a
new master is selected, it will not see the new StateContext.   In this
case, we have to consider this request to fail.  When the rack rejoins,
those nodes will have to restart and they have to sync up with the
StateContext from the new master.
