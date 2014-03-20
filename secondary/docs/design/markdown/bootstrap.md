##System Bootstrap Sequence


System bootstrap will be initiated by ns_server.

- __Process Init__ - ns_server will bring up all the processes with required parameters.
Once a process is up, it follows its own bootstrap sequence and then either waits for further commands to be given
or contacts Index Coordinator to get the latest StateContext and takes further actions based on that.

- __Process Watchdog__ - ns_server also watchdogs each process and restarts it in case of failure/crash.

- __Master Election__ - ns_server will designate one of the Indexer Nodes as master.
- __Node Failure Detection__ - In case a node goes down, ns\_server will detect that and pass this information to Index Coordinator Master. If Index Coordinator Master node has gone down, ns\_server will elect a new master.

![](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/Bootstrap.svg)

####Brief Steps

1. ns_server brings up Index Coordinator Master.
2. ns_server brings up Index Coordinator Replicas providing master endpoint as input.
3. All replicas handshake with master to sync up.
4. Index Coordinator chooses the latest StateContext among its local copy and all the replicas. Latest StateContext is replicated to all replicas.
5. Index Coordinator master sends ACK to ns_server.
6. ns_server brings up Projector/Router on all KV nodes. Projector and Router are stateless components. Both the components listen on their admin ports for messages from other indexing components.
7. ns_server provides the list of Projectors/Routers to Index Coordinator master.
8. Index Coordinator master updates the local StateContext with this information and persist it.
9. State Context is replicated to all Index Coordinator Replicas.
10. Index Coordinator master sends ACK to ns_server.
11. ns_server brings up Indexer process on all Indexer Nodes providing master endpoint as input.
12. Indexer process will handshake with Index Coordinator master and get the StateContext (which has index topology and list of projectors).
13. Index Coordinator will provide StateContext to Indexers.
14. Once all Indexers have done the handshake, Index Coordinator will send request to all Projectors to initiate `Topic Creation` for `MaintenanceStream` topic sending StateContext to Projector.
15. One or more Indexers may choose to create `CatchupStream` topic with projector based on if its in recovery.


####Detailed Steps

1. ns\_server will have a list of `Indexer Nodes` as part of its config. This list gets updated if nodes are added/removed from the Admin UI. When the system starts, ns\_server will designate one node as master and spawn an instance of Index Manager(called Index Coordinator from this point) with Master role. Index Coordinator Master will check if it has a locally persisted StateContext.
 - If this is a fresh election for this master, it will not find any local StateContext. It waits for replicas to register with it.
 - If this is a process restart(crash/node restart), it will find a local StateContext. It then waits for replicas to register with it.
 - If this is a promotion from replica to master, local StateContext will be found. New master needs to make sure it has the latest replicated StateContext. Again it waits for replicas to register with it. 

2. ns_server will further designate a set of nodes(based on config param) as replicas and spawn an instance of Index Manager(Index Coordinator) with Replica role providing the endpoint of Master.
 - It this is a fresh election for this replica, it will not find a local StateContext. It will register with master, asking for StateContext.
 - It this is a process restart(crash, node restart), it will find a local StateContext. It will register with master, providing the version number of its StateContext.
 - If ns_server elects a new master, it will inform all existing replicas with the new endpoint. All replicas need to register with the new master to sync up.

3. All replicas handshake with master to sync up.
4. Once all replicas have registered with Index Coordinator master, it will compare the local StateContext version with the version received from replicas and choose the highest version of StateContext. If it doesn't have the highest version, it will ask the replica to provide the StateContext. Latest StateContext is then replicated to all replicas.
 - If this was a process restart, indexers will automatically retry and re-establish the connection.

5. Index Coordinator master will send ACK to ns_server to proceed with rest of system bootstrap.
6. ns_server brings up Projector/Router on all KV nodes. Projector and Router are stateless components. Both the components listen on their admin ports for messages from other indexing components.
 - If this is a indexing system restart, ns_server doesn't need to restart projector/router.
 - If customer is using cache-only case, then there will be config flag that tells ns_server to skip the whole bootstrap sequence for indexing components.
7. ns_server provides the list of Projectors/Routers to Index Coordinator master.
 - A module sits between coordinator and ns_server to provide the mapping from projectors to vbuckets. This will help to encapsulate "physical" KV topology information from coordinator.
8. Index Coordinator master updates the local StateContext with this information and persist it.
9. State Context is replicated to all Index Coordinator Replicas.
10. Index Coordinator master sends ACK to ns_server.
11. ns_server brings up Indexer process on all Indexer Nodes providing master endpoint as input.
 - If the indexer process is already running (e.g. in case of new master election), it is provided with new master endpoint to handshake with.
 - If one indexer process crashes and its being brought up again by ns_server, same procedure is followed i.e. its provided with master endpoint to handshake.
12. Indexer process will handshake with Index Coordinator master and get the StateContext (which has index topology and list of projectors).
13. Index Coordinator will provide StateContext to Indexers.
14. Once all Indexers have done the handshake, Index Coordinator will send request to all Projectors to initiate `Topic Creation` for `MaintenanceStream` topic sending StateContext to Projector.
 - In case of a single indexer restart, Index Coordinator master makes this request to all Projectors so they can re-add this subscriber to `MaintenanceStream` topic.
13. One or more Indexers may choose to create `CatchupStream` based on if its in recovery.

