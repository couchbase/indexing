## IndexManager

IndexManager is the component that co-ordinate other components - like
projector, query, indexer nodes during bootstrap, rollback, reconnection,
local-indexer-restart etc.

Since rest of the system depends on IndexManager for both normal operation and
for failure recovery, restart etc. it can end up becoming single-point-of
failure within the system. To avoid this, multiple instance of IndexManager
will be running on different nodes, where one of them will be elected as
master, called Index-Coordinator, and others will act as replica, called
Index-Coordinator-Replica, to the current master.

### StateContext

State context acts as the reference point for rest of the system. Typically it
contains fields to manage index DDLs, topology, event-publisher etc.
Several API will be exposed by Index-Coordinator to update StateContext or
portion of StateContext.

### scope of IndexManager

1. handle scan and query request from client SDKs and N1QL clients.
2. co-operate with Index-Coordinator to generate stable scan.
3. co-operate with ns-server for master election and master notification.

### scope of Index-Coordinator

1. save and restore StateContext from persistent storage.
2. hand-shake with local-indexer-nodes confirming the topology for each index.
3. process index DDLs.
4. for new index, generate a topology based on,
   * administrator supplied configuration.
   * list of local-indexer-nodes and load handled by each of them.
5. co-ordinate index re-balance.
6. generate and publish persistence timestamps to local-indexer-nodes.
   * maintain a history of persistence timestamps for each bucket.
7. replicate changes in StateContext to other Index-Coordinator-Replica.
8. add, delete topics in pub-sub. subscribe, un-subscribe nodes from topics,
   optionally based on predicates.
9. provide network API to other components to access index-metadata,
   index topology and publish-subscribe framework.
10. generate restart timestamp for upr-reconnection.
11. negotiation with UPR producer for failover-log and restart sequence number.
12. create rollback context for kv-rollback and update rollback context based
    on how rollback evolves within the system.

### scope of ns-server

1. master-election is done by ns-server
2. master-election is done by ns-server during system start and whenever current
   master fail to respond for `hearbeat` request.
3. ns-server will be the central component that maintain the current master and
   list of active replica and list of indexer-nodes.
4. actively poll - master node, replica nodes and other local-indexer-nodes for
   its liveliness using `heartbeat` request.
5. provide API for IndexManagers and local-indexers to join or leave the cluster,
   to fetch current Index-Coordinator, Index-Coordinator-Replicas and list of
   indexer-nodes, and transaction API for updating StateContext.

### a note on topology

A collection of local-indexer nodes take part in building and servicing
secondary index. For any given index a subset of local-indexer nodes will be
responsible for building the index, some of them acting as master and few others
acting as active replicas.

Topology of any given index consist of the following elements,

* list of indexer-nodes, aka local-indexer-nodes, hosting the index.
* index slice, where each slice will hold a subset of index.
* index partition, where each partition is hosted by a master-local-indexer and
  zero or more replica-local-indexer. Each partition contains a collection of
  one or more slices.

### master election

We expect ns-server to elect a new master,

* during system bootstrap.
* during a master crash (when master fails to respond for heartbeat request).
* when a master voluntarily leaves the cluster.

after a new master is elected, ns-server should post a bootstrap request to
each IndexManager. There after IndexManager can fetch the current master from
ns-server and become an Index-Coordinator or Index-Coordinator-Replica.
