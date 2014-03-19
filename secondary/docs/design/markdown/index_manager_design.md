## IndexManager design

Each instance of IndexManager will be modeled as a state machine backed by a
data structure, called StateContext, that contains meta-data about the secondary
index, index topology, and meta-data for normal operation of IndexManager
cluster.

**relevant data structures**

```go
    type Timestamp   []uint64 // timestamp vector for vbuckets
    type Projectorid byte     // range of projectors
    type Nodeid      byte     // defines the range of indexer-nodes
    type Partitionid byte     // range of partition numbers

    type VbVector struct {
        vbuuid uint64   // vbucket unique id
        seqNo  uint64   // vbucket sequence number
    }

    type IndexDefinition struct {
        State      string    // "pending", "backfill", "maintanence"
        Name       string    // Name of the index
        Uuid       uint64    // unique id for every index
        Using      IndexType // indexing algorithm
        OnExprList []string  // expression list
        Bucket     string    // bucket name
        IsPrimary  bool
        Exprtype   ExprType
    }

    type StateContext struct {
        // value gets incremented after every updates.
        cas              uint64

        // list of projectors as `connectionAddr` that will accept
        // admin-messages.
        projectors       map[Projectorid]string

        // map of local-indexer-node id and its `connectionAddr`. Indexer node
        // will be identified from 0-250
        indexers         map[Nodeid]string          // indexed by indexer-id

        // per bucket current timestamp continuously updated by SYNC message.
        mutationTimestamp map[string][MAX_VBUCKETS]VbVector // indexed by bucketname

        // per bucket vector of vuuid corresponding to currentTimestamp and
        // continuously updated by SYNC message.
        backfillTimestamp map[string][MAX_VBUCKETS]VbVector // indexed by bucketname

        // Index info for all active indexes.
        indexeInfos      map[uint64]IndexDefinition // indexed by `indexid`

        // per index map of index topology
        indexesTopology  map[uint64]IndexTopology   // indexed by `indexid`
    }
```

## topology

Index-topology helps router to send key-versions to respective master
indexer-nodes and its active replica.

Topology of any given index consist of the following elements,

```go
    type IndexTopology struct {
        indexid       uint64

        // List of all local-indexer-nodes hosting this index.
        servers       []Nodeid

        // router interface. Will vary based on parition algorithm.
        topology      TopologyProvider

        // number of re-balances active for this index
        rebalances    []Rebalance

        rollback      Rollback
    }

    // TopologyProvider interface will encapsulate the partition algorithm and
    // required data structures. More APIs will be defined to educate the
    // algorithm about load distribution, rebalances etc..
    //
    // A `Type` implementing this interface will be embedded inside index-topology
    // structure and replicated across routers and indexers. `Type` depend on the
    // choice of partition algorithm.
    type TopologyProvider interface {
        // For given key-version identify the logical partition in which the
        // key is located.
        getPartition(keyVersion []byte) Partition

        // A logical partition can have more than one replica. A `Replica`
        // abstracts how sub-set of an index hosted by logical partition is
        // sharded across different nodes and sliced within each node.
        getReplicas() []Replica

        // For a given replica and key-version compute the slice and shard in
        // which key is stored. `Shard` provides the endpoint where the key is
        // located.
        locateKey(replica Replica, keyVersion []byte) (Shard, Slice)
    }
```

### failed, orphaned and outdated IndexManager

When an IndexManager crashes,
* it is restarted by ns-server, joins the cluster, subsequently it enters
  bootstrap state.
* when an IndexManager is outdated with respect to a new Index-Coordinator it
  will update itself as part of bootstrap handshake.

## IndexManager bootstrap

When an IndexManager starts-up it will load its local StateContext from
persistent storage and enter into bootstrap. While in bootstrap it will wait
for a message about the new Index-Coordinator's `connectionAddr`
* if `connectionAddr` belongs to itself, it will assume the responsibility of
  Index-Coordinator.
* otherwise, it will assume the role of Index-Coordinator-Replica.

## Index-Coordinator replica

* when a node move to replica role, it will first fetch the local StateContext
  and initiate a handshake with Index-Coordinator.
* it will co-operate with Index-Coordinator to synchronize itself with latest
  StateContext.

## Index-Coordinator bootstrap

Once IndexManager assumes the responsibility of Index-Coordinator, ns-server
shall provide the list of Index-Coordinator-Replicas and subsequently
Index-Coordinator shall wait for a handshake with all the replica.

**bootstrap handshake with replica**, during this handshake the
Index-Coordinator-Replicas will provide the CAS number of its local StateContext
to Index-Coorindator, once Index-Coordinator receives handshake for each of
the listed replicas, it will determine the latest version of StateContext. If
its local StateContext is not the latest one, it will fetch the latest
StateContext from one of its replica and publish that to all of its other
replicas.

**bootstrap handshake with local-indexer-node**, during this handshake
Index-Coordinator will get topology details from each of its
local-indexer-node and verify that with its StateContext. If there are any
changes it will update the topology and post new topology projectors, routers
and respective indexer nodes.

**currentTimestamp**, during the handshake with indexer-nodes, for each
IndexTopology, Index-Coordinator will get latest snapshot-timestamp from each
one of them as hw-timestamp and update its currentTimestamp by picking the
lowest of all sequence number.

Index-Coordinator finally moves to `active` state. After moving to
`active` state, whenever a new local indexer node registers with
Index-Coordinator it will go through a handshake for topology verification.

## Index-Coordinator active state

Any update made to its StateContext will increment the CAS field and replicated
to Index-Coordinator replicas. If a modification is relevant for projectors
and/or indexer-nodes post it to respective components.

* accept DDL changes from administrator and updates IndexDefinition list.
* accept local-indexer-node registration and gather topology information for
  them. Another alternative is to get the list of local-indexer-nodes from
  ns-server.
* accept projector registration.

Index-Coordinator periodically receives SYNC message and update its
`currentTimestamp` vector and `currentVbuuid` for each topology. If SYNC
message for a vbucket does not arrive for a prescribed period of time, it will
enquire for rollback.

### stability timestamp

Index-Coordinator will maintain a currentTimestamp vector globally for all index
definition in its StateContext, which will be updated using SYNC message received
from projector/router. The sync message will contain the latest `sequence-number`
for the vbucket and its `vbuuid`.

* periodically `currentTimestamp` will be promoted to stability-timestamp
* communicate `stabilityTimestamp` to all local-indexer-node hosting the
  indexes for that bucket.
* local-indexer-nodes will queue up incoming stability-timestamp and when its
  mutation queue aligns with stabilityTimestamp it creates a snapshot.
* if Index-Coordinator fails in between, new Index-Coordinator can either get
  the last stability timestamp from indexer node or it can maintian a copy of
  it in StateContext. As an optimization Index-Coordinator can use hash
  value of stability timestamp to publish it to indexer node and for
  replication.

The mutations in a snapshot must be smaller or equal to the new stability
timestamp, hence it is also called as snapshot-timestamp. As an optimization,
Index-Coordinator can consolidate stabilityTimestamp for all IndexTopologies
and publish them as single message to local-indexer-node.

Another alternative is,

Index-Coordinator will periodically recieve `HWHeartbeat` message from every
local-indexer-node. Based on HWHeartbeat metrics and/or query requirements,
Index-Coordinator will promote the currentTimestamp into a stability-timestamp
and publish it to all index-nodes hosting a index for that bucket.

This alternative is only going to handle the case when the coordinator is
running on a slow node.

**algorithm to compute stability-timestamp based on hw-timestamp**

Algorithm takes following as inputs.

- per bucket HighWatermark-timestamp from each of the local-indexer-node.
- available free size in local-indexer's mutation-queue.

* For each vbucket, compute the mean seqNo
* Use the mean seqNo to create a stabilityTimestamp
* If heartbeat messages indicate that the faster indexer's mutation queue is
  growing rapidly, it is possible to use a seqNo that matches that fast indexer
  closer
* If the local indexer that has not sent heartbeat messages within a certain
  time, skip the local indexer, or consult the cluster mgr on the indexer
  availability.
* If the new stabilityTimestamp is the less than equal to the last one, do
  nothing.

```go
    type HWHeartbeat struct {
        bucket           string
        indexid          []uint64    // list of index hosted for `bucket`.
        hw               Timestamp
        lastPersistence  uint64      // hash of Timestamp
        lastStability    uint64      // hash of Timestamp
        mutationQueue    uint64
    }
```

A problem with this approach would be, if there is a slow indexer-node that has
not yet caught up with stability-timestamps, it is possible that the indexer
will no longer be able to service consistent query.


### client interfacing with Index-Coordinator

* a client must first get current Index-Coordinator from ns-server. If it
  cannot get one, it must retry or fail.
* once network address of Index-Coordinator is obtained from ns-server, client
  can post update request to Index-Coordinator.

## index rebalance

```go
    type Rebalance struct {
        state              string // "start", "pending", "catchup", "done"
        slice_no           int // Slicen number undergoing rebalance
        srcShard           Shard // from this shard
        dstPartition       Shard // to this shard
        // one of the stability-timestamp picked by Index-Coordinator
        rebalanceTimestamp Timestamp
    }
```

Index-Coordinator to calculate re-balance strategy,
* by figuring out the slices (identified by slice-nos) to move from one
  shard to another.
* by identifying local-indexer-nodes hosting source and destination shards
  and use one of the stability-timestamp as rebalance-timestamp.
* construct a rebalance structure for each migrating slices and add them to
  index-topology structure and index-topology is published to local-indexer-nodes
  participating in rebalance.

Everytime IndexTopology is updated, it is broadcasted to all components.

Process of rebalance,
* Index-Coordinator instructs the index nodes to move the slices.
  * mark rebalance as "pending" state.
* local-indexer-node will scan the index data from the source node based on the
  rebalance timestamp and stream them across to destination node.
* once a slice have been streamed to their corresponding destination,
  destination node will intimate Index-Coordinator and request projector to
  open a catch-up connection. The catch-up connection is for bringing the pending
  slice up-to-date with the latest stability timestamp.
  * Index-Coordinator will mark this rebalance as "catchup" state.
* once destination node is caught-up with incremental index-stream it will
  activate the slice and post a request to Index-Coordinator.
* Index-Coordinator will update topology map and de-activate the slice in
  the source node.
  * mark rebalance as "done" state.
  * removing the migrating slice's rebalance structure and
  * updating the slice's partition map and removing the slice's rebalance
    structure will be atomically done one after the other.
* once a slice is de-activiate, it can be removed from the local indexer.

Meanwhile, to route a stream, router will also check IndexTopology's rebalance
list for migrating slice's destination Shard and route the messages to the
new-destination as well.

### rebalance algorithm

TBD

### UPR connection coordination

Index-Coordinator is responsible for starting the "maintanence stream" and
"backfill stream". Subsequently it is responsible for restarting them when,

* connection between projector and KV fails, consequently downstream connections
  will be terminated.
* kv-rebalance, during kv-rebalance one or more vbucket stream will switch to
  another node that will lead to stream-end on the projector's UPR connection.
* kv-rollback, during which kv has crashed and failover-log indicates that
  indexer-nodes need to rollback.

SYNC messages and connection termination will be used to infer that upstream
UPR connection has dropped or projector has crashed. STREAM_BEGIN and STREAM_END
will be used to infer a kv-rebalance.

### UPR connection during bootstrap

Index-Coordinator will handshake with projectors for failover-log.
Index-Coordinator should compute the restart-timestamp and use them to start
the stream.

* before computing the restart timestamp, the indexer manager pause processing
  any request that require membership change (e.g. startup/shutdown index nodes).
* Index-Coordinator first need to generate a participant list of the active
  local indexer (aka index node).
  * the participant list excludes local indexer that is in bootstrap or offline.
* given the participant list, the Index-Coordinator will poll each local indexer
  to ask for its high watermark (HW) timestamp.
* Index-Coordinator will resume processing any membership change request.

* Index-Coordinator will determine if the vbucket UUID from the failover log
  matches the UUID of every HW timestamp.
  * if the UUID matches, then it means there is no unclean vbucket takeover
    (failover).  The index manager will then compute the restart timestamp by
    finding the smallest seqNo for each vbucket based on all the HW timestamps.

The index manager will return the restart timestamp to projector.

## kv-rebalance

KV rebalance happens when a vbucket is migrating from one node to another.
On the projector side, one or more vbucket stream will end with a STREAM_END
message.

When a new stream starts on the projector, it will broadcast a STREAM_BEGIN
message to all indexer nodes hosting an index for that bucket, it will also
broadcast it to Index-Coordinator.

When an active stream gracefully ends with STREAM_END, projector will
broadcast it to all indexer nodes hosting an index for that bucket, it will also
broadcast it to Index-Coordinator.

Index-Coordinator should expect a matching STREAM_END on the same connection
until the connection is closed. It will honor a STREAM_BEGIN for a vbucket only
after a STREAM_END is received. In case projector crashes before sending
STREAM_END, restart-timestamp will be computed and stream request will be
posted to projector.

## kv-rollback

As part of handshake with projector when starting a stream, Index-Coordinator
will get the failoverlog and check for vbucket branch histories. If it detects
a branch history for a vbucket it will move the IndexTopology for all indexes
defined on that bucket into `rollback` mode and replicates them to
Index-Coordinator-Replicas.

Changes to IndexTopology will also be published to projectors which will stop
streaming UPR mutations for all the buckets. The reason we freeze entire
secondary-index system is because all buckets are hosted by all kv-nodes.

Rollback context,
```go
    type Rollback struct {
        // Will be set by Index-Coordinator during kv-rollback.
        //   "started",  means an index is entering into rollback.
        //   "prepare",  means failover-timesamp is computed and servers hosting
        //               the index will be communicated.
        //   "restart",  means restart-timestamp is computed and nodes can
        //               rollback.
        //   "rollback", means local-indexer-nodes are commanded to rollback.
        rollback          string
        failoverTimestamp Timestamp
        restartTimestamp  Timestamp
    }
```

Index-Coordinator notifies each indexer to enter into recovery mode by
passing the failover log received from the projector. Index-Coordinator would
establish a new UPR connection with all the projectors for index maintenance.
Each individual local indexer will start in recovery mode.  It will switch
over from recovery mode to normal mode when the catch-up traffic has a seqNo
that is equal or greater than the seqNo at the mutation queue.
At any point during rollback, if there is any local indexer being restarted,
the local indexer will enter into recovery mode.

## IndexManager APIs

### /cluster/heartbeat

To be called by ns-server. Node will respond back with SUCCESS irrespective of
role or state.

* if replica node does not respond back, it should be removed from active list.
* if master does not respond back, then ns-server can start a new
  master-election.

### /cluster/bootstrap

To be called by ns-server, ns-server is expected to make this request during
master election.

### /cluster/newmaster

**request:** <connectionAddr>

Once master election is completed ns-server will post the new master's
`connectionAddr` to the elected master and each of its new replica. After this,
one of the IndexManager will become Index-Coordinator and rest of them will
become Index-Coordinator's replicas



## Index-Coordinator APIs

### /coordinator/projector

When a new projector starts-up it will post its `connectionAddr` on this API.

**parameters**
- _"AddProjector"_, command name.
- _connectionAddr_, connection address to connect with the projector.

### /coordinator/local-indexer-node

When a new local-indexer-node starts-up it will post its `connectionAddr` on
this API.

**parameters**
- _"AddLocalIndexer"_, command name.
- _connectionAddr_, connection address to connect with local-indexer-node

### /coordinator/index

Create a new index specified by `IndexDefinition` field in request body.
Index-Coordinator will initialize a topology for the index, co-ordinate with
local-indexer-nodes hosting the new index.

**parameters**
- _"CreateIndex"_, command name.
- _IndexDefinition_, meta data about new index.

**response:**
- _Status_, status code for CREATE DDL
- _IndexDefinition_, returns back index meta-data along with `indexid` that can
  uniquely identify the new index.

### /coordinator/index

Drop all index listed by `[]uint64` field.

**parameters**
- _"DropIndex"_, command name.
- _[]indexid_, list of uint64 number that uniquely identifies an index.

**response**
- Status, status code for DROP DDL

### /coordinator/index

List index-info structures identified by `[]uint64` field. If it is empty,
list all active indexes.

**parameters:**
- _"ListIndex"_, command name.
- _[]indexid_, list of uint64 number that uniquely identifies an index.

**response:**
- _[]IndexDefinition_, list of index-meta data.

### /index-coordinator/hwtimestamp

Local-indexer-nodes can Periodically post its HW-timestamp to
Index-Coordinator as HWHeartbeat message.

### /index-coordinator/stabilityTimestamps

Index-Coordinator will return a last N timestamps that we promoted as
stability timestamps.



## ns-server API requirements:

### ns-server/indexCoordinator

Returns connection address for current master. If no master is currently elected
then return empty string.

### ns-server/indexCoordinatorReplicas

Returns list of connection address for all active replicas in the system.

### ns-server/indexerNodes

Returns list of connection address for all active local-indexer-nodes in the system.

### ns-server/join

For a node to join the cluster

### ns-server/leave

For a node to leave the cluster
