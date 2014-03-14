### failed, orphaned and outdated IndexManager

1. when an IndexManager crashes, it shall be restarted by ns-server, join the
   cluster, enter bootstrap state.
2. when a Index-Coordinator becomes unreachable to ns-server, it shall
   restart itself, by joining the cluster after a timeout period and enter
   bootstrap state.

### client interfacing with Index-Coordinator

1. a client must first get current Index-Coordinator from ns-server. If it
   cannot get one, it must retry or fail.
2. once network address of Index-Coordinator is obtained from ns-server, client
   can post update request to Index-Coordinator.
3. Index-Coordinator should create a new transaction in ns-server and then get
   the current list of Index-Coordinator-Replicas from ns-server.
   * if ns-server is unreachable or doesn't respond, return error to client.
5. Index-Coordinator goes through a 2-phase commit with its replicas and return
   success or failure to client.

### two-phase commit for co-ordination failures

**false positive**, is a scenario when client thinks that an update request
succeeded but the system is not yet updated, due to failures.

**false-negative**, is a scenario when client thinks that an update request has
failed but the system has applied the update into StateContext.

Meanwhile, when system is going through a rebalance or a kv-rollback or
executing a new DDL statement, Index-Coordinator can crash. But this situation
should not put the system in in-consistent state. To achieve this, we propose a
solution with the help of ns-server.

* Index-Coordinator and its replica shall maintain a persisted copy of its
  local StateContext.
* ns-server will act as commit-point-site.
* for every update, Index-Coordinator will increment the CAS and create a
  transaction tuple {CAS, status} with ns-server.
* status will be "initial" when it is created and later move to either "commit"
  or "rollback".
* for every update accepted by Index-Coordinator it shall create a new copy of
  StateContext, persist the new copy locally as log and push the copy to its
  replicas.
  * if one of the replica fails, Index-Coordinator will post "rollback" status
    to ns-server for this transaction and issue a rollback to each of the
    replica.
  * upon rollback, Index-Coordinator and its replica will delete the local
    StateContext log and return failure.
* when all the replicas accept the new copy of StateContext and persist it
  locally as a log, Index-Coordinator will post "commit" status to
  ns-server for this transaction and issue commit to each of the replica.
* during commit, local log will be switched as the new StateContext, and the
  log file will be deleted.

whenever a master or replica fails it will always go through a bootstrap phase
during which it shall detect a log of StateContext, consult with ns-server
for the corresponding transaction's status. If status is "commit" it will switch
the log as new StateContext, otherwise it shall delete the log and retain the
current StateContext.

* once ns-server creates a new transaction entry, it should not accept a new
  IndexManager into the cluster until the transaction moves to "commit" or
  "rollback" status.
* during a master election ns-server shall pick a master only from the set of
  IndexManagers that took part in the last-transaction.
* ns-server should maintain a rolling log of transaction status.
## IndexManager design

Each instance of IndexManager will be modeled as a state machine backed by a
data structure, called StateContext, that contains meta-data about the secondary
index, index topology, pub-sub data and meta-data for normal operation of
IndexManager cluster.

```

                           *--------------*
                           | IndexManager |
                           *--------------*
                                 |
                                 | (program start, new-master-elected)
                                 V
        elected             *-----------*
        master *------------| bootstrap |-------------* (not a master)
               |            *-----------*             |
               V                                      V
        *---------------*                        *-----------*
        | IMCoordinator |                        | IMReplica |
        *---------------*                        *-----------*
```

**TODO: Replace it with a pictorial diagram**

**main data structure that backs Index-Coordinator**

```go
    type Timestamp []uint64 // timestamp vector for vbuckets

    type Node struct {
        name           string
        connectionAddr string
        role           []string // list of roles performed by the node.
    }

    type IndexInfo struct {
        Name       string    // Name of the index
        Uuid       uint64    // unique id for every index
        Using      IndexType // indexing algorithm
        OnExprList []string  // expression list
        Bucket     string    // bucket name
        IsPrimary  bool
        Exprtype   ExprType
        Active     bool     // whether index topology is created and index is ready
    }

    type IndexTopology struct {
        indexinfo     IndexInfo
        numPartitions int
        servers       []Nodes  // servers hosting this index
        // server can be master or replica, where, len(partitionMap) == numPartitions
        // first integer-value in each map will index to master-server in
        // `servers` field, and remaining elements will point to replica-servers
        partitionMap  [][MAX_REPLICAS+1]int
        sliceMap      [MAX_SLICES][MAX_REPLICAS+1]int
    }

    type StateContext struct {
        // value gets incremented after every updates.
        cas           uint64

        // maximum number of persistence timestamp to maintain.
        ptsMaxHistory    int

        // per bucket timestamp continuously updated by sync message, and
        // promoted to persistence timestamp.
        currentTimestamp map[string]Timestamp

        // promoted list of persistence timestamp for each bucket.
        persistenceTimestamps map[string][]PersistenceTimestamp

        // Index info for every created index.
        indexeInfos      map[uint64]IndexInfo

        // per index map of index topology
        indexesTopology  map[string]IndexTopology

        // per index map of ongoing rebalance.
        rebalances       map[string]Rebalance

        // list of projectors
        projectors       []Node

        // list of routers
        routers          [] Node
    }
```

### bootstrap state for IndexManager

* all IndexManagers, when they start afresh, will be in bootstrap State.
* from bootstrap State, IndexManager can either become Index-Coordinator or
  Index-Replica.
* while a node is in bootstrap State it will wait for Index-Coordinator's
  connectionAddress from ns-server or poll from ns-server.
* if connectionAddress is same as the IndexManager instance, it will become the
  new Index-Coordinator.
* otherwise IndexManager will become a Replica to Index-Coordinator.

* **recovery from failure**, before getting the new Index-Coordinator's
connectionAddress IndexManager will check of un-commit transaction by checking
for StateContext's log.
  * If found, it will consult ns-server for transaction's status.
    * if status is "commit", it will switch the log as new StateContext.
    * else discard the log.

* IndexManager before leaving bootstrap state will restore the StateContext
  from its local persistence.

#### Index-Coordinator initialization

### normal state for Index-Coordinator

* restored StateContext will be used as the master copy. Optionally
  Index-Coordinator can consult its replica for latest StateContext based on
  CAS value.
* StateContext can be modified only by the master node. It is expected that
  other components within the system should somehow learn the current master
  (maybe from ns-server) and use master's API to modify StateContext.
* upon receiving a new update to StateContext will use 2-phase commit to
  replicate the update to its replica.

#### Index-Coordinator publishing persistence timestamp

Index-Coordinator will maintain a currentTimestamp vector for each bucket in
its StateContext, which will be updated using SYNC message received from
projector (projector will periodically send SYNC message for every vbucket in
a bucket). The sync message will contain the latest sequence-number for the
vbucket.

Index-Coordinator will periodically recieve <HWHeartbeat> message from every
local-indexer-node. Based on HWHeartbeat metrics and/or query requirements,
Index-Coordinator will promote the currentTimestamp into a
persistence-timestamp and publish it to all index-nodes hosting a index for
that bucket.

If,
  * Index-Coordinator crashes while publishing the persistence timestamp,
  * local-indexer-node did not receive the persistence-timestamp,
  * local-indexer node received the timestamp but could not create a
    snapshot due to a crash,
  * compaction kicks in,
then there is no gaurantee that local-indexer-nodes hosting an index will have
identical snapshots.

Whenever local-indexer node goes through a restart, it can fetch a log of
persistence-timestamp uptil the latest one from Index-Coordinator.

##### algorithm to compute persistence timestamp

Algorithm takes following as inputs.

- per bucket HighWatermark-timestamp from each of the local-indexer-node.
- available free size in local-indexer's mutation-queue.

1. For each vbucket, compute the mean seqNo
2. Use the mean seqNo to create a persistence timestamp
3. If heartbeat messages indicate that the faster indexer's mutation queue is
   growing rapidly, it is possible to use a seqNo that matches that fast indexer
   closer
4. If the local indexer that has not sent heartbeat messages within a certain
   time, skip the local indexer, or consult the cluster mgr on the indexer
   availability.
5. If the new persistent timestamp is the less than equal to the last one, do
   nothing.

**relevant data structur**

```go
    type PersistenceTimestamp struct {
        ts        Timestamp
        stability bool      // whether this timestamp is also treated as stability timestamp
    }
```

##### /index-coordinator/hwtimestamp

supported by Index-Coordinator

request:

    { "command": HWheartbeat,
      "payload": <HWHeartbeat>,
    }

response:

    { "cas": <uint64>,
      "status": ...
    }

##### /index-coordinator/logPersistenceTimestamps

supported by Index-Coordinator

request:

    { "command": PersistenceTimestampLog,
      "lastPersistenceTimestamp": Timestamp,
    }

response:

    { "cas":                   <uint64>,
      "persistenceTimestamps": []PersistenceTimestamp
      "status":                ...
    }

##### /local-indexer-node/persistenceTimestamp

supported by local-indexer-node

request:

    { "cas":     <uint64>,
      "command": NewPersistenceTimestamp,
      "payload": <HWHeartbeat>,
    }

response:

    { "status": ...
    }

### Index-Coordinator replica

* when a node move to replica State, it will first fetch the latest
  StateContext from the current master and persist as the local StateContext.
* if replica's StateContext is newer than the master's StateContext
  (detectable by comparing the CAS value), then latest mutations on the
  replica will be lost.
* replica can receive updates to StateContext only from master, if it receives
  from other components in the system, it will respond with error.
* upon receiving a new update to StateContext from master, replica will
  update its StateContext and persist the StateContext on durable media.
* in case if replica is unable to communicate with the master and comes back
  alive while rest of the system have moved ahead, it shall go through
  bootstrap state,
    * due to hearbeat failures
    * by detecting the CAS value in subsequent updates from master

### rollback

  * only master node can move to rollback mode.

  TBD

## Data structure

**cluster data structure**

```go
    type Cluster struct {
      masterAddr string   // connection address to master
      replicas   []string // list of connection address to replica nodes
      nodes      []string // list of connection address to indexer-nodes
    }
```

data structure is transient and maintains the current state of the
secondary-index cluster

**StateContext**

    type Rebalance struct {
        topology     IndexTopology
    }

    type HWHeartbeat struct {
        indexid          uint64
        bucket           string
        hw               Timestamp
        lastPersistence  uint64      // hash of Timestamp
        lastStability    uint64      // hash of Timestamp
        mutationQueue    uint64
    }
```

### IndexManager APIs

#### /cluster/heartbeat
request:

    { "current_term": <uint64> }

response:

    { "current_term": <uint64> }

To be called by ns-server. Node will respond back with SUCCESS irrespective of
role or state.

* if replica node does not respond back, it will be removed from active list.
* if master does not respond back, then ns-server can start a new
  master-election.

#### /cluster/bootstrap

To be called by ns-server, ns-server is expected to make this request during
master election.

* replica will cancel all outstanding updates and move to `bootstrap` state.
* master will cancel all outstanding updates and move to `bootstrap` state.

#### /cluster/newmaster
request:

    { "current_term": <uint64> }

response:

    { "current_term": <uint64>
      "status": ...
    }

Once master election is completed ns-server will post the new master and
election-term to the elected master and each of its new replica. After this,
IndexManager node shall enter into `master` or `replica` state.

### Index-Coordinator APIs

#### /cluster/index
request:

    { "current_term": <uint64>,
      "command": CreateIndex,
      "indexinfo": {},
    }

response:

    { "current_term": <uint64>
      "status": ...
      "indexinfo": {},
    }

Create a new index specified by `indexinfo` field in request body. `indexinfo`
property is same as defined by the `IndexInfo` structure above, except that
`id` field will be generated by the master IndexManager and the same
`indexinfo` structure will be sent back as response.

#### /cluster/index
request:

    { "current_term": <uint64>,
      "command": DropIndex,
      "indexid": <list-of-uint64>,
    }

response:

    { "current_term": <uint64>
      "status": ...
    }

Drop all index listed in `indexid` field.

#### /cluster/index
request:

    { "current_term": <uint64>,
      "command": ListIndex,
      "indexids": <list-of-uint64>,
    }

response:

    { "current_term": <uint64>,
      "status": ...
      "indexinfo": <list-of-indexinfo-structure>,
    }

List index meta-data structures identified by `indexids` in request body. If
it is empty, list all active indexes.

### ns-server API requirements:

#### GetIndexCoordinator()

Returns connection address for current master. If no master is currently elected
then return empty string.

#### GetIndexCoordinatorReplicas()

Returns list of connection address for all active replicas in the system.

#### GetIndexerNodes()

Returns list of connection address for all active local-indexer-nodes in the system.

#### Join()

For a node to join the cluster

#### Leave()

For a node to leave the cluster

## Appendix A: replication and fault-tolerance.

To avoid single-point-of-failure in the system we need to have multiple
nodes that are continuously synchronized with each other. So that in case of a
a master failure, one of the replica can be promoted to master. In this
section we explore the challenges involved in synchronous replication of data
from master to its replica nodes.

### full-replication

Full replication means, for every update master will copy the entire data
structure to its replica.

false-positive is possible when master crashes immediately and a new node,
that did not participate in the previous update, becomes a new master. This
can be avoided if master locks ns-server to prevent any new nodes from joining
the cluster until the update is completed.

false-negative is possible when master crashes before updating all replicas
and one of the updated replica is elected as new master.

Q:
1) When do the index manager needs to subscribe/unsubcribe directly?
   Should the router handles the change the subscriber based on the topology?
