## IndexManager

IndexManager is the component that co-ordinate other components - like
projector, query, indexer and local-indexer nodes during bootstrap, rollback,
reconnection, local-indexer-restart etc.

Since rest of the system depends on IndexManager for both normal operation and
for failure recovery, restart etc. it can end up becoming single-point-of
failure within the system. To avoid this, multiple instance of IndexManager
will be running on different nodes, where one of them will be elected as
master and others will act as replica to the current master.

From now on we will refer to each instance of IndexManager as a node.

### scope of IndexManager

1. create or drop index DDLs.
2. defining initial index topology, no. of indexer-nodes, partitions and
   slices for an index.
3. add, delete topics in pub-sub. subscribe, un-subscribe nodes from topics,
   optionally based on predicates.
4. provide network interface for other components to access index-metadata,
   index topology, publish-subscribe framework.
5. generate restart timestamp for upr-reconnection.
6. create rollback context and update rollback context based on how rollback
   evolves withing the system.
7. generate stability timestamps for purpose of query and rollback. co-ordinate
   with every indexer-node to generate stability snapshots.

### scope of ns-server

1. master-election is done by ns-server
2. master-election is done by ns-server during system start and whenever current
   master fail to respond for `hearbeat` request.
3. ns-server will be the central component that maintain the current master and
   list of active replica and list of indexer-nodes.
4. actively poll - master node, replica nodes and other local-indexer-nodes for
   its liveliness using `heartbeat` request.
5. provide API for index-manager instances and index-nodes to join or leave
   the cluster.

**Clarification needed**

1. When a client is interfacing with a master, will there be a situation for
   ns-server to conduct a new master-election ?

### client interfacing with IndexManager and typical update cycle

1. a client must first get current master from ns-server. If it cannot get
   one, it must retry or fail.
2. once network address of master is obtained from ns-server client can post
   update request to master.
3. master should get the current list of replica from ns-server.
   * if ns-server is unreachable or doesn't respond, return error to client.
4. master updates its local StateContext.
5. synchronously replicate its local StateContext on all replicas. If one of
   the replica is not responding, skip the replica.
6. notify ns-server about not responding replicas. If ns-server is unreachable
   ignore.
7. if master responds with anything other than SUCCESS to the client, retry from
   step 1.

### master election

We expect ns-server to elect a new master,

* during system bootstrap.
* during a master crash (when master fails to respond for heartbeat request).
* when a master voluntarily leaves the cluster.

election process,

1. ns-server will maintain a election-term number which shall be incremented
   before every master-election.
2. if ns-server cannot reach master, then it can retire master as failed node,
   increment election-term, and start master election process
3. ns-server polls for active replica.
3. pick one of the replica as master node.
4. post `bootstrap` request to each replica. If a replica is already in
   `bootstrap` state, it will do nothing.
   * an active replica shall cancel outstanding updates happening to its
     replicated data structure, move to bootstrap state, and wait for new
     master notification.
6. post new master's connectionAddress to all active instance of IndexManagers
   along with it the current election-term number.
7. current election-term number will be preserved by all instances of
   IndexManager and used in all request/response across the cluster.
8. in case of unexpected behavior like,
   * an active replica not responding
   * `bootstrap` request is not responding
   * `newmaster` request not responding

   ns-server will restart from step-2.
9. once an election is complete, with master and replicas identified,
   ns-server act on join and leave request from IndexManager nodes and
   indexer-nodes.

### failed, orphaned and outdated IndexManager

1. when an instance of IndexManager fails, it shall be restarted by ns-server,
   join the cluster, enter bootstrap state.
2. when a master IndexManager becomes unreachable to ns-server, it shall
   restart itself, by joining the cluster after a timeout period and enter
   bootstrap state.
3. when ever IndexManager instance receives a request or response who's
   current-election-term is higher than the local value, it will restart
   itself, join the cluster and enter bootstrap state.

### false-positive

false positive is a scenario when client thinks that an update request
succeeded but the system is not yet updated. This can happen when
master node is reachable to client but not reachable to its replicas and
ns-server, leading to a master-election, and ns-server elects a new-master that
has not received the updates from old-master.

### false-negative

false-negative is a scenario when client thinks that an update request has
failed but the system has applied the update into StateContext. This can happen
when client post an update to the StateContext which get persisted on
master/replica, but before replying success to client - master node fails,
there by leading to a situation where the client will re-post the same
update to new master.

## IndexManager design

Each instance of IndexManager will be modeled as a state machine backed by a
data structure, called StateContext, that contains meta-data about the secondary
index, index topology, pub-sub data and meta-data for normal operation of
IndexManager cluster. An instance of IndexManager can be a  `master` or a
`replica` operating in `bootstrap`, `normal` or `rollback` state

              | startup  |  master  | replica
    ----------|----------|----------|---------
    boostrap  |   yes    |          |
    normal    |          |   yes    |  yes
    rollback  |          |   yes    |

**TODO: Since the system is still evolving we expect changes to design of
IndexManager**
**TODO: Convert above table to state diagram**

### StateContext

* contains a CAS field that will be incremented for every mutation (insert,
  delete, update) that happen to the StateContext.
* several API will be exposed by IndexManager to CREATE, READ, UPDATE and
  DELETE StateContext or portion of StateContext.

### bootstrap

* all nodes, when they start afresh, will be in bootstrap State.
* from bootstrap State, a node can either move to master State or replica State.
* sometimes moving to master State can be transient, that is, if system was
  previously in rollback state and rollback context was replicated to other
  nodes, then the new master shall immediately switch to rollback State and
  continue co-ordinating system rollback activity.
* while a node is in bootstrap State it will wait for new master's
  connectionAddress to be posted via its API.
* if connectionAddress same as the IndexManager instance, it will move to
  master State.
* otherwise the node will move to replica State.

### master

* when a node move to master State, its local StateContext is restored from
  persistent storage and used as system-wide StateContext.

* StateContext can be modified only by the master node. It is expected that
  other components within the system should somehow learn the current master
  (maybe from ns-server) and use master's API to modify StateContext.
* upon receiving a new update to StateContext
  * master will fetch the active list of replica from ns-server
  * master will update its local StateContext and persist the StateContext
    on durable media, then it will post an update to each of its replica.
  * if one of the replica does not respond or respond back with failure,
    master shall notify ns-server regarding the same
  * master responds back to the original client that initiated the
    update-request.
* in case a master crashes, it shall start again from bootstrap State.

### replica

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

data structure is transient maintains the current state of the IndexManager
cluster

**StateContext**

```go
    type StateContext struct {
    }
```

### IndexManager APIs

API are defined and exposed by each and every IndexManager and explained with
HTTP URL and JSON arguments

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

    { "current_term": <uint64> }

Once the master election is completed ns-server will post the new master and
election-term to the elected master and each of its new replica. After this,
IndexManager node shall enter into `master` or `replica` state.

### ns-server API requirements:

#### GetClusterMaster()

Returns connection address for current master. If no master is currently elected
then return empty string.

#### Replicas()

Returns list of connection address for all active replicas in the system.

#### IndexerNodes()

Returns list of connection address for all active local-indexer-nodes in the system.
