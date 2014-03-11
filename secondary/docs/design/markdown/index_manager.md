## IndexManager

IndexManager is the component that co-ordinate other components - like
projector, query, indexer and local-indexer nodes during bootstrap, rollback,
reconnection, local-indexer-restart etc.

Since rest of the system depends on IndexManager for both normal operation and
for failure recovery, restart etc. it can end up becoming single-point-of
failure within the system. To avoid this, multiple instance of IndexManager
will be running on different nodes, where one of them will be elected as
master and other will act as replica to the current master.

**Master election**, we expect nsServer to elect a new master during system
bootstrap, and when ever there is a master crash. More specifically, we expect
nsServer to,

  * prepare an active list of IndexManager
  * pick one of the instance as master
  * post new master's connectionAddress to all active instance of IndexManagers
  * monitor the active list of IndexManager
  * handle Join/Leave request from IndexManager either to join or leave from the
    active list

Each instance of IndexManager will be modeled as a state machine backed by a
data structure, called StateContext, that contains meta-data about the secondary
index system and meta-data for normal operation of IndexManager cluster. An
instance of IndexManager can be in one of the following state.

  * bootstrap
  * master
  * replica
  * rollback

[TODO: Since the system is still evolving we expect changes to design of
 IndexManager]

### StateContext

  * contains a CAS field that will be incremented for every mutation (insert,
    delete, update) that happen to the StateContext.
  * several API will be exposed by IndexManager to CREATE, READ, UPDATE and
    DELETE StateContext or portion of StateContext.
  * false-positive cases are possible in the present definition. It can
    happen when a client post an update to the StateContext which get
    persisted only in master and not with any other replica, and subsequently
    the master fails, there by leading to a situation where the client assumes
    an update that is not present in the system.
  * false-negative cases are possible in the present definition. It can happen
    when a client post an update to the StateContext which get persisted on
    master/replica, but before replying success to client - master node fails,
    there by leading to a situation where the client will re-post the same
    update to StateContext.

From now on we will refer to each instance of IndexManager as a node.

### bootstrap
  * all nodes, when they start afresh, will be in bootstrap State.
  * from bootstrap State, a node can either move to master State or replica
    State.
  * sometimes moving to master State can be transient, that is, if system was
    previously in rollback state and rollback context was replicated to other
    nodes, then the new master shall immediately switch to rollback State and
    continue co-ordinating system rollback activity.
  * while a node is in bootstrap State it will wait for new master's
    connectionAddress to be posted via its API.
  * if new master is itself, detected by comparing connectionAddress,
    it shall move to master State.
  * otherwise the node will move to replica State.

### master
  * when a node move to master State, its local StateContext is restored from
    persistent storage and used as system-wide StateContext.

  * StateContext can be modified only by the master node. It is expected that
    other components within the system should somehow learn the current master
    (maybe from nsServer) and use master's API to modify StateContext.
  * upon receiving a new update to StateContext
    * master will fetch the active list of replica from nsServer
    * master will update its local StateContext and persist the StateContext
      on durable media, then it will post an update to each of its replica.
    * if one of the replica does not respond or respond back with failure,
      master shall notify nsServer regarding the same
    * master responds back to the original client that initiated the
      update-request.
  * in case a master crashes, it shall start again from bootstrap State.

  **[false-positives and false-negatives are possible in above scenario]**

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
