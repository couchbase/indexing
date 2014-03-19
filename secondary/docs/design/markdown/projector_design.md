## projector design

Zero or more indexes can be defined for each buckets managed by KV-store.
Design of projector involves interfacing with KV cluster, router and
local-indexer-nodes under normal operation. It is a critical path that must
ensure - safety, connection and performance.

**relevant data structures**

```go
    type Vbset struct {
        vbno        uint16
        sequence_no uint64
        vbuuid      uint64
    }

    type ProjectorState struct {
        // system-wide unique address to receive administration messages,
        // available via config parameter or via command-line parameter.
        listenAddr string

        // address to connect with KV cluster, available via config parameter
        // or via command-line parameter.
        kvAddr     string

        // per bucket vbuckets
        vbSet      map[string][]Vbset            // indexed by bucket-name

        // list of index-information for each bucket
        indexdefs  map[string][]IndexDefinition  // indexed by bucket-name
    }

    // Mutations from projector to router to indexer node
    message KeyVersions {
        required byte   type     = 1; // type of mutation, INSERT, DELETE, DSYNC, SYNC, STREAM_BEGIN, STREAM_END.
        required string bucket   = 2; // bucket name.
        required int32  vbucket  = 4; // 16 bit vbucket in which document is located
        required uint64 vbuuid   = 5; // unique id to detect branch history
        optional bytes  docid    = 3; // primary document id.
        optional uint64 sequence = 6; // sequence number corresponding to this mutation
        repeated uint32 indexid  = 7; // indexids, whose partition is hosted by targeted indexer-node
        repeated bytes  keys     = 8; // key-vers for each of above indexid
        repeated bytes  oldkeys  = 9; // key-vers from old copy of the document.
        optional byte   topicid  = 10; // only present from projector to router.
    }
```

## projector topology

Projector topology defines how projectors, UPR connections are started and
grouped across many nodes for several buckets to monitor mutations.

### co-location

Simplest topology is to co-locate projector and router as single process along
with KV node. A single kv-node can host more than one bucket and for each bucket
it will be hosting a subset of vbucket.

After projector initialization, it will wait for admin messages to start / restart
UPR connection.

### computing key-versions

Projector will pick every document from its per-vbucket input queue
and apply expressions from each index active on the stream. Say if there
are 3 indexes active on the stream, there will be three expressions to be
applied on each document coming from UPR mutation queue.

Types of upr mutations,
* UPR_MUTATION, either a document is inserted or updated.
  * for every mutated document, projector will compute 3 key-versions because
    there are three indexes defined for the bucket.
  * if previous version of document is available from UPR message, projector
    will compute 3 key-versions, called old-keys, from the older document.
    * only key versions that are different from the old key versions will be
      sent across to router.
  * if previous version of document is not available from UPR message,
    it is not possible to detect secondary-key changes and KeyVersions will
    be computed and sent to router.
* UPR_DELETION, when an existing document is deleted.

## connection, streams and flow control

There are three types of streams that can originate in projector,
* one `maintanence stream` for incremental build.
* one `backfill stream` for initial build.
* many `catchup stream` for local indexers. There can be only one
  outstanding catchup stream for each local indexer.

Different connection endpoints will be used for `mutation stream`, `backfill
stream` and `catchup stream` on the Index-Coordinator and indexer side. The
endpoints will be differentiated by fixed port-numbers based on the
stream-type.

* *stream end*, when ever a stream ends on a connection projector will
  try to restart the stream, using the last received `{vbuuid, seqNo}` until it
  receives NOT_MY_VBUCKET message.
* *cascading connection termination*, when ever a UPR connection is closed,
  corresponding downstream connection with Index-Coordinator and indexers will
  be closed. After which, it is up to the indexer or Index-Coordinator to
  restart the connection. Note that connection with indexer will be closed
  only for "catchup stream".

UPR connection request to projector should be one of the following form,
1. {"maintenance", map[bucket]Timestamp}, always started by Index-Coordinator.
2. {"backfill", map[bucket]Timestamp, []Indexid}, always started by
   Index-Coordinator.
3. {"catchup", map[bucket]Timestamp, []Indexid, Indexerid}, always started by
   local indexer node identifying itself via Indexerid.

* start-timestamp computed by indexer and Index-Coordinator during fresh start
  of a stream.
* restart-timestamp computed by indexer and Index-Coordinator during
  upr-connection loss, kv-rebalance, kv-rollback and Index-Coordinator
  restart.

Timestamp is a vector of sequence number for each vbucket. Optionally a vector
of vbuuid corresponding to sequence number can also be attached to
restart-timestamp to detect roll-back conditions.

The projector will start a UPR connection and vbucket streams with the sequence
number less than or equal to restart-timestamp.

### stream bootstrapping

Projector and downstream component will handshake to start a UPR connection.
The *downstream component* here is indexer or Index-Coordinator.

* downstream component fetches the failover log from projector.
* downstream component computes a failover-timestamp using failover log and
  its hw-timestamp. In case of Index-Coordinator it will fetch the
  hw-timestamp from all the indexer for calculating failover-timestamp.
* downstream component will decide if it needs a rollback by comparing the
  UUID between latest stability-timestamp and failover-timestamp.
* projector will receive a start stream request with restart-timestamp.
* projector will use failover-log and computes failover-timestamp, after
  starting the stream, gathers UPR-timestamp and sends back failover-timestamp
  and UPR-timestamp to the caller.
  * if projector detects that a failover has happened in between it will close
    the UPR connection and downstream connections.
* during stream start projector will always honor ROLLBACK response from UPR
  producer. It is up to indexer and Index-Coordinator to be aware of duplicate
  KeyVersions and ignore them.

If downstream component fails, it will repeat the whole process again.

### stream restart

Active streams may have to be restarted in the following cases,
* connection between projector and KV fails, consequently downstream connections
  will be terminated and Index-Coordinator will follow bootstrap sequence to
  compute restart-timestamp and make a stream request to previously
  disconnected projector. Same is applicable for "backfill stream".
* Index-Coordinator crash, follow the bootstrap sequence to compute
  restart-timestamp and post a stream request to projector. Same is applicable
  for "backfill stream".
* kv-rollback, Index-Coordinator will follow rollback sequence to compute
  restart-timestamp and post a stream request to projector. Same is applicable
  for "backfill stream".
* kv-rebalance, during kv-rebalance one or more vbucket stream will switch to
  another node that will lead to stream-end on the projector's UPR connection.
  It is upto indexer and Index-Coordinator to post a stream request for migrating
  vbucket to all projectors after identifying the restart-timestamp.

In all cases, when ever projector receives a stream request for a active stream
it will shutdown the stream and restart them according to new restart-timestamp.

### topic creation

A topic is created at the projector side and router will dynamically create
subscribers for a topic based on IndexTopology. There is also a proposal to
enable API based topic creation and topic subscription to be used by external
components.

*Topicid* is created by projector and communicated to router via KeyVersions
messages. Router will route KeyVersions based on IndexTopology compute
subscriber endpoints for topicid and publish the KeyVersions (minus topicid
field) to all subscriber endpoints.

For every stream request projector will compute the `Topicid` based on stream
type. For catchup stream Topicid will be same as Indexerid, where Indexerid
identifies the indexer node requesting the stream.

    *-----------------*------------*
    |   stream type   |  Topic id  |
    *-----------------*------------*
    |  "maintainence" |    255     |
    *-----------------*------------*
    |    "backfill"   |    254     |
    *-----------------*------------*
    |    "catchup"    |   0-250    |
    *-----------------*------------*

### projector operation

Subsequently projector will,
* start a thread/routine per kv-node called UPR thread. Each routine shall
  manage a UPR connection with kv-node.
* maintain separate queue for each vbucket, both on the input side and output
  side.
* apply DDL expressions on the document for each document from vbucket's input
  queue and generate a single `KeyVersions` event.
* push the event to vbucket's output queue.

Applying index DDL expressions on incoming mutations,
* DDL expressions are applied only for UPR_MUTATION type events.
* for "maintenance stream", projector will apply expressions from all index
  definitions that are either in "backfill" or "maintenance" state and
  router will identify the endpoints based on IndexTopology.
* for "backfill stream", projector will only apply expressions from index
  definitions that are in "backfill" state and router will identify the
  endpoints based on IndexTopology.
* for "catchup stream", projector will only apply expressions from []Indexid
  that are specified in stream request and router will identify endpoints
  based on Topicid field.

### flow control

Projector will use UPR's flow control mechanism by advertising its buffer size.

As an alternative if UPR's flow control is not available, UPR routine can end
the stream for the vbucket. There after it can restart the stream when buffer
gets emptied out.
