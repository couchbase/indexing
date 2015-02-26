## projector design

Zero or more indexes can be defined for each buckets managed by KV-store.
Design of projector involves interfacing with KV cluster, router and
Index-Coordinator under normal operation. It is a critical path that must
ensure safety and performance.

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
        // or via command-line parameter or from ns-server
        kvAddr     []string

        // per bucket vbuckets
        vbSet      map[string][]Vbset            // indexed by bucket-name

        // list of index-information for each bucket
        indexdefs  map[string][]IndexDefinition  // indexed by bucket-name
    }

    // Mutations from projector to router and subsequently to indexer nodes
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
    }
```

## projector topology

Projector topology defines how projectors and its DCP connections are started
and grouped across many nodes for all the buckets defined in KV.

### co-location

Simplest topology is to co-locate projector and router as single process along
with KV node. A single kv-node can host more than one bucket and for each bucket
it will be hosting a subset of vbucket.

After projector initialization, it will wait for admin messages to start / restart
DCP connection.

### computing key-versions

Projector will pick every document from its input queue and apply expressions
from each index active on the stream. Say if there are 3 indexes active on
the stream, there will be three expressions to be applied on each document
coming from DCP mutation queue.

Types of dcp mutations,
* DCP_MUTATION, either a document is inserted or updated.
  * for every mutated document, projector will compute 3 key-versions, say if
    there are three indexes defined for the bucket.
  * if previous version of document is available from DCP message, projector
    will compute 3 key-versions, called old-keys, from the older document.
    * only key versions that are different from the old key versions will be
      sent across to router.
  * if previous version of document is not available from DCP message,
    it is not possible to detect secondary-key changes and KeyVersions will
    be computed and sent to router.
* DCP_DELETION, when an existing document is deleted.

## connection, streams and flow control

Projector will start a DCP connection with one or more KV nodes, depending on
whether it is co-located with KV or not, for stream request. Stream request
can be for

* `maintanence stream` for incremental build.
* `backfill stream` for initial build.
* `catchup stream` for local indexers.

but the type of stream is opaque to projector.

* *stream end*, when ever a stream ends on a connection, projector will
  try to restart the stream, using the last received `{vbuuid, seqNo}` until it
  receives NOT_MY_VBUCKET message.
* *cascading connection termination*, when ever a DCP connection is closed,
  corresponding downstream connection will be closed. After which, it is up to
  the indexer or Index-Coordinator to restart the connection.

Stream request to projector should look like,
> {topic, map[bucket]Timestamp, map[Indexid]IndexDefinition, map[Indexid]IndexTopology}

for every `IndexDefinition` index's DDL expressions will be applied on the stream.
Stream requests are made by Index-Coordinator and local indexers, and the stream
thereafter is associated with `topic`. For every stream request a router thread
will be launched, passing `IndexTopology` and an exclusive channel/pipe between
projector and router, for that stream. All projected mutations on the topic will
be publised on that stream.

Timestamp will be computed by indexer and Index-Coordinator during fresh
start, dcp-connection loss, kv-rebalance, kv-rollback and Index-Coordinator
restart

Timestamp is a vector of sequence number for each vbucket. Optionally a vector
of vbuuid corresponding to sequence number can also be attached to
restart-timestamp to detect roll-back conditions.

The projector will start a DCP connection and vbucket streams with the sequence
number less than or equal to restart-timestamp.

### stream bootstrapping

Projector and downstream component will handshake to start a DCP connection.
The *downstream component* here is indexer or Index-Coordinator.

* downstream component fetches the failover log from projector.
* downstream component computes a failover-timestamp using failover log and
  latest DCP sequence-no.
* downstream component will decide if it needs a rollback by comparing the
  UUID between latest stability-timestamp and failover-timestamp.
* projector will receive a start stream request with restart-timestamp.
* projector will use failover-log and computes failover-timestamp. After
  starting the stream, gathers DCP-timestamp and sends back failover-timestamp
  and DCP-timestamp to the caller.
  * if projector detects that a failover has happened in between, it will close
    the DCP connection and downstream connections.
* during stream start projector will always honor ROLLBACK response from DCP
  producer. It is up to indexer and Index-Coordinator to be aware of duplicate
  KeyVersions and ignore them.

when ever down stream components are in bootstrap or recovery, they can use
"catchup stream" to come up to speed with "maintanence stream".

If downstream component fails, it will repeat the whole process again.

### stream restart

Active streams may have to be restarted in the following cases,
* connection between projector and KV fails, consequently downstream connections
  will be terminated and Index-Coordinator will follow bootstrap sequence to
  compute restart-timestamp and make a stream request to previously
  disconnected projector.
* Index-Coordinator crash, follow the bootstrap sequence to compute
  restart-timestamp and post a stream request to projector.
* kv-rollback, Index-Coordinator will follow rollback sequence to compute
  restart-timestamp and post a stream request to projector.
* kv-rebalance, during kv-rebalance one or more vbucket stream will switch to
  another node that will lead to stream-end on the projector's DCP connection.
  It is upto indexer and Index-Coordinator to post a stream request for migrating
  vbucket to all projectors after identifying the restart-timestamp.

In all cases, when ever projector receives a stream request for a active stream
it will shutdown the stream and restart them according to new restart-timestamp.

### topic creation

Every stream request must be uniquely identified by a topic string. And for
every stream request a router thread will launched which is reponsible for
topic and subscription.

Following is a sample set of topic names that Indexers and Index-Coordinator
can use,

1. **/maintenance**, started by Index-Coordinator.
2. **/backfill/<id>**, started by Index-Coordinator. In future releases,
   secondary index system might allow more than one backfill streams at the same
   time. `id` is generated by Index-Coordinator to differentiate between backfill
   streams.
3. **/catchup/<indexerId>**, will be started by local indexer node for
   catchup-streams.

Typical flow of topic creation and subscription in projector,

1. stream is started by Index-Coordinator or Indexer, associating a topic to it.
2. projector will publish projected mutations to all its subscribers.

### projector operation

As part of normal operation projector will,
* start a thread/routine per kv-node called DCP thread. Each routine shall
  manage a DCP connection with kv-node.
* maintain separate queue for each vbucket, both on the input side and output
  side.
* apply DDL expressions on the document for each document from vbucket's input
  queue and generate a single `KeyVersions` event.
* push the event to vbucket's output queue.

Applying index DDL expressions on incoming mutations,
* DDL expressions are applied only for DCP_MUTATION type events.
* projector will use the list of supplied indexids to fetch the list of index
  DDLs applicable on a stream.

### flow control

Projector will use DCP's flow control mechanism by advertising its buffer size.

As an alternative if DCP's flow control is not available, DCP routine can end
the stream for the vbucket. There after it can restart the stream when buffer
gets emptied out.
