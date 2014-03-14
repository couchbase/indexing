## Projector

* projector is a stateless component in the system. That is, it is not backed by
  any persistent storage.
* it fetches all relevant information form Index-Coordinator.
* connects with KV cluster for UPR mutations.
* evaluates each documents using index expression, if document belongs to the
  bucket.
* creates projected key-versions and sends them to router component.
* when a new index is created or old index deleted, Index-Coordinator will post
  the information to all projectors.

## projector bootstrap

* projector will get list of all active index-info from Index-Coordinator.
* projector will spawn a thread, referred to per-bucket-thread, for each
  bucket that has an index defined.
* per-bucket-thread will,
  * get a subset of vbuckets for which it had to start UPR streams.
  * get a list of subscribers for each of these subsets who would like to
    receive the key-version.
  * open a UPR connection with one or more kv-master-nodes and start a stream
    for each vbucket in its subset.
  * apply expressions from each index defined for this bucket and generate
    corresponding key-version.
  * publish the key-version to subscribers.

## changes to index DDLs

Changes in index DDLs can affect projectors,

* in case CREATE index DDL,
  * projectors may have to spawn or kill a per-bucket thread.
  * projectors may want the new index definition and its expression to generate
    key-versions for the new index
* in case of DROP index DDL,
  * projects may have kill a per-bucket thread.
  * projects may want to delete index definition and its expression from its
    local copy to stop generating key-versions for the delete index.
* sometimes to balance load, Index-Coordinator might change the subscriber
  list who receive index key-versions.

**relevant data structures**

```go
    type ProjectorState struct {
        listenAddr string      // address to send/receive administration messages
        kvAddr     string      // address to connect with KV cluster
        indexInfos []IndexInfo
    }

    type KeyVersion struct {
        docid      []byte // primary document id.
        vbucket    uint16 // vbucket in which document is located.
        vbuuid     uint64 // current required uint64 vbuuid   = 4;
        sequenceno uint64 // sequence number corresponding to this mutation
        indexid    uint64
        keys       []byte
    }
```
