## Projector

Application developers can use Data-Description-Language (DDLs) to define one
or more secondary index on primary Key-Value (KV) data-store. Typically DDLs
defined by applications will specify an expression that can be applied on each
JSON-document that is inserted or changed in the KV store. DDL-expression will
take the document as input, evaluate them, generate a json value which shall
be treated as **secondary-keys** for secondary indexes.

Following is an overview of the projector component,

* projector is a stateless component in the system, that is, it is not backed
  by any persistent storage.
* once a projector component starts up,
  * spawn an admin server that will wait for admin-messages.

* admin server
  * will wait for a new connection request
    `{bucketname, restart-timestamp, topicid}` to start UPR connections.
  * get index definitions that are applicable for projector connection.
  * get index topology based on which KeyVersions shall be routed to
    necessary local-indexer-nodes.

* evaluate each document using index's DDL expression and generate
  KeyVersions that will be pushed to the router component.

### SYNC message

For every vbucket, projector will use DELETE (corresponding to upr-deletion)
and LOCAL-DELETE (corresponding to upr-insertion to delete the old key) to
piggy-back SYNC message to router, which will then publish the message to all
known local-indexer-nodes from its index-topology list and also to
Index-Coordinator.

Note that the projector can periodically send out a SYNC message to router
to ensure that each local-indexer and Index-Coordinator to synchronize with
their high watermark timestamp.

We can also use UPR_SNAPSHOT_MARKER as SYNC message.
