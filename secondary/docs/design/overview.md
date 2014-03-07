## Secondary Index Design Document


###Overview

Secondary index provide the ability to query couchbase key-value data store.
In couchbase the primary data store is a key-value storage mainly backed by
main-memory and distributed across dozens of nodes. If you are new couchbase
or couchbase's secondary indexing solution, you may first want learn more about
the [terminologies](markdown/terminology.md) that we discuss here.

This document describes the High Level Design for Secondary Indexes. It also
describes the deployment options supported.

###Components


- __Projector__

The projector is responsible for mapping mutations to a set of key version.
The projector can reside within the master KV node in which the mutation is
generated or it can reside in separate node. The projector receives mutations
from ep-engine through UPR protocol. The projector sends the evaluated results
to router. [Details.](markdown/projector.md)

- __Router__

The router is responsible for sending key version to the index nodes. It relies
on the index distribution/partitioning topology to determine the indexer which
should receive the key version. The router resides in the same node as the
projector. [Details.](markdown/router.md)

- __Index Manager__

The index manager is responsible for receiving requests for indexing operations
(creation, deletion, maintenance, scan/lookup). The Index Manager is located in
the index node, which can be different from KV node.
[Details.](markdown/index_manager.md)

- __Indexer__

The indexer processes the key versions received from router and provides
persistence support for the index. Indexer also provides the interface for
query client to run index Scans and does scatter/gather for queries. The indexer
would reside in index node.   [Details.](markdown/indexer.md)

- __Query Catalog__

This component provides catalog implementation for the Query Server. This
component resides in the same node Query Server is running and allows Query
Server to perform Index DDL (Create, Drop) and Index Scan/Stats operations.

###System Diagram

- [KV-Index System Diagram](markdown/system.md)
- Query-Index System Diagram

###Execution Flow

* [Mutation Execution Flow](markdown/mutation.md)
* [Query Execution Flow](markdown/query.md)

###Bootstrap Sequence

* System Bootstrap
* Indexer Restart Bootstrap
* Projector Restart Bootstrap
* Router Restart Bootstrap
* Index Manager Restart Bootstrap

###Deployment

- [Deployment Options](markdown/deployment.md)

###Partition Management
* Milestone-1 will have Key-based partitioning support.
  * [John's Doc for Partitioning](https://docs.google.com/document/d/1eF3rJ63iv1awnfLkAQLmVmILBdgD4Vzc0IsCpTxmXgY/edit)

###Communication Protocols

* Projector and Ep-Engine Protocol
  * [UPR protocol](https://github.com/couchbaselabs/cbupr/blob/master/index.md) will be used to talk to Ep-engine in KV.

* Router and Indexer Protocol
* Query and Indexer Protocol
  * [Existing REST Based Protocol](https://docs.google.com/document/d/1j9D4ryOi1d5CNY5EkoRuU_fc5Q3i_QwIs3zU9uObbJY/edit)

###Storage Management

* Persistent Snapshot

###Cluster Management

* Master Election
* Communication with ns_server

### Meta-data Management

* Metadata Replication
* Metadata Recovery

###Recovery

* [Recovery Document](https://docs.google.com/document/d/1rNJSVs80TtvY0gpoebsBwzhqWRBJnieSuLTnxuDzUTQ/edit)

###Replication

* Replication Strategy
* Failure Recovery

###Rebalance

* Rebalance Strategy
* Failure Recovery

