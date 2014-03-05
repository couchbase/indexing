##Secondary Index Design Document


###Overview

This document describes the High Level Design for Secondary Indexes. It also describes the deployement options supported.

###Components


- __Projector__

  The projector is responsible for mapping mutations to a set of key version. The projector can reside within the master KV node in which the mutation is generated or it can reside in separate node. The projector receives mutations from ep-engine through UPR protocol. The projector sends the evaluated results to router. [Details.](https://github.com/deepkaran/sandbox/blob/master/indexing/markdown/projector.md)

- __Router__

  The router is responsible for sending key version to the index nodes. It relies on the index distribution/partitioning topology to determine the indexer which should receive the key version. The router resides in the same node as the projector. [Details.](https://github.com/deepkaran/sandbox/blob/master/indexing/markdown/router.md)
  
- __Index Manager__

  The index manager is responsible for receiving requests for indexing operations (creation, deletion, maintenance, scan/lookup). The Index Manager is located in the index node, which can be different from KV node. [Details.](https://github.com/deepkaran/sandbox/blob/master/indexing/markdown/index_manager.md)
  
- __Indexer__

  The indexer provides persistence support for the index. The indexer would reside in index node.     [Details.](https://github.com/deepkaran/sandbox/blob/master/indexing/markdown/indexer.md)
  
- __Query Catalog__

  This component provides catalog implementation for the Query Server. This component resides in the same node Query Server is running and allows Query Server to perform Index DDL (Create, Drop) and Index Scan/Stats operations.


###System Diagram
![KV And Index Cluster](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/SystemDiagram.svg)

###Deployment Diagram
![](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/Deployment.svg)

###Bootstrap Sequence

* System Bootstrap
* Indexer Restart Bootstrap
* Projector Restart Bootstrap
* Router Restart Bootstrap
* Index Manager Restart Bootstrap

###Mutation Workflow

* Insert/Update Mutation Workflow
![](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/InsertWorkflow.svg)
* Delete Mutation Workflow
![](https://rawgithub.com/deepkaran/sandbox/master/indexing/images/DeleteWorkflow.svg)

###Query Workflow

* Create/Drop DDL Workflow
* Scan Request Workflow
* Stats Request Workflow
* Nodes (Meta) Request Workflow

###Partition Management
* Milestone1 will have Key-based partitioning support. 
  * [John's Doc for Partitioning](https://docs.google.com/document/d/1eF3rJ63iv1awnfLkAQLmVmILBdgD4Vzc0IsCpTxmXgY/edit)

###Communication Protocols

* Projector and Ep-Engine Protocol 

  Projector will use the UPR protocol to talk to Ep-engine in KV. 
  [UPR Design Specs](https://github.com/couchbaselabs/cbupr/blob/master/index.md) are here.
  
* Router and Indexer Protocol
* Query and Indexer Protocol
  * [Existing REST Based Protocol](https://docs.google.com/document/d/1j9D4ryOi1d5CNY5EkoRuU_fc5Q3i_QwIs3zU9uObbJY/edit)

###Storage Management
* Persistent Snapshot 

###Cluster Management
* Master Election
* Communication with ns_server

###Metadata Management
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

###Terminology

- High-Watermark Timestamp
- Stability Timestamp
- Restart Timestamp
- Mutation Queue
- Catchup Queue
- Stability Snapshot
- Persistent Snapshot
- Partition
- Slice
