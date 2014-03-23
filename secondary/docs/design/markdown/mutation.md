##Mutation Execution Flow

This document describes how KV mutations are received and processed by the Secondary Indexes.

####Insert/Update Mutation
![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/InsertWorkflow.svg)


Insert/Update Mutation Workflow can be divided into 3 phases:

* __Key Distribution Phase(Step 1-7)__

  During this phase, [Projector](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) receives mutations from [UPR](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) (Step 1-2), 
runs mapping functions and extracts secondary keys(Step 3). 
These are then sent to [Router](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) (Step 4) to be sent to individual [Indexers](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) running on different nodes(Step 5-7).

  An important thing to note here is that an INSERT/UPDATE mutation causes 2 messages(Step 6 and 7).
The DELETE msg is to ensure that the old secondary key entry gets deleted if the partition key of 
KV document has been modified(and it causes the location of its secondary key entry to be changed). <br>
INSERT/UPDATE message is sent to _ONLY_ the new location of the secondary key while 
DELETE message is sent to _ALL_ indexers where the index is located. 
This is required as at Router component level, the old key location cannot be determined.


* __Checkpoint Accumulation Phase(Step 8-11)__

  During this phase, all local Indexers accept messages from Router and store it in [Mutation Queue](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) (Step 8).
The [High-Watermark Timestamp](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) of local node is updated to reflect the current state of Mutation Queue (Step 9-11).


* __Stable Persistence Phase(Step 12-16)__

  [Index Manager](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/overview.md#components) (based on the SYNC messages received and some algorithm) decides to create [Stability Timestamp](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) (Step 12).
  This decision along with the timestamp is broadcasted to all Indexers(Step 13). This triggers the processing of Mutation Queue at Indexer. Message are applied/skipped as required. In this example, Indexer1 will skip the DELETE msg received in Step10 since it has applied the INSERT already.

  Messages from Mutation Queue are processed in order and applied as batch to the persistent storage(Step14).
Indexer will process the messages for each vbucket only till the Max Sequence Number in Stability Timestamp.

  Once all Indexers respond to Index Manager about successful Stability Timestamp creation(Step 15), Index Manager
will update its Stability Timestamp(Step 16).
This method ensures all Indexers have common stability points across which these can offer Index Scans.

  These common timestamps are necessary to prevent problems such as [Tearing Reads](https://github.com/couchbase/indexing/blob/master/secondary/docs/design/markdown/terminology.md) while doing a distributed range scan.


####Delete Mutation
![](https://rawgithub.com/couchbase/indexing/master/secondary/docs/design/images/DeleteWorkflow.svg)

Delete Mutations follow similar workflow as Insert/Update Mutation:

* Key Distribution Phase(Step 1-4) <br>
* Checkpoint Accumulation Phase(Step 5-6) <br>
* Stable Persistence Phase(Step 7-11) <br>

A notable point here is that _ALL_ Delete Mutations in KV will result in a broadcast message
to be sent to all Indexers. This is due to the fact that there is no
way to determine the secondary key's location based on KV's DocId
(which is the only information available in delete).

*For more details, see [John's Execution Flow Document](https://docs.google.com/document/d/11IojzquMYrOO0NNu7P52oQb6lai0ooXrugcEMEyXsUc/edit#).*
