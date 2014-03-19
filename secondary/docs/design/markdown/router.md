## Router

Router is responsible for identifying local-indexer endpoints that should
receive secondary keys from projector. On one side it talks to projector
to get KeyVersions for a vbucket, and on the other side it is connected with
local indexer nodes that will persist KeyVersions on durable storage.

Router is part of projector process, hence it uses projector's admin interface
to get IndexTopology.

* manages per vbucket input queue which is populated with projector's
  KeyVersions.
* uses hash-partition or key-partition. Partitioning algorithm is pluggable
  and partitioning algorithms like range partition and adaptive partition will
  be added at later point.
* partition algorithm maps a secondary key to slice-no.
* `Topicid` and `IndexTopology` is used to identify local-indexer-node hosting the
  partition containing that slice.
* manages a streaming connection with local indexer nodes to push Mutation
  events to them.
* for every local indexer node, router will have upto 3 endpoints, one each
  for "maintenance", "backfill", "catchup" stream.
* based on Topicid, router will either publish it to all indexer-nodes in
  the topology, or only to catchup indexer-node.

## Index topology

Index-topology helps router to send key-versions to respective master
indexer-nodes and its active replica. Typical sequence of execution would be,

* call a partition-algorithm with key-version. partition-algorithm can be
  backed by static tables or backed by tables that are continuously updated
  based on heuristics.
* partition algorithm is abstracted by TopologyProvider interface for the
  consumption of router.
* router can locate the shard that contains the key, where each shard is
  hosted by a local indexer node.

### Topics and subscribers

Three types of topics are honored for UPR data path.
* "maintenance stream" for incremental index, 
* "backfill stream" for initial index.
* "catchup stream" for local indexer recovery.

* At any given time only one system wide "maintenance" and "backfill" topic
  are allowed.
* At any given time only one "catchup" topic per indexer-node is allowed in
  the system
* From local-indexer point of view, at any given time it can simultaneously
  receive one "maintenance stream", one "backfill stream" and one "catchup
  stream"

Identifying subscribers,
* if Topicid is 255, use IndexTopology to identify the list of local
  indexer-nodes hosting the partition/shard. If there is an ongoing rebalance
  more than one shard will be hosting the same slice. Publish stream messages
  to shard's "maintanence" endpoint.
* if Topicid is 254, use IndexTopology to identify the list of
  local indexer nodes hosting the partition. Publish stream messages to
  identified endpoint's "backfill" port.
* if Topicid is 0-250, use IndexTopology to identify the list of
  local indexer nodes and pick one indexer node whose Indexerid is same as
  Topicid. Publish stream messages to endpoint's "catchup" port.
