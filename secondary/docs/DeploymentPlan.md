
##Secondary Index Deployment Options

###Overview

[Deployment Plan Design Doc](https://docs.google.com/document/d/1z0C7OodlagDnesvmL6OwbNcnpCrbkN8T_K4_Y6PKwgk/edit#heading=h.jyuvpp7j9swu) provides an overview of this feature.

###Syntax

```CREATE INDEX index_type
ON `beer-sample`(type) 
USING GSI 
WITH `{"nodes": ["node_addr"], "defer_build": true}````

**node_addr** refers to the couchbase cluster address for the index node ie. "hostname:port" (e.g. "127.0.0.1:8091", "127.0.0.1:9000" etc)

**defer_build** determines if index build is immediate or after an explicit build index command

**Note** Currently due to a bug the **node_addr** needs to have the **indexAdmin** port rather than the ns_server port. This port can be discovered for an Indexer Node from the nodeServices url (e.g.  http://127.0.0.1:9000/pools/default/nodeServices and look up indexAdmin port for the current node). If the indexAdmin port is 9100, the **node_addr** would be "127.0.0.1:9100".

###Examples of Deployment Plan Usage

####Setup
 
**Node1**(172.16.1.174:9000) - kv+index+n1ql  

**Node2**(127.0.0.1:9001)    - index 




####Create Index On A Node With Deployment Plan



```CREATE INDEX index_abv 
ON `beer-sample`(abv) 
USING GSI 
WITH `{"nodes": ["172.16.1.174:9000"]}`;```

Index index_abv gets deployed on node 172.16.1.174:9000 and build is triggered immediately.

```CREATE INDEX index_type
ON `beer-sample`(type) 
USING GSI 
WITH `{"nodes": ["127.0.0.1:9001"]}`;```

Index index_type gets deployed on node 127.0.0.1:9001 and build is triggered immediately.




####Create Index On A Node In Deferred Mode



```CREATE INDEX index_abv 
ON `beer-sample`(abv) 
USING GSI 
WITH `{"defer_build": true}`;```

Index index_abv gets deployed on a randomly choosen Indexer node and build is not triggered.

```CREATE INDEX index_type
ON `beer-sample`(type) 
USING GSI 
WITH `{"defer_build": true}`;```

Index index_type gets deployed on a randomly choosen Indexer node and build is not triggered.


```BUILD INDEX ON `beer-sample`(index_abv, index_type) USING GSI;```

Build for indexes index_abv and index_type gets triggered.




####Create Index With Both Deployment and Deferred Mode Option



```CREATE INDEX index_abv 
ON `beer-sample`(abv) 
USING GSI 
WITH `{"nodes": ["172.16.1.174:9000"], "defer_build": true}`;```

Index index_abv gets deployed on node 172.16.1.174:9000 and build is not triggered.

```CREATE INDEX index_type
ON `beer-sample`(type) 
USING GSI 
WITH `{"nodes": ["127.0.0.1:9100"], "defer_build": true}`'```

Index index_abv gets deployed on node 127.0.0.1:9001 and build is not triggered.

```BUILD INDEX ON `beer-sample`(index_abv, index_type) USING GSI;```

Build for indexes index_abv and index_type gets triggered.
