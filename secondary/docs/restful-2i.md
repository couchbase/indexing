HTTP REST API was originally proposed by Roy Fielding for building distributed
hyper-media systems
[refer](http://www.ics.uci.edu/~fielding/pubs/dissertation/rest_arch_style.htm)

Properties of a REST architecture are:

* Uniform Interface
* Stateless
* Cacheable
* Client-Server
* Layered System
* Code on Demand (optional)

Further detail on each of these properties can be found in
http://www.restapitutorial.com.

### General request format:

Following arguments make up a HTTP REST request to GSI server:

```text

{METHOD, URL, Headers, Body}

METHOD:
    is mandatory argument and must be one of the following
    GET, PUT, POST, DELETE.

URL:
    is mandatory argument and must be path url, query params and fragment.

Headers:
    every request should carry following header properties
        "Content-Type: application/json"
        "Accept: application/json"

Body:
    body is always a JSON property and the schema is defined for each API.
```

**HTTP method and CRUD logic:**

| Method | CRUD   |
|--------|--------|
| POST   | Create |
| GET    | Read   |
| PUT    | Update |
| DELETE | Delete |

other HTTP methods like HEAD, OPTIONS, TRACE shall be defined as per
future requirement.

**Method idempotence and safety:**

From a RESTful service standpoint, for an operation (or service call) to be
idempotent, clients can make that same call repeatedly while producing the
same result. In other words, making multiple identical requests has the same
effect as making a single request. Note that while idempotent operations
produce the same result on the server (no side effects), the response itself
may not be the same (e.g. a resource's state may change between requests).

From RESTful service standpoint, for an operation to be safe it shall not
introduce any side effects in the server, eg: GET, HEAD etc..

| Method | Idempotent | Safe |
|--------|------------|------|
| POST   |    no      | no   |
| GET    |    yes     | yes  |
| PUT    |    yes     | no   |
| DELETE |    yes     | no   |

### CreateIndex API:

Create a new index using GSI services. If ``defer_build`` is false, this
API will immediately kick of the index build and the API will not return
until the build is completed. On the other hand, if ``defer_build`` is true,
this API will create the index and return back immediately.

**Request:**

```text
METHOD: POST
URL   : /internal/indexes?create=true
HEADER:
    "Content-Type: application/json"
    "Accept: application/json"
```

Body:

```javascript
    { "name": "myindex",            // name of the index, as string
      "bucket": "default",          // name of the bucket, as string
      "using": "memdb",             // "forestdb", "memdb", as string
      "exprType": "N1QL",           // "N1QL", as string
      "partnExpr": "",              // expression, as string
      "whereExpr": "type=\"user\"", // expression, as string
      "secExprs": ["age","city"],   // list of expressions, as array of string
      "isPrimary": false,           // whether index is primary, as boolean
      "with": ""                    // GSI specific attributes, as JSON property
    }
```

*optional fields:*

* ``using`` (default is "memdb")
* ``exprType`` (default is "N1QL")
* ``partnExpr`` (default is empty string)
* ``whereExpr`` (default is empty string)
* ``isPrimary`` (default is boolean)
* ``with``, refer to with clause section

**Response:**

```text
STATUS:
    201 Created
    400 Bad Request
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

Body:

```javascript
    { "id": "4177358991687653693", // decimal representation of id
    }
```

### BuildIndex API one or more indexes:

Build one or more index specified by the list of index ids in the request
body, the index(es) must have been already created. Building multiple
indexes at the same time is more efficient than building them one at a time.

**Request:**

```text
METHOD: PUT
URL   : /internal/indexes?build=true
HEADER:
    "Content-Type: application/json"
```

Body:

```javascript
    { "ids": ["4177358991687653693","2348132490871209493"]}
```

**Response:**

```text
STATUS:
    202 Accepted
    400 Bad Request
    500 Internal Server Error
```

### Get all indexes hosted by 2i:

Get the details of all index, its instances and its states hosted by 2i
cluster.

**Request:**

```text
METHOD: GET
URL   : /internal/indexes
```

**Response:**

```text
STATUS:
    202 Accepted
    400 Bad Request
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

Body:

```javascript
    { index, index, ... }
```

please refer to response body of GET single index API to know the shape of the
response text.


### BuildIndex API for single index:

Build a single index specified by the index id, the index must have
been created already.

**Request:**

```text
METHOD: PUT
URL   : /internal/index/{id}?build=true
```

**Response:**

```text
STATUS:
    202 Accepted
    400 Bad Request
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

### Drop an index:

**Request:**

```text
METHOD: DELETE
URL   : /internal/index/{id}
```

**Response:**

```text
STATUS:
    202 Accepted
    400 Bad Request
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

### Get an index's details:

**Request:**

```text
METHOD: GET
URL   : /internal/index/{id}
HEADER:
    "Accept: application/json"
```

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

Body:

```javascript
    {"definition": {}, // json property describing the index
     "instances": [],  // list json property describing index instances.
     "error": "",
    }
    definition = {
        "defnId": "4177358991687653693", // unique index id, as string
        "bucket": "default",             // name of the bucket, as string
        "name": "myindex",               // name of the index as string
        "using": "memdb",
        "bucketUUID": "23167657800098787", // unique bucket id, as string
        "isPrimary": false,                // whether index is primary as boolean
        "exprType": "N1QL",
        "partitionScheme": "single",
        "secExprs": ["age","city"],     // list of expressions, as array of str
        "partitionKey": "",             // expression, as tring
        "where": "type=\"user\"",       // expression, as string
        "deferred": false, // deferred property from with clause, as boolean
        "nodes": [], list of nodes hosting the index, as string
    }
    instance = {
        "instId": "3429874298742984",    // unique instance id, as string
        "state":   "INDEX_STATE_ACTIVE", // index state, as string
        "buildTime": [], // list of numbers as array of strings
        "indexerId": "", // unique id of hosting indexer node, as string
        "endpoints": [], // list of dataport IP for hosting indexer
    }
```

* if ``error`` is empty,
  - ``definition`` field should be present.
  - ``instances`` field should be an array with size >= 1.
* if ``error`` is not-empty, then ``definition`` and ``instances`` might be
  missing.

**Index States:**

|  State               | Description                                    |
|----------------------|------------------------------------------------|
| INDEX_STATE_CREATED  | create index statement is processed            |
| INDEX_STATE_READY    | index is created and ready to start the build  |
| INDEX_STATE_INITIAL  | index has started building via initial-stream  |
| INDEX_STATE_CATCHUP  | index build is catching up with maint. stream  |
| INDEX_STATE_ACTIVE   | build completed and index is active            |
| INDEX_STATE_DELETED  | index is dropped                               |

### Lookup index

Lookup index using a specific index key or prefix of an index key.

**Request:**

```text
METHOD: GET
URL   : /internal/index/{id}?lookup=true
HEADER:
    "Accept: application/json"
```

Body:

```javascript
    { "equal": "[78,\"newyork\"]",  // key to match or prefix match
	  "distinct": false,            // return only unique keys
      "limit": 100,                 // number of entries to return
      "stale": "partial",           // "ok", "false", "partial"
      "timestamp": {                      // in case of "partial",
        "30": ["213423442342342", "350"], //  {vbno:[vbuuid, seqno]} as
        ...                               // consistency constraint
      }
    }
```

*optional fields:*

* ``distinct`` (default is false)
* ``limit`` (default is 100)
* ``stale`` (default is "ok")
* ``timestamp`` (default is nil)

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

Body:
return the list of entries in chunked encoding mode - each of these chunks
contain one or more entities, until the last chunk.

```javascript
    [ {"key": "key1", "docid": "docid1"},
      {"key": "key1", "docid": "docid1"},
      ...
    ]
```

or in case of error,

```javascript
    {"error": ""} // error string.
```

### Range scan

Range scan a single index from specified low key to high key.


```text
METHOD: GET
URL   : /internal/index/{id}?range=true
HEADER:
    "Accept: application/json"
```

Body:

```javascript
    { "startkey": "[78,\"athens\"]", // return entries matching from
      "endkey": "[78,"\"newyork\"]", // return entries matching till
      "inclusion": "both",           // "low", "high", "both", "none"
	  "distinct": false,             // return only unique keys
      "limit": 100,                  // number of entries to return
      "stale": "partial",            // "ok", "false", "partial"
      "timestamp": {                      // in case of "partial",
        "30": ["213423442342342", "350"], //  {vbno:[vbuuid, seqno]} as
        ...                               // consistency constraint
      }
    }
```

* if ``startkey`` is null, then entries are matched from index beginning.
* if ``endkey`` is null, then entries are matched till end of index.

*optional fields:*

* ``inclusion`` (default is "both")
* ``distinct`` (default is false)
* ``limit`` (default is 100)
* ``stale`` (default is "ok")
* ``timestamp`` (default is nil)

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

return the list of entries in chunked encoding mode - each of these chunks
contain one or more entities, until the last chunk.

```javascript
    [ {"key": "key1", "docid": "docid1"},
      {"key": "key1", "docid": "docid1"},
      ...
    ]
```

or in case of error,

```javascript
    {"error": ""} // error string.
```

### MultiScan scan

MultiScan takes a collection scans with composite filters
and returns the results in sorted order.


```text
METHOD: GET
URL   : /internal/index/{id}?multiscan=true
HEADER:
    "Accept: application/json"
```

Body:

```javascript
    { "scans": [{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3}]}]  // list of scans with composite filters
      "projection": {"EntryKeys":[1],"PrimaryKey":false}  // specifies keys to return
      "distinct": false,            // return only unique keys if true
      "reverse": false,             // reverse the scan if true
      "offset": 0,                  // skip offset number of results
      "limit": 100,                 // number of entries to return
      "stale": "partial",            // "ok", "false", "partial"
      "timestamp": {                      // in case of "partial",
        "30": ["213423442342342", "350"], //  {vbno:[vbuuid, seqno]} as
        ...                               // consistency constraint
      }
    }
```

* if ``Seek`` is null, then composite filters are applied.
* if ``Seek`` is not null, then lookup is performed
* if ``Filter`` is null, then full table scan is performed

*optional fields:*

* ``distinct`` (default is false)
* ``reverse`` (default is false)
* ``offset`` (default is 0)
* ``limit`` (default is 100)
* ``stale`` (default is "ok")
* ``timestamp`` (default is nil)

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

return the list of entries in chunked encoding mode - each of these chunks
contain one or more entities, until the last chunk.

```javascript
    [ {"key": "key1", "docid": "docid1"},
      {"key": "key1", "docid": "docid1"},
      ...
    ]
```

or in case of error,

```javascript
    {"error": ""} // error string.
```

### Full table scan

Scan all entries in an index.

**Request:**

```text
METHOD: GET
URL   : /internal/index/{id}?scanall=true
HEADER:
    "Accept: application/json"
```

Body:

```javascript
    { "stale": "partial",           // "ok", "false", "partial"
      "timestamp": {                      // in case of "partial",
        "30": ["213423442342342", "350"], //  {vbno:[vbuuid, seqno]} as
        ...                               // consistency constraint
      }
    }
```


*optional fields:*

* ``stale`` (default is "ok")
* ``timestamp`` (default is nil)

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

return the list of entries in chunked encoding mode - each of these chunks
contain one or more entities, until the last chunk.

```javascript
    [ {"key": "key1", "docid": "docid1"},
      {"key": "key1", "docid": "docid1"},
      ...
    ]
```

or in case of error,

```javascript
    {"error": ""} // error string.
```

### Count items within a range of lowkey and highkey

Count number of items in an index, within a range of lowkey and highkey.

**Request:**

```text
METHOD: GET
URL   : /internal/index/{id}?count=true
HEADER:
    "Accept: application/json"
```

Body:

```javascript
    { "startkey": "[78,\"athens\"]", // return entries matching from
      "endkey": "[78,"\"newyork\"]", // return entries matching till
      "inclusion": "both",           // "low", "high", "both", "none"
      "stale": "partial",           // "ok", "false", "partial"
      "timestamp": {                      // in case of "partial",
        "30": ["213423442342342", "350"], //  {vbno:[vbuuid, seqno]} as
        ...                               // consistency constraint
      }
    }
```
*optional fields:*

* ``stale`` (default is "ok")
* ``timestamp`` (default is nil)

**Response:**

```text
STATUS:
    200 OK
    400 Bad Request
    404 Not Found
    500 Internal Server Error
HEADER:
    "Content-Type: application/json"
```

return integer-to-ascii representation of number of items.

```javascript
    100000
```

### With clause:

with clause is GSI specific JSON property object, with following attributes:

* ``"defer_build"``: boolean flag will create the index and wait for user
  to call build-index API.
* ``"nodes"``: list of indexer host IP that shall host the created index,
  as  array of strings
* ``"immutable"``: if where-expression is specified and then this boolean flag
  specifies that the fields on which the expression is defined are immutable.
* ``"index_type"``: to pick indexing algorithm, as string.

### consistency parameters:

scan queries like Lookup(), Range(), ScanAll() etc.. can be have 3 types of
consistency clause.

any-consistency:
    with parameter stake="ok", would mean 2i can return stale results.

session-consistency:
    with parameter stale="false", would mean 2i waits for indexer to
    catch up with KV before scanning for index keys.

query-consistency:
    with paramter stake="partial", accompanied by "timestamp" argument would
    mean 2i waits for indexer to catchup with KV alteast for the specified
    list of vbucets and their seqno before scanning for index keys.

### Etag and Last-Modified:

As of this writing 2i cluster do not track resource modification time and
resource-hashes. As and when those features are available the REST
interface can be extended to support Etags and Last-Modified headers in
the response and subsequent requests.
