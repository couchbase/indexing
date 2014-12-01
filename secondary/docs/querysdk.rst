This writeup aims to start a discussion on protocol that can be used for
querying secondary index and API interface that will be exposed to SDK, N1QL
and other clients.

From the SDK point of view access to 2i will be catogarised for,

* Meta access like Create(), Delete() and List() index for one or more
  buckets. Clients shall use http/json to contact 2i `coordinator`.
* Query access like Scan(), ScanAll(), Statistics() on a live secondary
  index. Clients shall use a custom high-performance protocol to query
  secondary index.

TBD - authentication and authorization model for accessing 2i cluster.

**Meta access**

Following APIs will be exported via HTTP-REST using json as the payload.

**POST /create**

Request-Method: POST
Request-Url: /create
Request-Header:
    ContentType: "application/json"
Request-Body: {
    "version": <uint64>, // can be used for rolling upgrade.
    "type":    "create", // request-type, can be "create", "list", "drop"
    "index":   {        // called as index-info structure.
        "name":      <string>,  // name of the index
        "bucket":    <string>,  // name of the bucket for which index created
        "defnID":    <string>,  // empty, will be assigned by server
        "using":     "lsm",
        "exprType":  "N1QL",
        "partnExpr": <string>,   // expression string in N1QL format
        "secExprs":  []<string>, // list of expressions in N1QL format
        "whereExpr": <string>,   // predicate-expression in N1QL format
        "isPrimary": <bool>      // to be set as true for primary index
    }
}

Response-body {
    "version": <uint64>,         // can be used for rolling upgrade
    "status":  <string>,         // can be "success", or error-string
    "indexes": [1]<index-info>,  // structure is described above
    "errors":  [1]<index-error>[{
        "code": <string>,
        "msg":  <string> 
    }]
}

**POST /list**

Request-Method: POST
Request-Url: /list
Request-Header:
    ContentType: "application/json"
Request-Body: {
    "version": <uint64>, // can be used for rolling upgrade.
    "type":    "list"    // request-type, can be "create", "list", "drop"
}

Response-body {
    "version": <uint64>,         // can be used for rolling upgrade
    "status":  <string>,         // can be "success", or error-string
    "indexes": [N]<index-info>   // structure is described above
}

TBD - add API to obtain topology information ?
TBD - how to detect changes in topology information ?
TBD - how to detect changes in index state ?
TBD - how will the client know when knew index is created via a different
      client?

**POST /drop**

Request-Method: POST
Request-Url: /drop
Request-Header:
    ContentType: "application/json"
Request-Body: {
    "version": <uint64>, // can be used for rolling upgrade.
    "type":    "drop",   // request-type, can be "create", "list", "drop"
    "index":   {         // called as index-info structure.
        "defnID": <string> // identify the index to drop.
    }
}

Response-body {
    "version": <uint64>, // can be used for rolling upgrade
    "status":  <string>  // can be "success", or error-string
}

**Query access**

Query access protocol shall specify two parts, the transport and payload.
Transport will describe the necessary framing to transfer data from one node
to another and payload will describe structure and content of data that shall
be transported to the other end.

* for transport, we propose custom framing.
* for payload, we propose to use protobuf messages that shall support streaming
  data transfer.
* the structure of the payload can be extended as long as it does not violate
  the transport and streaming spec.

simple non-streaming post by client::

            client                              server
              |                                   |
              |            post-request           |
              | --------------------------------> |
              |                                   |

* a `request` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by the transport.
* server does not respond back.

simple non-streaming request-response between client and server::

            client                              server
              |                                   |
              |            request                |
              | --------------------------------> |
              |                                   |
              |            response               |
              | <-------------------------------- |
              |                                   |

* a `request` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by the transport.
* server should interpret the request message and reply back with a single
  `response` message.
* a `response` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by transport.

streaming request-response between client and server, and the streaming is
stopped by server::

            client                              server
              |                                   |
              |            request                |
              | --------------------------------> |
              |                                   |
              |         response-stream           |
              | <-------------------------------- |
              |                                   |
              |         response-stream           |
              | <-------------------------------- |
              |                                   |
              |          StreamEndResponse        |
              | <-------------------------------- |
              |                                   |

* a `request` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by the transport.
* server shall interpret a `request` message, and send back one or more
  response one after the other.
* finally when there are no more response to stream back, server shall send
  `StreamEndResponse` message to the client.
* a `response` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by transport.

streaming request-response between client and server, and the streaming is
stopped by client::

            client                              server
              |                                   |
              |            request                |
              | --------------------------------> |
              |                                   |
              |            response               |
              | <-------------------------------- |
              |                                   |
              |            response               |
              | <-------------------------------- |
              |                                   |
              |          EndStreamRequest         |
              | --------------------------------> |
              |                                   |
              |        residue-response           |
              | <-------------------------------- |
              |               ...                 |
              |               ...                 |
              |          StreamEndResponse        |
              | <-------------------------------- |
              |                                   |

* a `request` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by the transport.
* server shall interpret a `request` message, and send back one or more
  response one after the other.
* in case, client finds that it does not want any more messages to be
  streamed, it shall transmit a `EndStreamRequest` message to server.
* server shall send `StreamEndResponse`, when there are no more response to
  send back or when it receives an `EndStreamRequest` message from client.
* a `response` can be any message defined by protobuf (or by any encoding
  scheme) that will be treated as raw-bytes by transport.


**multiplexing many request on the same connection**

We also propose to use the same tcp-connection to handle simultaneous
requests. To achieve this every `request` message from client will now have
a 32-bit ``opaque`` value that must be unique across request and the same opaque
value shall be used by the client while sending `EndStreamRequest`.

Likewise, the server will retrieve the opaque value from request and supply
the same value for every `response` message including
`StreamEndResponse`, corresponding to that request::

            client                              server
              |                                   |
              |            request-opaque1        |
              | --------------------------------> |
              |                                   |
              |      opaque1-response-stream      |
              | <-------------------------------- |
              |                                   |
              |            request-opaque2        |
              | --------------------------------> |
              |                                   |
              |      opaque2-response-stream      |
              | <-------------------------------- |
              |                                   |
              |      opaque1-response-stream      |
              | <-------------------------------- |
              |                                   |
              |      opaque2-response-stream      |
              | <-------------------------------- |
              |               ...                 |
              |               ...                 |
              |                                   |
              |      EndStreamRequest-opaque1     |
              | --------------------------------> |
              |                                   |
              |      opaque2-StreamEndResponse    |
              | <-------------------------------- |
              |                                   |
              |      opaque1-StreamEndResponse    |
              | <-------------------------------- |
              |                                   |


**protocol-transport:**

bit/byte representation of protobuf framing::

    0               8               16              24            31
    +---------------+---------------+---------------+---------------+
    |                   payload-length (uint32)                     |
    +---------------+---------------+---------------+---------------+
    |             flags             |                               |
    |  ...........................  |           payload             |
    | COMP. |  ENC. |    undefined  |                               |
    +---------------+---------------+---------------+---------------+
    |                   payload ....                                |
    +---------------+---------------+---------------+---------------+

* COMP is a 4-bit value that denotes compression type used on payload.
  - a value of `0`, means no-compression.

* ENC is a 4-bit value that denotes payload's encoding type.
  - a value of `0` means no-encoding.
  - a value of `1` means protobuf is used for encoding.

**protocol-payload**

.. code-block:: protobuf

    // QueryPayload should contain atleast one of the optional field
    // identified by `msgType`.
    message QueryPayload {
        // All messages will include version field, which can used to
        // implement rolling-upgrade.
        required uint32             version           = 1;
        required byte               msgType           = 2;

        // All messages will include opaque field to handle simultaneous
        // request on the same connection
        required uint32             opaque            = 3;

        // Statistics.
        optional StatisticsRequest  statisticsRequest = 4;
        optional StatisticsResponse statistics        = 5;

        // Scan.
        optional ScanRequest        scanRequest       = 6;
        optional ScanAllRequest     scanAllRequest    = 7;
        optional ResponseStream     stream            = 8;

        // handshake message for streaming.
        optional EndStreamRequest   endStream         = 9;
        optional StreamEndResponse  streamEnd         = 10;
    }

    message StatisticsRequest {
        required string bucket    = 1;
        required string indexName = 2;

        // if span is null, StatisticsResponse will cover the full data-set of
        // the index
        optional Span   span      = 3;
    }

    // If span is specified, response will provide the stats for a index-data
    // subset.
    message StatisticsResponse {
        required IndexStatistics stats = 1;
        optional string          err   = 2; // empty string denotes no-error
    }

    // Scan request to indexer.
    message ScanRequest {
        required string bucket    = 1;
        required string indexName = 2;
        required Span   span      = 3;
        required bool   distinct  = 4;
        required int64  limit     = 5;
    }

    // Full table scan request from indexer.
    message ScanAllRequest {
        required string bucket    = 1;
        required string indexName = 2;
        required int64  limit     = 4;
    }

    // Requested by client to stop streaming the query results.
    message EndStreamRequest {
    }

    message ResponseStream {
        repeated IndexEntry entries = 1;
        optional string     err     = 2; // empty string denotes no-error
    }

    // Last response packet sent by server to end query results.
    message StreamEndResponse {
        optional string err = 1; // empty string denotes no-error
    }

    // Query messages / arguments for indexer
    message Span {
        required Range  range = 1;
        repeated bytes  equal = 2; // can be used for lookup-scan.
    }

    // Can be used for range-scan.
    message Range {
        required bytes  low       = 1;
        required bytes  high      = 2;
        required uint32 inclusion = 3;
    }

    // IndexEntry represents a single secondary-index entry.
    message IndexEntry {
        required bytes  entryKey   = 1;
        required bytes  primaryKey = 2;
    }

    // Statistics of a given index.
    message IndexStatistics {
        required uint64 count      = 1;
        required uint64 uniqueKeys = 2;
        required bytes  min        = 3;
        required bytes  max        = 4;
    }
