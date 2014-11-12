Integration points between secondary index and query.

**Projection using query.expression.Evaluate()**

Will use the below code snippet to evaluate a document using DDL expression.
For composite-keys, the document will be evaluated for every expression
supplied in the DDL statement.

.. code-block::
    key, err := expr.Evaluate(qvalue.NewValue(document), context)
    if err != nil {
        return nil, nil
    }

* Evaluate() shall return a special error-value that will indicate `missing`.
* Evaluate() API shall be extended to receive document's metadata.

Partition key expression, from CREATE INDEX DDL statement, will be used to
evaluate partition-key. Partition-key will be used to pick the indexer node
that is hosting the index-partition.

**WHERE clause in DDL**

WHERE clause can be specified in CREATE INDEX DDL to skip documents that does
not evaluate to true. On the projector, this means -

* If WHERE clause predicate is empty, always proceed evaluating for
  secondary-keys like described earlier.
* Else,
  * If predicate is true, proceed evaluating for secondary-keys like described
    earlier.
  * Else, skip document and broadcast UpsertDeletion for docid.

**Read your own write**

Scan() API shall be extended to include a timestamp vector, a list of
[{vbno, seqno}, ...], that can be used by scan-coordinator to pick a snapshot
that is **greater than or equal to** the supplied timestamp.

Wildcard comparison shall be used for unspecified vbuckets in the supplied
timestamp.

**query path**

Query path is the read path between N1QL and indexer-node(s). Initiated by
N1QL, the results are streamed back to N1QL server until there are no more
entries to be posted or until the N1QL's query.datastore.StopChannel is
closed.

Query can be,

1. pointed query, supplied by query.datastore.Span.Equal field or,
2. range query, supplied by query.datastore.Range field.

In both cases value.Value will be converted to binary blob by indexer-node to
look-up for entries.

a. In case of (1.), pointed query -
   i. value can just be a scalar `key` value, in which case zero or more
      entry will be streamed back.
   ii. value can be a two element array of `[key, docid]`, in which case zero
       or one entry will be streamed back.
b. In case of (2.), range query, start-key and end-key which can be a
   simple-key or composite-key shall be converted to an internal encoding.
   zero or more entries will be streamed back.

TBD1, it is not possible to `pause` a Scan() request and `continue` later.

**transactional index scans**

TBD2.

N1QL's planner might fetch, as part of same query-statement, ranges from
more than one index with the intend of composing an intersection or union of
resulting sets.

**Min and Max in a range**

TBD3.

Pending discussion with forestdb on support for bi-directional scans.

**clustering**

Secondary-index has its own clustering mechanism with some help from
NS-server.

1. Get NS-server cluster address (similar to localhost:9000).
2. From NS-server get an ensemble of, list of <host:port>, gometa servers
   that manages DDLs statements and indexer topology.
3. From gometa fetch the DDL and topology so that N1QL datastore client (aka
   catalog) knows which indexer node host what index.
4. For range partitioned distributed index, one or more indexer node will be
   picked based on start-key and end-key supplied with Scan().

* TBD4, does Scan() supply a partition-key while quering ?
  will WHERE clause, from SELECT statement, be used for that ?
* TBD5, how should multiple N1QL servers and gometa synchronise each other.
* TBD6, should N1QL datastore API need to be extended to pass down
  cluster-address ?

**index statistics**

Not available for secondary-index version-1.

**replicas**

Not available for secondary-index version-1.

**computed index / expression index**

TBD7
