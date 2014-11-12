Shape of scan results for N1QL consumption
------------------------------------------

**How secondary keys are generated from projector ?**

Secondary-keys are projected based on expression supplied via `CREATE-INDEX`
DDL. If the DDL supplies only one expression for evaluation then the generated
secondary-key is a scalar-value - `nil`, `float`, `boolean`, `string`, `array`
and `property` (aka object). On the other hand, if the DDL supplies an
array of expressions to be evaluated then the generated secondary-key will be
an `array` of scalar-values.

**How keys are stored in indexer nodes ?**

Keys are stored as 8-bit bytes of binary data. Semantically a secondary key,
the binary blob, is a JSON value representing - nil, float, boolean, string,
array and property (aka object). Although the key is in JSON format it will
be sorted based on N1QL specification and persisted in the back-end (like
forestdb).

**Scan() from N1QL**

N1QL's datastore API define a Scan() method on secondary index to fetch one or
more results based on arguments like start-key, end-key, limit etc ...
The Scan() shall be marshalled across to Scan-coordinator, residing in indexer
node, and results shall be streamed back to N1QL.

Incoming {secondary-key, primary-docid}, both of them a binary blob, will be
converted to datastore:IndexEntry{} structure and posted to `EntryChannel`.

**Shape of IndexEntry{}**

IndexEntry:EntryKey will be constructed using,
* query.value.NULL_VALUE for `nil`
* query.value.MISSING_VALUE for `missing`, only applicable to items in composite
  keys that were not skipped
* query.value.floatValue(val) for `float`
* query.value.boolValue(val) for `boolean`
* query.value.stringValue(val) for `string`
* query.value.sliceValue(val) for `array`
* query.value.objectValue(val) for `property`

IndexEntry:PrimaryKey will consucted using query.value.NewValue(val)

**behaviour for missing secondary-key**

* For a secondary index that stores only a scalar value, missing
  secondary-keys shall be skipped - not persisted.

* For secondary index that stores composite keys, all entries will be stored
  **except** when the first item, or first N items, in the composite-key is
  missing.
