* codec.Encode() converts the input JSON to golang native before
  applying the encoding algorithm,
    if err := json.Unmarshal(text, &m); err != nil {
        return nil, err
    }
  explore possibilities to avoid a call to json.Unmarshal()

* codec.Decode() returns JSON output, for couchbase 2i project
  the JSON string will always the following JSON format.
        [expr1, docid] - for simple key
        [expr1, expr2, ..., docid] - for composite key
  it would be a good optimization to implement a variant of Decode()
  that will return as,
        [expr1], docid - for simple key
        [expr1, expr2 ...], docid - for composite key

* Jens' comments,
  * Also BTW, there’s a lot of appending of byte slices going on in
    collate.go. I suspect this is inefficient, allocating lots of small slices
    and then copying them together. It’s probably cheaper (and simpler) to use
    an io.Writer instead.
  * The CouchDB collation spec uses Unicode collation, and strangely enough
    the collation order for ASCII characters is not the same as ASCII order. I
    solved this by creating a mapping table that converts the bytes 0-127 into
    their priority in the Unicode collation.

* create a new directory examples_len/ that contains the sorted list of json
  items without using `lenprefix`

* Are we going to differentiate between float and integer ?
  Looks like dparval is parsing input json's number type as all float values.

* JSON supports integers of arbitrary size ? If so how to do collation on
  big-integers ?
  Even big-integers are parsed are returned as float by dparval.

* Encoding and decoding of utf8 strings.
