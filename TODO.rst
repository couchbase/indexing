* Jens' comments,
  * Also BTW, there’s a lot of appending of byte slices going on in
    collate.go. I suspect this is inefficient, allocating lots of small slices
    and then copying them together. It’s probably cheaper (and simpler) to use
    an io.Writer instead.
  * The CouchDB collation spec uses Unicode collation, and strangely enough
    the collation order for ASCII characters is not the same as ASCII order. I
    solved this by creating a mapping table that converts the bytes 0-127 into
    their priority in the Unicode collation.

* If a string contains escaped null values it will conflict with TERMINATOR
  encoding. JSON strings must be byte stuffed for TERMINATOR byte.

* create a new directory examples_len/ that contains the sorted list of json
  items without using `lenprefix`

* How to handle missing value ?

* Are we going to differentiate between float and integer ?
  Looks like dparval is parsing input json's number type as all float values.

* JSON supports integers of arbitrary size ? If so how to do collation on
  big-integers ?
  Even big-integers are parsed are returned as float by dparval.

* Encoding and decoding of utf8 strings.
