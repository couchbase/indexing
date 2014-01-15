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

* In parse.go, using the parser combinator for builtin json parsing, use
  built in sort instead of a dumb bubble sort.

* Figure out a strategy for error handling in Encode() and Decode() execution
  path.
