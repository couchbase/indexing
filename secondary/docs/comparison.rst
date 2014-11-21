Primary use of collatejson is to do binary comparison on two json strings.
Binary comparison (aka memcmp) can be several times faster than any other
custom JSON parser.

memcmp:
-------

.. code-block:: C
    int memcmp(const void *s1, const void *s2, size_t n);

where, n = Min(s1, s2)

JSON:
-----

JSON, as defined by spec, can represent following elements,
  1. nil.
  2. boolean.
  3. numbers in integer form, or floating-point form.
  4. string.
  5. array of JSON elements.
  6. object of key,value properties, where key is represetned as string and
     value can be any JSON elements.

nil, boolean, numbers, strings:
-------------------------------

Basic elements can be compared without any special care.

array:
------

By default array is not prefixed with length of the array, which means
elements are compared one by one until binary-compare returns EQ, GT or
LT. This is assuming that elements in both arrays (key1 and key2) have
one-to-one correspondence with each other. **Suppose number of elements
in key1 is less that key2, or vice-versa, prune the last byte from the
encoded text of smaller array and continue with binary comparison.**

object:
-------

By default objects are prefixed with length of the object (ie) number of
elements in the object. This means objects with more number of {key,value}
properties will sort after.

While encoding collatejson will sort the object properties based on keys.
This means the property key will be compared first and if equal, comparison
will continue to its value.

Note:

1. Wildcards are not accepted in elements. For instance, it is not possible to
   select all Cities starting with "San". To get all cities starting with
   "San" perform a ">=" operation on storage and stop iterating when returned
   value is not prefixed with "San"
