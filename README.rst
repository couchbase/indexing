Collatejson library, written in golang, provides encoding and decoding function
to transform JSON text into binary representation without loosing information.
That is,

* binary representation should preserve the sort order such that, sorting
  binary encoded json documents much match sorting by functions that parse
  and compare JSON documents.
* it must be possible to get back the original document, in semantically
  correct form, from its binary representation.

Notes:

* items in a property object are sorted by its property name before they
are compared with other property object.

for api documentation and bench marking try,

.. code-block:: bash

    godoc github.com/couchbaselabs/go-collatejson | less
    cd go-collatejson
    go test -test.bench=.

to measure relative difference in sorting 100K elements using encoding/json
library and this library try,

.. code-block:: bash

    go test -test.bench=Sort

examples/* contains reference sort ordering for different json elements.

For known issues refer to `TODO.rst`
