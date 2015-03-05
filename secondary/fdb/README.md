# goforestdb

Go bindings for ForestDB

## Building

1.  Obtain and build forestdb: https://github.com/couchbaselabs/forestdb (run `make install` to install the library)
1.  Install header files to system location
  1. On Ubuntu 14.04: `cd <forestdb_project_dir> && mkdir /usr/local/include/libforestdb && cp include/libforestdb/* /usr/local/include/libforestdb`
1.  `go get -u -v -t github.com/couchbaselabs/goforestdb`

## Documentation

See [godocs](http://godoc.org/github.com/couchbaselabs/goforestdb)

## Sample usage (without proper error handling):

	// Open a database
	db, _ := Open("test", nil)

	// Close it properly when we're done
	defer db.Close()

	// Store the document
	doc, _ := NewDoc([]byte("key"), nil, []byte("value"))
	defer doc.Close()
	db.Set(doc)

	// Lookup the document
	doc2, _ := NewDoc([]byte("key"), nil, nil)
	defer doc2.Close()
	db.Get(doc2)

	// Delete the document
	doc3, _ := NewDoc([]byte("key"), nil, nil)
	defer doc3.Close()
	db.Delete(doc3)
