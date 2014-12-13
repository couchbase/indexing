## Tests for 2i

# Status
    Currently, it has framework utilities required for basic functional testcases. The utitilies are - KV data utilities (for SET, GET, DEL of KV), JSON data utilties (to load json from file, to scan json document), 2i API wrappers (Range, Lookup, Create 2i, Drop 2i, List indexes)

# Todo
	Need to modify the logic for computing expected scan results from JSON document

# Usage
    Tests can be run using "go test" command from /indexing/secondary/tests/functionaltests/ location

# 2i APIs and helper methods used in tests
	Create 2i
	Drop 2i
	List indexes
	Range
	ResponseReader.GetEntries
	client.NewClient
	client.NewClusterClient
	client.Remoteaddr
	client.Inclusion
	common.SystemConfig.SectionConfig
	common.SecondaryKey
	go-couchbase - Get, Set, Delete
	n1ql - n1ql.ParseExpression