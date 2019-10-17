package common

import n1ql "github.com/couchbase/query/value"

// A map that holds response results from 2i Scan APIs as well as from JSON document scan
// Key = Primary key of scan response
// Value = Secondary key which can be a simple value or a JSON itself
type ScanResponse map[string][]interface{}     // expected results
type ScanResponseActual map[string]n1ql.Values // actual results

type ArrayIndexScanResponse map[string][][]interface{}     // expected results
type ArrayIndexScanResponseActual map[string][]n1ql.Values // actual results

type GroupAggrScanResponse [][]interface{}     // expected results
type GroupAggrScanResponseActual []n1ql.Values // actual results

// With the introduction of CJson as the data format between GSI client
// and indexer, the actual results returned by n1ql client are n1ql values.
// The SecondaryKey representation is now obsolete. Here, changing the
// representation of ALL expected results to n1ql values is too much of
// code change. So, for time being, maintain two different representations
// and convert []interface{} to n1ql.Values during validation.

// Map of key and JSON as value
// Key = string (doc key)
// Value = any JSON object
type KeyValues map[string]interface{}

type ClusterConfiguration struct {
	KVAddress            string
	Username             string
	Password             string
	IndexUsing           string
	Nodes                []string
	MultipleIndexerTests bool
}
