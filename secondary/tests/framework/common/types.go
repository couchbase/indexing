package common

// A map that holds response results from 2i Scan APIs as well as from JSON document scan
// Key = Primary key of scan response
// Value = Secondary key which can be a simple value or a JSON itself
type ScanResponse map[string][]interface{}

type ArrayIndexScanResponse map[string] [][]interface{}

// Map of key and JSON as value
// Key = string (doc key)
// Value = any JSON object
type KeyValues map[string]interface{}

type ClusterConfiguration struct {
	KVAddress  string
	Username   string
	Password   string
	IndexUsing string
}
