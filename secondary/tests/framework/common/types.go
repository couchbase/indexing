package common

// A map that holds response results from 2i Scan APIs as well as from JSON document scan
type ScanResponse map[string][]interface{}

type ClusterConfiguration struct {
	KVAddress              string
	IndexManagementAddress string
	IndexScanAddress       string
}
