package common

import (
	"github.com/couchbase/cbauth/service"
	n1ql "github.com/couchbase/query/value"
)
import "github.com/couchbase/indexing/secondary/common"

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

// IndexStatus type is same type returned by /getIndexStatus REST call.
type IndexStatus struct {
	DefnId       common.IndexDefnId `json:"defnId,omitempty"`
	InstId       common.IndexInstId `json:"instId,omitempty"`
	Name         string             `json:"name,omitempty"`
	Bucket       string             `json:"bucket,omitempty"`
	IsPrimary    bool               `json:"isPrimary,omitempty"`
	SecExprs     []string           `json:"secExprs,omitempty"`
	WhereExpr    string             `json:"where,omitempty"`
	IndexType    string             `json:"indexType,omitempty"`
	Status       string             `json:"status,omitempty"`
	Definition   string             `json:"definition"`
	Hosts        []string           `json:"hosts,omitempty"`
	Error        string             `json:"error,omitempty"`
	Completion   int                `json:"completion"`
	Progress     float64            `json:"progress"`
	Scheduled    bool               `json:"scheduled"`
	Partitioned  bool               `json:"partitioned"`
	NumPartition int                `json:"numPartition"`
	PartitionMap map[string][]int   `json:"partitionMap"`
	NodeUUID     string             `json:"nodeUUID,omitempty"`
	NumReplica   int                `json:"numReplica"`
	IndexName    string             `json:"indexName"`
	ReplicaId    int                `json:"replicaId"`
	Stale        bool               `json:"stale"`
	LastScanTime string             `json:"lastScanTime,omitempty"`
}

// IndexStatusResponse is a copy of the same type from generic_service_manager.go, as the tests
// won't build if they try to import the indexer package (e.g. sigar code won't build).
type IndexStatusResponse struct {
	Version     uint64        `json:"version,omitempty"`
	Code        string        `json:"code,omitempty"`
	Error       string        `json:"error,omitempty"`
	FailedNodes []string      `json:"failedNodes,omitempty"`
	Status      []IndexStatus `json:"status,omitempty"`
}

// GetTaskListResponse is a copy of the same type from generic_service_manager.go, as the tests
// won't build if they try to import the indexer package (e.g. sigar code won't build).
type GetTaskListResponse struct {
	Code     string            `json:"code,omitempty"`
	Error    string            `json:"error,omitempty"`
	TaskList *service.TaskList `json:"taskList,omitempty"`
}
