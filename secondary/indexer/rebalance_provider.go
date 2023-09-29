package indexer

import "github.com/couchbase/indexing/secondary/manager"

type RebalanceProvider interface {
	Cancel()
	InitGlobalTopology(*manager.ClusterIndexMetadata)
}
