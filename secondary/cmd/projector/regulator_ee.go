// +build !community

package main

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/projector"
	regulator "github.com/couchbase/regulator/factory"
)

func startRegulator() {
	// MB-52130: start the KV regulator (piggybacked in the projector process)
	// TODO: only start when in serverless
	regulator.InitKvRegulator(service.NodeID(projector.GetNodeUUID()), options.caFile, options.kvaddrs)
}