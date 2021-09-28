// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	// kjc waiting for ns_server implementation: "github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// AutofailoverServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// AutofailoverServiceManager provides the implementation of the ns_server RPC interface
// AutofailoverManager (defined in cbauth/service/interface.go) which this class registers as the
// GSI handler for by calling RegisterAutofailoverManager (defined in cbauth/service/revrpc.go).
type AutofailoverServiceManager struct {
	cpuThrottle  *CpuThrottle        // CPU throttler
	ctExpirer    *cpuThrottleExpirer // CPU throttle automatic expirer when Autofailover is off
	diskFailures unsafe.Pointer      // *uint64 ever-increasing count of disk read/write failures
	httpAddr     string              // local indexer host:port for HTTP, e.g. "127.0.0.1:9102", 9108,...
}

// NewAutofailoverServiceManager is the constructor for the AutofailoverServiceManager class.
// httpAddr gives the host:port of the local node for Index Service HTTP calls.
func NewAutofailoverServiceManager(httpAddr string, cpuThrottle *CpuThrottle) *AutofailoverServiceManager {
	m := &AutofailoverServiceManager{
		cpuThrottle: cpuThrottle,
		ctExpirer:   newCpuThrottleExpirer(cpuThrottle),
		httpAddr:    httpAddr,
	}
	var diskFailures uint64 = 0
	m.diskFailures = (unsafe.Pointer)(&diskFailures)

	go m.registerWithServer()
	return m
}

// AddDiskFailures adds the value of the argument to m.diskFailures. Thread-safe.
func (m *AutofailoverServiceManager) AddDiskFailures(diskFailures uint64) {
	atomic.AddUint64((*uint64)(m.diskFailures), diskFailures)
}

// getDiskFailures returns the value of m.diskFailures. Thread-safe.
func (m *AutofailoverServiceManager) getDiskFailures() uint64 {
	return atomic.LoadUint64((*uint64)(m.diskFailures))
}

// registerWithServer runs in a goroutine that registers this object as the singleton handler
// implementing the ns_server RPC interface AutofailoverManager (defined in
// cbauth/service/interface.go) by calling RegisterAutofailoverManager
// (defined in cbauth/service/revrpc.go).
func (m *AutofailoverServiceManager) registerWithServer() {
	const method = "AutofailoverServiceManager::registerWithServer:" // for logging

	// kjc waiting for RegisterAutofailoverManager from ns_server: err := service.RegisterAutofailoverManager(m, nil)
	err := error(nil) // kjc temp replacement for the above
	if err != nil {
		logging.Errorf("%v Failed to register with Cluster Manager. err: %v", method, err)
		return
	}
	logging.Infof("%v Registered with Cluster Manager", method)
}

// getLastKnownIndexTopology sends a loopback HTTP REST call to the local Index service to get the
// laset known index topology of all Index nodes. For speed this uses getCachedIndexTopology which
// was custom coded for this purpose. It gets all topology info from local memory cache and does not
// use ClusterInfoCache which has a mutex that is sometimes held for long periods.
func (m *AutofailoverServiceManager) getLastKnownIndexTopology() (*manager.IndexStatusResponse, error) {
	const method = "AutofailoverServiceManager::getLastKnownIndexTopology:" // for logging

	url := m.httpAddr + "/getCachedIndexTopology"
	resp, err := getWithAuth(url)
	if err != nil {
		logging.Errorf("%v Error getting index topology. url: %v, err: %v", method, url, err)
		return nil, err
	}

	defer resp.Body.Close()
	statusResp := new(manager.IndexStatusResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &statusResp); err != nil {
		logging.Errorf("%v Error unmarshaling index topology. url: %v, err: %v",
			method, url, err)
		return nil, err
	}

	return statusResp, nil
}

// mapifyTopology takes a slice of (non-consolidated) index statuses and creates a map from each
// bucket:scope:collection:index described in them to sets of all partition numbers of each that are
// listed in the statuses. (Non-partitioned indexes always have one "partition" numbered 0.
// Partitioned indexes have 1-based partn #s.) This tells us the totality of all index partitions
// (both masters and replicas) hosted on all the nodes described by the slice of statuses.
func mapifyTopology(indexStatuses []*manager.IndexStatus) map[string]map[int]struct{} {
	result := make(map[string]map[int]struct{})
	for _, idxStatPtr := range indexStatuses {
		indexKey := strings.Join([]string{idxStatPtr.Bucket, idxStatPtr.Scope,
			idxStatPtr.Collection, idxStatPtr.IndexName}, ".") // . is N1Ql naming separator
		partnSet := result[indexKey] // set of partn #s for this indexKey
		if partnSet == nil {
			partnSet = make(map[int]struct{})
			result[indexKey] = partnSet
		}
		for _, partn := range idxStatPtr.PartitionMap[idxStatPtr.Hosts[0]] { // Hosts[0] is sole entry
			partnSet[partn] = struct{}{}
		}
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// cpuThrottleExpirer class
////////////////////////////////////////////////////////////////////////////////////////////////////

// CPU_THROTTLE_EXPIRY_SEC is # of seconds after last HealthCheck call to asssume Autofailover is
// disabled and thus CPU throttling should be turned off.
const CPU_THROTTLE_EXPIRY_SEC = 10

// cpuThrottleExpirer is a helper class for the AutofailoverServiceManager class that automatically
// shuts off CPU throttling if Autofailover is detected to be disabled based on no HealthCheck call
// received in CPU_THROTTLE_EXPIRY_SEC seconds.
type cpuThrottleExpirer struct {
	cpuThrottle *CpuThrottle // CPU throttler from parent

	expiryMutex  sync.Mutex   // protects expiryTicker
	expiryTicker *time.Ticker // single-fire ticker to shut down CPU throttling; nil if not running
}

// newCpuThrottleExpirer is the constructor for the cpuThrottleExpirer class.
func newCpuThrottleExpirer(cpuThrottle *CpuThrottle) *cpuThrottleExpirer {
	cte := &cpuThrottleExpirer{
		cpuThrottle: cpuThrottle,
	}
	return cte
}

// resetCpuThrottleExpiry starts or resets the expiryTicker. If starting it, this also launches the
// goroutine that will turn off CPU throttling if and when the expiration is reached.
func (this *cpuThrottleExpirer) resetCpuThrottleExpiry() {
	this.expiryMutex.Lock()
	if this.expiryTicker == nil { // starting a new ticker
		this.expiryTicker = time.NewTicker(CPU_THROTTLE_EXPIRY_SEC * time.Second)
		this.expiryMutex.Unlock() // unlock here so expiryTicker inits before runExpiryCountdown uses it

		go this.runExpiryCountdown()
	} else { // resetting existing ticker
		this.expiryTicker.Reset(CPU_THROTTLE_EXPIRY_SEC * time.Second)
		this.expiryMutex.Unlock()
	}
}

// runExpiryCountdown runs as a goroutine when expiryTicker is running. If the ticker fires this
// shuts down CPU throttling, shuts down expiryTicker and sets it to nil, and exits.
func (this *cpuThrottleExpirer) runExpiryCountdown() {
	select {
	case <-this.expiryTicker.C:
		this.cpuThrottle.SetCpuThrottling(false)

		this.expiryMutex.Lock()
		this.expiryTicker.Stop()
		this.expiryTicker = nil
		this.expiryMutex.Unlock()
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  service.AutofailoverManager interface implementation
//  Interface defined in cbauth/service/interface.go
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// HealthCheck will be called by ns_server ONLY when Autofailover is enabled, on a 2-second ticker,
// to check Index Service responsivity. It will make the next call on the next tick after the prior
// call responds. Normally responding will only take milliseconds, so the calls will be 2 seconds
// apart, but if a response takes e.g. 3.2 seconds then the next call will be made at the 4-second
// mark as that is the next tick. If it takes longer than one 2-sec tick for HealthCheck to reply,
// ns_server immediately considers the node "unhealthy," but a node needs to *remain* continuously
// unhealthy for the full Autofailover threshold time set by the user (default 120 sec but can be
// reduced to as little as 5 sec) before ns_server will actually trigger an Autofailover.
//
// Returns
//   diskFailures -- ever-increasing count of disk read/write failures Index (including Plasma and
//      MOI) have detected since boot. ns_server uses another Autofailover user-setting to determine
//      whether Index needs to be failed over based on sustained disk failures.
func (m *AutofailoverServiceManager) HealthCheck() (diskFailures uint64) {
	const method = "AutofailoverServiceManager::HealthCheck:" // for logging
	logging.Infof("%v Called", method)

	// CPU throttling is only used when Autofailover is enabled
	m.cpuThrottle.SetCpuThrottling(true)
	m.ctExpirer.resetCpuThrottleExpiry()

	diskFailures = m.getDiskFailures()
	logging.Infof("%v Returning diskFailures: %v", method, diskFailures)
	return diskFailures
}

// IsAutofailoverSafe will be called by ns_server on a random healthy Index node if it wants to
// Autofailover a set of unhealthy Index nodes and the safety decision is not being made by
// co-located KV. "Safe" means no indexes or partitions would be lost by failing over all the
// specified nodes, as they all have at least one replica on another Index node not being failed
// over. This uses a custom /getCachedIndexTopology loopback REST call to obtain the most recently
// memory cached index topology of all Index nodes. This check can be affected by stale cache.
//
// Inputs
//   nodeUUIDs -- UUIDs of the nodes proposed to be failed over
//
// Returns
//   error -- nil if failover is safe, non-nil if it is unsafe. If unsafe the value can either be an
//     actual error or, if none, a user-readable message indicating what index partitions would be
//     lost by this failover. In the unsafe case, ns_server will pass this to the UI to aid the user
//     in making a decision whether to manually failover.
func (m *AutofailoverServiceManager) IsAutofailoverSafe(nodeUUIDs []string) (error error) {
	const method = "AutofailoverServiceManager::IsAutofailoverSafe:" // for logging
	logging.Infof("%v Called with nodeUUIDs %v", method, nodeUUIDs)

	statusResp, err := m.getLastKnownIndexTopology()
	if err != nil {
		error = fmt.Errorf("%v Error retrieving current index topology: %v",
			method, err.Error())
		logging.Errorf("%v Returning error: %v", method, error)
		return error
	}
	indexStatuses := statusResp.Status // detailed status of index topology for all nodes

	// Create set of nodes proposed to be failed over
	failoverNodeUUIDs := make(map[string]struct{})
	for _, nodeUUID := range nodeUUIDs {
		failoverNodeUUIDs[nodeUUID] = struct{}{}
	}

	// Create map from nodeUUID to host:index_http_port for userMessage.
	// Also separate the statuses of kept vs to-be-failed-over nodes.
	nodeToHostMap := make(map[string]string)
	keepStatuses := make([]*manager.IndexStatus, 0)
	failoverStatuses := make([]*manager.IndexStatus, 0)
	numStatuses := len(indexStatuses)
	for statusIdx := 0; statusIdx < numStatuses; statusIdx++ { // avoid range; it would make copies
		indexStatusPtr := &indexStatuses[statusIdx]
		nodeToHostMap[indexStatusPtr.NodeUUID] = indexStatusPtr.Hosts[0]
		if _, member := failoverNodeUUIDs[indexStatusPtr.NodeUUID]; member {
			failoverStatuses = append(failoverStatuses, indexStatusPtr)
		} else {
			keepStatuses = append(keepStatuses, indexStatusPtr)
		}
	}

	// Create maps of all bucket.scope.collection.name to partition #s on keep vs failover nodes
	keepPartns := mapifyTopology(keepStatuses)
	failoverPartns := mapifyTopology(failoverStatuses)

	// For each bucket.scope.collection.name partn # to be failed over, check if it also exists on a
	// kept node. If not, failover is not safe so add its info to the userMessage.
	var bld strings.Builder // for userMessage return
	for failIdxKey, failPartnSet := range failoverPartns {
		// failoverPartns: map[string]map[int]struct{}
		// failIdxKey: string "bucket.scope.collection.name"
		// failPartnSet: map[int]struct{}
		keepPartnSet := keepPartns[failIdxKey]
		lostOneYet := false // lost any partitions of the current index (failIdxKey) yet?
		for failPartNum := range failPartnSet {
			lost := false // will this partition be lost?
			if keepPartnSet == nil {
				lost = true
			} else if _, member := keepPartnSet[failPartNum]; !member {
				lost = true
			}
			if lost {
				if bld.Len() == 0 { // add initial boilerplate
					bld.WriteString("Failing over nodes")
					for _, nodeUUID := range nodeUUIDs {
						fmt.Fprintf(&bld, " %v(%v)",
							manager.Key2host(nodeToHostMap[nodeUUID]), nodeUUID)
					}
					bld.WriteString(" would lose the following indexes/partitions:")
				}
				if !lostOneYet { // add fully qualified index name
					lostOneYet = true
					fmt.Fprintf(&bld, " %v", failIdxKey)
				}
				fmt.Fprintf(&bld, " %v", failPartNum) // add partn #
			}
		}
	}

	if bld.Len() > 0 { // unsafe; not really an error
		error = fmt.Errorf(bld.String())
	}
	logging.Infof("%v Returning user message: %v", method, error)
	return error
}
