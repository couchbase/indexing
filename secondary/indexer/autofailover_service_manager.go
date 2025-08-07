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
	"time"

	"github.com/couchbase/cbauth/service"
	// "github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// AutofailoverServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// AutofailoverServiceManager provides the implementation of the ns_server RPC interface
// AutofailoverManager (defined in cbauth/service/interface.go).
type AutofailoverServiceManager struct {
	cpuThrottle *CpuThrottle        // CPU throttler
	ctExpirer   *cpuThrottleExpirer // CPU throttle automatic expirer when Autofailover is off
	httpAddr    string              // local host:port for HTTP: "127.0.0.1:9102", 9108, ...

	lastHealthCheckMux  sync.Mutex // mutex for lastHealthCheckTime
	lastHealthCheckTime time.Time  // last time HealthCheck was called

	lastStatusLogMux  sync.Mutex // mutex for lastStatusLogTime
	lastStatusLogTime time.Time  // last time isSafeLogDetails logged index statuses
}

// NewAutofailoverServiceManager is the constructor for the AutofailoverServiceManager class.
// httpAddr gives the host:port of the local node for Index Service HTTP calls.
func NewAutofailoverServiceManager(httpAddr string, cpuThrottle *CpuThrottle) *AutofailoverServiceManager {
	m := &AutofailoverServiceManager{
		cpuThrottle: cpuThrottle,
		ctExpirer:   newCpuThrottleExpirer(cpuThrottle),
		httpAddr:    httpAddr,
	}
	return m
}

// getLastKnownIndexTopology sends a loopback HTTP REST call to the local Index service to get the
// last known index topology of all Index nodes.
//
// cached: true -- For speed this uses getCachedIndexTopology which was custom coded for this
//   purpose. It gets all topology info from local memory cache and does not use ClusterInfoCache
//   which triggers a scatter-gather in ns_server and also has a mutex that can be held for long
//   periods.
// cached: false -- Does a regular getIndexStatus call, which uses ClusterInfoCache to get current
//   set of Index nodes and contacts them all to get latest index statuses, only serving out of
//   cache for nodes that do not respond or when a node replies with HTTP code 304 Not Modified.
func (m *AutofailoverServiceManager) getLastKnownIndexTopology(cached bool) ([]IndexStatus, error) {
	const _getLastKnownIndexTopology = "AutofailoverServiceManager::getLastKnownIndexTopology:"

	var url string
	if cached {
		url = m.httpAddr + "/getCachedIndexTopology"
	} else {
		url = m.httpAddr + "/getIndexStatus?getAll=true&omitScheduled=true"
	}
	resp, err := getWithAuth(url)
	if err != nil {
		logging.Errorf("%v Error getting index topology. url: %v, err: %v", _getLastKnownIndexTopology, url, err)
		return nil, err
	}

	defer resp.Body.Close()
	statusResp := new(IndexStatusResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &statusResp); err != nil {
		logging.Errorf("%v Error unmarshaling index topology. url: %v, err: %v",
			_getLastKnownIndexTopology, url, err)
		return nil, err
	}

	return statusResp.Status, nil
}

// mapifyTopology takes a slice of (non-consolidated) index statuses and creates a map from each
// bucket:scope:collection:index described in them to sets of all partition numbers of each that are
// listed in the statuses. (Non-partitioned indexes always have one "partition" numbered 0.
// Partitioned indexes have 1-based partn #s.) This tells us the totality of all index partitions
// (both masters and replicas) hosted on all the nodes described by the slice of statuses.
func mapifyTopology(indexStatuses []*IndexStatus) map[string]map[int]struct{} {
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
const CPU_THROTTLE_EXPIRY_SEC = 300

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

// isSafeLogDetails runs as a goroutine helper for IsSafe to log the cached index statuses that
// method got and also the (possibly much slower to get) current, retrieved index statuses, which
// will attempt to retrieve the latest from all the available Index nodes.
func (m *AutofailoverServiceManager) isSafeLogDetails(indexStatusesCached []IndexStatus) {
	const _isSafeLogDetails = "AutofailoverServiceManager::isSafeLogDetails:"

	// Only log if last time was long ago as the messages, esp indexStatusesCurrent, can be huge,
	// and if an Autofailover attempt is deemed unsafe, ns_server keeps retrying it every 2 seconds
	callTime := time.Now()
	m.lastStatusLogMux.Lock()
	if callTime.Sub(m.lastStatusLogTime) < 30*time.Minute {
		m.lastStatusLogMux.Unlock()
		return
	}
	m.lastStatusLogTime = callTime
	m.lastStatusLogMux.Unlock()

	var bytes []byte // JSON encoding of index statuses

	bytes, err := json.Marshal(indexStatusesCached)
	if err != nil {
		logging.Errorf("%v Error marshaling indexStatusesCached: %v", _isSafeLogDetails, err)
	} else {
		logging.Infof("%v Cached index statuses: %v", _isSafeLogDetails, string(bytes))
	}

	// Do full index status retrieval (not just from local cache)
	indexStatusesCurrent, err := m.getLastKnownIndexTopology(false)
	if err != nil {
		logging.Errorf("%v Error retrieving current index topology: %v", _isSafeLogDetails, err)
	} else {
		bytes, err = json.Marshal(indexStatusesCurrent)
		if err != nil {
			logging.Errorf("%v Error marshaling indexStatusesCurrent: %v", _isSafeLogDetails, err)
		} else {
			logging.Infof("%v Current index statuses: %v", _isSafeLogDetails, string(bytes))
		}
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
// Returns the following field populated in HealthInfo:
//   DiskFailures -- ever-increasing count of disk read/write failures Index (including Plasma and
//      MOI) have detected since boot
func (m *AutofailoverServiceManager) HealthCheck() (*service.HealthInfo, error) {
	const _HealthCheck = "AutofailoverServiceManager::HealthCheck:"

	callTime := time.Now()
	m.lastHealthCheckMux.Lock()
	priorTime := m.lastHealthCheckTime
	m.lastHealthCheckTime = callTime
	m.lastHealthCheckMux.Unlock()

	// CPU throttling is only used when Autofailover is enabled
	m.cpuThrottle.SetCpuThrottling(true)
	m.ctExpirer.resetCpuThrottleExpiry()

	healthInfo := &service.HealthInfo{
		// DiskFailures: int(iowrap.GetDiskFailures()),
		DiskFailures: 0, // TODO Replace this with the prior commented-out line for release. This is
		// meant to be disabled until 100% of the work is complete so error counting is released
		// all-or-nothing, not piecemeal.
	}

	// To avoid log flooding, only log slow heartbeats or calls. Heartbeat period is 2 seconds.
	if !priorTime.IsZero() {
		dur := callTime.Sub(priorTime) // time since prior call
		if dur >= 2100*time.Millisecond {
			logging.Warnf("%v Slow heartbeat %v. priorTime: %v, callTime: %v, healthInfo: %+v",
				_HealthCheck, dur, priorTime, callTime, *healthInfo)
		}
	}
	doneTime := time.Now()
	dur := doneTime.Sub(callTime) // time spent in this call
	if dur >= 100*time.Millisecond {
		logging.Warnf("%v Slow call %v. callTime: %v, doneTime: %v, healthInfo: %+v",
			_HealthCheck, dur, callTime, doneTime, *healthInfo)
	}
	return healthInfo, nil
}

// IsSafe will be called by ns_server on a random healthy Index node if it wants to
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
func (m *AutofailoverServiceManager) IsSafe(nodeUUIDs []service.NodeID) (error error) {
	const _IsSafe = "AutofailoverServiceManager::IsSafe:"
	logging.Infof("%v Called with nodeUUIDs %v", _IsSafe, nodeUUIDs)

	indexStatuses, err := m.getLastKnownIndexTopology(true)
	if err != nil {
		error = fmt.Errorf("%v Error retrieving current index topology: %v", _IsSafe, err)
		logging.Errorf("%v Returning error: %v", _IsSafe, error)
		return error
	}

	// Create set of nodes proposed to be failed over
	failoverNodeUUIDs := make(map[service.NodeID]struct{})
	for _, nodeUUID := range nodeUUIDs {
		failoverNodeUUIDs[nodeUUID] = struct{}{}
	}

	// Create map from nodeUUID to host:index_http_port for userMessage.
	// Also separate the statuses of kept vs to-be-failed-over nodes.
	nodeToHostMap := make(map[service.NodeID]string)
	keepStatuses := make([]*IndexStatus, 0)
	failoverStatuses := make([]*IndexStatus, 0)
	numStatuses := len(indexStatuses)
	for statusIdx := 0; statusIdx < numStatuses; statusIdx++ { // avoid range; it would make copies
		indexStatusPtr := &indexStatuses[statusIdx]
		nodeToHostMap[service.NodeID(indexStatusPtr.NodeUUID)] = indexStatusPtr.Hosts[0]
		if _, member := failoverNodeUUIDs[service.NodeID(indexStatusPtr.NodeUUID)]; member {
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
							Key2host(nodeToHostMap[nodeUUID]), nodeUUID)
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
	logging.Infof("%v Returning user message: %v", _IsSafe, error)
	go m.isSafeLogDetails(indexStatuses) // async log both cached and current index statuses
	return error
}
