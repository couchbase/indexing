// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

//
// Iternal version:
// Services like indexer can keep track of cluster version using ns_server
// cluster APIs. But those APIs can be used to get only the major version
// and the minor version of the release. The patch version is not a part
// of existing ns_server API.
//
// Indexer service can introduce minor fuctionalities in the patch releases
// given the severity and priority. These functionalities sometimes require
// cluster wide version check before they can be safely enabled.
//
// Internal version framework provides the ability to perform cluster version
// check for the patch releases as well.
//
// Notes:
// - The internal version is a string reprsenting the ns_server's version
//   string. For example, string "7.0.1" means, major verion 7, minor
//   version 0 and patch version 1.
// - Internal version for indexing service is hardcoded to the same value
//   as that of ns_server value.
//   -  This needs to change if ns_server naming convention changes in
//      the future.
// - Internal version needs to be updated only when new features or
//   functionalities (which require cluster wide version check) are being
//   added in a patch release.
//

//------------------------------------------------------------
// Constants
//------------------------------------------------------------

const localVersion = "7.1.2"

const MIN_VER_STD_GSI_EPHEMERAL = "7.0.2"

const MIN_VER_SRV_AUTH = "7.0.4"

const MIN_VER_MISSING_LEADING_KEY = "7.1.2"

const ENABLE_INT_VER_TICKER = false

const INT_VER_TICKER_INTERVAL = 30 // Seconds

//------------------------------------------------------------
// Types
//------------------------------------------------------------

type InternalVersion string

type InternalVersionJson struct {
	Version string `json:"version,omitempty"`
}

//
// Couchbase nodes can transition from active -> failed -> active any
// number of times. While node is failed, it may not respond to the
// rest call for getting internal version. So, last known value of
// internal version will be cached and used for failed nodes.
//
type internalVersionCache struct {
	lck  sync.RWMutex
	cmap map[string]InternalVersion // node uuid -> last known internal version
}

//
// nodeList encapsulates list of nodes by type, needed by internal
// version checker.
//
type nodeList struct {

	// node uuid -> node object

	idxs   map[string]couchbase.Node
	fidxs  map[string]couchbase.Node
	kvs    map[string]couchbase.Node
	fkvs   map[string]couchbase.Node
	n1qls  map[string]couchbase.Node
	fn1qls map[string]couchbase.Node
}

//
// internalVersionChecker is responsible for getting internal version
// of all nodes of the specified type.
//
type internalVersionChecker struct {
	ninfo            NodesInfoProvider
	svcs             []string
	checkFailedNodes bool
	lck              sync.Mutex
	wg               sync.WaitGroup
	versions         []InternalVersion
	errors           map[string]error
	svcPortMap       map[string]string
	svcUrlMap        map[string]string
	cache            *internalVersionCache
}

//
// internalVersionMonitor periodically checks for version upgrades
// and stops monitoring when expected version is reached.
//
type internalVersionMonitor struct {
	ninfo        NodesInfoProvider
	cache        *internalVersionCache
	termVer      int64
	termIntVer   InternalVersion
	urlStr       string
	param        *security.RequestParams
	tickerCh     chan bool
	tickerStopCh chan bool
	notifCh      chan bool
	notifStopCh  chan bool
}

//------------------------------------------------------------
// Global Variables
//------------------------------------------------------------

//
// intClustVer maintains the last internal cluster version
// observed by the internal version monitor.
// intClustVerPtr holds the pointer to intClustVer.
//

var intClustVer InternalVersion = InternalVersion("")

var intClustVerPtr unsafe.Pointer = (unsafe.Pointer)(&intClustVer)

//------------------------------------------------------------
// Constructors
//------------------------------------------------------------

func newInternalVersionCache() *internalVersionCache {
	return &internalVersionCache{
		cmap: make(map[string]InternalVersion),
	}
}

func newNodeList() *nodeList {
	return &nodeList{
		idxs:   make(map[string]couchbase.Node),
		fidxs:  make(map[string]couchbase.Node),
		kvs:    make(map[string]couchbase.Node),
		fkvs:   make(map[string]couchbase.Node),
		n1qls:  make(map[string]couchbase.Node),
		fn1qls: make(map[string]couchbase.Node),
	}
}

func newInternalVersionChecker(
	ninfo NodesInfoProvider,
	svcs []string,
	checkFailedNodes bool,
	cache *internalVersionCache) *internalVersionChecker {

	ivc := &internalVersionChecker{
		ninfo:            ninfo,
		svcs:             svcs,
		checkFailedNodes: checkFailedNodes,
		errors:           make(map[string]error),
		versions:         make([]InternalVersion, 0),
		cache:            cache,
	}

	ivc.svcPortMap = map[string]string{
		"index": INDEX_HTTP_SERVICE,
		"kv":    INDEX_PROJECTOR,
		"n1ql":  CBQ_SERVICE,
	}

	ivc.svcUrlMap = map[string]string{
		"index": "/getInternalVersion",
		"kv":    "/getInternalVersion",
		"n1ql":  "/gsi/getInternalVersion",
	}

	return ivc
}

func newInternalVersionMonitor(
	ninfo NodesInfoProvider,
	termVer int64,
	termIntVer InternalVersion,
	clusterAddr string) *internalVersionMonitor {

	mon := &internalVersionMonitor{
		ninfo:        ninfo,
		termVer:      termVer,
		termIntVer:   termIntVer,
		tickerCh:     make(chan bool),
		tickerStopCh: make(chan bool),
		notifCh:      make(chan bool),
		notifStopCh:  make(chan bool),
	}

	mon.cache = newInternalVersionCache()

	mon.urlStr = fmt.Sprintf("http://%s/poolsStreaming/%s", clusterAddr, DEFAULT_POOL)
	mon.param = &security.RequestParams{
		UserAgent: "internalVersionMonitor",
	}

	go mon.ticker()
	go mon.notifier()

	return mon
}

//------------------------------------------------------------
// Member functions: InternalVersion
//------------------------------------------------------------

func (one InternalVersion) LessThan(other InternalVersion) bool {
	// TODO: Do we need format validation?

	return one < other
}

func (iv InternalVersion) String() string {
	return string(iv)
}

func (one InternalVersion) Equals(other InternalVersion) bool {

	return one == other
}

func (one InternalVersion) GreaterThan(other InternalVersion) bool {

	return one > other
}

//------------------------------------------------------------
// Member functions: internalVersionCache
//------------------------------------------------------------

func (cache *internalVersionCache) Get(key string) InternalVersion {
	cache.lck.RLock()
	defer cache.lck.RUnlock()

	return cache.cmap[key]
}

func (cache *internalVersionCache) Put(key string, val InternalVersion) {
	cache.lck.Lock()
	defer cache.lck.Unlock()

	cache.cmap[key] = val
}

//------------------------------------------------------------
// Member functions: internalVersionChecker
//------------------------------------------------------------

func (ivc *internalVersionChecker) getNodes() *nodeList {

	nodes := newNodeList()

	populate := func(nodeList []couchbase.Node, nodeMap map[string]couchbase.Node) {
		for _, node := range nodeList {
			nodeMap[node.NodeUUID] = node
		}
	}

	for _, svc := range ivc.svcs {

		switch svc {

		case "index":
			populate(ivc.ninfo.GetNodesByServiceType(INDEX_HTTP_SERVICE), nodes.idxs)
			if ivc.checkFailedNodes {
				populate(ivc.ninfo.GetFailedIndexerNodes(), nodes.fidxs)
			}

		case "kv":
			populate(ivc.ninfo.GetNodesByServiceType(KV_SERVICE), nodes.kvs)
			if ivc.checkFailedNodes {
				populate(ivc.ninfo.GetFailedKVNodes(), nodes.fkvs)
			}

		case "n1ql":
			populate(ivc.ninfo.GetNodesByServiceType(CBQ_SERVICE), nodes.n1qls)
			if ivc.checkFailedNodes {
				populate(ivc.ninfo.GetFailedQueryNodes(), nodes.fn1qls)
			}
		}
	}

	return nodes
}

func (ivc *internalVersionChecker) getNodeVersion(node couchbase.Node, svc string) {

	defer ivc.wg.Done()

	setError := func(err error) {
		ivc.lck.Lock()
		defer ivc.lck.Unlock()

		logging.Errorf("%v", err)
		ivc.errors[node.NodeUUID] = err
	}

	nid, found := ivc.ninfo.GetNodeIdByUUID(node.NodeUUID)
	if !found {
		err := fmt.Errorf("internalVersionChecker:getNodeVersion nid for %v not found", node.NodeUUID)
		setError(err)
		return
	}

	addr, err := ivc.ninfo.GetServiceAddress(nid, ivc.svcPortMap[svc], true)
	if err != nil {
		err := fmt.Errorf("internalVersionChecker:getNodeVersion error in GetServiceAddress for %v", node.NodeUUID)
		setError(err)
		return
	}

	url := addr + ivc.svcUrlMap[svc]
	params := &security.RequestParams{Timeout: 120 * time.Second}
	response, err := security.GetWithAuth(url, params)
	if err != nil {
		err1 := fmt.Errorf("internalVersionChecker:getNodeVersion error (%v) in GetWithAuth for %v", err, node.NodeUUID)
		setError(err1)
		return
	}

	defer response.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(response.Body); err != nil {
		err1 := fmt.Errorf("internalVersionChecker:getNodeVersion error %v in reading response from node %v", err, node.NodeUUID)
		setError(err1)
		return
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
		err := fmt.Errorf("internalVersionChecker:getNodeVersion error %v for node %v", response.StatusCode, node.NodeUUID)
		setError(err)
		return
	}

	if response.StatusCode == http.StatusNotFound {
		ivc.lck.Lock()
		defer ivc.lck.Unlock()

		ivc.versions = append(ivc.versions, InternalVersion(""))
		logging.Verbosef("internalVersionChecker:getNodeVersion status code for node %v is StatusNotFound", node.NodeUUID)

		if ivc.cache != nil {
			ivc.cache.Put(node.NodeUUID, InternalVersion(""))
		}

		return
	}

	var ver InternalVersion
	if ver, err = UnmarshalInternalVersion(buf.Bytes()); err != nil {
		err1 := fmt.Errorf("internalVersionChecker:getNodeVersion error %v in unmarshalling response from node %v", err, node.NodeUUID)
		setError(err1)
		return
	}

	logging.Verbosef("internalVersionChecker:getNodeVersion got version %v for node %v", ver, node.NodeUUID)

	ivc.lck.Lock()
	defer ivc.lck.Unlock()

	ivc.versions = append(ivc.versions, ver)

	if ivc.cache != nil {
		ivc.cache.Put(node.NodeUUID, ver)
	}
}

func (ivc *internalVersionChecker) getVersion() (InternalVersion, error) {

	ivc.ninfo.RLock()
	defer ivc.ninfo.RUnlock()

	nodes := ivc.getNodes()

	ivc.startWorkers(nodes)

	ivc.wg.Wait()

	if len(ivc.errors) != 0 {
		for nodeUUID, err := range ivc.errors {
			if ivc.cache != nil {
				ver := ivc.cache.Get(nodeUUID)
				logging.Verbosef("internalVersionChecker:getVersion using cached "+
					"version due to error for node %v, ver %v", nodeUUID, ver)
				ivc.versions = append(ivc.versions, ver)
				continue
			}

			return InternalVersion(""), err
		}
	}

	min := InternalVersion(localVersion)
	for _, ver := range ivc.versions {
		if ver.LessThan(min) {
			min = ver
		}
	}

	fvers := ivc.getVerFailNodes(nodes)

	for _, fver := range fvers {
		if fver.LessThan(min) {
			min = fver
		}
	}

	logging.Verbosef("internalVersionChecker:getVersion version %v", min)

	return min, nil
}

func (ivc *internalVersionChecker) startWorkers(nodes *nodeList) {

	// Get versions for active indexers
	for _, node := range nodes.idxs {
		ivc.wg.Add(1)

		go ivc.getNodeVersion(node, "index")
	}

	// Get versions for active kv nodes
	for _, node := range nodes.kvs {
		if _, ok := nodes.idxs[node.NodeUUID]; ok {
			continue
		}

		ivc.wg.Add(1)

		go ivc.getNodeVersion(node, "kv")
	}

	// Get versions for active n1ql nodes
	for _, node := range nodes.n1qls {
		if _, ok := nodes.idxs[node.NodeUUID]; ok {
			continue
		}

		if _, ok := nodes.kvs[node.NodeUUID]; ok {
			continue
		}

		ivc.wg.Add(1)

		go ivc.getNodeVersion(node, "n1ql")
	}
}

func (ivc *internalVersionChecker) getVerFailNodes(nodes *nodeList) []InternalVersion {

	versions := make([]InternalVersion, 0)

	// Get versions for failed indexers
	for _, node := range nodes.fidxs {
		var ver = InternalVersion("")
		if ivc.cache != nil {
			ver = ivc.cache.Get(node.NodeUUID)
			logging.Verbosef("internalVersionChecker:getVerFailNodes using cached "+
				"version for failed node %v, ver %v", node.NodeUUID, ver)
		}

		versions = append(versions, ver)
	}

	// Get versions for failed kv nodes
	for _, node := range nodes.fkvs {
		if _, ok := nodes.fidxs[node.NodeUUID]; ok {
			continue
		}

		var ver = InternalVersion("")
		if ivc.cache != nil {
			ver = ivc.cache.Get(node.NodeUUID)
			logging.Verbosef("internalVersionChecker:getVerFailNodes using cached "+
				"version for failed node %v, ver %v", node.NodeUUID, ver)
		}

		versions = append(versions, ver)
	}

	// Get versions for failed kv nodes
	for _, node := range nodes.fn1qls {
		if _, ok := nodes.fidxs[node.NodeUUID]; ok {
			continue
		}

		if _, ok := nodes.fkvs[node.NodeUUID]; ok {
			continue
		}

		var ver = InternalVersion("")
		if ivc.cache != nil {
			ver = ivc.cache.Get(node.NodeUUID)
			logging.Verbosef("internalVersionChecker:getVerFailNodes using cached "+
				"version for failed node %v, ver %v", node.NodeUUID, ver)
		}
		versions = append(versions, ver)

	}

	return versions
}

//------------------------------------------------------------
// Member functions: internalVersionMonitor
//------------------------------------------------------------

func (mon *internalVersionMonitor) getVersion() InternalVersion {

	var err error
	func() {
		mon.ninfo.Lock()
		defer mon.ninfo.Unlock()

		err = mon.ninfo.FetchNodesAndSvsInfo()
		if err != nil {
			logging.Errorf("internalVersionMonitor:monitor unexpected error in FetchNodesAndSvsInfo %v", err)
		}
	}()
	if err != nil {
		return GetInternalVersion()
	}

	over := GetInternalVersion()
	ivc := newInternalVersionChecker(mon.ninfo, []string{"index", "kv", "n1ql"}, true, mon.cache)
	ver, err := ivc.getVersion()
	if err != nil {
		logging.Errorf("internalVersionMonitor:monitor error in getVersion %v", err)
		return GetInternalVersion()
	}

	if over.LessThan(ver) {
		setInternalVersion(ver)
	}

	return GetInternalVersion()
}

func (mon *internalVersionMonitor) monitor() {
	//
	// Is sleep necessary before starting the monitor ?
	// time.Sleep(10 * time.Second)
	//

	logging.Infof("internalVersionMonitor:monitor starting. Term versions (%v:%v)", mon.termVer, mon.termIntVer)

	processEvent := func() bool {
		clustVer := GetClusterVersion()
		if clustVer >= mon.termVer {
			logging.Infof("internalVersionMonitor:monitor terminate. Cluster version reached %v", clustVer)
			return true
		}

		ver := mon.getVersion()
		if !ver.LessThan(mon.termIntVer) {
			logging.Infof("internalVersionMonitor:monitor terminate. Internal version reached %v", ver)
			return true
		}

		return false
	}

	defer mon.close()

	for {
		select {

		// TODO: Restructure this code to avoid duplication
		case _, ok := <-mon.tickerCh:
			if !ok {
				return
			}

			terminate := processEvent()
			if terminate {
				return
			}

		case _, ok := <-mon.notifCh:
			if !ok {
				return
			}

			terminate := processEvent()
			if terminate {
				return
			}
		}
	}
}

func (mon *internalVersionMonitor) waitForChange(res *http.Response, reader *bufio.Reader) error {

	var p couchbase.Pool

	for {
		bs, err := reader.ReadBytes('\n')
		if err != nil {
			logging.Errorf("internalVersionMonitor:waitForChange Error while reading body, err: %v", err)
			return err
		}

		if len(bs) == 1 && bs[0] == '\n' {
			continue
		}

		err = json.Unmarshal(bs, &p)
		if err != nil {
			logging.Errorf("internalVersionMonitor:waitForChange Error while unmarshalling pools, bs: %s, err: %v", bs, err)
			return err
		}

		logging.Verbosef("internalVersionMonitor:waitForChange received pool change")

		return nil
	}
}

func (mon *internalVersionMonitor) ticker() {
	// TODO: Make ticker configurable.

	tick := time.NewTicker(INT_VER_TICKER_INTERVAL * time.Second)

	logging.Infof("internalVersionMonitor:ticker starting ...")

	defer tick.Stop()

	for {
		select {

		case <-tick.C:
			logging.Verbosef("internalVersionMonitor:ticker ... ")
			if !ENABLE_INT_VER_TICKER {
				logging.Verbosef("internalVersionMonitor:ticker disabled")
			}

			mon.tickerCh <- true

		case <-mon.tickerStopCh:
			logging.Infof("internalVersionMonitor:ticker stopping ...")
			return
		}
	}
}

func (mon *internalVersionMonitor) notifier() {

	selfRestart := func() {
		time.Sleep(1 * time.Second)
		go mon.notifier()
		return
	}

	// Start Pool streaming
	res, err := security.GetWithAuthNonTLS(mon.urlStr, mon.param)
	if err != nil {
		logging.Errorf("internalVersionMonitor:notifier Error while getting with auth, err: %v", err)
		selfRestart()
		return
	}

	if res.StatusCode != 200 {
		// Not reading to EOF before close as streaming API does not end with EOF in all cases
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()

		logging.Errorf("internalVersionMonitor:notifier HTTP error %v getting %q: %s", res.Status, mon.urlStr, bod)
		selfRestart()
		return
	}

	// Not reading to EOF before close as streaming API does not end with EOF in all cases
	defer res.Body.Close()

	logging.Infof("internalVersionMonitor:notifier starting ...")
	reader := bufio.NewReader(res.Body)

	for {
		select {

		case <-mon.notifStopCh:
			logging.Infof("internalVersionMonitor:notifier stopping ...")
			return

		default:
			err := mon.waitForChange(res, reader)
			if err != nil {
				selfRestart()
				return
			}

			mon.notifCh <- true
		}
	}
}

func (mon *internalVersionMonitor) close() {
	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("internalVersionMonitor:close recovered %v", r)
		}
	}()

	close(mon.notifStopCh)
	close(mon.tickerStopCh)
}

//------------------------------------------------------------
// APIs and utility functions
//------------------------------------------------------------
func GetLocalInternalVersion() InternalVersion {

	return InternalVersion(localVersion)
}

func GetMarshalledInternalVersion() ([]byte, error) {
	iv := &InternalVersionJson{
		Version: localVersion,
	}

	return json.Marshal(&iv)
}

func UnmarshalInternalVersion(data []byte) (InternalVersion, error) {
	iv := &InternalVersionJson{}

	err := json.Unmarshal(data, iv)
	if err != nil {
		return InternalVersion(""), err
	}

	return InternalVersion(iv.Version), nil
}

//
// GetInternalClusterVersion gets the internal version for the entire GSI cluster.
// The version check considers all gsi services ie. indexer, projector and gsi n1ql client.
//
func GetInternalClusterVersion(ninfo NodesInfoProvider, checkFailedNodes bool) (InternalVersion, error) {

	ivc := newInternalVersionChecker(ninfo, []string{"index", "kv", "n1ql"}, checkFailedNodes, nil)
	return ivc.getVersion()
}

//
// GetInternalIndexerVersion gets the internal version for all the indexer nodes.
// It ignores the projector version and gsi n1ql client version.
//
func GetInternalIndexerVersion(ninfo NodesInfoProvider, checkFailedNodes bool) (InternalVersion, error) {

	ivc := newInternalVersionChecker(ninfo, []string{"index"}, checkFailedNodes, nil)
	return ivc.getVersion()
}

func GetInternalVersion() InternalVersion {
	return *(*InternalVersion)(atomic.LoadPointer(&intClustVerPtr))
}

func setInternalVersion(iv InternalVersion) {
	atomic.StorePointer(&intClustVerPtr, unsafe.Pointer(&iv))
}

func MonitorInternalVersion(
	termVer int64,
	termIntVer InternalVersion,
	clusterAddr string) {

	cinfo, err := FetchNewClusterInfoCache(clusterAddr, DEFAULT_POOL, "MonitorInternalVersion")
	if err != nil {
		logging.Fatalf("MonitorInternalVersion: error %v in GetNodesInfoProvider", err)
		return
	}

	ninfo := NodesInfoProvider(cinfo)

	mon := newInternalVersionMonitor(ninfo, termVer, termIntVer, clusterAddr)

	go mon.monitor()
}
