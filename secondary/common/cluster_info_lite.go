package common

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common/collections"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

/*
1.  nodesInfo, collectionInfo, bucketInfo are the data structures that are formed
    after getting data from ns_server
2.  ClusterInfoCacheLite will have all the data cached in atomic holders. Pointers
    to above data are updated atomically on update using these holders.
3.  ClusterInfoCacheLiteManager will watch the streaming endpoints and the clients
    and update the cache, by fetching again from ns_server if needed.
4.  ClusterInfoCacheLiteClient will provide all the APIs to access and use the
    data which can have some customization at user level if needed.
5.  Indices to data like NodeId can become invalid on update. So they must not
    be used across multiple instances. Eg: GetNodeInfo will give us a nodeInfo
    pointer. nodeInfo.GetNodesByServiceType will give us NodeIds these should not
    be used with another instance of nodeInfo fetched again later.
*/

var singletonCICLContainer struct {
	sync.Mutex
	ciclMgr  *clusterInfoCacheLiteManager
	refCount int // RefCount of ciclMgr to close it when it is 0
}

var majorVersionCICL uint32

const MAJOR_VERSION_7 = 7

func SetMajorVersionCICL(ver uint32) {
	logging.Infof("SetMajorVersionCICL: Setting cluster version to %v", ver)
	atomic.StoreUint32(&majorVersionCICL, ver)
}

func GetMajorVersionCICL() uint32 {
	return atomic.LoadUint32(&majorVersionCICL)
}

var ErrorEventWaitTimeout = errors.New("error event wait timeout")
var ErrorUnintializedNodesInfo = errors.New("error uninitialized nodesInfo")
var ErrorThisNodeNotFound = errors.New("error thisNode not found")
var ErrorSingletonCICLMgrNotFound = errors.New("singleton manager not found")

func SetCICLMgrTimeDiffToForceFetch(minutes uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	if mgr := singletonCICLContainer.ciclMgr; mgr != nil {
		mgr.setTimeDiffToForceFetch(minutes)
	} else {
		logging.Warnf("SetCICLMgrTimeDiffToForceFetch: Singleton Manager in ClusterInfoCacheLite is not set")
	}
}

func SetCICLMgrSleepTimeOnNotifierRestart(milliSeconds uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	if mgr := singletonCICLContainer.ciclMgr; mgr != nil {
		mgr.setNotifierRetrySleep(milliSeconds)
	} else {
		logging.Warnf("SetCICLMgrSleepTimeOnNotifierRestart: Singleton Manager in ClusterInfoCacheLite is not set")
	}
}

func GetCICLStats() (Statistics, error) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()

	s := make(map[string]interface{})
	mgr := singletonCICLContainer.ciclMgr
	if mgr == nil {
		s["ref_count"] = 0
		s["status"] = ErrorSingletonCICLMgrNotFound.Error()
		return NewStatistics(s)
	}

	s["ref_count"] = singletonCICLContainer.refCount
	s["status"] = "singleton manager running"
	// TODO: Add more stats later as needed
	return NewStatistics(s)
}

func HandleCICLStats(w http.ResponseWriter, r *http.Request) {
	_, valid, err := IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		audit.Audit(AUDIT_UNAUTHORIZED, r, "StatsManager::handleCICLStats", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(HTTP_STATUS_UNAUTHORIZED)
		return
	}

	if r.Method == "GET" {
		stats, err := GetCICLStats()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			errStr := fmt.Sprintf("error while retrieving stats: %v", err)
			w.Write([]byte(errStr))
		}

		data, err := stats.Encode()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			errStr := fmt.Sprintf("error while marshaling stats: %v", err)
			w.Write([]byte(errStr))
		}

		w.WriteHeader(http.StatusOK)
		w.Write(data)

	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported method"))
	}
}

//
// Nodes Info
//

type NodesInfo struct {
	version          uint32
	minorVersion     uint32
	nodes            []couchbase.Node
	nodesExt         []couchbase.NodeServices
	addNodes         []couchbase.Node
	failedNodes      []couchbase.Node
	encryptedPortMap map[string]string
	node2group       map[NodeId]string
	clusterURL       string
	bucketNames      []couchbase.BucketName
	bucketURLMap     map[string]string

	// Note: Static port information is populated with information from the
	// command line. This is only used to get the port information on local
	// node. This is needed as PoolChangeNotification does not have port
	// numbers specific to indexer till service manager register with ns_server
	// but we will need this port information before that registration in the
	// boot process
	useStaticPorts bool
	servicePortMap map[string]string

	valid         bool
	errList       []error
	lastUpdatedTs time.Time

	nodeServicesHash string

	StubRWMutex // Stub to make NodesInfo replaceable with ClusterInfoCache
}

func newNodesInfo(pool *couchbase.Pool, clusterURL, nodeSvsHash string) *NodesInfo {
	var nodes []couchbase.Node
	var failedNodes []couchbase.Node
	var addNodes []couchbase.Node
	version := uint32(math.MaxUint32)
	minorVersion := uint32(math.MaxUint32)

	for _, n := range pool.Nodes {
		if n.ClusterMembership == "active" {
			nodes = append(nodes, n)
		} else if n.ClusterMembership == "inactiveFailed" {
			// node being failed over
			failedNodes = append(failedNodes, n)
		} else if n.ClusterMembership == "inactiveAdded" {
			// node being added (but not yet rebalanced in)
			addNodes = append(addNodes, n)
		} else {
			logging.Warnf("newNodesInfo: unrecognized node membership %v", n.ClusterMembership)
		}

		// Find the minimum cluster compatibility
		v := uint32(n.ClusterCompatibility / 65536)
		minorv := uint32(n.ClusterCompatibility) - (v * 65536)
		if v < version || (v == version && minorv < minorVersion) {
			version = v
			minorVersion = minorv
		}
	}

	if version == math.MaxUint32 {
		version = 0
	}

	newNInfo := &NodesInfo{
		nodes:            nodes,
		addNodes:         addNodes,
		failedNodes:      failedNodes,
		version:          version,
		minorVersion:     minorVersion,
		clusterURL:       clusterURL,
		nodeServicesHash: nodeSvsHash,
		node2group:       make(map[NodeId]string),
	}

	for i, node := range nodes {
		newNInfo.node2group[NodeId(i)] = node.ServerGroup
	}

	if len(pool.BucketNames) != 0 {
		bucketNames := make([]couchbase.BucketName, len(pool.BucketNames))
		for i, bn := range pool.BucketNames {
			bucketNames[i] = bn
		}
		newNInfo.bucketNames = bucketNames
	}

	bucketURLMap := make(map[string]string)
	for k, v := range pool.BucketURL {
		bucketURLMap[k] = v
	}
	newNInfo.bucketURLMap = bucketURLMap

	if ServiceAddrMap != nil {
		newNInfo.useStaticPorts = true
		newNInfo.servicePortMap = ServiceAddrMap
	}

	return newNInfo
}

func newNodesInfoWithError(err error) *NodesInfo {
	ni := &NodesInfo{}
	ni.valid = false
	ni.errList = append(ni.errList, err)
	return ni
}

func (ni *NodesInfo) setNodesInfoWithError(err error) *NodesInfo {
	ni.valid = false
	ni.errList = append(ni.errList, err)
	return ni
}

// Clone will do shallow copy and hence will have new copy of mutable valid and
// errList fields and hence the *NodesInfo returned can be used to replace the
// value of nih in cicl atomically
func (ni *NodesInfo) clone() *NodesInfo {

	newNInfo := &NodesInfo{
		version:          ni.version,
		minorVersion:     ni.minorVersion,
		clusterURL:       ni.clusterURL,
		nodeServicesHash: ni.nodeServicesHash,
	}

	newNInfo.nodes = append(newNInfo.nodes, ni.nodes...)
	newNInfo.addNodes = append(newNInfo.addNodes, ni.addNodes...)
	newNInfo.failedNodes = append(newNInfo.failedNodes, ni.failedNodes...)
	newNInfo.bucketNames = append(newNInfo.bucketNames, ni.bucketNames...)

	newNInfo.node2group = make(map[NodeId]string)
	for i, node := range ni.nodes {
		newNInfo.node2group[NodeId(i)] = node.ServerGroup
	}

	newNInfo.bucketURLMap = make(map[string]string)
	for k, v := range ni.bucketURLMap {
		newNInfo.bucketURLMap[k] = v
	}

	if ServiceAddrMap != nil {
		newNInfo.useStaticPorts = true
		newNInfo.servicePortMap = ServiceAddrMap
	}

	return newNInfo
}

func (ni *NodesInfo) setNodesExt(nodesExt []couchbase.NodeServices) {
	if nodesExt == nil {
		logging.Warnf("setNodesExt: nodesExt is nil")
		return
	}
	for _, ns := range nodesExt {
		nns := couchbase.NodeServices{
			ThisNode: ns.ThisNode,
			Hostname: ns.Hostname,
			Services: make(map[string]int),
		}
		for k, v := range ns.Services {
			nns.Services[k] = v
		}
		ni.nodesExt = append(ni.nodesExt, nns)
	}
	ni.encryptedPortMap = buildEncryptPortMapping(ni.nodesExt)
}

func (ni *NodesInfo) validateNodesAndSvs(connHost string) {
	found := false
	for _, node := range ni.nodes {
		if node.ThisNode {
			found = true
		}
	}

	if !found {
		ni.valid = false
		ni.errList = append(ni.errList, ErrorThisNodeNotFound)
		logging.Warnf("ThisNode not found for any node in pool")
		return
	}

	if len(ni.nodes) == 0 || len(ni.nodesExt) == 0 {
		ni.valid = false
		ni.errList = append(ni.errList, ErrValidationFailed)
		return
	}

	//validation not required for single node setup(MB-16494)
	if len(ni.nodes) == 1 && len(ni.nodesExt) == 1 {
		ni.valid = true
		ni.lastUpdatedTs = time.Now()
		return
	}

	var hostsFromNodes []string
	var hostsFromNodesExt []string

	for _, n := range ni.nodes {
		hostsFromNodes = append(hostsFromNodes, n.Hostname)
	}

	for _, svc := range ni.nodesExt {
		h := svc.Hostname
		if h == "" {
			// 1. For nodeServices if the configured hostname is 127.0.0.1
			//    hostname is not emitted client should use the hostname
			//    it’s already using to access other ports for that node
			// 2. For pools/default if the configured hostname is 127.0.0.1
			//    hostname is emitted as the interface on which the
			//    pools/default request is received
			h, _, _ = net.SplitHostPort(connHost)
		}
		p := svc.Services["mgmt"]
		hp := net.JoinHostPort(h, fmt.Sprint(p))

		hostsFromNodesExt = append(hostsFromNodesExt, hp)
	}

	if len(ni.nodes) != len(ni.nodesExt) {
		logging.Warnf("validateNodesAndSvs - Failed as len(nodes): %v != len(nodesExt): %v", len(ni.nodes),
			len(ni.nodesExt))
		logging.Warnf("HostNames Nodes: %v NodesExt: %v", hostsFromNodes, hostsFromNodesExt)
		ni.valid = false
		ni.errList = append(ni.errList, ErrValidationFailed)
		return
	}

	for i, hn := range hostsFromNodesExt {
		if hostsFromNodes[i] != hn {
			logging.Warnf("validateNodesAndSvs - Failed as hostname in nodes: %s != the one from nodesExt: %s", hostsFromNodes[i], hn)
			ni.valid = false
			ni.errList = append(ni.errList, ErrValidationFailed)
			return
		}
	}

	ni.valid = true
	ni.lastUpdatedTs = time.Now()
	return
}

//
// Collection Info
//

type collectionInfo struct {
	bucketName string
	manifest   *collections.CollectionManifest

	valid         bool
	errList       []error
	lastUpdatedTs time.Time

	StubRWMutex // Stub to make CollectionInfo replaceable with ClusterInfoCache
}

func newCollectionInfo(bucketName string, manifest *collections.CollectionManifest) *collectionInfo {
	return &collectionInfo{
		bucketName:    bucketName,
		manifest:      manifest,
		valid:         true,
		lastUpdatedTs: time.Now(),
	}
}

func newCollectionInfoWithErr(bucketName string, err error) *collectionInfo {
	ci := &collectionInfo{
		bucketName: bucketName,
	}
	ci.valid = false
	ci.errList = append(ci.errList, err)
	return ci
}

//
// Bucket Info
//

type bucketInfo struct {
	bucket     *couchbase.Bucket
	clusterURL string

	valid         bool
	errList       []error
	lastUpdatedTs time.Time

	StubRWMutex // Stub to make BucketInfo replaceable with ClusterInfoCache
}

func newBucketInfo(tb *couchbase.Bucket, connHost string) *bucketInfo {
	bi := &bucketInfo{
		bucket: tb,
	}
	bi.bucket.NormalizeHostnames(connHost)

	bi.valid = true
	bi.lastUpdatedTs = time.Now()

	return bi
}

func newBucketInfoWithErr(bucketName string, err error) *bucketInfo {
	bi := &bucketInfo{
		bucket: &couchbase.Bucket{Name: bucketName},
	}
	bi.valid = false
	bi.errList = append(bi.errList, err)
	return bi
}

func (bi *bucketInfo) setClusterURL(u string) {
	bi.clusterURL = u
}

//
// PoolInfo
//

type PoolInfo struct {
	pool *couchbase.Pool
}

func newPoolInfo(p *couchbase.Pool) *PoolInfo {
	return &PoolInfo{pool: p}
}

//
// Cluster Info Cache Lite
//

type clusterInfoCacheLite struct {
	logPrefix string
	nih       nodesInfoHolder

	cihm     map[string]collectionInfoHolder
	cihmLock sync.RWMutex

	bihm     map[string]bucketInfoHolder
	bihmLock sync.RWMutex

	pih poolInfoHolder
}

func newClusterInfoCacheLite(logPrefix string) *clusterInfoCacheLite {

	c := &clusterInfoCacheLite{logPrefix: logPrefix}
	c.cihm = make(map[string]collectionInfoHolder)
	c.bihm = make(map[string]bucketInfoHolder)
	c.nih.Init()
	c.pih.Init()

	return c
}

// nodesInfo used by manager during API calls if this is nil the API cannot use
// the data. This should not be nil as constructor of manager will ensure atleast
// one pool is fetched before it returns a manager object
func (cicl *clusterInfoCacheLite) nodesInfo() *NodesInfo {
	if ptr := cicl.nih.Get(); ptr != nil {
		return ptr
	} else {
		return newNodesInfoWithError(ErrorUnintializedNodesInfo)
	}
}

// getNodesInfo is used by manager while handling the notifications
func (cicl *clusterInfoCacheLite) getNodesInfo() *NodesInfo {
	return cicl.nih.Get()
}

// setNodesInfo is used by manager while handling the notifications
func (cicl *clusterInfoCacheLite) setNodesInfo(ni *NodesInfo) {
	cicl.nih.Set(ni)
}

// TODO : Check log redaction and add other objects for printing.
func (cicl *clusterInfoCacheLite) String() string {
	ni := cicl.nih.Get()
	if ni != nil {
		return fmt.Sprintf("%v", ni)
	}
	return ""
}

func (cicl *clusterInfoCacheLite) addCollnInfo(bucketName string,
	ci *collectionInfo) error {
	cicl.cihmLock.Lock()
	defer cicl.cihmLock.Unlock()

	if _, ok := cicl.cihm[bucketName]; ok {
		return ErrBucketAlreadyExist
	}

	cih := collectionInfoHolder{}
	cih.Init()
	cih.Set(ci)
	cicl.cihm[bucketName] = cih
	return nil
}

func (cicl *clusterInfoCacheLite) deleteCollnInfo(bucketName string) error {
	cicl.cihmLock.Lock()
	defer cicl.cihmLock.Unlock()

	if _, ok := cicl.cihm[bucketName]; !ok {
		return ErrBucketNotFound
	}
	delete(cicl.cihm, bucketName)
	return nil
}

func (cicl *clusterInfoCacheLite) updateCollnInfo(bucketName string,
	ci *collectionInfo) error {

	cicl.cihmLock.RLock()
	defer cicl.cihmLock.RUnlock()

	cih, ok := cicl.cihm[bucketName]
	if !ok {
		return ErrBucketNotFound
	}
	cih.Set(ci)
	return nil
}

// getCollnInfo will retun *collectionInfo it can be valid or not
// error returned is related to the availability of data in cache and is used by
// CICL Manager. When data is not available in cache an invalid *collectionInfo is
// returned with errList[0] set to error
func (cicl *clusterInfoCacheLite) getCollnInfo(bucketName string) (*collectionInfo,
	error) {
	cicl.cihmLock.RLock()
	defer cicl.cihmLock.RUnlock()

	cih, ok := cicl.cihm[bucketName]
	if !ok {
		ci := newCollectionInfoWithErr(bucketName, ErrBucketNotFound)
		return ci, ci.errList[0]
	}
	ci := cih.Get()
	if ci == nil {
		ci := newCollectionInfoWithErr(bucketName, ErrUnInitializedClusterInfo)
		return ci, ci.errList[0]
	}
	return ci, nil
}

func (cicl *clusterInfoCacheLite) addBucketInfo(bucketName string, bi *bucketInfo) error {
	cicl.bihmLock.Lock()
	defer cicl.bihmLock.Unlock()

	if _, ok := cicl.bihm[bucketName]; ok {
		return ErrBucketAlreadyExist
	}

	bih := bucketInfoHolder{}
	bih.Init()
	bih.Set(bi)
	cicl.bihm[bucketName] = bih
	return nil
}

func (cicl *clusterInfoCacheLite) deleteBucketInfo(bucketName string) error {
	cicl.bihmLock.Lock()
	defer cicl.bihmLock.Unlock()

	if _, ok := cicl.bihm[bucketName]; !ok {
		return ErrBucketNotFound
	}

	delete(cicl.bihm, bucketName)
	return nil
}

func (cicl *clusterInfoCacheLite) updateBucketInfo(bucketName string,
	bi *bucketInfo) error {

	cicl.bihmLock.RLock()
	defer cicl.bihmLock.RUnlock()

	bih, ok := cicl.bihm[bucketName]
	if !ok {
		return ErrBucketNotFound
	}
	bih.Set(bi)
	return nil
}

// getBucketInfo returns bi it can be valid or invalid. error is used by CICL
// manager to check if bucket exists in cache or not. error here does not indicate
// validity of bucketInfo returned its error related to availability of bi in cache.
// Invalid bi is returned with error set if the data is not available in cache.
func (cicl *clusterInfoCacheLite) getBucketInfo(bucketName string) (*bucketInfo,
	error) {
	cicl.bihmLock.RLock()
	defer cicl.bihmLock.RUnlock()

	bih, ok := cicl.bihm[bucketName]
	if !ok {
		bi := newBucketInfoWithErr(bucketName, ErrBucketNotFound)
		return bi, bi.errList[0]
	}
	bi := bih.Get()
	if bi == nil {
		bi := newBucketInfoWithErr(bucketName, ErrUnInitializedClusterInfo)
		return bi, bi.errList[0]
	}
	return bi, nil
}

func (cicl *clusterInfoCacheLite) setPoolInfo(pi *PoolInfo) {
	cicl.pih.Set(pi)
}

func (cicl *clusterInfoCacheLite) getPoolInfo() *PoolInfo {
	return cicl.pih.Get()
}

//
// Cluster Info Cache Lite Manager
//

type clusterInfoCacheLiteManager struct {
	clusterURL string
	poolName   string
	logPrefix  string

	cicl *clusterInfoCacheLite

	// Used for making adhoc queries to ns_server
	client couchbase.Client

	ticker               *time.Ticker
	timeDiffToForceFetch uint32 // In minutes

	poolsStreamingCh chan Notification

	collnManifestCh          chan Notification
	perBucketCollnManifestCh map[string]chan Notification
	collnBucketsHash         string

	bucketInfoCh          chan Notification
	bucketInfoChPerBucket map[string]chan Notification
	bucketURLMap          map[string]string
	bInfoBucketsHash      string

	eventMgr *eventManager
	eventCtr uint64

	maxRetries         uint32
	retryInterval      uint32
	notifierRetrySleep uint32

	closeCh chan bool
}

func newClusterInfoCacheLiteManager(cicl *clusterInfoCacheLite, clusterURL,
	poolName, logPrefix string, config Config) (*clusterInfoCacheLiteManager,
	error) {

	cicm := &clusterInfoCacheLiteManager{
		poolName:                 poolName,
		logPrefix:                logPrefix,
		cicl:                     cicl,
		timeDiffToForceFetch:     config["force_after"].Uint32(), // In Minutes
		poolsStreamingCh:         make(chan Notification, 1000),
		notifierRetrySleep:       config["notifier_restart_sleep"].Uint32(), // In Milliseconds
		retryInterval:            uint32(CLUSTER_INFO_DEFAULT_RETRY_INTERVAL.Seconds()),
		collnManifestCh:          make(chan Notification, 10000),
		perBucketCollnManifestCh: make(map[string]chan Notification),

		bucketInfoCh:          make(chan Notification, 10000),
		bucketInfoChPerBucket: make(map[string]chan Notification),

		closeCh: make(chan bool),
	}

	var err error
	cicm.clusterURL, err = ClusterAuthUrl(clusterURL)
	if err != nil {
		return nil, err
	}

	cicm.client, err = couchbase.Connect(cicm.clusterURL)
	if err != nil {
		return nil, err
	}

	cicm.eventMgr, err = newEventManager(1, 500)
	if err != nil {
		return nil, err
	}

	cicm.client.SetUserAgent(logPrefix)

	// Try fetching few times in the constructor
	// With 2 Sec retry interval by default try for 120 Sec and then give up
	cicm.maxRetries = 60

	ni, p := cicm.FetchNodesInfo(nil)
	if ni == nil {
		// At least one successful pool call is needed
		return nil, ErrUnInitializedClusterInfo
	}
	cicm.cicl.setNodesInfo(ni)
	SetMajorVersionCICL(ni.version)

	pi := newPoolInfo(p)
	cicm.cicl.setPoolInfo(pi)

	// Try Fetching default number of times else where
	cicm.maxRetries = CLUSTER_INFO_DEFAULT_RETRIES

	cicm.bucketURLMap = make(map[string]string)
	for k, v := range ni.bucketURLMap {
		cicm.bucketURLMap[k] = v
	}

	go cicm.handlePoolsChangeNotifications()
	for _, bn := range ni.bucketNames {
		msg := &couchbase.Bucket{Name: bn.Name}
		notif := Notification{Type: ForceUpdateNotification, Msg: msg}

		if ni.version >= MAJOR_VERSION_7 {
			ch := make(chan Notification, 100)
			cicm.perBucketCollnManifestCh[bn.Name] = ch
			go cicm.handlePerBucketCollectionManifest(bn.Name, ch)
			ch <- notif
		}

		ch1 := make(chan Notification, 100)
		cicm.bucketInfoChPerBucket[bn.Name] = ch1
		go cicm.handleBucketInfoChangesPerBucket(bn.Name, ch1)
		ch1 <- notif
	}
	go cicm.handleCollectionManifestChanges()
	go cicm.handleBucketInfoChanges()
	go cicm.watchClusterChanges(false)
	go cicm.periodicUpdater()

	logging.Infof("newClusterInfoCacheLiteManager: started New clusterInfoCacheManager")
	return cicm, nil
}

func readWithTimeout(ch <-chan interface{}, timeout uint32) (interface{}, error) {
	if timeout == 0 {
		event := <-ch
		return event, nil
	}

	select {
	case msg := <-ch:
		return msg, nil
	case <-time.After(time.Duration(timeout) * time.Second):
		return nil, ErrorEventWaitTimeout
	}
}

func (cicm *clusterInfoCacheLiteManager) sendToCollnManifestCh(msg Notification) {
	if GetMajorVersionCICL() >= MAJOR_VERSION_7 {
		cicm.collnManifestCh <- msg
	}
}

func (cicm *clusterInfoCacheLiteManager) close() {
	close(cicm.closeCh)

	cicm.ticker.Stop()

	close(cicm.poolsStreamingCh)

	close(cicm.collnManifestCh)
	for _, ch := range cicm.perBucketCollnManifestCh {
		close(ch)
	}

	close(cicm.bucketInfoCh)
	for _, ch := range cicm.bucketInfoChPerBucket {
		close(ch)
	}

	logging.Infof("closed clusterInfoCacheLiteManager")
}

func (cicm *clusterInfoCacheLiteManager) setTimeDiffToForceFetch(minutes uint32) {
	logging.Infof("clusterInfoCacheLiteManager: Setting time difference interval in manager to force fetch to %d minutes", minutes)
	atomic.StoreUint32(&cicm.timeDiffToForceFetch, minutes)
}

func (cicm *clusterInfoCacheLiteManager) setMaxRetries(maxRetries uint32) {
	logging.Infof("clusterInfoCacheLiteManager: Setting max retries in manager to %d", maxRetries)
	atomic.StoreUint32(&cicm.maxRetries, maxRetries)
}

func (cicm *clusterInfoCacheLiteManager) setRetryInterval(seconds uint32) {
	logging.Infof("clusterInfoCacheLiteManager: Setting retry interval in manager to %d seconds", seconds)
	atomic.StoreUint32(&cicm.retryInterval, seconds)
}

func (cicm *clusterInfoCacheLiteManager) setNotifierRetrySleep(milliSeconds uint32) {
	logging.Infof("clusterInfoCacheLiteManager: Setting sleep interval upon notifier restart in manager to %d milliSeconds", milliSeconds)
	atomic.StoreUint32(&cicm.notifierRetrySleep, milliSeconds)
}

func (cicm *clusterInfoCacheLiteManager) nodesInfo() (*NodesInfo, error) {
	ni := cicm.cicl.nodesInfo()
	if !ni.valid {
		return ni, ni.errList[0]
	} else {
		return ni, nil
	}
}

func (cicm *clusterInfoCacheLiteManager) nodesInfoSync(eventTimeoutSeconds uint32) (
	*NodesInfo, error) {
	ni := cicm.cicl.nodesInfo()
	if !ni.valid {
		id := fmt.Sprintf("%d", atomic.AddUint64(&cicm.eventCtr, 1))
		evtCount := cicm.eventMgr.count(EVENT_NODEINFO_UPDATED)
		ch, err := cicm.eventMgr.register(id, EVENT_NODEINFO_UPDATED)
		if err != nil {
			return nil, err
		}

		if len(cicm.poolsStreamingCh) == 0 && evtCount == 0 {
			notif := Notification{
				Type: ForceUpdateNotification,
				Msg:  &couchbase.Pool{},
			}
			cicm.poolsStreamingCh <- notif
		}

		msg, err := readWithTimeout(ch, eventTimeoutSeconds)
		if err != nil {
			return nil, err
		}

		// NodeInfo event is notified only when its valid
		// If command channel goes empty and it its still invalid
		// periodic check will restart the processing or after timeout
		// user can trigger the command again
		ni = msg.(*NodesInfo)
	}
	return ni, nil
}

// bucketInfo will return *bucketInfo object it can be valid or not
func (cicm *clusterInfoCacheLiteManager) bucketInfo(bucketName string) *bucketInfo {
	bi, _ := cicm.cicl.getBucketInfo(bucketName)
	return bi
}

// bucketInfoSync will return (valid *bucketInfo, nil) when valid and (nil, err) when not valid
func (cicm *clusterInfoCacheLiteManager) bucketInfoSync(bucketName string, eventWaitTimeoutSeconds uint32) (
	*bucketInfo, error) {
	bi, _ := cicm.cicl.getBucketInfo(bucketName)
	if bi.valid {
		return bi, nil
	}

	id := fmt.Sprintf("%d", atomic.AddUint64(&cicm.eventCtr, 1))
	evtType := getBucketInfoEventType(bucketName)
	evtCount := cicm.eventMgr.count(evtType)

	ch, err := cicm.eventMgr.register(id, evtType)
	if err != nil {
		return nil, err
	}
	if evtCount == 0 {
		msg := Notification{
			Type: ForceUpdateNotification,
			Msg:  &couchbase.Bucket{Name: bucketName},
		}
		cicm.bucketInfoCh <- msg
	}

	msg, err := readWithTimeout(ch, eventWaitTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	bi = msg.(*bucketInfo)
	if !bi.valid {
		return nil, bi.errList[0]
	}

	return bi, nil
}

// collectionInfo returns *collectionInfo it can be valid or invalid
func (cicm *clusterInfoCacheLiteManager) collectionInfo(bucketName string) *collectionInfo {
	ci, _ := cicm.cicl.getCollnInfo(bucketName)
	return ci
}

// collectionInfoSync will return (*collectionInfo, nil) when valid and (nil, err) if not valid
func (cicm *clusterInfoCacheLiteManager) collectionInfoSync(bucketName string,
	eventTimeoutSeconds uint32) (*collectionInfo, error) {
	if GetMajorVersionCICL() < MAJOR_VERSION_7 {
		return nil, ErrBucketNotFound
	}

	ci, _ := cicm.cicl.getCollnInfo(bucketName)
	if ci.valid {
		return ci, nil
	}

	id := fmt.Sprintf("%d", atomic.AddUint64(&cicm.eventCtr, 1))
	evtType := getClusterInfoEventType(bucketName)
	evtCount := cicm.eventMgr.count(evtType)
	ch, err := cicm.eventMgr.register(id, evtType)
	if err != nil {
		return nil, err
	}

	if evtCount == 0 {
		msg := Notification{
			Type: ForceUpdateNotification,
			Msg:  &couchbase.Bucket{Name: ci.bucketName},
		}
		// Version check for 7.0 is added above in this function
		cicm.collnManifestCh <- msg
	}

	msg, err := readWithTimeout(ch, eventTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	// ci can be invalid when bucket is deleted and we are trying to
	// fetch the data
	ci = msg.(*collectionInfo)
	if !ci.valid {
		return nil, ci.errList[0]
	}

	return ci, nil
}

func (cicm *clusterInfoCacheLiteManager) periodicUpdater() {
	cicm.ticker = time.NewTicker(time.Duration(1) * time.Minute)
	for range cicm.ticker.C {
		t := atomic.LoadUint32(&cicm.timeDiffToForceFetch)

		ni := cicm.cicl.nih.Get()
		if ni != nil &&
			time.Since(ni.lastUpdatedTs) >
				time.Duration(t)*time.Minute &&
			len(cicm.poolsStreamingCh) == 0 {
			notif := Notification{
				Type: PeriodicUpdateNotification,
				Msg:  &couchbase.Pool{},
			}
			cicm.poolsStreamingCh <- notif
		}

		cicm.cicl.cihmLock.RLock()
		for name, cih := range cicm.cicl.cihm {
			ci := cih.Get()
			if ci == nil || time.Since(ci.lastUpdatedTs) >
				time.Duration(t)*time.Minute {
				msg := Notification{
					Type: PeriodicUpdateNotification,
					Msg: &couchbase.Bucket{
						Name: name,
					},
				}
				cicm.sendToCollnManifestCh(msg)
			}
		}
		cicm.cicl.cihmLock.RUnlock()

		cicm.cicl.bihmLock.Lock()
		for name, bih := range cicm.cicl.bihm {
			bi := bih.Get()
			if bi == nil || time.Since(bi.lastUpdatedTs) >
				time.Duration(t)*time.Minute {
				msg := Notification{
					Type: PeriodicUpdateNotification,
					Msg:  &couchbase.Bucket{Name: name},
				}
				cicm.bucketInfoCh <- msg
			}
		}
		cicm.cicl.bihmLock.Unlock()
	}
}

func (cicm *clusterInfoCacheLiteManager) handlePoolsChangeNotifications() {
	for notif := range cicm.poolsStreamingCh {
		logging.Tracef("handlePoolChangeNotification got notification %v", notif)
		p := (notif.Msg).(*couchbase.Pool)

		var ni, nni *NodesInfo
		var err error
		nodeServicesHash := ""
		fetch := false

		if notif.Type == ForceUpdateNotification ||
			notif.Type == PeriodicUpdateNotification {
			// Force fetch nodesInfo
			fetch = true
			// Use old NodesInfo when there is error while fetching
			ni = cicm.cicl.getNodesInfo()
		} else if nodeServicesHash, err = p.GetNodeServicesVersionHash(); err != nil {
			logging.Errorf("handlePoolsChangeNotifications: GetNodeServicesVersionHash returned err: %v uri: %s", err, p.NodeServicesUri)
			fetch = true
			ni = cicm.cicl.getNodesInfo()
		} else if notif.Type == PoolChangeNotification {
			// Try to use nodes data from Notification
			ni = newNodesInfo(p, cicm.clusterURL, nodeServicesHash)

			oni := cicm.cicl.getNodesInfo()
			if oni == nil || oni.nodesExt == nil {
				// No proper old data so force fetch
				fetch = true
			} else {
				// Try to use nodesExt from old nodesInfo
				ni.setNodesExt(oni.nodesExt)

				if ni.nodeServicesHash == "" || oni.nodeServicesHash != ni.nodeServicesHash {
					// Hash value changed so no need to validate too nodeSvs changed so force fetch
					fetch = true
				} else if ni.validateNodesAndSvs(cicm.client.BaseURL.Host); !ni.valid {
					// Validation failure so force fetch
					fetch = true
				} else {
					// Set the new nodesInfo to the validated ni
					nni = ni
				}
			}
		} else {
			logging.Errorf("handlePoolsChangeNotifications: unexpected notification %v", notif)
		}

		if fetch {
			// pass ni i.e. the old or the one the PoolChangeNotification data
			// Fetch will use this data on error else we will get new data. ni cannot be nil as we
			// are starting with atleast one pool object in managers constructor
			nni, p = cicm.FetchNodesInfo(ni)
		}

		// update cache
		cicm.cicl.setNodesInfo(nni)
		if ni.valid {
			// notify uses who saw invalid data and are waiting for update
			cicm.eventMgr.notify(EVENT_NODEINFO_UPDATED, nni)
			// On Periodic update send the Pool to bucket and collection channel to check the bucket names
			if notif.Type == PeriodicUpdateNotification && p != nil {
				notif := Notification{
					Type: PoolChangeNotification,
					Msg:  p,
				}
				cicm.sendToCollnManifestCh(notif)
				cicm.bucketInfoCh <- notif
			}
		}
	}
}

func (cicm *clusterInfoCacheLiteManager) addOrRemoveBuckets(newBucketMap,
	oldBucketMap map[string]bool, start, cleanup func(bName string)) {
	logging.Tracef("OldBucketMap %v NewBucketMap %v", oldBucketMap, newBucketMap)

	// cleanup all buckets that are in oldBucketMap and not in newBucketMap
	for oldName, _ := range oldBucketMap {
		if _, ok := newBucketMap[oldName]; !ok {
			cleanup(oldName)
		}
	}

	// start all buckets that are in newBucketMap and not in oldBucketMap
	for newName, _ := range newBucketMap {
		if _, ok := oldBucketMap[newName]; !ok {
			start(newName)
		}
	}
}

func (cicm *clusterInfoCacheLiteManager) handleCollectionManifestChanges() {

	notify := func(bName string, ci *collectionInfo) {
		// If any API is waiting for notification return error
		evtType := getClusterInfoEventType(bName)
		cicm.eventMgr.notify(evtType, ci)
	}

	cleanup := func(bName string) {
		ch := cicm.perBucketCollnManifestCh[bName]
		close(ch)
		delete(cicm.perBucketCollnManifestCh, bName)

		ci := newCollectionInfoWithErr(bName, ErrBucketNotFound)
		notify(bName, ci)
		logging.Infof("handleCollectionManifestChanges: stopped observing collection manifest for bucket %v", bName)
	}

	start := func(bName string) {
		ch := make(chan Notification, 100)
		cicm.perBucketCollnManifestCh[bName] = ch
		go cicm.handlePerBucketCollectionManifest(bName, ch)
		logging.Infof("handleCollectionManifestChanges: started observing collection manifest for bucket %v", bName)
	}

	// sendToCollnManifestCh() sends to collnManifestCh only when the cluster
	// major version is >= 7 so need not check for version explicitly here
	for notif := range cicm.collnManifestCh {
		if notif.Type == PoolChangeNotification {
			p := (notif.Msg).(*couchbase.Pool)

			hash, err := p.GetBucketURLVersionHash()
			if err != nil {
				logging.Errorf("handleCollectionManifestChanges: GetBucketURLVersionHash returned error %v", err)
			} else if hash == cicm.collnBucketsHash {
				continue
			} else {
				cicm.collnBucketsHash = hash
			}

			newBucketMap := make(map[string]bool, len(p.BucketNames))
			for _, bn := range p.BucketNames {
				newBucketMap[bn.Name] = true
			}
			oldBucketMap := make(map[string]bool, len(cicm.perBucketCollnManifestCh))
			for bn, _ := range cicm.perBucketCollnManifestCh {
				oldBucketMap[bn] = true
			}
			cicm.addOrRemoveBuckets(newBucketMap, oldBucketMap, start, cleanup)
			continue
		}

		b := (notif.Msg).(*couchbase.Bucket)
		ch, ok := cicm.perBucketCollnManifestCh[b.Name]
		if !ok {
			exists := cicm.verifyBucketExist(b.Name)
			if exists {
				logging.Infof("handleCollectionManifestChanges: started observing collection manifest for bucket %v on getting %v", b.Name, notif)
				start(b.Name)
				ch = cicm.perBucketCollnManifestCh[b.Name]
			} else {
				logging.Warnf("handleCollectionManifestChanges: ignoring %v as bucket is not found bucket %v", notif, b.Name)
				if notif.Type == ForceUpdateNotification {
					ci := newCollectionInfoWithErr(b.Name, ErrBucketNotFound)
					notify(b.Name, ci)
				}
				continue
			}
		}

		ch <- notif
	}
}

func (cicm *clusterInfoCacheLiteManager) handlePerBucketCollectionManifest(
	bucketName string, ch chan Notification) {

	for notif := range ch {
		b := (notif.Msg).(*couchbase.Bucket)
		newBucket := false
		logging.Tracef("handlePerBucketCollectionManifest: got %v for bucket", notif, b.Name)

		oci, err := cicm.cicl.getCollnInfo(bucketName)
		if err == ErrBucketNotFound {
			newBucket = true
		}
		if notif.Type == CollectionManifestChangeNotification && oci.valid &&
			oci.manifest.UID == b.CollectionManifestUID {
			continue
		}

		ci := cicm.FetchCollectionInfo(bucketName)
		if !ci.valid {
			logging.Warnf("handlePerBucketCollectionManifest error while fetching collection manifest for bucket: %s", bucketName)
		}

		if newBucket {
			cicm.cicl.addCollnInfo(bucketName, ci)
		} else {
			cicm.cicl.updateCollnInfo(bucketName, ci)
		}

		if ci.valid {
			evtType := getClusterInfoEventType(bucketName)
			cicm.eventMgr.notify(evtType, ci)
		}
	}
	cicm.cicl.deleteCollnInfo(bucketName)
}

func (cicm *clusterInfoCacheLiteManager) handleBucketInfoChanges() {
	notify := func(bName string, bi *bucketInfo) {
		// If any API is waiting for notification return error
		evtType := getBucketInfoEventType(bName)
		cicm.eventMgr.notify(evtType, bi)
	}

	cleanup := func(bName string) {
		ch := cicm.bucketInfoChPerBucket[bName]
		close(ch)
		delete(cicm.bucketInfoChPerBucket, bName)

		bi := newBucketInfoWithErr(bName, ErrBucketNotFound)
		notify(bName, bi)
		logging.Infof("handleBucketInfoChanges: stopped observing collection manifest for bucket %v", bName)
	}

	start := func(bName string) {
		ch := make(chan Notification, 100)
		cicm.bucketInfoChPerBucket[bName] = ch
		go cicm.handleBucketInfoChangesPerBucket(bName, ch)
		logging.Infof("handleBucketInfoChanges: started observing collection manifest for bucket %v", bName)
	}

	for notif := range cicm.bucketInfoCh {
		if notif.Type == PoolChangeNotification {
			p := (notif.Msg).(*couchbase.Pool)

			hash, err := p.GetBucketURLVersionHash()
			if err != nil {
				logging.Errorf("handleBucketInfoChanges: GetBucketURLVersionHash returned error %v", err)
			} else if hash == cicm.bInfoBucketsHash {
				continue
			} else {
				cicm.bInfoBucketsHash = hash
			}

			newBucketMap := make(map[string]bool, len(p.BucketNames))
			for _, bn := range p.BucketNames {
				newBucketMap[bn.Name] = true
			}
			oldBucketMap := make(map[string]bool, len(cicm.bucketInfoChPerBucket))
			for bn, _ := range cicm.bucketInfoChPerBucket {
				oldBucketMap[bn] = true
			}
			cicm.addOrRemoveBuckets(newBucketMap, oldBucketMap, start, cleanup)
			continue
		}

		b := (notif.Msg).(*couchbase.Bucket)
		ch, ok := cicm.bucketInfoChPerBucket[b.Name]
		if !ok {
			exists := cicm.verifyBucketExist(b.Name)
			if exists {
				logging.Infof("handleBucketInfoChanges: started observing collection manifest for bucket %v on getting %v", b.Name, notif)
				start(b.Name)
				ch = cicm.bucketInfoChPerBucket[b.Name]
			} else {
				logging.Warnf("handleBucketInfoChanges: ignoring %v as bucket is not found bucket %v", notif, b.Name)
				if notif.Type == ForceUpdateNotification {
					bi := newBucketInfoWithErr(b.Name, ErrBucketNotFound)
					notify(b.Name, bi)
				}
				continue
			}
		}

		ch <- notif
	}
}

func (cicm *clusterInfoCacheLiteManager) handleBucketInfoChangesPerBucket(
	bucketName string, ch chan Notification) {
	connHost := cicm.client.BaseURL.Host
	for notif := range ch {
		b := (notif.Msg).(*couchbase.Bucket)

		newBucket := false
		_, err := cicm.cicl.getBucketInfo(bucketName)
		if err == ErrBucketNotFound {
			newBucket = true
		}

		var bi *bucketInfo
		if notif.Type == CollectionManifestChangeNotification {
			bi = newBucketInfo(b, connHost)
			bi.setClusterURL(cicm.clusterURL)
		} else {
			bi, _ = cicm.FetchTerseBucketInfo(bucketName)
		}

		if newBucket {
			cicm.cicl.addBucketInfo(bucketName, bi)
		} else {
			cicm.cicl.updateBucketInfo(bucketName, bi)
		}

		if bi.valid {
			evtType := getBucketInfoEventType(bucketName)
			cicm.eventMgr.notify(evtType, bi)
		}
	}
	cicm.cicl.deleteBucketInfo(bucketName)
}

func (cicm *clusterInfoCacheLiteManager) watchClusterChanges(isRestart bool) {
	selfRestart := func() {
		logging.Infof("clusterInfoCacheLiteManager watchClusterChanges: restarting..")
		r := atomic.LoadUint32(&cicm.notifierRetrySleep)
		time.Sleep(time.Duration(r) * time.Millisecond)
		go cicm.watchClusterChanges(true)
	}

	var poolReceived bool
	var poolNotifOnSelfRestartCh chan Notification
	if isRestart {
		poolReceived = false
		poolNotifOnSelfRestartCh = make(chan Notification, 1)
		// Service Notifier Instance gets deleted on bucket delete and we get a selfRestart
		// * Try to fetch the Pool, update bucket list and force fetch the data to account
		// for any missed notifications during wait time before restarting.
		// * Fetching Pool in a go routine here to avoid blocking further notifications.
		// * Out of order PoolChangeNotification should be ok as the nodeInfo will be fetched
		// again on hash change or validation failure. Bucket level go routines for collnInfo
		// and bucketInfo will get updated eventually with PoolChangeNotifications
		// * If this errors out, periodic update will fix the bucket name list.
		// * If we already got a PoolChangeNotification after we fetch the data we can skip this
		go func() {
			p, err := cicm.client.GetPoolWithoutRefresh(cicm.poolName)
			if err != nil {
				logging.Errorf("clusterInfoCacheLiteManager watchClusterChanges GetPoolWithoutRefresh failed with error %v", err)
				return
			}

			notif := Notification{
				Type: PoolChangeNotification,
				Msg:  &p,
			}
			poolNotifOnSelfRestartCh <- notif

			for _, bn := range p.BucketNames {
				msg := &couchbase.Bucket{Name: bn.Name}
				notif = Notification{Type: ForceUpdateNotification, Msg: msg}
				cicm.sendToCollnManifestCh(notif)
				cicm.bucketInfoCh <- notif
			}
		}()
	}

	scn, err := NewServicesChangeNotifier(cicm.clusterURL, cicm.poolName)
	if err != nil {
		logging.Errorf("clusterInfoCacheLiteManager NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	ch := scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				selfRestart()
				return
			}
			switch notif.Type {
			case PoolChangeNotification:
				switch (notif.Msg).(type) {
				case *couchbase.Pool:
					poolReceived = true

					p := (notif.Msg).(*couchbase.Pool)
					pi := newPoolInfo(p)
					cicm.cicl.setPoolInfo(pi)
					newMajorVer := p.GetVersion()
					if newMajorVer > GetMajorVersionCICL() {
						SetMajorVersionCICL(newMajorVer)
					}

					cicm.poolsStreamingCh <- notif
					cicm.sendToCollnManifestCh(notif)
					cicm.bucketInfoCh <- notif
				default:
					logging.Errorf("clusterInfoCacheLiteManager (PoolChangeNotification): Invalid message type: %T", notif.Msg)
				}
			case CollectionManifestChangeNotification:
				switch (notif.Msg).(type) {
				case *couchbase.Bucket:
					cicm.sendToCollnManifestCh(notif)
					cicm.bucketInfoCh <- notif
				default:
					logging.Errorf("clusterInfoCacheLiteManager (CollectionManifestChangeNotification): Invalid message type: %T", notif.Msg)
				}
			}
		case notif, _ := <-poolNotifOnSelfRestartCh:
			if !poolReceived {
				switch (notif.Msg).(type) {
				case *couchbase.Pool:
					p := (notif.Msg).(*couchbase.Pool)
					pi := newPoolInfo(p)
					cicm.cicl.setPoolInfo(pi)
					newMajorVer := p.GetVersion()
					if newMajorVer > GetMajorVersionCICL() {
						SetMajorVersionCICL(newMajorVer)
					}

					cicm.poolsStreamingCh <- notif
					cicm.sendToCollnManifestCh(notif)
					cicm.bucketInfoCh <- notif
					logging.Infof("clusterInfoCacheLiteManager: watchClusterChanges sent notification on self restart")
				default:
					logging.Errorf("clusterInfoCacheLiteManager (Pool Change on Restart): Invalid message type: %T", notif.Msg)
				}
			}
			poolNotifOnSelfRestartCh = nil
		case <-cicm.closeCh:
			return
		}
	}
}

func (cicm *clusterInfoCacheLiteManager) FetchNodesInfo(oldNInfo *NodesInfo) (*NodesInfo, *couchbase.Pool) {
	var retryCount uint32 = 0
	maxRetries := atomic.LoadUint32(&cicm.maxRetries)
	r := atomic.LoadUint32(&cicm.retryInterval)
	retryInterval := time.Duration(r) * time.Second
retry:
	p, err := cicm.client.GetPoolWithoutRefresh(cicm.poolName)
	if err != nil {
		logging.Errorf("clusterInfoCacheLiteManager::FetchNodesInfo Error while fetching "+
			"pools info from pool: %v, err: %v", cicm.poolName, err)
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			if oldNInfo == nil {
				// Initially in the constructor oldNInfo is nil else we will have atleast
				// a pool object
				return nil, nil
			} else {
				ni := oldNInfo.clone()
				ni.setNodesExt(oldNInfo.nodesExt)
				return ni.setNodesInfoWithError(err), nil
			}
		}
	}

	nodeServicesHash, err := p.GetNodeServicesVersionHash()
	if err != nil {
		logging.Errorf("FetchNodesInfo: GetNodeServicesVersionHash returned err: %v uri: %s", err, p.NodeServicesUri)
		nodeServicesHash = ""
	}

	ps, err := cicm.client.GetPoolServices(cicm.poolName)
	if err != nil {
		logging.Errorf("clusterInfoCacheLiteManager::FetchNodesInfo Error while fetching "+
			"services info from pool: %v, err: %v", cicm.poolName, err)
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			n := newNodesInfo(&p, cicm.clusterURL, nodeServicesHash)
			// NodesExt should not be used on error but setting this for using later
			if oldNInfo != nil {
				n.setNodesExt(oldNInfo.nodesExt)
			}
			return n.setNodesInfoWithError(err), &p
		}
	}

	ni := newNodesInfo(&p, cicm.clusterURL, nodeServicesHash)
	ni.setNodesExt(ps.NodesExt)

	ni.validateNodesAndSvs(cicm.client.BaseURL.Host)
	if !ni.valid {
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			return ni, &p
		}
	}

	return ni, &p
}

func (cicm *clusterInfoCacheLiteManager) FetchCollectionInfo(bucketName string) *collectionInfo {
	var retryCount uint32 = 0
	maxRetries := atomic.LoadUint32(&cicm.maxRetries)
	r := atomic.LoadUint32(&cicm.retryInterval)
	retryInterval := time.Duration(r) * time.Second
retry:
	doRetry, cm, err := cicm.client.GetCollectionManifest(bucketName)
	if doRetry && retryCount < maxRetries {
		logging.Errorf("clusterInfoCacheLiteManager::FetchingCollectionInfo Error while fetching "+
			"collection info for bucket: %v, err: %v", bucketName, err)
		retryCount++
		if retryCount%5 == 0 {
			logging.Infof("clusterInfoCacheLiteManager:FetchingCollectionInfo: retrying %v time for bucket %v", retryCount, bucketName)
			exists := cicm.verifyBucketExist(bucketName)
			if !exists {
				// BackOff and wait for BucketDeleteNotification
				return newCollectionInfoWithErr(bucketName, err)
			}
		}
		time.Sleep(retryInterval)
		goto retry
	}

	if err != nil {
		ci := newCollectionInfoWithErr(bucketName, err)
		return ci
	}

	ci := newCollectionInfo(bucketName, cm)
	return ci
}

func (cicm *clusterInfoCacheLiteManager) FetchTerseBucketInfo(bucketName string) (
	*bucketInfo, error) {
	terseBucketsBase := cicm.bucketURLMap["terseBucketsBase"]
	connHost := cicm.client.BaseURL.Host
	var retryCount uint32 = 0
	maxRetries := atomic.LoadUint32(&cicm.maxRetries)
	r := atomic.LoadUint32(&cicm.retryInterval)
	retryInterval := time.Duration(r) * time.Second
retry:
	doRetry, tb, err := cicm.client.GetTerseBucket(terseBucketsBase, bucketName)
	if doRetry && retryCount < maxRetries {
		logging.Errorf("clusterInfoCacheLiteManager::FetchTerseBucketInfo Error while fetching "+
			"bucket info for bucket: %v, err: %v", bucketName, err)
		retryCount++
		time.Sleep(retryInterval)
		goto retry
	}

	if err != nil {
		bi := newBucketInfoWithErr(bucketName, err)
		return bi, err
	}

	bi := newBucketInfo(tb, connHost)
	bi.setClusterURL(cicm.clusterURL)
	return bi, nil
}

func (cicm *clusterInfoCacheLiteManager) GetBucketNames() []couchbase.BucketName {
	p := cicm.cicl.getPoolInfo()
	return p.pool.BucketNames
}

func (cicm *clusterInfoCacheLiteManager) verifyBucketExist(bucketName string) bool {
	bns := cicm.GetBucketNames()
	for _, bn := range bns {
		if bn.Name == bucketName {
			return true
		}
	}
	return false
}

//
// Cluster Info Cache Lite Client
//

type ClusterInfoCacheLiteClient struct {
	ciclMgr *clusterInfoCacheLiteManager

	clusterURL string
	poolName   string
	logPrefix  string

	eventWaitTimeoutSeconds uint32
}

func NewClusterInfoCacheLiteClient(clusterURL, poolName, userAgent string,
	config Config) (ciclClient *ClusterInfoCacheLiteClient, err error) {

	ciclClient = &ClusterInfoCacheLiteClient{
		clusterURL: clusterURL,
		poolName:   poolName,
		logPrefix:  userAgent,
	}

	t := uint32(CLUSTER_INFO_DEFAULT_RETRY_INTERVAL.Seconds()) * CLUSTER_INFO_DEFAULT_RETRIES
	ciclClient.eventWaitTimeoutSeconds = t

	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()

	if singletonCICLContainer.ciclMgr == nil {
		cicl := newClusterInfoCacheLite("SingletonCICL")

		ciclMgr, err := newClusterInfoCacheLiteManager(cicl, clusterURL,
			poolName, "SingletonCICLMgr", config)
		if err != nil {
			return nil, err
		}

		singletonCICLContainer.ciclMgr = ciclMgr
	}

	ciclClient.ciclMgr = singletonCICLContainer.ciclMgr
	singletonCICLContainer.refCount++

	logging.Infof("NewClusterInfoCacheLiteClient started new cicl client for %v", userAgent)
	return ciclClient, err
}

func (c *ClusterInfoCacheLiteClient) Close() {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()

	singletonCICLContainer.refCount--
	//c.ciclMgr = nil
	if singletonCICLContainer.refCount == 0 {
		singletonCICLContainer.ciclMgr.close()
		singletonCICLContainer.ciclMgr = nil
	}

	logging.Infof("ClusterInfoCacheLiteClient:Close[%v] closed clusterInfoCacheLiteClient", c.logPrefix)
}

func (cicl *ClusterInfoCacheLiteClient) GetNodesInfoProvider() (NodesInfoProvider,
	error) {
	ni, err := cicl.GetNodesInfo()
	if err != nil {
		return nil, err
	}
	return NodesInfoProvider(ni), err
}

func (cicl *ClusterInfoCacheLiteClient) GetCollectionInfoProvider(bucketName string) (
	CollectionInfoProvider, error) {
	ci, err := cicl.GetCollectionInfo(bucketName)
	if err != nil {
		return nil, err
	}
	return CollectionInfoProvider(ci), nil
}

func (cicl *ClusterInfoCacheLiteClient) GetBucketInfoProvider(bucketName string) (
	BucketInfoProvider, error) {
	bi, err := cicl.GetBucketInfo(bucketName)
	if err != nil {
		return nil, err
	}
	return BucketInfoProvider(bi), nil
}

func (c *ClusterInfoCacheLiteClient) SetLogPrefix(logPrefix string) {
	c.logPrefix = logPrefix
	logging.Infof("ClusterInfoCacheLiteClient setting logPrefix to %v", logPrefix)
}

func (c *ClusterInfoCacheLiteClient) SetUserAgent(logPrefix string) {
	c.SetLogPrefix(logPrefix)
}

func (c *ClusterInfoCacheLiteClient) SetEventWaitTimeout(seconds uint32) {
	c.eventWaitTimeoutSeconds = seconds
}

func (c *ClusterInfoCacheLiteClient) SetMaxRetries(retries uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setMaxRetries(retries)
}

func (c *ClusterInfoCacheLiteClient) SetRetryInterval(seconds uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setRetryInterval(seconds)
}

func (c *ClusterInfoCacheLiteClient) GetNodesInfo() (*NodesInfo, error) {
	ni, _ := c.ciclMgr.nodesInfo()
	if ni.valid {
		return ni, nil
	}

	logging.Tracef("ClusterInfoCacheLiteClient:GetNodesInfo[%v] NodesInfo Invalid trying to force fetch", c.logPrefix)
	return c.ciclMgr.nodesInfoSync(c.eventWaitTimeoutSeconds)
}

func (ni *NodesInfo) GetClusterVersion() uint64 {
	return GetVersion(ni.version, ni.minorVersion)
}

func (ni *NodesInfo) GetNodesByServiceType(srvc string) (nids []NodeId) {

	for i, svs := range ni.nodesExt {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (ni *NodesInfo) GetCurrentNode() NodeId {

	for i, node := range ni.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}

	return NodeId(-1)
}

func (ni *NodesInfo) GetServiceAddress(nid NodeId, srvc string,
	useEncryptedPortMap bool) (addr string, err error) {

	if int(nid) >= len(ni.nodesExt) {
		err = ErrInvalidNodeId
		return
	}

	return ni.getServiceAddress(nid, srvc, useEncryptedPortMap)
}

func (ni *NodesInfo) GetNodeUUID(nid NodeId) string {

	return ni.nodes[nid].NodeUUID
}

func (ni *NodesInfo) GetNodeIdByUUID(uuid string) (NodeId, bool) {
	for nid, node := range ni.nodes {
		if node.NodeUUID == uuid {
			return NodeId(nid), true
		}
	}

	return NodeId(-1), false
}

func (ni *NodesInfo) GetServerGroup(nid NodeId) string {
	return ni.node2group[nid]
}

func (ni *NodesInfo) GetServerVersion(nid NodeId) (int, error) {
	if int(nid) >= len(ni.nodes) {
		return 0, ErrInvalidNodeId
	}
	return getServerVersionFromVersionString(ni.nodes[nid].Version)
}

func (ni *NodesInfo) GetLocalNodeUUID() string {
	for _, node := range ni.nodes {
		if node.ThisNode {
			return node.NodeUUID
		}
	}
	return ""
}

func (ni *NodesInfo) GetLocalHostname() (string, error) {

	cUrl, err := url.Parse(ni.clusterURL)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	h, _, _ := net.SplitHostPort(cUrl.Host)

	nid := ni.GetCurrentNode()
	if nid == NodeId(-1) {
		return "", ErrorThisNodeNotFound
	}

	if int(nid) >= len(ni.nodesExt) {
		return "", ErrInvalidNodeId
	}

	node := ni.nodesExt[nid]
	if node.Hostname == "" {
		node.Hostname = h
	}

	return node.Hostname, nil
}

func (ni *NodesInfo) GetLocalHostAddress() (string, error) {

	cUrl, err := url.Parse(ni.clusterURL)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	_, p, _ := net.SplitHostPort(cUrl.Host)

	h, err := ni.GetLocalHostname()
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(h, p), nil

}

func (ni *NodesInfo) GetLocalServerGroup() (string, error) {
	node := ni.GetCurrentNode()
	if node == NodeId(-1) {
		return "", ErrorThisNodeNotFound
	}

	return ni.GetServerGroup(node), nil
}

func (ni *NodesInfo) GetLocalServicePort(srvc string, useEncryptedPortMap bool) (string, error) {
	addr, err := ni.GetLocalServiceAddress(srvc, useEncryptedPortMap)
	if err != nil {
		return addr, err
	}

	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return p, e
	}

	return net.JoinHostPort("", p), nil
}

func (ni *NodesInfo) GetLocalServiceHost(srvc string, useEncryptedPortMap bool) (string, error) {

	addr, err := ni.GetLocalServiceAddress(srvc, useEncryptedPortMap)
	if err != nil {
		return addr, err
	}

	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return h, nil
}

func (ni *NodesInfo) getStaticServicePort(srvc string) (string, error) {
	if p, ok := ni.servicePortMap[srvc]; ok {
		return p, nil
	} else {
		return "", errors.New(ErrInvalidService.Error() + fmt.Sprintf(": %v", srvc))
	}
}

func (ni *NodesInfo) GetLocalServiceAddress(srvc string, useEncryptedPortMap bool) (srvcAddr string, err error) {
	if ni.useStaticPorts {
		h, err := ni.GetLocalHostname()
		if err != nil {
			return "", err
		}

		p, e := ni.getStaticServicePort(srvc)
		if e != nil {
			return "", e
		}
		srvcAddr = net.JoinHostPort(h, p)
		if useEncryptedPortMap {
			srvcAddr, _, _, err = security.EncryptPortFromAddr(srvcAddr)
			if err != nil {
				return "", err
			}
		}
	} else {
		node := ni.GetCurrentNode()
		if node == NodeId(-1) {
			return "", ErrorThisNodeNotFound
		}

		srvcAddr, err = ni.GetServiceAddress(node, srvc, useEncryptedPortMap)
		if err != nil {
			return "", err
		}
	}

	return srvcAddr, nil
}

func (ni *NodesInfo) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(ni.nodes) {
		return false, ErrInvalidNodeId
	}

	return ni.nodes[nid].Status == "healthy", nil
}

func (ni *NodesInfo) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(ni.nodes) {
		return "", ErrInvalidNodeId
	}

	return ni.nodes[nid].Status, nil
}

func (ni *NodesInfo) Nodes() []couchbase.Node {
	return ni.nodes
}

func (ni *NodesInfo) EncryptPortMapping() map[string]string {
	return ni.encryptedPortMap
}

func (ni *NodesInfo) getServiceAddress(nid NodeId, srvc string,
	useEncryptedPortMap bool) (addr string, err error) {

	node := ni.nodesExt[nid]

	port, ok := node.Services[srvc]
	if !ok {
		logging.Errorf("nodesInfo:getServiceAddress Invalid Service %v for node %v. Nodes %v \n NodeServices %v",
			srvc, node, ni.nodes, ni.nodesExt)
		err = errors.New(ErrInvalidService.Error() + fmt.Sprintf(": %v", srvc))
		return
	}

	// For current node, hostname might be empty
	// Insert hostname used to connect to the cluster
	if node.Hostname == "" {
		cUrl, err := url.Parse(ni.clusterURL)
		if err != nil {
			return "", errors.New("Unable to parse cluster url - " + err.Error())
		}
		h, _, _ := net.SplitHostPort(cUrl.Host)
		node.Hostname = h
	}

	var portStr string
	if useEncryptedPortMap {
		portStr = security.EncryptPort(node.Hostname, fmt.Sprint(port))
	} else {
		portStr = fmt.Sprint(port)
	}

	addr = net.JoinHostPort(node.Hostname, portStr)
	return
}

func (c *ClusterInfoCacheLiteClient) GetClusterVersion() uint64 {
	ni, _ := c.ciclMgr.nodesInfo()
	return GetVersion(ni.version, ni.minorVersion)
}

func (c *ClusterInfoCacheLiteClient) GetLocalServiceAddress(srvc string,
	useEncryptedPortMap bool) (srvcAddr string, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return "", err
	}

	return ni.GetLocalServiceAddress(srvc, useEncryptedPortMap)
}

func (c *ClusterInfoCacheLiteClient) GetLocalNodeUUID() (string, error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return "", err
	}

	for _, node := range ni.nodes {
		if node.ThisNode {
			return node.NodeUUID, nil
		}
	}
	return "", fmt.Errorf("no node has ThisNode set")
}

func (ni *NodesInfo) GetActiveIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (ni *NodesInfo) GetFailedIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range ni.failedNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (ni *NodesInfo) GetActiveKVNodes() (nodes []couchbase.Node) {
	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (ni *NodesInfo) GetFailedKVNodes() (nodes []couchbase.Node) {
	for _, n := range ni.failedNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (ni *NodesInfo) GetActiveQueryNodes() (nodes []couchbase.Node) {
	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "n1ql" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (ni *NodesInfo) GetFailedQueryNodes() (nodes []couchbase.Node) {
	for _, n := range ni.failedNodes {
		for _, s := range n.Services {
			if s == "n1ql" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCacheLiteClient) GetActiveIndexerNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	return ni.GetActiveIndexerNodes(), nil
}

func (c *ClusterInfoCacheLiteClient) GetFailedIndexerNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	for _, n := range ni.failedNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCacheLiteClient) GetNewIndexerNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	for _, n := range ni.addNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCacheLiteClient) GetActiveKVNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCacheLiteClient) GetAllKVNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	for _, n := range ni.failedNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	for _, n := range ni.addNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCacheLiteClient) ClusterVersion() uint64 {
	ni, _ := c.ciclMgr.nodesInfo()
	return ni.GetClusterVersion()
}

//
// API Using Bucket Info
//

// GetBucketInfo returns Valid bucketInfo when there is no error and returns nil on error
func (c *ClusterInfoCacheLiteClient) GetBucketInfo(bucketName string) (
	*bucketInfo, error) {
	bi := c.ciclMgr.bucketInfo(bucketName)
	if bi.valid {
		return bi, nil
	}

	return c.ciclMgr.bucketInfoSync(bucketName, c.eventWaitTimeoutSeconds)
}

// User must ensure that bucketInfo object is valid
func (bi *bucketInfo) GetServiceAddress(nid NodeId, srvc string,
	useEncryptedPortMap bool) (addr string, err error) {

	ns := bi.bucket.NodesExt[nid]

	port, ok := ns.Services[srvc]
	if !ok {
		logging.Errorf("bucketInfo:GetServiceAddress Invalid Service %v for node %v. Nodes %v \n NodeServices %v",
			srvc, ns, bi.bucket.NodesJSON, bi.bucket.NodesExt)
		err = errors.New(ErrInvalidService.Error() + fmt.Sprintf(": %v", srvc))
		return
	}

	var portStr string
	if useEncryptedPortMap {
		portStr = security.EncryptPort(ns.Hostname, fmt.Sprint(port))
	} else {
		portStr = fmt.Sprint(port)
	}

	addr = net.JoinHostPort(ns.Hostname, portStr)
	return
}

func (bi *bucketInfo) findVBMapServerListIndex(nid NodeId) (int, error) {
	b := bi.bucket

	vbmap := b.VBServerMap()
	serverList := vbmap.ServerList
	nodesExt := b.NodesExt

	ns := nodesExt[nid]
	nsHostName, err := ns.GetHostNameWithPort(KV_SERVICE)
	if err != nil {
		return -1, err
	}

	for i, n := range serverList {
		if n == nsHostName {
			return i, nil
		}
	}

	return -1, ErrNodeNotBucketMember
}

// Note: bucket string is not used but kept for BucketInfoProvider Interface implementation
func (bi *bucketInfo) GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error) {
	b := bi.bucket

	idx, err := bi.findVBMapServerListIndex(nid)
	if err != nil {
		return nil, err
	}

	vbmap := b.VBServerMap()

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint32(vb))
		}
	}

	return vbs, nil
}

func (bi *bucketInfo) GetNodesByBucket(bucket string) (nids []NodeId, err error) {
	b := bi.bucket

	vbmap := b.VBServerMap()
	serverList := vbmap.ServerList
	nodesExt := b.NodesExt

	for _, n := range serverList {
		for id, ns := range nodesExt {
			currentHostName, err := ns.GetHostNameWithPort(KV_SERVICE)
			if err != nil {
				return nil, err
			}
			if n == currentHostName {
				nids = append(nids, NodeId(id))
				break
			}
		}
	}
	return
}

func (bi *bucketInfo) IsEphemeral(bucket string) (bool, error) {
	t := bi.bucket.Type
	return strings.EqualFold(t, "ephemeral"), nil
}

func (bi *bucketInfo) IsMagmaStorage(bucket string) (bool, error) {
	backend := bi.bucket.StorageBackend
	return strings.EqualFold(backend, "magma"), nil
}

func (bi *bucketInfo) GetLocalVBuckets(bucketName string) (
	vbs []uint16, err error) {

	b := bi.bucket

	nodesExt := b.NodesExt
	currentHostName := ""
	for _, ns := range nodesExt {
		if ns.ThisNode {
			currentHostName, err = ns.GetHostNameWithPort(KV_SERVICE)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	vbmap := b.VBServerMap()
	serverList := vbmap.ServerList

	idx := -1
	for i, n := range serverList {
		if n == currentHostName {
			idx = i
		}
	}
	if idx == -1 {
		err = fmt.Errorf("ThisNode is not in nodes list")
		return
	}

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint16(vb))
		}
	}

	return
}

// GetBucketUUID returns UUID if user is able to get bucketInfo from GetBucketInfo
// For deleted buckets GetBucketInfo will not return bucketInfo and error out
// User must fetch a new pointer every time to avoid having stale pointer due to
// atomic updates from the cache manager
func (bi *bucketInfo) GetBucketUUID(bucket string) (uuid string) {
	b := bi.bucket
	return b.UUID
}

func (bi *bucketInfo) GetVBmap(kvaddrs []string) (map[string][]uint16, error) {
	return bi.bucket.GetVBmap(kvaddrs)
}

func (cicl *ClusterInfoCacheLiteClient) GetLocalVBuckets(bucketName string) (
	vbs []uint16, err error) {
	bi, err := cicl.GetBucketInfo(bucketName)
	if err != nil {
		return nil, err
	}

	return bi.GetLocalVBuckets(bucketName)
}

// GetBucketUUID returns error and BUCKET_UUID_NIL for deleted buckets
func (cicl *ClusterInfoCacheLiteClient) GetBucketUUID(bucketName string) (uuid string,
	err error) {
	bi, err := cicl.GetBucketInfo(bucketName)
	if err == ErrBucketNotFound {
		return BUCKET_UUID_NIL, nil
	} else if err != nil {
		return BUCKET_UUID_NIL, err
	}

	return bi.GetBucketUUID(bucketName), nil
}

func (cicl *ClusterInfoCacheLiteClient) IsEphemeral(bucketName string) (bool, error) {
	bi, err := cicl.GetBucketInfo(bucketName)
	if err != nil {
		return false, err
	}

	return bi.IsEphemeral(bucketName)
}

func (cicl *ClusterInfoCacheLiteClient) ValidateBucket(bucketName string, uuids []string) (resp bool) {
	validateBucket := func() bool {
		bi, err := cicl.GetBucketInfo(bucketName)
		if err != nil {
			logging.Errorf("ValidateBucket: Unable to GetBucketInfo due to err %v", err)
			return false
		}

		if nids, err := bi.GetNodesByBucket(bucketName); err == nil && len(nids) != 0 {
			// verify UUID
			currentUUID := bi.GetBucketUUID(bucketName)
			for _, uuid := range uuids {
				if uuid != currentUUID {
					return false
				}
			}
			return true
		} else {
			logging.Fatalf("Error Fetching Bucket Info: %v Nids: %v", err, nids)
			return false
		}
	}

	resp = validateBucket()
	if resp == false {
		return validateBucket()
	}
	return resp
}

func (cicl *ClusterInfoCacheLiteClient) IsMagmaStorage(bucketName string) (bool, error) {
	isMagma := func() (bool, error) {
		bi, err := cicl.GetBucketInfo(bucketName)
		if err != nil {
			return false, err
		}

		return bi.IsMagmaStorage(bucketName)
	}

	resp, err := isMagma()
	if err != nil {
		resp, err = isMagma()
	}
	return resp, err
}

//
// API using Collection Info
//

func (c *ClusterInfoCacheLiteClient) GetCollectionInfo(bucketName string) (
	*collectionInfo, error) {
	ci := c.ciclMgr.collectionInfo(bucketName)
	if ci.valid {
		return ci, nil
	}

	logging.Tracef("ClusterInfoCacheLiteClient:GetCollectionInfo[%v] CollectionInfo is not valid force fetching data", c.logPrefix)

	ci, err := c.ciclMgr.collectionInfoSync(bucketName, c.eventWaitTimeoutSeconds)
	if err == ErrBucketNotFound {
		ci = newCollectionInfoWithErr(bucketName, err)
		return ci, nil
	}
	return ci, err
}

func (ci *collectionInfo) CollectionID(bucket, scope, collection string) string {
	if len(ci.errList) > 0 && ci.errList[0] == ErrBucketNotFound {
		return collections.COLLECTION_ID_NIL
	}
	return ci.manifest.GetCollectionID(scope, collection)
}

func (ci *collectionInfo) ScopeID(bucket, scope string) string {
	if len(ci.errList) > 0 && ci.errList[0] == ErrBucketNotFound {
		return collections.SCOPE_ID_NIL
	}
	return ci.manifest.GetScopeID(scope)
}

func (ci *collectionInfo) ScopeAndCollectionID(bucket, scope, collection string) (string, string) {
	if len(ci.errList) > 0 && ci.errList[0] == ErrBucketNotFound {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL
	}
	return ci.manifest.GetScopeAndCollectionID(scope, collection)
}

func (ci *collectionInfo) GetIndexScopeLimit(bucket, scope string) (uint32, error) {
	if len(ci.errList) > 0 && ci.errList[0] == ErrBucketNotFound {
		return collections.NUM_INDEXES_NIL, nil
	}
	return ci.manifest.GetIndexScopeLimit(scope), nil
}

func (c *ClusterInfoCacheLiteClient) GetCollectionID(bucket, scope, collection string) string {
	ci, err := c.GetCollectionInfo(bucket)
	if err != nil {
		return collections.COLLECTION_ID_NIL
	}

	return ci.CollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCacheLiteClient) GetScopeID(bucket, scope string) string {
	ci, err := c.GetCollectionInfo(bucket)
	if err != nil {
		return collections.SCOPE_ID_NIL
	}

	return ci.ScopeID(bucket, scope)
}

func (c *ClusterInfoCacheLiteClient) GetScopeAndCollectionID(bucket, scope, collection string) (string, string) {
	ci, err := c.GetCollectionInfo(bucket)
	if err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL
	}

	return ci.ScopeAndCollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCacheLiteClient) GetIndexScopeLimit(bucket, scope string) (uint32, error) {
	ci, err := c.GetCollectionInfo(bucket)
	if err != nil {
		return 0, err
	}

	return ci.GetIndexScopeLimit(bucket, scope)
}

func (c *ClusterInfoCacheLiteClient) ValidateCollectionID(bucket, scope,
	collection, collnID string, retry bool) bool {
	validateKeyspace := func() bool {
		ci, err := c.GetCollectionInfo(bucket)
		if err != nil {
			return false
		}

		cid := ci.CollectionID(bucket, scope, collection)
		if cid != collnID {
			return false
		}
		return true
	}

	resp := validateKeyspace()
	if resp == false && retry == true {
		return validateKeyspace()
	}
	return resp
}

// Stub function to implement ClusterInfoProvider interface
func (cicl *ClusterInfoCacheLiteClient) FetchWithLock() error {
	return nil
}

//
// Stub functions to make nodesInfo replaceable with clusterInfoCache
//

func (ni *NodesInfo) SetUserAgent(userAgent string)     {}
func (ni *NodesInfo) FetchNodesAndSvsInfo() (err error) { return nil }

func (ni *bucketInfo) FetchBucketInfo(bucketName string) error { return nil }
func (ni *bucketInfo) FetchWithLock() error                    { return nil }

func (ci *collectionInfo) FetchBucketInfo(bucketName string) error   { return nil }
func (ci *collectionInfo) FetchManifestInfo(bucketName string) error { return nil }
