package common

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

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
    pointer. nodeInfo.GetNodesByServiceType will give us NodeIds these should be
    used with another instance of nodeInfo fetched again later.
*/

var singletonCICLContainer struct {
	sync.Mutex
	ciclMgr  *clusterInfoCacheLiteManager
	refCount int // RefCount of ciclMgr to close it when it is 0
}

var ErrorEventWaitTimeout = errors.New("error event wait timeout")
var ErrorUnintializedNodesInfo = errors.New("error uninitialized nodesInfo")
var ErrorThisNodeNotFound = errors.New("error thisNode not found")

//
// Nodes Info
//

type nodesInfo struct {
	version          uint32
	minorVersion     uint32
	nodes            []couchbase.Node
	nodesExt         []couchbase.NodeServices
	addNodes         []couchbase.Node
	failedNodes      []couchbase.Node
	encryptedPortMap map[string]string
	node2group       map[NodeId]string
	clusterURL       string

	valid         bool
	errList       []error
	lastUpdatedTs time.Time
}

func newNodesInfo(pool *couchbase.Pool) *nodesInfo {
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

	newNInfo := &nodesInfo{
		nodes:        nodes,
		addNodes:     addNodes,
		failedNodes:  failedNodes,
		version:      version,
		minorVersion: minorVersion,
		node2group:   make(map[NodeId]string),
	}

	for i, node := range nodes {
		newNInfo.node2group[NodeId(i)] = node.ServerGroup
	}

	return newNInfo
}

func newNodesInfoWithError(err error) *nodesInfo {
	ni := &nodesInfo{}
	ni.valid = false
	ni.errList = append(ni.errList, err)
	return ni
}

func (ni *nodesInfo) setNodesExt(nodesExt []couchbase.NodeServices) {
	if nodesExt == nil {
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

func (ni *nodesInfo) setClusterURL(u string) {
	ni.clusterURL = u
}

func (ni *nodesInfo) validateNodesAndSvs(connHost string) {
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
// Cluster Info Cache Lite
//

type clusterInfoCacheLite struct {
	logPrefix string
	isIPv6    bool
	nih       nodesInfoHolder
}

func newClusterInfoCacheLite(logPrefix string) *clusterInfoCacheLite {

	c := &clusterInfoCacheLite{logPrefix: logPrefix}
	c.nih.Init()

	return c
}

func (cicl *clusterInfoCacheLite) nodesInfo() *nodesInfo {
	if ptr := cicl.nih.Get(); ptr != nil {
		return ptr
	} else {
		return newNodesInfoWithError(ErrorUnintializedNodesInfo)
	}
}

// TODO : Check log redaction
func (cicl *clusterInfoCacheLite) String() string {
	ni := cicl.nih.Get()
	if ni != nil {
		return fmt.Sprintf("%v", ni)
	}
	return ""
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

	eventMgr *eventManager
	eventCtr uint64

	maxRetries         uint32
	retryInterval      uint32
	notifierRetrySleep uint32
}

func newClusterInfoCacheLiteManager(cicl *clusterInfoCacheLite, clusterURL,
	poolName, logPrefix string) (*clusterInfoCacheLiteManager,
	error) {

	cicm := &clusterInfoCacheLiteManager{
		poolName:             poolName,
		logPrefix:            logPrefix,
		cicl:                 cicl,
		timeDiffToForceFetch: 5, // In Minutes
		poolsStreamingCh:     make(chan Notification, 100),
		notifierRetrySleep:   2,
		retryInterval:        uint32(CLUSTER_INFO_DEFAULT_RETRY_INTERVAL.Seconds()),
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

	cicm.cicl.isIPv6 = cicm.client.Info.IsIPv6
	cicm.client.SetUserAgent(logPrefix)

	// Try fetching only once in the constructor
	cicm.maxRetries = 1

	ni := cicm.FetchNodesInfo()
	cicm.cicl.nih.Set(ni)

	// Try Fetching default number of times else where
	cicm.maxRetries = CLUSTER_INFO_DEFAULT_RETRIES

	go cicm.watchClusterChanges()
	go cicm.handlePoolsChangeNotifications()
	go cicm.periodicUpdater()

	logging.Infof("Started New clusterInfoCacheManager")
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

func (cicm *clusterInfoCacheLiteManager) close() {
	logging.Infof("Closing clusterInfoCacheManager")
	cicm.ticker.Stop()
	close(cicm.poolsStreamingCh)
	cicm.cicl = nil
}

func (cicm *clusterInfoCacheLiteManager) setTimeDiffToForceFetch(minutes uint32) {
	atomic.StoreUint32(&cicm.timeDiffToForceFetch, minutes)
}

func (cicm *clusterInfoCacheLiteManager) setMaxRetries(maxRetries uint32) {
	atomic.StoreUint32(&cicm.maxRetries, maxRetries)
}

func (cicm *clusterInfoCacheLiteManager) setRetryInterval(seconds uint32) {
	atomic.StoreUint32(&cicm.retryInterval, seconds)
}

func (cicm *clusterInfoCacheLiteManager) setNotifierRetrySleep(seconds uint32) {
	atomic.StoreUint32(&cicm.notifierRetrySleep, seconds)
}

func (cicm *clusterInfoCacheLiteManager) nodesInfo() (*nodesInfo, error) {
	ni := cicm.cicl.nodesInfo()
	if !ni.valid {
		return ni, ni.errList[0]
	} else {
		return ni, nil
	}
}

func (cicm *clusterInfoCacheLiteManager) nodesInfoSync(eventTimeoutSeconds uint32) (
	*nodesInfo, error) {
	ni := cicm.cicl.nodesInfo()
	if !ni.valid {
		id := fmt.Sprintf("%d", atomic.AddUint64(&cicm.eventCtr, 1))
		evtCount := cicm.eventMgr.count(EVENT_NODEINFO_UPDATED)
		ch, err := cicm.eventMgr.register(id, EVENT_NODEINFO_UPDATED)
		if err != nil {
			return nil, err
		}
		defer cicm.eventMgr.unregister(id, EVENT_NODEINFO_UPDATED)

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

		ni = msg.(*nodesInfo)
	}
	return ni, nil
}

func (cicm *clusterInfoCacheLiteManager) periodicUpdater() {
	cicm.ticker = time.NewTicker(time.Duration(1) * time.Minute)
	for range cicm.ticker.C {
		ni := cicm.cicl.nih.Get()
		t := atomic.LoadUint32(&cicm.timeDiffToForceFetch)
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
	}
}

func (cicm *clusterInfoCacheLiteManager) handlePoolsChangeNotifications() {
	for notif := range cicm.poolsStreamingCh {
		logging.Tracef("handlePoolChangeNotification got notification %v", notif)
		p := (notif.Msg).(*couchbase.Pool)

		var ni *nodesInfo
		fetch := false

		if notif.Type == ForceUpdateNotification ||
			notif.Type == PeriodicUpdateNotification {
			// Force fetch nodesInfo
			fetch = true

		} else if notif.Type == PoolChangeNotification {
			// Try to use nodes data from Notification
			ni = newNodesInfo(p)
			ni.setClusterURL(cicm.clusterURL)

			// Try to use nodesExt from old nodesInfo
			oldNInfo := cicm.cicl.nih.Get()
			if oldNInfo != nil && oldNInfo.nodesExt != nil {
				ni.setNodesExt(oldNInfo.nodesExt)
			}

			// Validate and check if its valid
			ni.validateNodesAndSvs(cicm.client.BaseURL.Host)
			if !ni.valid {
				// fetch if invalid
				fetch = true
			}
		}

		if fetch {
			ni = cicm.FetchNodesInfo()
		}

		cicm.cicl.nih.Set(ni)
		if ni.valid {
			cicm.eventMgr.notify(EVENT_NODEINFO_UPDATED, ni)
		}
	}
}

func (cicm *clusterInfoCacheLiteManager) watchClusterChanges() {
	selfRestart := func() {
		r := atomic.LoadUint32(&cicm.notifierRetrySleep)
		time.Sleep(time.Duration(r) * time.Millisecond)
		go cicm.watchClusterChanges()
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
					cicm.poolsStreamingCh <- notif
				default:
					logging.Errorf("ClusterInfoClientLite (PoolChangeNotification): Invalid message type: %T", notif.Msg)
				}
			}
		}
	}
}

func (cicm *clusterInfoCacheLiteManager) FetchNodesInfo() *nodesInfo {
	var retryCount uint32 = 0
	maxRetries := atomic.LoadUint32(&cicm.maxRetries)
	r := atomic.LoadUint32(&cicm.retryInterval)
	retryInterval := time.Duration(r) * time.Second
retry:
	p, err := cicm.client.GetPoolWithoutRefresh(cicm.poolName)
	if err != nil {
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			return newNodesInfoWithError(err)
		}
	}

	ps, err := cicm.client.GetPoolServices(cicm.poolName)
	if err != nil {
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			return newNodesInfoWithError(err)
		}
	}

	ni := newNodesInfo(&p)
	ni.setNodesExt(ps.NodesExt)
	ni.setClusterURL(cicm.clusterURL)

	ni.validateNodesAndSvs(cicm.client.BaseURL.Host)
	if !ni.valid {
		if retryCount < maxRetries {
			retryCount++
			time.Sleep(retryInterval)
			goto retry
		} else {
			return ni
		}
	}

	return ni
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

func NewClusterInfoCacheLiteClient(clusterURL, poolName string,
	config Config) (ciclClient *ClusterInfoCacheLiteClient, err error) {

	ciclClient = &ClusterInfoCacheLiteClient{
		clusterURL: clusterURL,
		poolName:   poolName,
	}

	t := uint32(CLUSTER_INFO_DEFAULT_RETRY_INTERVAL.Seconds()) * CLUSTER_INFO_DEFAULT_RETRIES
	ciclClient.eventWaitTimeoutSeconds = t

	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()

	if singletonCICLContainer.ciclMgr == nil {
		cicl := newClusterInfoCacheLite("SingletonCICL")

		ciclMgr, err := newClusterInfoCacheLiteManager(cicl, clusterURL,
			poolName, "SingletonCICLMgr")
		if err != nil {
			return nil, err
		}

		singletonCICLContainer.ciclMgr = ciclMgr
	}

	ciclClient.ciclMgr = singletonCICLContainer.ciclMgr
	singletonCICLContainer.refCount++

	return ciclClient, err
}

func (c *ClusterInfoCacheLiteClient) Close() {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()

	singletonCICLContainer.refCount--
	c.ciclMgr = nil
	if singletonCICLContainer.refCount == 0 {
		singletonCICLContainer.ciclMgr.close()
		singletonCICLContainer.ciclMgr = nil
	}
}

func (c *ClusterInfoCacheLiteClient) SetLogPrefix(logPrefix string) {
	c.logPrefix = logPrefix
}

func (c *ClusterInfoCacheLiteClient) SetEventWaitTimeout(seconds uint32) {
	c.eventWaitTimeoutSeconds = seconds
}

func (c *ClusterInfoCacheLiteClient) SetTimeDiffToForceFetchInMgr(minutes uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setTimeDiffToForceFetch(minutes)
}

func (c *ClusterInfoCacheLiteClient) SetMaxRetriesInMgr(retries uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setMaxRetries(retries)
}

func (c *ClusterInfoCacheLiteClient) SetRetryIntervalInMgr(seconds uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setRetryInterval(seconds)
}

func (c *ClusterInfoCacheLiteClient) SetNotifierRetrySleepInMgr(seconds uint32) {
	singletonCICLContainer.Lock()
	defer singletonCICLContainer.Unlock()
	c.ciclMgr.setNotifierRetrySleep(seconds)
}

func (c *ClusterInfoCacheLiteClient) GetNodesInfo() (*nodesInfo, error) {
	ni, _ := c.ciclMgr.nodesInfo()
	if ni.valid {
		return ni, nil
	}

	logging.Tracef("NodesInfo Invalid trying to force fetch")
	return c.ciclMgr.nodesInfoSync(c.eventWaitTimeoutSeconds)
}

func (ni *nodesInfo) GetClusterVersion() uint64 {
	return GetVersion(ni.version, ni.minorVersion)
}

func (ni *nodesInfo) GetNodesByServiceType(srvc string) (nids []NodeId) {

	for i, svs := range ni.nodesExt {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (ni *nodesInfo) GetCurrentNode() NodeId {

	for i, node := range ni.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}

	return NodeId(-1)
}

func (ni *nodesInfo) GetServiceAddress(nid NodeId, srvc string,
	useEncryptedPortMap bool) (addr string, err error) {

	if int(nid) >= len(ni.nodesExt) {
		err = ErrInvalidNodeId
		return
	}

	return ni.getServiceAddress(nid, srvc, useEncryptedPortMap)
}

func (ni *nodesInfo) GetNodeUUID(nid NodeId) string {

	return ni.nodes[nid].NodeUUID
}

func (ni *nodesInfo) GetNodeIdByUUID(uuid string) (NodeId, bool) {
	for nid, node := range ni.nodes {
		if node.NodeUUID == uuid {
			return NodeId(nid), true
		}
	}

	return NodeId(-1), false
}

func (ni *nodesInfo) GetServerGroup(nid NodeId) string {
	return ni.node2group[nid]
}

func (ni *nodesInfo) GetServerVersion(nid NodeId) (int, error) {
	if int(nid) >= len(ni.nodes) {
		return 0, ErrInvalidNodeId
	}
	return getServerVersionFromVersionString(ni.nodes[nid].Version)
}

func (ni *nodesInfo) GetLocalNodeUUID() string {
	for _, node := range ni.nodes {
		if node.ThisNode {
			return node.NodeUUID
		}
	}
	return ""
}

func (ni *nodesInfo) GetLocalHostname() (string, error) {

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

func (ni *nodesInfo) GetLocalHostAddress() (string, error) {

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

func (ni *nodesInfo) GetLocalServerGroup() (string, error) {
	node := ni.GetCurrentNode()
	if node == NodeId(-1) {
		return "", ErrorThisNodeNotFound
	}

	return ni.GetServerGroup(node), nil
}

func (ni *nodesInfo) GetLocalServicePort(srvc string, useEncryptedPortMap bool) (string, error) {
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

func (ni *nodesInfo) GetLocalServiceHost(srvc string, useEncryptedPortMap bool) (string, error) {

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

func (ni *nodesInfo) GetLocalServiceAddress(srvc string, useEncryptedPortMap bool) (srvcAddr string, err error) {
	node := ni.GetCurrentNode()
	if node == NodeId(-1) {
		return "", ErrorThisNodeNotFound
	}

	srvcAddr, err = ni.GetServiceAddress(node, srvc, useEncryptedPortMap)
	if err != nil {
		return "", err
	}

	return srvcAddr, nil
}

func (ni *nodesInfo) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(ni.nodes) {
		return false, ErrInvalidNodeId
	}

	return ni.nodes[nid].Status == "healthy", nil
}

func (ni *nodesInfo) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(ni.nodes) {
		return "", ErrInvalidNodeId
	}

	return ni.nodes[nid].Status, nil
}

func (ni *nodesInfo) Nodes() []couchbase.Node {
	return ni.nodes
}

func (ni *nodesInfo) EncryptPortMapping() map[string]string {
	return ni.encryptedPortMap
}

func (ni *nodesInfo) getServiceAddress(nid NodeId, srvc string,
	useEncryptedPortMap bool) (addr string, err error) {

	node := ni.nodesExt[nid]

	port, ok := node.Services[srvc]
	if !ok {
		logging.Errorf("ClusterInfoCacheLiteClient:getServiceAddress Invalid Service %v for node %v. Nodes %v \n NodeServices %v",
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

func (c *ClusterInfoCacheLiteClient) GetLocalServiceAddress(srvc string,
	useEncryptedPortMap bool) (srvcAddr string, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return "", err
	}

	var nid NodeId
	for i, ns := range ni.nodesExt {
		if ns.ThisNode {
			nid = NodeId(i)
		}
	}

	return ni.getServiceAddress(nid, srvc, useEncryptedPortMap)
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

func (c *ClusterInfoCacheLiteClient) GetActiveIndexerNodes() (
	nodes []couchbase.Node, err error) {
	ni, err := c.GetNodesInfo()
	if err != nil {
		return nil, err
	}

	for _, n := range ni.nodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
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
