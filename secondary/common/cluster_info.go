package common

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

var (
	ErrInvalidNodeId       = errors.New("Invalid NodeId")
	ErrInvalidService      = errors.New("Invalid service")
	ErrNodeNotBucketMember = errors.New("Node is not a member of bucket")
	ErrValidationFailed    = errors.New("ClusterInfo Validation Failed")
	ErrInvalidVersion      = errors.New("Invalid couchbase-server version")
)

var ServiceAddrMap map[string]string

const (
	INDEX_ADMIN_SERVICE = "indexAdmin"
	INDEX_SCAN_SERVICE  = "indexScan"
	INDEX_HTTP_SERVICE  = "indexHttp"
	INDEX_HTTPS_SERVICE = "indexHttps"
	KV_SERVICE          = "kv"
	KV_SSL_SERVICE      = "kvSSL"
	MGMT_SERVICE        = "mgmt"
	MGMT_SSL_SERVICE    = "mgmtSSL"
	CBQ_SERVICE         = "n1ql"
	CBQ_SSL_SERVICE     = "n1qlSSL"
	INDEX_PROJECTOR     = "projector"
	INDEX_DATA_INIT     = "indexStreamInit"
	INDEX_DATA_MAINT    = "indexStreamMaint"
	INDEX_DATA_CATUP    = "indexStreamCatchup"
	INDEX_RPC_SERVICE   = "indexStreamCatchup"
	NODE_IPV6           = "inet6"
)

const CLUSTER_INFO_DEFAULT_RETRIES = 300
const CLUSTER_INFO_DEFAULT_RETRY_INTERVAL = 2 * time.Second // Seconds
const CLUSTER_INFO_DEFAULT_RETRY_FACTOR = 1                 // Exponential back off
const CLUSTER_INFO_VALIDATION_RETRIES = 10

const BUCKET_UUID_NIL = ""

// Helper object for fetching cluster information
// Can be used by services running on a cluster node to connect with
// local management service for obtaining cluster information.
// Info cache can be updated by using Refresh() method.
type ClusterInfoCache struct {
	sync.RWMutex
	url       string
	poolName  string
	logPrefix string
	userAgent string

	retries       uint32
	retryInterval time.Duration
	retryFactor   int

	useStaticPorts bool
	servicePortMap map[string]string

	client       couchbase.Client
	pool         couchbase.Pool
	nodes        []couchbase.Node
	nodesvs      []couchbase.NodeServices
	node2group   map[NodeId]string // node->group
	failedNodes  []couchbase.Node
	addNodes     []couchbase.Node
	version      uint32
	minorVersion uint32

	encryptPortMapping map[string]string
}

// Helper object that keeps an instance of ClusterInfoCache cached
// and updated periodically or when things change in the cluster
// Readers/Consumers must lock cinfo before using it
type ClusterInfoClient struct {
	cinfo                     *ClusterInfoCache
	clusterURL                string
	pool                      string
	servicesNotifierRetryTm   int
	finch                     chan bool
	fetchDataOnHashChangeOnly bool
	bucketsHash               string
	serverGroupsHash          string
	nodeUUID2HashMap          map[string]int
	userAgent                 string
}

type NodeId int

// NewClusterInfoCache constructs a new, non-shared ClusterInfoCache object, so consumer does not
// need to lock it unless it is multi-threaded.
func NewClusterInfoCache(clusterUrl string, pool string) (*ClusterInfoCache, error) {
	c := &ClusterInfoCache{
		url:           clusterUrl,
		poolName:      pool,
		retries:       CLUSTER_INFO_DEFAULT_RETRIES,
		retryInterval: CLUSTER_INFO_DEFAULT_RETRY_INTERVAL,
		retryFactor:   CLUSTER_INFO_DEFAULT_RETRY_FACTOR,
		node2group:    make(map[NodeId]string),
	}

	return c, nil
}

// FetchNewClusterInfoCache returns a pointer to a new instance, so the anonymous mutex in the returned
// object only needs to be locked if the caller shares it among goroutines.
func FetchNewClusterInfoCache(clusterUrl string, pool string, userAgent string) (*ClusterInfoCache, error) {

	url, err := ClusterAuthUrl(clusterUrl)
	if err != nil {
		return nil, err
	}

	c, err := NewClusterInfoCache(url, pool)
	if err != nil {
		return nil, err
	}

	c.SetUserAgent(userAgent)

	if ServiceAddrMap != nil {
		c.SetServicePorts(ServiceAddrMap)
	}

	if err := c.Fetch(); err != nil {
		return nil, err
	}

	return c, nil
}

// FetchNewClusterInfoCache2 does not do an Implicit Fetch() like FetchNewClusterInfoCache
func FetchNewClusterInfoCache2(clusterUrl string, pool string, userAgent string) (*ClusterInfoCache, error) {

	url, err := ClusterAuthUrl(clusterUrl)
	if err != nil {
		return nil, err
	}

	c, err := NewClusterInfoCache(url, pool)
	if err != nil {
		return nil, err
	}

	c.SetUserAgent(userAgent)

	if ServiceAddrMap != nil {
		c.SetServicePorts(ServiceAddrMap)
	}

	return c, nil
}

func SetServicePorts(portMap map[string]string) {
	ServiceAddrMap = portMap
}

func (c *ClusterInfoCache) SetLogPrefix(p string) {
	c.logPrefix = p
}

func (c *ClusterInfoCache) SetMaxRetries(r uint32) {
	c.retries = r
}

// Seconds
func (c *ClusterInfoCache) SetRetryInterval(seconds uint32) {
	c.retryInterval = time.Duration(seconds) * time.Second
}

func (c *ClusterInfoCache) SetRetryBackoffFactor(f int) {
	c.retryFactor = f
}

func (c *ClusterInfoCache) SetUserAgent(userAgent string) {
	c.userAgent = userAgent
}

func (c *ClusterInfoCache) SetServicePorts(portMap map[string]string) {

	c.useStaticPorts = true
	c.servicePortMap = portMap

}

func (c *ClusterInfoCache) Connect() (err error) {
	cl, err := couchbase.Connect(c.url)
	if err != nil {
		return err
	}
	c.client = cl

	c.client.SetUserAgent(c.userAgent)
	return nil
}

// Note: This function does not fetch BucketMap and Manifest data in c.pool
func (c *ClusterInfoCache) FetchNodesData() (err error) {
	p, err := c.client.GetPoolWithoutRefresh(c.poolName)
	if err != nil {
		return err
	}
	c.pool = p

	found := c.updateNodesData()
	if !found {
		return errors.New("Current node's cluster membership is not active")
	}
	return nil
}

func (c *ClusterInfoCache) FetchNodeSvsData() (err error) {
	var poolServs couchbase.PoolServices

	poolServs, err = c.client.GetPoolServices(c.poolName)
	if err != nil {
		return err
	}

	c.nodesvs = poolServs.NodesExt
	c.buildEncryptPortMapping()
	return nil
}

// FetchForBucket loads a ClusterInfoCache with bucket-specific info.
func (c *ClusterInfoCache) FetchForBucket(bucketName string, getTerseBucketInfo bool,
	getBucketManifest bool, getServerGroups bool, getNodeSvs bool) error {

	fn := func(retryNum int, errPrior error) error {
		if errPrior != nil {
			logging.Infof("%vError occurred during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, errPrior, retryNum)
		}

		cl, err := couchbase.Connect(c.url)
		if err != nil {
			return err
		}
		c.client = cl
		c.client.SetUserAgent(c.userAgent)

		if err = c.FetchNodesData(); err != nil {
			return err
		}

		if getTerseBucketInfo {
			if err = c.pool.RefreshBucket(bucketName, true); err != nil {
				return err
			}
		}

		if getBucketManifest {
			if err = c.pool.RefreshManifest(bucketName, true); err != nil {
				return err
			}
		}

		if getServerGroups {
			if err = c.FetchServerGroups(); err != nil {
				return err
			}
		}

		if getNodeSvs {
			if err = c.FetchNodeSvsData(); err != nil {
				return err
			}
			if !c.validateCache(c.client.Info.IsIPv6) {
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(int(c.retries), c.retryInterval, c.retryFactor, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) GetIndexScopeLimit(bucketn, scope string) (uint32, error) {
	cl, err := couchbase.Connect(c.url)
	if err != nil {
		return 0, err
	}
	c.client = cl
	c.client.SetUserAgent(c.userAgent)
	return c.client.GetIndexScopeLimit(bucketn, scope)
}

func (c *ClusterInfoCache) Fetch() error {

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occurred during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		vretry := 0
	retry:
		cl, err := couchbase.Connect(c.url)
		if err != nil {
			return err
		}
		c.client = cl
		c.client.SetUserAgent(c.userAgent)

		p, err := c.client.GetPool(c.poolName)
		if err != nil {
			return err
		}
		c.pool = p

		var nodes []couchbase.Node
		var failedNodes []couchbase.Node
		var addNodes []couchbase.Node
		version := uint32(math.MaxUint32)
		minorVersion := uint32(math.MaxUint32)
		for _, n := range c.pool.Nodes {
			if n.ClusterMembership == "active" {
				nodes = append(nodes, n)
			} else if n.ClusterMembership == "inactiveFailed" {
				// node being failed over
				failedNodes = append(failedNodes, n)
			} else if n.ClusterMembership == "inactiveAdded" {
				// node being added (but not yet rebalanced in)
				addNodes = append(addNodes, n)
			} else {
				logging.Warnf("ClusterInfoCache: unrecognized node membership %v", n.ClusterMembership)
			}

			// Find the minimum cluster compatibility
			v := uint32(n.ClusterCompatibility / 65536)
			minorv := uint32(n.ClusterCompatibility) - (v * 65536)
			if v < version || (v == version && minorv < minorVersion) {
				version = v
				minorVersion = minorv
			}
		}
		c.nodes = nodes
		c.failedNodes = failedNodes
		c.addNodes = addNodes

		c.version = version
		c.minorVersion = minorVersion
		if c.version == math.MaxUint32 {
			c.version = 0
		}

		found := false
		for _, node := range c.nodes {
			if node.ThisNode {
				found = true
			}
		}

		if !found {
			return errors.New("Current node's cluster membership is not active")
		}

		var poolServs couchbase.PoolServices
		poolServs, err = c.client.GetPoolServices(c.poolName)
		if err != nil {
			return err
		}
		c.nodesvs = poolServs.NodesExt
		c.buildEncryptPortMapping()

		if err := c.FetchServerGroups(); err != nil {
			return err
		}

		if !c.validateCache(c.client.Info.IsIPv6) {
			if vretry < CLUSTER_INFO_VALIDATION_RETRIES {
				vretry++
				logging.Infof("%vValidation Failed for cluster info.. Retrying(%d)",
					c.logPrefix, vretry)
				goto retry
			} else {
				logging.Infof("%vValidation Failed for cluster info.. %v",
					c.logPrefix, c)
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(int(c.retries), c.retryInterval, c.retryFactor, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) ForceFetch() error {
	return c.FetchWithLock()
}

func (c *ClusterInfoCache) FetchWithLock() error {
	c.Lock()
	defer c.Unlock()

	return c.Fetch()
}

func (c *ClusterInfoCache) updateNodesData() bool {
	var nodes []couchbase.Node
	var failedNodes []couchbase.Node
	var addNodes []couchbase.Node
	version := uint32(math.MaxUint32)
	minorVersion := uint32(math.MaxUint32)

	for _, n := range c.pool.Nodes {
		if n.ClusterMembership == "active" {
			nodes = append(nodes, n)
		} else if n.ClusterMembership == "inactiveFailed" {
			// node being failed over
			failedNodes = append(failedNodes, n)
		} else if n.ClusterMembership == "inactiveAdded" {
			// node being added (but not yet rebalanced in)
			addNodes = append(addNodes, n)
		} else {
			logging.Warnf("ClusterInfoCache: unrecognized node membership %v", n.ClusterMembership)
		}

		// Find the minimum cluster compatibility
		v := uint32(n.ClusterCompatibility / 65536)
		minorv := uint32(n.ClusterCompatibility) - (v * 65536)
		if v < version || (v == version && minorv < minorVersion) {
			version = v
			minorVersion = minorv
		}
	}

	c.nodes = nodes
	c.failedNodes = failedNodes
	c.addNodes = addNodes
	c.version = version
	c.minorVersion = minorVersion
	if c.version == math.MaxUint32 {
		c.version = 0
	}

	found := false
	for _, node := range c.nodes {
		if node.ThisNode {
			found = true
		}
	}
	return found
}

func (c *ClusterInfoCache) FetchWithLockForPoolChange() error {
	c.Lock()
	defer c.Unlock()

	return c.FetchForPoolChange()
}

func (c *ClusterInfoCache) FetchForPoolChange() error {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occurred during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		vretry := 0
	retry:
		cl, err := couchbase.Connect(c.url)
		if err != nil {
			return err
		}
		c.client = cl
		c.client.SetUserAgent(c.userAgent)

		np, err := c.client.GetPoolWithoutRefresh(c.poolName)
		if err != nil {
			return err
		}

		err = c.updatePool(&np)
		if err != nil {
			return err
		}

		found := c.updateNodesData()
		if !found {
			return errors.New("Current node's cluster membership is not active")
		}

		var poolServs couchbase.PoolServices
		poolServs, err = c.client.GetPoolServices(c.poolName)
		if err != nil {
			return err
		}
		c.nodesvs = poolServs.NodesExt
		c.buildEncryptPortMapping()

		if err := c.FetchServerGroups(); err != nil {
			return err
		}

		if !c.validateCache(c.client.Info.IsIPv6) {
			if vretry < CLUSTER_INFO_VALIDATION_RETRIES {
				vretry++
				logging.Infof("%vValidation Failed for cluster info.. Retrying(%d)",
					c.logPrefix, vretry)
				goto retry
			} else {
				logging.Infof("%vValidation Failed for cluster info.. %v",
					c.logPrefix, c)
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(int(c.retries), c.retryInterval, c.retryFactor, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) FetchNodesAndSvsInfoWithLock() (err error) {
	c.Lock()
	defer c.Unlock()

	return c.FetchNodesAndSvsInfo()
}

func (c *ClusterInfoCache) FetchNodesAndSvsInfo() (err error) {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occurred during nodes and nodesvs update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		vretry := 0
	retry:
		if err = c.Connect(); err != nil {
			return err
		}

		if err = c.FetchNodesData(); err != nil {
			return err
		}

		if err = c.FetchNodeSvsData(); err != nil {
			return err
		}

		if !c.validateCache(c.client.Info.IsIPv6) {
			if vretry < CLUSTER_INFO_VALIDATION_RETRIES {
				vretry++
				logging.Infof("%vValidation Failed while updating nodes and nodesvs.. Retrying(%d)",
					c.logPrefix, vretry)
				goto retry
			} else {
				logging.Errorf("%vValidation Failed while updating nodes and nodesvs.. %v",
					c.logPrefix, c)
				return ErrValidationFailed
			}
		}

		return nil
	}

	rh := NewRetryHelper(int(c.retries), c.retryInterval, c.retryFactor, fn)
	return rh.Run()
}

func (c *ClusterInfoCache) FetchManifestInfoOnUIDChange(bucketName string, muid string) error {
	c.Lock()
	defer c.Unlock()

	p := &c.pool
	m, ok := p.Manifest[bucketName]
	if !ok || m.UID != muid {
		return p.RefreshManifest(bucketName, false)
	}
	return nil
}

func (c *ClusterInfoCache) FetchManifestInfo(bucketName string) error {
	c.Lock()
	defer c.Unlock()

	pool := &c.pool
	return pool.RefreshManifest(bucketName, false)
}

// Note: This function does not update c.pool.nodes but updates
// c.pool.BucketMap[bucketName] and hence the bucket's nodelist it is
// assumed that c.Fetch() is called atleast once after a new server is
// added. So nodelist received from terseBucket endpoint will be a subset
// of nodes received from poolsStreaming endpoint previously.
func (c *ClusterInfoCache) FetchBucketInfo(bucketName string) error {
	c.Lock()
	defer c.Unlock()

	pool := &c.pool
	return pool.RefreshBucket(bucketName, false)
}

func buildEncryptPortMapping(nodesvs []couchbase.NodeServices) map[string]string {
	mapping := make(map[string]string)

	//default (hardcode in ns-server)
	mapping["11210"] = "11207" //kv
	mapping["8093"] = "18093"  //cbq
	mapping["9100"] = "9100"   //gsi admin
	mapping["9101"] = "9101"   //gsi scan
	mapping["9102"] = "19102"  //gsi http
	mapping["9103"] = "9103"   //gsi init stream
	mapping["9104"] = "9104"   //gsi catchup stream
	mapping["9105"] = "9105"   //gsi maint stream
	mapping["9999"] = "9999"   //gsi http

	// go through service port map for floating ports
	for _, node := range nodesvs {
		_, ok := node.Services[INDEX_HTTP_SERVICE]
		_, ok1 := node.Services[INDEX_HTTPS_SERVICE]
		if ok && ok1 {
			mapping[fmt.Sprint(node.Services[INDEX_HTTP_SERVICE])] = fmt.Sprint(node.Services[INDEX_HTTPS_SERVICE])
		}

		if _, ok := node.Services[INDEX_SCAN_SERVICE]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_SCAN_SERVICE])] = fmt.Sprint(node.Services[INDEX_SCAN_SERVICE])
		}

		if _, ok := node.Services[INDEX_ADMIN_SERVICE]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_ADMIN_SERVICE])] = fmt.Sprint(node.Services[INDEX_ADMIN_SERVICE])
		}

		if _, ok := node.Services[INDEX_PROJECTOR]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_PROJECTOR])] = fmt.Sprint(node.Services[INDEX_PROJECTOR])
		}

		if _, ok := node.Services[INDEX_DATA_INIT]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_DATA_INIT])] = fmt.Sprint(node.Services[INDEX_DATA_INIT])
		}

		if _, ok := node.Services[INDEX_DATA_MAINT]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_DATA_MAINT])] = fmt.Sprint(node.Services[INDEX_DATA_MAINT])
		}

		if _, ok := node.Services[INDEX_DATA_CATUP]; ok {
			mapping[fmt.Sprint(node.Services[INDEX_DATA_CATUP])] = fmt.Sprint(node.Services[INDEX_DATA_CATUP])
		}

		_, ok = node.Services[KV_SERVICE]
		_, ok1 = node.Services[KV_SSL_SERVICE]
		if ok && ok1 {
			mapping[fmt.Sprint(node.Services[KV_SERVICE])] = fmt.Sprint(node.Services[KV_SSL_SERVICE])
		}
		_, ok = node.Services[CBQ_SERVICE]
		_, ok1 = node.Services[CBQ_SSL_SERVICE]
		if ok && ok1 {
			mapping[fmt.Sprint(node.Services[CBQ_SERVICE])] = fmt.Sprint(node.Services[CBQ_SSL_SERVICE])
		}

		// As ns_server does not send encrypted ports in all APIs we will need this in map.
		_, ok = node.Services[MGMT_SERVICE]
		_, ok1 = node.Services[MGMT_SSL_SERVICE]
		if ok && ok1 {
			mapping[fmt.Sprint(node.Services[MGMT_SERVICE])] = fmt.Sprint(node.Services[MGMT_SSL_SERVICE])
		}
	}

	return mapping
}

func (c *ClusterInfoCache) EncryptPortMapping() map[string]string {
	return c.encryptPortMapping
}

func (c *ClusterInfoCache) buildEncryptPortMapping() {

	c.encryptPortMapping = buildEncryptPortMapping(c.nodesvs)
}

func (c *ClusterInfoCache) FetchServerGroups() error {

	// ServerGroupsUri is empty in CE
	if c.pool.ServerGroupsUri == "" {
		return nil
	}

	groups, err := c.pool.GetServerGroups()
	if err != nil {
		return err
	}

	result := make(map[NodeId]string)
	for nid, cached := range c.nodes {
		found := false
		for _, group := range groups.Groups {
			for _, node := range group.Nodes {
				if node.Hostname == cached.Hostname {
					result[NodeId(nid)] = group.Name
					found = true
				}
			}
		}
		if !found {
			logging.Warnf("ClusterInfoCache Initialization: Unable to identify server group for node %v.", cached.Hostname)
		}
	}

	c.node2group = result
	return nil
}

func (c *ClusterInfoCache) GetClusterVersion() uint64 {
	return GetVersion(c.version, c.minorVersion)
}

func (c *ClusterInfoCache) GetServerGroup(nid NodeId) string {

	return c.node2group[nid]
}

func (c *ClusterInfoCache) GetNodeUUID(nid NodeId) string {

	return c.nodes[nid].NodeUUID
}

func (c *ClusterInfoCache) GetNodeIdByUUID(uuid string) (NodeId, bool) {
	for nid, node := range c.nodes {
		if node.NodeUUID == uuid {
			return NodeId(nid), true
		}
	}

	return NodeId(-1), false
}

func (c *ClusterInfoCache) Nodes() []couchbase.Node {
	return c.nodes
}

func (c *ClusterInfoCache) GetNodeIdsByServiceType(srvc string) (nids []NodeId) {
	for i, svs := range c.nodesvs {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (c *ClusterInfoCache) GetNodesByIds(nids []NodeId) (nodes []couchbase.Node) {
	for _, nid := range nids {
		nodes = append(nodes, c.nodes[nid])
	}
	return
}

// The functions output comes from querying the /pools/default/nodeServices endpoint
// thus it returns list of node where input service is active.

func (c *ClusterInfoCache) GetNodesByServiceType(srvc string) (nodes []couchbase.Node) {
	nids := c.GetNodeIdsByServiceType(srvc)
	return c.GetNodesByIds(nids)
}

// The output of this function comes from querying the /pools/default endpoint
// Thus the function returns list of nodes where "cluster membership of node is active".
// Which is essentially a confirmation of fact that a service is configured on a node and
// does not always mean that service is active on a node.
// If a list of nodes with active services is needed then one needs to query /pools/default/nodeServices endpoint
// which is done by GetNodesByServiceType and GetNodeIdsByServiceType functions.
// (Use of endpoints and their meaning based on MB-51274)
func (c *ClusterInfoCache) GetActiveIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetActiveIndexerNodesStrMap() map[string]bool {
	out := make(map[string]bool)
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "index" {
				out[n.NodeUUID] = true
			}
		}
	}

	return out
}

func (c *ClusterInfoCache) GetServiceFromPort(addr string) (string, error) {

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	for _, node := range c.nodesvs {
		for svc, svcPort := range node.Services {
			if fmt.Sprint(svcPort) == port {
				return svc, nil
			}
		}
	}

	return "", fmt.Errorf("Port number %v not found", port)
}

//
// Translate a hostport for one service for a node to the hostport
// for the other service on the same node.
//
// Example:
// Translate indexer scanport to httpport:
//     TranslatePort("127.0.0.1:9101", INDEX_SCAN_SERVICE, INDEX_HTTP_SERVICE)
//     returns "127.0.0.1:9102"
//

func (c *ClusterInfoCache) TranslatePort(host, src, dest string) (string, error) {
	for _, nodeServices := range c.nodesvs {
		if addr, _ := nodeServices.GetHostNameWithPort(src); len(addr) != 0 && addr == host {
			return nodeServices.GetHostNameWithPort(dest)
		}
	}

	return "", nil
}

func (c *ClusterInfoCache) GetFailedIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNewIndexerNodes() (nodes []couchbase.Node) {
	for _, n := range c.addNodes {
		for _, s := range n.Services {
			if s == "index" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

// The output of this function comes from querying the /pools/default endpoint
// Thus the function returns list of nodes where "cluster membership of node is active".
// Which is essentially a confirmation of fact that a service is configured on a node and
// does not always mean that service is active on a node.
// If a list of nodes with active services is needed then one needs to query /pools/default/nodeServices endpoint
// which is done by GetNodesByServiceType and GetNodeIdsByServiceType functions.
// (Use of endpoints and their meaning based on MB-51274)
func (c *ClusterInfoCache) GetActiveKVNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetFailedKVNodes() (nodes []couchbase.Node) {
	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

// The output of this function comes from querying the /pools/default endpoint
// Thus the function returns list of nodes where "cluster membership of node is active".
// Which is essentially a confirmation of fact that a service is configured on a node and
// does not always mean that service is active on a node.
// If a list of nodes with active services is needed then one needs to query /pools/default/nodeServices endpoint
// which is done by GetNodesByServiceType and GetNodeIdsByServiceType functions.
// (Use of endpoints and their meaning based on MB-51274)
func (c *ClusterInfoCache) GetActiveQueryNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "n1ql" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetFailedQueryNodes() (nodes []couchbase.Node) {
	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "n1ql" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetAllKVNodes() (nodes []couchbase.Node) {
	for _, n := range c.nodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	for _, n := range c.failedNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	for _, n := range c.addNodes {
		for _, s := range n.Services {
			if s == "kv" {
				nodes = append(nodes, n)
			}
		}
	}

	return
}

func (c *ClusterInfoCache) GetNodesByBucket(bucket string) (nids []NodeId, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	for i := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			nids = append(nids, nid)
		}
	}

	return
}

// Return UUID of a given bucket.
func (c *ClusterInfoCache) GetBucketUUID(bucket string) (uuid string) {

	// This function retuns an error if bucket not found
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return BUCKET_UUID_NIL
	}
	defer b.Close()

	if len(b.HibernationState) != 0 {
		return b.UUID
	}

	// This node recognize this bucket.   Make sure its vb is resided in at least one node.
	for i := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			// find the bucket resides in at least one node
			return b.UUID
		}
	}

	// no nodes recognize this bucket
	return BUCKET_UUID_NIL
}

// Note: Currently, ns_server does not provide any streaming rest endpoint for
// observing collections manifest. So, serviceChangeNotifier will not refresh
// the clusterInfoCache incase of a change in collection manifest. This might
// result in stale manifest with cluster info cache.
//
// Till the time, ns_server provides a streaming rest endpoint, it is advisable
// to manually refresh the cluster info cache before retrieving the collectionID
// i.e. use common.GetCollectionID() instead of directly calling cinfo.GetCollectionID()
//
// As of this patch, only IndexManager calls cluster info cache without any manual
// refresh as IndexManager is built on top of clusterInfoClient
func (c *ClusterInfoCache) GetCollectionID(bucket, scope, collection string) string {
	return c.pool.GetCollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCache) CollectionID(bucket, scope, collection string) string {
	c.RLock()
	defer c.RUnlock()
	return c.pool.GetCollectionID(bucket, scope, collection)
}

// See the comment for clusterInfoCache.GetCollectionID
func (c *ClusterInfoCache) GetScopeID(bucket, scope string) string {
	return c.pool.GetScopeID(bucket, scope)
}

func (c *ClusterInfoCache) ScopeID(bucket, scope string) string {
	c.RLock()
	defer c.RUnlock()
	return c.pool.GetScopeID(bucket, scope)
}

// See the comment for clusterInfoCache.GetCollectionID
func (c *ClusterInfoCache) GetScopeAndCollectionID(bucket, scope, collection string) (string, string) {
	return c.pool.GetScopeAndCollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCache) ScopeAndCollectionID(bucket, scope, collection string) (string, string) {
	c.RLock()
	defer c.RUnlock()
	return c.pool.GetScopeAndCollectionID(bucket, scope, collection)
}

func (c *ClusterInfoCache) IsEphemeral(bucket string) (bool, error) {
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return false, err
	}
	defer b.Close()
	return strings.EqualFold(b.Type, "ephemeral"), nil
}

func (c *ClusterInfoCache) IsMagmaStorage(bucket string) (bool, error) {
	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return false, err
	}
	defer b.Close()
	return strings.EqualFold(b.StorageBackend, "magma"), nil
}

func (c *ClusterInfoCache) GetCurrentNode() NodeId {
	for i, node := range c.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}
	// TODO: can we avoid this panic ?
	panic("Current node is not in active membership")
}

func (c *ClusterInfoCache) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(c.nodes) {
		return false, ErrInvalidNodeId
	}

	return c.nodes[nid].Status == "healthy", nil
}

func (c *ClusterInfoCache) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(c.nodes) {
		return "", ErrInvalidNodeId
	}

	return c.nodes[nid].Status, nil
}

func (c *ClusterInfoCache) GetServiceAddress(nid NodeId, srvc string, useEncryptedPortMap bool) (addr string, err error) {
	var port int
	var ok bool

	if int(nid) >= len(c.nodesvs) {
		err = ErrInvalidNodeId
		return
	}

	node := c.nodesvs[nid]

	if port, ok = node.Services[srvc]; !ok {
		logging.Errorf("%vInvalid Service %v for node %v. Nodes %v \n NodeServices %v",
			c.logPrefix, srvc, node, c.nodes, c.nodesvs)
		err = errors.New(ErrInvalidService.Error() + fmt.Sprintf(": %v", srvc))
		return
	}

	// For current node, hostname might be empty
	// Insert hostname used to connect to the cluster
	if node.Hostname == "" {
		cUrl, err := url.Parse(c.url)
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

func (c *ClusterInfoCache) GetLocalVBuckets(bucket string) (vbs []uint16, err error) {
	nid := c.GetCurrentNode()

	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	idx, ok := c.findVBServerIndex(b, nid)
	if !ok {
		err = errors.New(ErrNodeNotBucketMember.Error() + fmt.Sprintf(": %v", c.nodes[nid].Hostname))
		return
	}

	vbmap := b.VBServerMap()

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint16(vb))
		}
	}

	return
}

func (c *ClusterInfoCache) GetNumVBuckets(bucket string) (numVBuckets int, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	numVBs := b.NumVBuckets

	if numVBs < MIN_VBUCKETS_ALLOWED || numVBs > MAX_VBUCKETS_ALLOWED {
		logging.Errorf("ClusterInfoCache::GetNumVBuckets, err: %v, bucket: %v, numVBuckets: %v",
			ErrNumVbRange, bucket, numVBs)
	}

	return numVBs, nil
}

func (c *ClusterInfoCache) GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	idx, ok := c.findVBServerIndex(b, nid)
	if !ok {
		err = errors.New(ErrNodeNotBucketMember.Error() + fmt.Sprintf(": %v", c.nodes[nid].Hostname))
		return
	}

	vbmap := b.VBServerMap()

	for vb, idxs := range vbmap.VBucketMap {
		if idxs[0] == idx {
			vbs = append(vbs, uint32(vb))
		}
	}

	return
}

func (c *ClusterInfoCache) findVBServerIndex(b *couchbase.Bucket, nid NodeId) (int, bool) {
	bnodes := b.Nodes()

	for idx, n := range bnodes {
		if c.sameNode(n, c.nodes[nid]) {
			return idx, true
		}
	}

	return 0, false
}

func (c *ClusterInfoCache) sameNode(n1 couchbase.Node, n2 couchbase.Node) bool {
	return n1.Hostname == n2.Hostname
}

func (c *ClusterInfoCache) GetLocalServiceAddress(srvc string, useEncryptedPortMap bool) (srvcAddr string, err error) {

	if c.useStaticPorts {

		h, err := c.GetLocalHostname()
		if err != nil {
			return "", err
		}

		p, e := c.getStaticServicePort(srvc)
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
		node := c.GetCurrentNode()
		srvcAddr, err = c.GetServiceAddress(node, srvc, useEncryptedPortMap)
		if err != nil {
			return "", err
		}
	}

	return srvcAddr, nil
}

func (c *ClusterInfoCache) GetLocalServicePort(srvc string, useEncryptedPortMap bool) (string, error) {
	addr, err := c.GetLocalServiceAddress(srvc, useEncryptedPortMap)
	if err != nil {
		return addr, err
	}

	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return p, e
	}

	return net.JoinHostPort("", p), nil
}

func (c *ClusterInfoCache) GetLocalServiceHost(srvc string, useEncryptedPortMap bool) (string, error) {

	addr, err := c.GetLocalServiceAddress(srvc, useEncryptedPortMap)
	if err != nil {
		return addr, err
	}

	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return h, nil
}

func (c *ClusterInfoCache) GetLocalServerGroup() (string, error) {
	node := c.GetCurrentNode()
	return c.GetServerGroup(node), nil
}

func (c *ClusterInfoCache) GetLocalHostAddress() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	_, p, _ := net.SplitHostPort(cUrl.Host)

	h, err := c.GetLocalHostname()
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(h, p), nil

}

func (c *ClusterInfoCache) GetLocalHostname() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}

	h, _, _ := net.SplitHostPort(cUrl.Host)

	nid := c.GetCurrentNode()

	if int(nid) >= len(c.nodesvs) {
		return "", ErrInvalidNodeId
	}

	node := c.nodesvs[nid]
	if node.Hostname == "" {
		node.Hostname = h
	}

	return node.Hostname, nil

}

// NodeUUID is a part of cluster info cache from 6.5.0
func (c *ClusterInfoCache) GetLocalNodeUUID() string {
	for _, node := range c.nodes {
		if node.ThisNode {
			return node.NodeUUID
		}
	}
	return ""
}

func (c *ClusterInfoCache) GetServerVersion(nid NodeId) (int, error) {
	if int(nid) >= len(c.nodes) {
		return 0, ErrInvalidNodeId
	}
	return readVersionForNode(c.nodes[nid])
}

func readVersionForNode(node couchbase.Node) (int, error) {
	var ver, minorv uint32 = readVersionFromString(node.Version)
	if ver == 0 {
		ver, minorv = readVersionFromClusterComptability(node.ClusterCompatibility)
	}
	return computeServerVersion(ver, minorv)
}

func readVersionFromClusterComptability(clusterCompat int) (uint32, uint32) {
	v := uint32(clusterCompat / 65536)
	minorv := uint32(clusterCompat) - (v * 65536)
	return v, minorv
}

func readVersionFromString(v string) (uint32, uint32) {
	// Couchbase-server version will be of the form
	// <major>.<minor>.<maint_release>-<build_number>-<community/enterprise>
	// E.g. 6.5.0-0000-enterprise, 6.0.3-2855-enterprise etc.
	versionStr := strings.Split(v, ".")
	if len(versionStr) < 3 {
		return 0, 0
	}
	ver, err := strconv.Atoi(versionStr[0])
	if err != nil {
		logging.Warnf("ClusterInfoCache:readVersionFromString - Failed as version is not parsable err: %v version: %v", err, versionStr)
		return 0, 0
	}
	minorv, err := strconv.Atoi(versionStr[1])
	if err != nil {
		logging.Warnf("ClusterInfoCache:readVersionFromString - Failed as minor version is not parsable err: %v version: %v", err, versionStr)
		return 0, 0
	}
	return uint32(ver), uint32(minorv)
}

func computeServerVersion(v, minorv uint32) (int, error) {
	if v == 0 {
		return 0, fmt.Errorf("invalid version %v.%v", v, minorv)
	}

	if v < 5 {
		return INDEXER_45_VERSION, nil
	}
	if v == 5 {
		if minorv < 5 {
			return INDEXER_50_VERSION, nil
		}
		if minorv >= 5 {
			return INDEXER_55_VERSION, nil
		}
	}
	if v == 6 {
		if minorv >= 5 {
			return INDEXER_65_VERSION, nil
		}
	}
	if v == 7 {
		if minorv >= 6 {
			return INDEXER_76_VERSION, nil
		} else if minorv == 5 {
			return INDEXER_75_VERSION, nil
		} else if minorv == 2 {
			return INDEXER_72_VERSION, nil
		} else if minorv == 1 {
			return INDEXER_71_VERSION, nil
		}
		return INDEXER_70_VERSION, nil
	}
	return INDEXER_55_VERSION, nil
}

func (c *ClusterInfoCache) validateCache(isIPv6 bool) bool {

	if len(c.nodes) != len(c.nodesvs) {
		logging.Warnf("ClusterInfoCache:validateCache - Failed as len(c.nodes): %v != len(c.nodesvs): %v", len(c.nodes), len(c.nodesvs))
		return false
	}

	//validation not required for single node setup(MB-16494)
	if len(c.nodes) == 1 && len(c.nodesvs) == 1 {
		return true
	}

	var hostList1 []string
	var addressFamily []string

	// Validate nodes version
	for _, n := range c.nodes {
		hostList1 = append(hostList1, n.Hostname)
		addressFamily = append(addressFamily, n.AddressFamily)

		_, err := readVersionForNode(n)
		if err != nil {
			// Skip version validation if node is not healthy
			if strings.ToLower(n.Status) != "healthy" {
				logging.Warnf("ClusterInfoCache:validateCache - skipping node %v as it is not healthy", n.Hostname)
				continue
			}

			logging.Warnf("ClusterInfoCache:validateCache - Failed as node version is not parsable err: %v node: %v", err, n)
			return false
		}
	}

	for i, svc := range c.nodesvs {
		h := svc.Hostname
		p := svc.Services["mgmt"]

		if h == "" {
			h = GetLocalIpAddr(isIPv6 || (addressFamily[i] == NODE_IPV6))
		}

		hp := net.JoinHostPort(h, fmt.Sprint(p))

		if hostList1[i] != hp {
			logging.Warnf("ClusterInfoCache:validateCache - Failed as hostname in nodes: %s != the one from nodesvs: %s", hostList1[i], hp)
			return false
		}
	}

	return true
}

func (c *ClusterInfoCache) getStaticServicePort(srvc string) (string, error) {

	if p, ok := c.servicePortMap[srvc]; ok {
		return p, nil
	} else {
		return "", errors.New(ErrInvalidService.Error() + fmt.Sprintf(": %v", srvc))
	}

}

// updatePool will fetch bucket info if the verion hash in bucketURL changes
// else it will copy it from existing pool avoiding REST Calls to ns-server.
func (c *ClusterInfoCache) updatePool(np *couchbase.Pool) (err error) {
	ovh, err := c.pool.GetBucketURLVersionHash()
	if err != nil {
		return err
	}

	nvh, err := np.GetBucketURLVersionHash()
	if err != nil {
		return err
	}

	if ovh != nvh {
		err = np.Refresh()
		if err != nil {
			return err
		}
		c.pool = *np
	} else {
		np.BucketMap = c.pool.BucketMap
		np.Manifest = c.pool.Manifest
		c.pool.BucketMap = nil
		c.pool.Manifest = nil
		c.pool = *np
	}

	return nil
}

// IPv6 Support
func GetLocalIpAddr(isIPv6 bool) string {
	if isIPv6 {
		return "::1"
	}
	return "127.0.0.1"
}

func GetLocalIpUrl(isIPv6 bool) string {
	if isIPv6 {
		return "[::1]"
	}
	return "127.0.0.1"
}

func NewClusterInfoClient(clusterURL, pool, userAgent string, config Config) (c *ClusterInfoClient, err error) {
	cic := &ClusterInfoClient{
		clusterURL:                clusterURL,
		pool:                      pool,
		finch:                     make(chan bool),
		fetchDataOnHashChangeOnly: true,
		nodeUUID2HashMap:          make(map[string]int),
		userAgent:                 userAgent,
	}
	cic.servicesNotifierRetryTm = 1000 // TODO: read from config

	cinfo, err := FetchNewClusterInfoCache(clusterURL, pool, "")
	if err != nil {
		return nil, err
	}
	cinfo.SetUserAgent(userAgent)
	cic.cinfo = cinfo

	go cic.watchClusterChanges()
	return cic, err
}

// GetClusterInfoCache returns a pointer to an existing, shared instance.
// Consumer must lock returned cinfo before using it
func (c *ClusterInfoClient) GetClusterInfoCache() *ClusterInfoCache {
	return c.cinfo
}

func (c *ClusterInfoClient) GetNodesInfoProvider() (NodesInfoProvider, error) {
	return NodesInfoProvider(c.cinfo), nil
}

func (c *ClusterInfoClient) GetCollectionInfoProvider(bucketName string) (
	CollectionInfoProvider, error) {
	return CollectionInfoProvider(c.cinfo), nil
}

func (c *ClusterInfoClient) GetBucketInfoProvider(buckeName string) (
	BucketInfoProvider, error) {
	return BucketInfoProvider(c.cinfo), nil
}

func (c *ClusterInfoClient) WatchRebalanceChanges() {
	c.fetchDataOnHashChangeOnly = false
}

func (c *ClusterInfoClient) SetUserAgent(userAgent string) {
	cinfo := c.GetClusterInfoCache()
	cinfo.Lock()
	defer cinfo.Unlock()

	cinfo.SetUserAgent(userAgent)
}

func (c *ClusterInfoClient) updateHashValues(p *couchbase.Pool,
	bucketsHash, serverGroupsHash string) {
	logging.Tracef("ClusterInfoClient(%v): Old Hash values bucketsHash: %s serverGroupsHash: %s nodeHashMap: %v",
		c.cinfo.userAgent, c.bucketsHash, c.serverGroupsHash, c.nodeUUID2HashMap)
	c.bucketsHash = bucketsHash
	c.serverGroupsHash = serverGroupsHash
	c.nodeUUID2HashMap = make(map[string]int)
	for _, n := range p.Nodes {
		c.nodeUUID2HashMap[n.NodeUUID] = n.NodeHash
	}
	logging.Debugf("ClusterInfoClient(%v): New Hash values bucketsHash: %s serverGroupsHash: %s nodeHashMap: %v",
		c.cinfo.userAgent, c.bucketsHash, c.serverGroupsHash, c.nodeUUID2HashMap)
}

func (c *ClusterInfoClient) checkPoolsDataHashChanged(p *couchbase.Pool) bool {
	bucketsHash, err := p.GetBucketURLVersionHash()
	if err != nil {
		return true
	}

	serverGroupsHash, err := p.GetServerGroupsVersionHash()
	if err != nil {
		return true
	}

	defer c.updateHashValues(p, bucketsHash, serverGroupsHash)

	if bucketsHash != c.bucketsHash {
		return true
	}

	if serverGroupsHash != c.serverGroupsHash {
		return true
	}

	if len(p.Nodes) != len(c.nodeUUID2HashMap) {
		return true
	}

	for _, n := range p.Nodes {
		nh, ok := c.nodeUUID2HashMap[n.NodeUUID]
		if !ok || n.NodeHash != nh {
			return true
		}
	}

	return false
}

func (c *ClusterInfoClient) watchClusterChanges() {
	selfRestart := func() {
		time.Sleep(time.Duration(c.servicesNotifierRetryTm) * time.Millisecond)
		go c.watchClusterChanges()
	}

	clusterAuthURL, err := ClusterAuthUrl(c.clusterURL)
	if err != nil {
		logging.Errorf("ClusterInfoClient ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}

	// When this method starts due to selfRestart(), the cluster
	// info cache could have missed the notificiations atlease since
	// `servicesNotifierRetryTm` time interval. The next update happens
	// only after 5 min if no other notification is received.
	// Some queries to cluster info cache would return stale results
	// and can fail some operations. To avoid such staleness, fetch
	// cluster info cache at the beginning of this method
	if err := c.cinfo.FetchWithLock(); err != nil {
		logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
		selfRestart()
		return
	}

	scn, err := NewServicesChangeNotifier(clusterAuthURL, c.pool, c.userAgent)
	if err != nil {
		logging.Errorf("ClusterInfoClient NewServicesChangeNotifier(%v): %v\n", c.userAgent, err)
		selfRestart()
		return
	}
	defer scn.Close()

	ticker := time.NewTicker(time.Duration(5) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				selfRestart()
				return
			}
			if notif.Type == CollectionManifestChangeNotification {
				// Read the bucket info from msg and fetch bucket information for that bucket
				switch (notif.Msg).(type) {
				case *couchbase.Bucket:
					bucket := (notif.Msg).(*couchbase.Bucket)
					if err := c.cinfo.FetchManifestInfoOnUIDChange(bucket.Name, bucket.CollectionManifestUID); err != nil {
						logging.Errorf("cic.cinfo.FetchManifestInfo(): %v\n", err)
						selfRestart()
						return
					}
				default:
					logging.Errorf("ClusterInfoClient(%v): Invalid CollectionManifestChangeNotification type", c.cinfo.userAgent)
					// Fetch full cluster info cache
					if err := c.cinfo.FetchWithLock(); err != nil {
						logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
						selfRestart()
						return
					}
				}
			} else if notif.Type == PoolChangeNotification {
				// Hash Value of Buckets URL in PoolChangeNotification will not change
				// during rebalance it will only be updated at the begining and end
				// if we want to watch real time changes during rebalance query terse
				// Bucket endpoint. Other than that we can stop querying buckets URI
				// if the hash value did not change.
				if c.fetchDataOnHashChangeOnly {
					changed := c.checkPoolsDataHashChanged((notif.Msg).(*couchbase.Pool))
					if !changed {
						logging.Debugf("ClusterInfoClient(%v): No change in data needed from PoolChangeNotification", c.cinfo.userAgent)
						continue
					}
					if err := c.cinfo.FetchWithLockForPoolChange(); err != nil {
						logging.Errorf("cic.cinfo.FetchForPoolChangeNotification(): %v\n", err)
						selfRestart()
						return
					}
				} else {
					if err := c.cinfo.FetchWithLock(); err != nil {
						logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
						selfRestart()
						return
					}
				}
			} else if err := c.cinfo.FetchWithLock(); err != nil {
				logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
				selfRestart()
				return
			}
		case <-ticker.C:
			if err := c.cinfo.FetchWithLock(); err != nil {
				logging.Errorf("cic.cinfo.FetchWithLock(): %v\n", err)
				selfRestart()
				return
			}
		case <-c.finch:
			return
		}
	}
}

// ValidateCollectionID will get CollectionID for a given bucket, scope
// and collection and check if its equal to a given collnID
func (cic *ClusterInfoClient) ValidateCollectionID(bucket, scope,
	collection, collnID string, retry bool) bool {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	validateKeyspace := func() bool {
		cid := cinfo.GetCollectionID(bucket, scope, collection)
		if cid != collnID {
			return false
		}
		return true
	}

	resp := validateKeyspace()
	if resp == false && retry == true {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchWithLock()
		cinfo.RLock()

		return (err == nil) && validateKeyspace()
	}
	return resp
}

func (cic *ClusterInfoClient) ValidateBucket(bucket string, uuids []string) bool {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	validateBucket := func() bool {
		if nids, err := cinfo.GetNodesByBucket(bucket); err == nil && len(nids) != 0 {
			// verify UUID
			currentUUID := cinfo.GetBucketUUID(bucket)
			for _, uuid := range uuids {
				if uuid != currentUUID {
					return false
				}
			}
			return true
		} else if b, err := cinfo.pool.GetBucket(bucket); err == nil && len(b.HibernationState) != 0 {
			defer b.Close()
			logging.Infof("ValidateBucket: Bucket(%v) in Hibernation state (%v)", bucket, b.HibernationState)
			return true
		} else {
			logging.Fatalf("Error Fetching Bucket Info: %v Nids: %v", err, nids)
			return false
		}
	}

	resp := validateBucket()
	if resp == false {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchWithLock()
		cinfo.RLock()

		return (err == nil) && validateBucket()
	}
	return resp
}

func (cic *ClusterInfoClient) IsEphemeral(bucket string) (bool, error) {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	ephemeral, err := cinfo.IsEphemeral(bucket)
	if err != nil {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchWithLock()
		cinfo.RLock()
		if err != nil {
			return false, err
		} else {
			return cinfo.IsEphemeral(bucket)
		}
	}
	return ephemeral, nil
}

func (cic *ClusterInfoClient) IsMagmaStorage(bucket string) (bool, error) {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	isMagma, err := cinfo.IsMagmaStorage(bucket)
	if err != nil {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchWithLock()
		cinfo.RLock()
		if err != nil {
			return false, err
		} else {
			return cinfo.IsMagmaStorage(bucket)
		}
	}
	return isMagma, nil
}

func (cic *ClusterInfoClient) GetNumVBuckets(bucket string) (numVBuckets int,
	err error) {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	getNumVBuckets := func() (int, error) {
		numVBuckets, err := cinfo.GetNumVBuckets(bucket)
		if err != nil {
			return 0, err
		}
		return numVBuckets, nil
	}

	numVBs, err := getNumVBuckets()
	if err != nil || numVBuckets == 0 {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchBucketInfo(bucket)
		cinfo.RLock()
		if err != nil {
			return 0, err
		} else {
			return getNumVBuckets()
		}
	}
	return numVBs, nil
}

func (cic *ClusterInfoClient) GetBucketUUID(bucket string) (string, error) {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	getBucketUUID := func() (string, error) {
		nids, err := cinfo.GetNodesByBucket(bucket)
		b, err1 := cinfo.pool.GetBucket(bucket)

		if err == nil && len(nids) != 0 {
			// verify UUID
			return cinfo.GetBucketUUID(bucket), nil
		} else if err1 == nil && len(b.HibernationState) != 0 {
			logging.Infof("Bucket(%v) in Hibernation state %v", bucket, b.HibernationState)
			return b.UUID, nil
		} else if err == nil {
			logging.Fatalf("Error Fetching Bucket Info: %v Nids: %v", err, nids)
		} else if err1 == nil {
			logging.Fatalf("Error Fetching Bucket Info: %v", err1)
			err = err1
		}

		return BUCKET_UUID_NIL, err
	}

	uuid, err := getBucketUUID()
	if err != nil || uuid == BUCKET_UUID_NIL {
		// Force fetch cluster info cache to avoid staleness in cluster info cache
		cinfo.RUnlock()
		err := cinfo.FetchWithLock()
		cinfo.RLock()
		if err != nil {
			return BUCKET_UUID_NIL, err
		} else {
			return getBucketUUID()
		}
	}
	return uuid, nil
}

func (c *ClusterInfoClient) GetCollectionID(bucket, scope, collection string) string {
	cinfo := c.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	return cinfo.pool.GetCollectionID(bucket, scope, collection)
}

func (cic *ClusterInfoClient) ClusterVersion() uint64 {

	cinfo := cic.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	return cinfo.GetClusterVersion()
}

func (c *ClusterInfoClient) ForceFetch() error {
	return c.FetchWithLock()
}

func (c *ClusterInfoClient) FetchWithLock() error {
	return c.cinfo.FetchWithLock()
}

// Seconds
func (c *ClusterInfoClient) SetRetryInterval(seconds uint32) {
	c.cinfo.SetRetryInterval(seconds)
}

func (c *ClusterInfoClient) SetMaxRetries(retries uint32) {
	c.cinfo.SetMaxRetries(retries)
}

func (c *ClusterInfoClient) Close() {
	defer func() { recover() }() // in case async Close is called. Do we need this?

	close(c.finch)
}
