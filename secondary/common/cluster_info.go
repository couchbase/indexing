package common

import "github.com/couchbase/indexing/secondary/dcp"
import "github.com/couchbase/indexing/secondary/logging"
import "errors"
import "fmt"
import "time"
import "net"
import "net/url"
import "sync"

var (
	ErrInvalidNodeId       = errors.New("Invalid NodeId")
	ErrInvalidService      = errors.New("Invalid service")
	ErrNodeNotBucketMember = errors.New("Node is not a member of bucket")
)

const (
	INDEX_ADMIN_SERVICE = "indexAdmin"
	INDEX_SCAN_SERVICE  = "indexScan"
	INDEX_HTTP_SERVICE  = "indexHttp"
)

const BUCKET_UUID_NIL = ""

// Helper object for fetching cluster information
// Can be used by services running on a cluster node to connect with
// local management service for obtaining cluster information.
// Info cache can be updated by using Refresh() method.
type ClusterInfoCache struct {
	sync.Mutex
	url       string
	poolName  string
	logPrefix string
	retries   int

	client  couchbase.Client
	pool    couchbase.Pool
	nodes   []couchbase.Node
	nodesvs []couchbase.NodeServices
}

type ServiceAddressProvider interface {
	GetLocalServiceAddress(srvc string) (string, error)
	GetLocalServicePort(srvc string) (string, error)
	GetLocalServiceHost(srvc string) (string, error)
	GetLocalHostAddress() (string, error)
}

type NodeId int

func NewClusterInfoCache(clusterUrl string, pool string) (*ClusterInfoCache, error) {
	c := &ClusterInfoCache{
		url:      clusterUrl,
		poolName: pool,
		retries:  0,
	}

	return c, nil
}

func (c *ClusterInfoCache) SetLogPrefix(p string) {
	c.logPrefix = p
}

func (c *ClusterInfoCache) SetMaxRetries(r int) {
	c.retries = r
}

func (c *ClusterInfoCache) Fetch() error {

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Infof("%vError occured during cluster info update (%v) .. Retrying(%d)",
				c.logPrefix, err, r)
		}

		c.client, err = couchbase.Connect(c.url)
		if err != nil {
			return err
		}

		c.pool, err = c.client.GetPool(c.poolName)
		if err != nil {
			return err
		}

		var nodes []couchbase.Node
		for _, n := range c.pool.Nodes {
			if n.ClusterMembership == "active" {
				nodes = append(nodes, n)
			}
		}
		c.nodes = nodes

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

		return nil
	}

	rh := NewRetryHelper(c.retries, time.Second, 1, fn)
	return rh.Run()
}

func (c ClusterInfoCache) GetNodesByServiceType(srvc string) (nids []NodeId) {
	for i, svs := range c.nodesvs {
		if _, ok := svs.Services[srvc]; ok {
			nids = append(nids, NodeId(i))
		}
	}

	return
}

func (c ClusterInfoCache) GetNodesByBucket(bucket string) (nids []NodeId, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			nids = append(nids, nid)
		}
	}

	return
}

//
// Return UUID of a given bucket.
//
func (c ClusterInfoCache) GetBucketUUID(bucket string) (uuid string) {

	b, err := c.pool.GetBucket(bucket)
	if err != nil {
		return BUCKET_UUID_NIL
	}
	defer b.Close()

	// This node recognize this bucket.   Make sure its vb is resided in at least one node.
	for i, _ := range c.nodes {
		nid := NodeId(i)
		if _, ok := c.findVBServerIndex(b, nid); ok {
			// find the bucket resides in at least one node
			return b.UUID
		}
	}

	// no nodes recognize this bucket
	return BUCKET_UUID_NIL
}

func (c ClusterInfoCache) GetCurrentNode() NodeId {
	for i, node := range c.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}
	// TODO: can we avoid this panic ?
	panic("Current node is not in active membership")
}

func (c ClusterInfoCache) IsNodeHealthy(nid NodeId) (bool, error) {
	if int(nid) >= len(c.nodes) {
		return false, ErrInvalidNodeId
	}

	return c.nodes[nid].Status == "healthy", nil
}

func (c ClusterInfoCache) GetNodeStatus(nid NodeId) (string, error) {
	if int(nid) >= len(c.nodes) {
		return "", ErrInvalidNodeId
	}

	return c.nodes[nid].Status, nil
}

func (c ClusterInfoCache) GetServiceAddress(nid NodeId, srvc string) (addr string, err error) {
	var port int
	var ok bool

	if int(nid) >= len(c.nodesvs) {
		err = ErrInvalidNodeId
		return
	}

	node := c.nodesvs[nid]
	if port, ok = node.Services[srvc]; !ok {
		err = ErrInvalidService
		return
	}

	// For current node, hostname might be empty
	// Insert hostname used to connect to the cluster
	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}
	h, _, _ := net.SplitHostPort(cUrl.Host)
	if node.Hostname == "" {
		node.Hostname = h
	}

	addr = net.JoinHostPort(node.Hostname, fmt.Sprint(port))
	return
}

func (c ClusterInfoCache) GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error) {
	b, berr := c.pool.GetBucket(bucket)
	if berr != nil {
		err = berr
		return
	}
	defer b.Close()

	idx, ok := c.findVBServerIndex(b, nid)
	if !ok {
		err = ErrNodeNotBucketMember
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

func (c ClusterInfoCache) findVBServerIndex(b *couchbase.Bucket, nid NodeId) (int, bool) {
	bnodes := b.Nodes()

	for idx, n := range bnodes {
		if c.sameNode(n, c.nodes[nid]) {
			return idx, true
		}
	}

	return 0, false
}

func (c ClusterInfoCache) sameNode(n1 couchbase.Node, n2 couchbase.Node) bool {
	return n1.Hostname == n2.Hostname
}

func (c ClusterInfoCache) GetLocalServiceAddress(srvc string) (string, error) {
	node := c.GetCurrentNode()
	return c.GetServiceAddress(node, srvc)
}

func (c ClusterInfoCache) GetLocalServicePort(srvc string) (string, error) {
	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	_, p, e := net.SplitHostPort(addr)
	if e != nil {
		return p, e
	}

	return net.JoinHostPort("", p), nil
}

func (c ClusterInfoCache) GetLocalServiceHost(srvc string) (string, error) {

	addr, err := c.GetLocalServiceAddress(srvc)
	if err != nil {
		return addr, err
	}

	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return h, nil
}

func (c ClusterInfoCache) GetLocalHostAddress() (string, error) {

	cUrl, err := url.Parse(c.url)
	if err != nil {
		return "", errors.New("Unable to parse cluster url - " + err.Error())
	}
	_, p, _ := net.SplitHostPort(cUrl.Host)

	h, err := c.GetLocalServiceHost(INDEX_ADMIN_SERVICE)
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(h, p), nil
}
