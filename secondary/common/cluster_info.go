package common

import "github.com/couchbase/indexing/secondary/dcp"
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

	client          couchbase.Client
	pool            couchbase.Pool
	nodes           []couchbase.Node
	nodesvs         []couchbase.NodeServices
	poolsvsCh       chan couchbase.PoolServices
	poolsvsIsActive bool
	poolsvsErr      error
}

type NodeId int

func NewClusterInfoCache(clusterUrl string, pool string) (*ClusterInfoCache, error) {
	c := &ClusterInfoCache{
		url:             clusterUrl,
		poolName:        pool,
		poolsvsCh:       make(chan couchbase.PoolServices),
		poolsvsIsActive: false,
		retries:         0,
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
			Infof("%vError occured during cluster info update (%v) .. Retrying(%d)",
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

		// Streaming nodeServices API background callback setup
		// If Fetch() is called for the first time, nodeServices callback is setup
		// Otherwise if streaming API handler is in error state, that means handler
		// is not present anymore and we need to setup a new handler.
		if c.poolsvsIsActive == false || c.poolsvsErr != nil {
			err = c.client.NodeServicesCallback(c.poolName, c.nodeServicesCallback)
			if err != nil {
				return err
			}
			c.poolsvsIsActive = true
			c.poolsvsErr = nil
		}

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

func (c *ClusterInfoCache) nodeServicesCallback(ps couchbase.PoolServices, err error) bool {
	if err != nil {
		c.poolsvsErr = err
		close(c.poolsvsCh)
		return false
	}

	c.poolsvsCh <- ps
	return true
}

func (c *ClusterInfoCache) WaitAndUpdateServices() error {
	if c.poolsvsErr != nil {
		return c.poolsvsErr
	}

	ps := <-c.poolsvsCh
	if c.poolsvsErr == nil {
		c.nodesvs = ps.NodesExt
	}

	return c.poolsvsErr
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

func (c ClusterInfoCache) GetCurrentNode() NodeId {
	for i, node := range c.nodes {
		if node.ThisNode {
			return NodeId(i)
		}
	}
	// TODO: can we avoid this panic ?
	panic("Current node is not in active membership")
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
