package common

import "github.com/couchbase/indexing/secondary/dcp"
import "errors"
import "fmt"
import "time"
import "net"
import "strings"

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
	url       string
	poolName  string
	logPrefix string
	retries   int

	nodes   []couchbase.Node
	client  couchbase.Client
	pool    couchbase.Pool
	nodesvs []couchbase.NodeServices
}

type NodeId int

func NewClusterInfoCache(cluster string, pool string) *ClusterInfoCache {
	if !strings.HasPrefix(cluster, "http://") {
		cluster = "http://" + cluster
	}
	c := &ClusterInfoCache{
		url:      cluster,
		poolName: pool,
		retries:  0,
	}

	return c
}

func (c *ClusterInfoCache) SetLogPrefix(p string) {
	c.logPrefix = p
}

func (c *ClusterInfoCache) SetMaxRetries(r int) {
	c.retries = r
}

func (c *ClusterInfoCache) Fetch() error {
	var poolServs couchbase.PoolServices

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

		for _, n := range c.pool.Nodes {
			if n.ClusterMembership == "active" {
				c.nodes = append(c.nodes, n)
			}
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
	for i, _ := range c.nodes {
		if _, ok := c.nodesvs[i].Services[srvc]; ok {
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
