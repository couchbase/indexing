package client

import "sync"

import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	rw          sync.RWMutex // protects `topology`
	clusterURL  string
	serviceAddr string
	// TODO: Can we assume that nodeId will be unique to each indexer ?
	nodes      []common.NodeId
	mdClient   *mclient.MetadataProvider
	adminports map[common.NodeId]string
	queryports map[common.NodeId]string
	// sherlock topology management, multi-node & single-partition.
	topology map[common.NodeId][]*mclient.IndexMetadata
}

func newMetaBridgeClient(
	cluster, serviceAddr string) (c *metadataClient, err error) {

	cinfo := common.NewClusterInfoCache(cluster, "default" /*pooln*/) // TODO
	if err = cinfo.Fetch(); err != nil {
		return nil, err
	}
	b := &metadataClient{
		clusterURL:  cluster,
		serviceAddr: serviceAddr,
		nodes:       cinfo.GetNodesByServiceType("indexAdmin"),
	}
	// initialize meta-data-provide.
	b.mdClient, err = mclient.NewMetadataProvider(b.serviceAddr)
	if err != nil {
		return nil, err
	}
	// populate indexers' adminport and queryport
	if b.adminports, err = getIndexerAdminports(cinfo, b.nodes); err != nil {
		return nil, err
	}
	if b.queryports, err = getIndexerQueryports(cinfo, b.nodes); err != nil {
		return nil, err
	}
	// watch all indexers
	for _, adminport := range b.adminports {
		b.mdClient.WatchMetadata(adminport)
	}
	b.Refresh()
	return b, nil
}

// Refresh list all indexes.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, error) {
	mindexes := b.mdClient.ListIndex()
	indexes := make([]*mclient.IndexMetadata, 0, len(mindexes))
	for _, mindex := range mindexes {
		indexes = append(indexes, mindex)
	}
	b.rw.Lock()
	defer b.rw.Unlock()
	b.topology = make(map[common.NodeId][]*mclient.IndexMetadata)
	// gather topology of each index.
	// TODO: how to gather topology from each index.
	//for _, index := range indexes {
	//}
	return indexes, nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool) (common.IndexDefnId, error) {

	nodeId, ok := b.makeTopology()
	if !ok {
		return common.IndexDefnId(0), ErrorNoHost
	}
	return b.mdClient.CreateIndex(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		b.adminports[nodeId], secExprs, isPrimary)
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID common.IndexDefnId) error {
	nodeId, ok := b.getNode(defnID)
	if !ok {
		return ErrorIndexNotFound
	}
	return b.mdClient.DropIndex(defnID, b.adminports[nodeId])
}

// GetQueryports implements BridgeAccessor{} interface.
func (b *metadataClient) GetQueryports() (queryports []string) {
	if len(b.queryports) == 0 {
		return nil
	}
	queryports = make([]string, 0, len(b.queryports))
	for _, queryport := range b.queryports {
		queryports = append(queryports, queryport)
	}
	return queryports
}

// GetQueryport implements BridgeAccessor{} interface.
func (b *metadataClient) GetQueryport(
	defnID common.IndexDefnId) (queryport string, ok bool) {

	nodeId, ok := b.getNode(defnID)
	if !ok {
		return "", false
	}
	queryport, ok = b.queryports[nodeId]
	return queryport, ok
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	b.mdClient.Close()
}

// makeTopology for a new index based on round robin method.
func (b *metadataClient) makeTopology() (pickNode common.NodeId, ok bool) {
	b.rw.RLock()
	defer b.rw.RUnlock()

	if len(b.topology) == 0 {
		return 0, false
	}
	nIndexes := 0xFFFFFFFF
	for nodeId, indexes := range b.topology {
		if len(indexes) <= nIndexes {
			pickNode, nIndexes = nodeId, nIndexes
		}
	}
	return pickNode, true
}

// getNode hosting index with `defnID`.
func (b *metadataClient) getNode(
	defnID common.IndexDefnId) (nodeId common.NodeId, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()

	for nodeId, indexes := range b.topology {
		for _, index := range indexes {
			if defnID == index.Definition.DefnId {
				return nodeId, true
			}
		}
	}
	return 0, false
}

// return adminports for all known indexers.
func getIndexerAdminports(
	cinfo *common.ClusterInfoCache,
	nodes []common.NodeId) (map[common.NodeId]string, error) {

	iAdminports := make(map[common.NodeId]string)
	for _, node := range nodes {
		adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
		if err != nil {
			return nil, err
		}
		iAdminports[node] = adminport
	}
	return iAdminports, nil
}

// return queryports for all known indexers.
func getIndexerQueryports(
	cinfo *common.ClusterInfoCache,
	nodes []common.NodeId) (map[common.NodeId]string, error) {

	iQueryports := make(map[common.NodeId]string)
	for _, node := range nodes {
		queryport, err := cinfo.GetServiceAddress(node, "indexScan")
		if err != nil {
			return nil, err
		}
		iQueryports[node] = queryport
	}
	return iQueryports, nil
}
