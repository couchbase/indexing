package client

import "sync"

import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	rw          sync.RWMutex // protects `topology`
	clusterURL  string
	serviceAddr string
	mdClient    *mclient.MetadataProvider
	adminports  []string          // list of nodes represented by its adminport.
	queryports  map[string]string // adminport -> queryport
	// sherlock topology management, multi-node & single-partition.
	topology map[string][]*mclient.IndexMetadata // adminport -> indexes
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
	}
	// initialize meta-data-provide.
	b.mdClient, err = mclient.NewMetadataProvider(b.serviceAddr)
	if err != nil {
		return nil, err
	}
	// populate indexers' adminport and queryport
	if b.adminports, err = getIndexerAdminports(cinfo); err != nil {
		return nil, err
	}
	if b.queryports, err = getIndexerQueryports(cinfo); err != nil {
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
	b.topology = make(map[string][]*mclient.IndexMetadata)
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

	adminport, ok := b.makeTopology()
	if !ok {
		return common.IndexDefnId(0), ErrorNoHost
	}
	return b.mdClient.CreateIndex(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		adminport, secExprs, isPrimary)
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID common.IndexDefnId) error {
	adminport, ok := b.getNode(defnID)
	if !ok {
		return ErrorIndexNotFound
	}
	return b.mdClient.DropIndex(defnID, adminport)
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

	adminport, ok := b.getNode(defnID)
	if !ok {
		return "", false
	}
	queryport, ok = b.queryports[adminport]
	return queryport, ok
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	b.mdClient.Close()
}

// makeTopology for a new index based on round robin method.
func (b *metadataClient) makeTopology() (adminport string, ok bool) {
	b.rw.RLock()
	defer b.rw.RUnlock()

	if len(b.topology) == 0 {
		return "", false
	}
	nIndexes := 0xFFFFFFFF
	for addr, indexes := range b.topology {
		if len(indexes) <= nIndexes {
			adminport, nIndexes = addr, nIndexes
		}
	}
	return adminport, true
}

// getNode hosting index with `defnID`.
func (b *metadataClient) getNode(
	defnID common.IndexDefnId) (adminport string, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()

	for addr, indexes := range b.topology {
		for _, index := range indexes {
			if defnID == index.Definition.DefnId {
				return addr, true
			}
		}
	}
	return "", false
}

// return adminports for all known indexers.
func getIndexerAdminports(
	cinfo *common.ClusterInfoCache) ([]string, error) {

	iAdminports := make([]string, 0)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
		if err != nil {
			return nil, err
		}
		iAdminports = append(iAdminports, adminport)
	}
	return iAdminports, nil
}

// return queryports for all known indexers.
func getIndexerQueryports(
	cinfo *common.ClusterInfoCache) (map[string]string, error) {

	iQueryports := make(map[string]string)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
		if err != nil {
			return nil, err
		}
		queryport, err := cinfo.GetServiceAddress(node, "indexScan")
		if err != nil {
			return nil, err
		}
		iQueryports[adminport] = queryport
	}
	return iQueryports, nil
}
