package client

import "sync"
import "fmt"
import "encoding/json"

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

	cinfo, err := common.NewClusterInfoCache(common.ClusterUrl(cluster), "default" /*pooln*/)
	if err != nil {
		return nil, err
	}
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

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, error) {
	mindexes := b.mdClient.ListIndex()
	indexes := make([]*mclient.IndexMetadata, 0, len(mindexes))
	for _, mindex := range mindexes {
		indexes = append(indexes, mindex)
	}
	b.rw.Lock()
	defer b.rw.Unlock()

	b.topology = make(map[string][]*mclient.IndexMetadata)
	for _, adminport := range b.adminports {
		b.topology[adminport] = make([]*mclient.IndexMetadata, 0)
	}
	// gather topology of each index.
	for _, index := range indexes {
		for _, instance := range index.Instances {
			for _, queryport := range instance.Endpts {
				adminport := b.queryport2adminport(string(queryport))
				b.topology[adminport] = append(b.topology[adminport], index)
			}
		}
	}
	return indexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() (map[string]string, error) {
	nodes := make(map[string]string)
	for adminport, queryport := range b.queryports {
		nodes[adminport] = queryport
	}
	return nodes, nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	with []byte) (common.IndexDefnId, error) {

	withm := make(map[string]interface{})

	err := json.Unmarshal(with, &withm)
	if err != nil {
		return common.IndexDefnId(0), err
	}
	nodes := withm["nodes"].([]interface{})

	if len(nodes) < 1 {
		return common.IndexDefnId(0), ErrorEmptyDeployment
	} else if len(nodes) > 1 {
		return common.IndexDefnId(0), ErrorManyDeployment
	}

	adminport, ok := nodes[0].(string) // topology
	if !ok {
		return common.IndexDefnId(0), ErrorInvalidDeploymentNode
	}

	return b.mdClient.CreateIndex(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		adminport, secExprs, isPrimary)
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []common.IndexDefnId) error {
	_, ok := b.getNodes(defnIDs)
	if !ok {
		return ErrorIndexNotFound
	}

	dispatch := make(map[string][]common.IndexDefnId) // adminport -> []indexes
	for _, defnID := range defnIDs {
		adminport, ok := b.getNode(defnID)
		if !ok {
			return ErrorIndexNotFound
		}
		if _, ok := dispatch[adminport]; !ok {
			dispatch[adminport] = make([]common.IndexDefnId, 0)
		}
		dispatch[adminport] = append(dispatch[adminport], defnID)
	}

	// TODO: This needs to be added at the metadata-client.
	//for adminport, defnIDs := range dispatch {
	//  b.mdClient.BuildIndexes(adminport, defnIDs)
	//}
	return nil
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

// getNodes return the set of nodes hosting the specified set
// of indexes
func (b *metadataClient) getNodes(
	defnIDs []common.IndexDefnId) (adminport []string, ok bool) {

	m := make(map[string]bool)
	for _, defnID := range defnIDs {
		adminport, ok := b.getNode(defnID)
		if !ok {
			return nil, false
		}
		m[adminport] = true
	}

	adminports := make([]string, 0)
	for adminport := range m {
		adminports = append(adminports, adminport)
	}
	return adminports, true
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

// given queryport fetch the corresponding adminport for the indexer node.
func (b *metadataClient) queryport2adminport(queryport string) string {
	for adminport, qport := range b.queryports {
		if qport == queryport {
			return adminport
		}
	}
	panic(fmt.Errorf("cannot find adminport for %v", queryport))
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
