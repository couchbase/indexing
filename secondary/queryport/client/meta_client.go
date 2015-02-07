package client

import "sync"
import "fmt"
import "errors"
import "encoding/json"

import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	clusterURL string
	mdClient   *mclient.MetadataProvider
	rw         sync.RWMutex // protects all fields listed below

	topology map[common.IndexerId][]*mclient.IndexMetadata // indexerId -> indexes
	indexers map[string]common.IndexerId                   // indexerId -> indexes

	// shelock load replicas.
	replicas map[common.IndexDefnId][]common.IndexDefnId
	// shelock load balancing.
	loads map[common.IndexDefnId]*loadHeuristics // adminport -> loadHeuristics
}

func newMetaBridgeClient(cluster string) (c *metadataClient, err error) {

	cinfo, err := common.NewClusterInfoCache(common.ClusterUrl(cluster), "default" /*pooln*/)
	if err != nil {
		return nil, err
	}
	if err = cinfo.Fetch(); err != nil {
		return nil, err
	}
	b := &metadataClient{
		clusterURL: cluster,
		indexers:   make(map[string]common.IndexerId),
		loads:      make(map[common.IndexDefnId]*loadHeuristics),
	}
	// initialize meta-data-provide.
	uuid, err := common.NewUUID()
	if err != nil {
		common.Errorf("Could not generate UUID in common.NewUUID")
		return nil, err
	}
	b.mdClient, err = mclient.NewMetadataProvider(uuid.Str())
	if err != nil {
		return nil, err
	}

	adminports, err := getIndexerAdminports(cinfo)
	if err != nil {
		return nil, err
	}

	// watch all indexers
	for _, adminport := range adminports {
		b.indexers[adminport], _ = b.mdClient.WatchMetadata(adminport)
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

	b.topology = make(map[common.IndexerId][]*mclient.IndexMetadata)
	for _, indexerId := range b.indexers {
		if indexerId != common.INDEXER_ID_NIL {
			b.topology[indexerId] = make([]*mclient.IndexMetadata, 0)
		}
	}

	for _, index := range indexes {
		indexerId := index.Instances[0].IndexerId
		b.topology[indexerId] = append(b.topology[indexerId], index)
	}

	// compute replicas
	b.replicas = b.computeReplicas()
	return indexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() (map[string]string, error) {
	b.rw.Lock()
	defer b.rw.Unlock()

	nodes := make(map[string]string)

	for _, indexerId := range b.indexers {
		if indexerId != common.INDEXER_ID_NIL {
			adminport, queryport, err := b.mdClient.FindServiceForIndexer(indexerId)
			if err == nil {
				nodes[adminport] = queryport
			}
		}
	}

	return nodes, nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	planJSON []byte) (common.IndexDefnId, error) {

	createPlan := make(map[string]interface{})
	plan := map[string]interface{}{ // with default values
		"nodes":       nil,
		"defer_build": false,
	}

	if planJSON != nil && len(planJSON) > 0 {
		err := json.Unmarshal(planJSON, &createPlan)
		if err != nil {
			return common.IndexDefnId(0), err
		}
		for key, value := range createPlan { // override default values
			plan[key] = value
		}
	}

	defnID, err := b.mdClient.CreateIndexWithPlan(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, plan)
	b.Refresh() // refresh so that we too have IndexMetadata table.
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []common.IndexDefnId) error {

	if err := b.mdClient.BuildIndexes(defnIDs); err != nil {
		return errors.New("Fail to build indexes")
	}

	return nil
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID common.IndexDefnId) error {
	return b.mdClient.DropIndex(defnID)
}

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	b.rw.Lock()
	defer b.rw.Unlock()

	queryports = make([]string, 0)

	for _, indexerId := range b.indexers {
		if indexerId != common.INDEXER_ID_NIL {
			_, queryport, err := b.mdClient.FindServiceForIndexer(indexerId)
			if err == nil {
				queryports = append(queryports, queryport)
			}
		}
	}

	common.Debugf("Scan ports %v for all indexes", queryports)
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(
	defnID common.IndexDefnId) (queryport string, ok bool) {

	defnID = b.pickOptimal(defnID) // defnID (aka index) under least load

	_, queryport, err := b.mdClient.FindServiceForIndex(defnID)
	if err != nil {
		return "", false
	}

	common.Debugf("Scan port %s for index %d", queryport, defnID)
	return queryport, true
}

// Timeit implement BridgeAccessor{} interface.
func (b *metadataClient) Timeit(defnID uint64, value float64) {
	b.rw.Lock()
	defer b.rw.Unlock()

	id := common.IndexDefnId(defnID)
	if load, ok := b.loads[id]; !ok {
		b.loads[id] = &loadHeuristics{avgLoad: value, count: 1}
	} else {
		// compute incremental average.
		avg, n := load.avgLoad, load.count
		load.avgLoad = (float64(n)*avg + float64(value)) / float64(n+1)
		load.count = n + 1
	}
}

// IndexState implement BridgeAccessor{} interface.
func (b *metadataClient) IndexState(defnID uint64) (common.IndexState, error) {
	b.Refresh()

	b.rw.RLock()
	defer b.rw.RUnlock()

	for _, indexes := range b.topology {
		for _, index := range indexes {
			if index.Definition.DefnId == common.IndexDefnId(defnID) {
				if index.Instances != nil && len(index.Instances) > 0 {
					state := index.Instances[0].State
					if len(index.Instances) == 0 {
						err := fmt.Errorf("no instance for %q", defnID)
						return state, err
					} else if index.Instances[0].Error != "" {
						return state, errors.New(index.Instances[0].Error)
					} else {
						return state, nil
					}
				}
				return common.INDEX_STATE_ERROR, ErrorInstanceNotFound
			}
		}
	}
	return common.INDEX_STATE_ERROR, ErrorIndexNotFound
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	b.mdClient.Close()
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// compute a map of replicas for each index in 2i.
func (b *metadataClient) computeReplicas() map[common.IndexDefnId][]common.IndexDefnId {
	replicaMap := make(map[common.IndexDefnId][]common.IndexDefnId, 0)
	for indexerId1, indexes1 := range b.topology {
		for _, index1 := range indexes1 {
			replicas := make([]common.IndexDefnId, 0)
			replicas = append(replicas, index1.Definition.DefnId) // add itself
			for indexerId2, indexes2 := range b.topology {
				if indexerId1 == indexerId2 { // skip colocated indexes
					continue
				}
				for _, index2 := range indexes2 {
					if b.equivalentIndex(index1, index2) { // pick equivalents
						replicas = append(replicas, index2.Definition.DefnId)
					}
				}
			}
			replicaMap[index1.Definition.DefnId] = replicas // map it
		}
	}
	return replicaMap
}

// compare whether two index are equivalent.
func (b *metadataClient) equivalentIndex(
	index1, index2 *mclient.IndexMetadata) bool {

	d1, d2 := index1.Definition, index2.Definition
	if d1.Using != d1.Using ||
		d1.Bucket != d2.Bucket ||
		d1.IsPrimary != d2.IsPrimary ||
		d1.ExprType != d2.ExprType ||
		d1.PartitionScheme != d2.PartitionScheme ||
		d1.PartitionKey != d2.PartitionKey {

		return false
	}

	for _, s1 := range d1.SecExprs {
		for _, s2 := range d2.SecExprs {
			if s1 != s2 {
				return false
			}
		}
	}
	return true
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// manage load statistics.
type loadHeuristics struct {
	avgLoad float64
	count   uint64
}

// pick an optimal replica for the index `defnID` under least load.
func (b *metadataClient) pickOptimal(
	defnID common.IndexDefnId) common.IndexDefnId {

	b.rw.Lock()
	defer b.rw.Unlock()

	optimalID, currLoad := defnID, 0.0
	if load, ok := b.loads[defnID]; ok {
		currLoad = load.avgLoad
	}
	for _, replicaID := range b.replicas[defnID] {
		load, ok := b.loads[replicaID]
		if !ok { // no load for this replica
			return replicaID
		}
		if currLoad == 0.0 || load.avgLoad < currLoad {
			// found an index under less load
			optimalID, currLoad = replicaID, load.avgLoad
		}
	}
	return optimalID
}

//----------------
// local functions
//----------------

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
