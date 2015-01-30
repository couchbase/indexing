package client

import "sync"
import "fmt"
import "strings"
import "time"
import "errors"
import "encoding/json"
import "math/rand"

import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	clusterURL  string
	serviceAddr string
	mdClient    *mclient.MetadataProvider
	rw          sync.RWMutex      // protects all fields listed below
	adminports  []string          // list of nodes represented by its adminport.
	queryports  map[string]string // adminport -> queryport
	// sherlock topology management, multi-node & single-partition.
	topology map[string][]*mclient.IndexMetadata // adminport -> indexes
	// shelock load replicas.
	replicas map[common.IndexDefnId][]common.IndexDefnId
	// shelock load balancing.
	loads map[common.IndexDefnId]*loadHeuristics // adminport -> loadHeuristics
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
		adminports:  make([]string, 0),
		queryports:  make(map[string]string, 0),
		loads:       make(map[common.IndexDefnId]*loadHeuristics),
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
	// compute replicas
	b.replicas = b.computeReplicas()
	return indexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() (map[string]string, error) {
	b.rw.Lock()
	defer b.rw.Unlock()

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
	planJSON []byte) (common.IndexDefnId, error) {

	createPlan := make(map[string]interface{})
	// plan may not be provided, pick a random indexer node
	time.Sleep(1 * time.Millisecond) // sleep to make a random seed.
	seed := time.Now().UTC().Second()
	rnd := rand.New(rand.NewSource(int64(seed))).Intn(100000)
	n := (rnd / (10000 / len(b.adminports))) % len(b.adminports)
	plan := map[string]interface{}{ // with default values
		"nodes":       []interface{}{b.adminports[n]},
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

	errMessages := make([]string, 0)
	for adminport, defnIDs := range dispatch {
		err := b.mdClient.BuildIndexes(adminport, defnIDs)
		if err != nil {
			msg := fmt.Sprintf("build error with %q indexer: %v", adminport, err)
			errMessages = append(errMessages, msg)
		}
	}
	if len(errMessages) > 0 {
		return fmt.Errorf(strings.Join(errMessages, "\n"))
	}
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

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	b.rw.Lock()
	defer b.rw.Unlock()

	if len(b.queryports) == 0 {
		return nil
	}
	queryports = make([]string, 0, len(b.queryports))
	for _, queryport := range b.queryports {
		queryports = append(queryports, queryport)
	}
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(
	defnID common.IndexDefnId) (queryport string, ok bool) {

	defnID = b.pickOptimal(defnID) // defnID (aka index) under least load
	adminport, ok := b.getNode(defnID)
	if !ok {
		return "", false
	}

	b.rw.Lock()
	defer b.rw.Unlock()
	queryport, ok = b.queryports[adminport]
	return queryport, ok
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
	for adminport1, indexes1 := range b.topology {
		for _, index1 := range indexes1 {
			replicas := make([]common.IndexDefnId, 0)
			replicas = append(replicas, index1.Definition.DefnId) // add itself
			for adminport2, indexes2 := range b.topology {
				if adminport1 == adminport2 { // skip colocated indexes
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
