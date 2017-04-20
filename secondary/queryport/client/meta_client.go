package client

import "sync"
import "fmt"
import "sort"
import "errors"
import "strings"
import "math/rand"
import "time"
import "unsafe"
import "sync/atomic"
import "encoding/json"
import "net/http"
import "bytes"
import "io/ioutil"
import "io"
import "math"

import "github.com/couchbase/cbauth"

import "github.com/couchbase/indexing/secondary/logging"
import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	cluster  string
	finch    chan bool
	mdClient *mclient.MetadataProvider
	indexers unsafe.Pointer // *indexTopology
	// config
	servicesNotifierRetryTm int
	logtick                 time.Duration
	randomWeight            float64 // value between [0, 1.0)
	equivalenceFactor       float64 // value between [0, 1.0)

	topoChangeLock sync.Mutex
	metaCh         chan bool
	mdNotifyCh     chan bool

	settings *ClientSettings
}

// sherlock topology management, multi-node & single-partition.
type indexTopology struct {
	version    uint64
	adminports map[string]common.IndexerId // book-keeping for cluster changes
	topology   map[common.IndexerId][]*mclient.IndexMetadata
	queryports map[common.IndexerId]string
	replicas   map[common.IndexDefnId][]common.IndexInstId
	rw         sync.RWMutex
	loads      map[common.IndexInstId]*loadHeuristics
	// insts could include pending RState inst if there is no corresponding active instance
	insts           map[common.IndexInstId]*mclient.InstanceDefn
	defns           map[common.IndexDefnId]*mclient.IndexMetadata
	replicasInRebal map[common.IndexDefnId][]*mclient.InstanceDefn
	allIndexes      []*mclient.IndexMetadata
}

func newMetaBridgeClient(
	cluster string, config common.Config, metaCh chan bool, settings *ClientSettings) (c *metadataClient, err error) {

	b := &metadataClient{
		cluster:    cluster,
		finch:      make(chan bool),
		metaCh:     metaCh,
		mdNotifyCh: make(chan bool, 1),
		settings:   settings,
	}
	b.servicesNotifierRetryTm = config["servicesNotifierRetryTm"].Int()
	b.logtick = time.Duration(config["logtick"].Int()) * time.Millisecond
	b.randomWeight = config["load.randomWeight"].Float64()
	b.equivalenceFactor = config["load.equivalenceFactor"].Float64()
	// initialize meta-data-provide.
	uuid, err := common.NewUUID()
	if err != nil {
		logging.Errorf("Could not generate UUID in common.NewUUID\n")
		return nil, err
	}
	b.mdClient, err = mclient.NewMetadataProvider(uuid.Str(), b.mdNotifyCh, b.settings)
	if err != nil {
		return nil, err
	}

	if err := b.updateIndexerList(false); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		b.mdClient.Close()
		return nil, err
	}

	go b.watchClusterChanges() // will also update the indexer list
	go b.logstats()
	return b, nil
}

// Sync will update the indexer list.
func (b *metadataClient) Sync() error {
	if err := b.updateIndexerList(true); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		return err
	}
	return nil
}

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, uint64, uint64, error) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if currmeta.version < b.mdClient.GetMetadataVersion() {
		b.safeupdate(nil, false)
		currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
	}

	return currmeta.allIndexes, currmeta.version, b.mdClient.GetIndexerVersion(), nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() ([]*IndexerService, error) {
	// gather Indexer services
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	nodes := make(map[string]*IndexerService)
	for indexerID := range currmeta.topology {
		if indexerID != common.INDEXER_ID_NIL {
			a, q, _, err := b.mdClient.FindServiceForIndexer(indexerID)
			if err == nil {
				nodes[a] = &IndexerService{
					Adminport: a, Queryport: q, Status: "initial",
				}
			}
		}
	}
	// gather indexer status
	for _, indexer := range b.mdClient.CheckIndexerStatus() {
		if node, ok := nodes[indexer.Adminport]; ok && indexer.Connected {
			node.Status = "online"
		}
	}
	services := make([]*IndexerService, 0, len(nodes))
	for _, node := range nodes {
		services = append(services, node)
	}
	return services, nil
}

// GetIndexDefn implements BridgeAccessor{} interface.
func (b *metadataClient) GetIndexDefn(defnID uint64) *common.IndexDefn {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		return index.Definition
	}

	return nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool,
	planJSON []byte) (uint64, error) {

	plan := make(map[string]interface{})
	if planJSON != nil && len(planJSON) > 0 {
		err := json.Unmarshal(planJSON, &plan)
		if err != nil {
			return 0, err
		}
	}

	refreshCnt := 0
RETRY:
	defnID, err, needRefresh := b.mdClient.CreateIndexWithPlan(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, desc, isPrimary, plan)

	if needRefresh && refreshCnt == 0 {
		fmsg := "GsiClient: Indexer Node List is out-of-date.  Require refresh."
		logging.Debugf(fmsg)
		if err := b.updateIndexerList(false); err != nil {
			logging.Errorf("updateIndexerList(): %v\n", err)
			return uint64(defnID), err
		}
		refreshCnt++
		goto RETRY
	}
	return uint64(defnID), err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []uint64) error {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for _, defnId := range defnIDs {
		if _, ok := currmeta.defns[common.IndexDefnId(defnId)]; !ok {
			return ErrorIndexNotFound
		}
	}

	ids := make([]common.IndexDefnId, len(defnIDs))
	for i, id := range defnIDs {
		ids[i] = common.IndexDefnId(id)
	}
	return b.mdClient.BuildIndexes(ids)
}

// MoveIndex implements BridgeAccessor{} interface.
func (b *metadataClient) MoveIndex(defnID uint64, planJSON map[string]interface{}) error {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if _, ok := currmeta.defns[common.IndexDefnId(defnID)]; !ok {
		return ErrorIndexNotFound
	}

	var httpport string
	for indexerId, _ := range currmeta.topology {
		var err error
		if _, _, httpport, err = b.mdClient.FindServiceForIndexer(indexerId); err == nil {
			break
		}
	}

	if httpport == "" {
		return ErrorNoHost
	}

	timeout := time.Duration(120 * time.Second)
	if planJSON != nil {
		if t, ok := planJSON["timeout"]; ok {
			timeout = time.Duration(t.(float64)) * time.Second
		}
	}

	idList := IndexIdList{DefnIds: []uint64{defnID}}
	ir := IndexRequest{IndexIds: idList, Plan: planJSON}
	body, err := json.Marshal(&ir)
	if err != nil {
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	url := "/moveIndex"
	resp, err := postWithAuth(httpport+url, "application/json", bodybuf, timeout)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	response := new(IndexResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &response); err != nil {
		return err
	}
	if response.Code == RESP_ERROR {
		return errors.New(response.Error)
	}

	return nil
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID uint64) error {
	err := b.mdClient.DropIndex(common.IndexDefnId(defnID))
	if err == nil { // cleanup index local cache.
		b.safeupdate(nil, false /*force*/)
	}
	return err
}

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	queryports = make([]string, 0)
	for _, queryport := range currmeta.queryports {
		queryports = append(queryports, queryport)
	}
	logging.Debugf("Scan ports %v for all indexes", queryports)
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(
	defnID uint64, retry int, excludes map[uint64]bool) (qp string, targetDefnID uint64, targetInstID uint64, ok bool) {

	var inst *mclient.InstanceDefn

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if rand.Float64() < b.randomWeight {
		var replicas [128]uint64
		n := 0
		for _, replicaID := range currmeta.replicas[common.IndexDefnId(defnID)] {
			replicas[n] = uint64(replicaID)
			n++
		}
		inst, ok = b.pickRandom(replicas[:n], defnID, excludes)
	} else {
		inst, ok = b.pickOptimal(defnID, excludes)
	}

	if !ok {
		return "", 0, 0, false
	}

	targetInstID = uint64(inst.InstId)
	targetDefnID = uint64(inst.DefnId)

	qp, ok = currmeta.queryports[inst.IndexerId]
	if !ok {
		return "", 0, 0, false
	}

	fmsg := "Scan port %s for index defnID %d of equivalent index defnId %d"
	logging.Debugf(fmsg, qp, targetDefnID, defnID)
	return qp, targetDefnID, targetInstID, true
}

// Timeit implement BridgeAccessor{} interface.
func (b *metadataClient) Timeit(instID uint64, value float64) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	// currmeta.loads is immutable once constructed
	load, ok := currmeta.loads[common.IndexInstId(instID)]
	if !ok {
		// it should not happen. But if it does, just return.
		return
	}

	load.updateValue(value)
}

// IsPrimary implement BridgeAccessor{} interface.
func (b *metadataClient) IsPrimary(defnID uint64) bool {
	b.Refresh()
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		return index.Definition.IsPrimary
	}
	return false
}

// IndexState implement BridgeAccessor{} interface.
func (b *metadataClient) IndexState(defnID uint64) (common.IndexState, error) {
	b.Refresh()
	return b.indexState(defnID)
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	defer func() { recover() }() // in case async Close is called.
	b.mdClient.Close()
	close(b.finch)
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// compute a map of replicas for each index in 2i.
func (b *metadataClient) computeReplicas(
	topo map[common.IndexerId][]*mclient.IndexMetadata) map[common.IndexDefnId][]common.IndexInstId {

	replicaMap := make(map[common.IndexDefnId][]common.IndexInstId)
	for _, indexes1 := range topo {
		for _, index1 := range indexes1 {

			if _, ok := replicaMap[index1.Definition.DefnId]; ok {
				continue
			}

			replicas := make([]common.IndexInstId, 0)

			// Instance cannot be of any of valid instance state (READY, INITIAL, CATCHUP, ACTIVE),
			// but RState is always ACTIVE. Add all of them to replica list regardless of the instance
			// state. When picking instance for scanning, only ACTIVE will use picked.
			for _, inst1 := range index1.Instances {
				replicas = append(replicas, inst1.InstId) // add itself
			}

			for id2, indexes2 := range topo {

				for _, index2 := range indexes2 {
					if index1.Definition.DefnId == index2.Definition.DefnId { // skip replica
						continue
					}

					if b.equivalentIndex(index1, index2) { // pick equivalents
						for _, inst2 := range index2.Instances {

							if inst2.IndexerId == id2 { // only add instance from this indexer
								replicas = append(replicas, inst2.InstId)
							}
						}
					}
				}
			}

			replicaMap[index1.Definition.DefnId] = replicas // map it
		}
	}
	return replicaMap
}

func (b *metadataClient) computeReplicasInRebalance(
	topo map[common.IndexerId][]*mclient.IndexMetadata) map[common.IndexDefnId][]*mclient.InstanceDefn {

	replicaMap := make(map[common.IndexDefnId][]*mclient.InstanceDefn)
	for _, indexes1 := range topo {
		for _, index1 := range indexes1 {

			if _, ok := replicaMap[index1.Definition.DefnId]; ok {
				continue
			}

			replicas := make([]*mclient.InstanceDefn, 0)

			for _, inst1 := range index1.InstsInRebalance {
				replicas = append(replicas, inst1) // add itself
			}

			for id2, indexes2 := range topo {

				for _, index2 := range indexes2 {

					if index1.Definition.DefnId == index2.Definition.DefnId { // skip replica
						continue
					}

					if b.equivalentIndex(index1, index2) { // pick equivalents
						for _, inst2 := range index2.InstsInRebalance {
							if inst2.IndexerId == id2 { // only add instance from this indexer
								replicas = append(replicas, inst2)
							}
						}
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
		d1.PartitionKey != d2.PartitionKey ||
		d1.WhereExpr != d2.WhereExpr {

		return false
	}

	if len(d1.SecExprs) != len(d2.SecExprs) {
		return false
	}

	for i, s1 := range d1.SecExprs {
		if s1 != d2.SecExprs[i] {
			return false
		}
	}

	if len(d1.Desc) != len(d2.Desc) {
		return false
	}

	for i, b1 := range d1.Desc {
		if b1 != d2.Desc[i] {
			return false
		}
	}

	return true
}

//--------------------------------------
// local functions to work with replicas
//--------------------------------------

// manage load statistics.
type loadHeuristics struct {
	avgLoad uint64
}

func (b *loadHeuristics) updateValue(value float64) {

	avgLoadInt := atomic.LoadUint64(&b.avgLoad)
	avgLoad := math.Float64frombits(avgLoadInt)

	// compute incremental average.
	avgLoad = (avgLoad + float64(value)) / 2.0

	avgLoadInt = math.Float64bits(avgLoad)
	atomic.StoreUint64(&b.avgLoad, avgLoadInt)
}

func (b *loadHeuristics) getValue() float64 {

	avgLoadInt := atomic.LoadUint64(&b.avgLoad)
	avgLoad := math.Float64frombits(avgLoadInt)

	return avgLoad
}

// pick a random replica from the list.
func (b *metadataClient) pickRandom(replicas []uint64, defnID uint64, excludes map[uint64]bool) (*mclient.InstanceDefn, bool) {
	var actvReplicas [128]uint64
	n := 0
	for _, replicaID := range replicas {
		state, _ := b.indexInstState(uint64(replicaID))
		if state == common.INDEX_STATE_ACTIVE && (excludes == nil || !excludes[uint64(replicaID)]) {
			actvReplicas[n] = uint64(replicaID)
			n++
		}
	}

	if n == 0 {
		if inst, ok := b.pickReplicaInRebalance(defnID); ok {
			return inst, true
		}
	}

	if n > 0 {
		chosen := actvReplicas[rand.Intn(n*10)%n]

		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		if inst, ok := currmeta.insts[common.IndexInstId(chosen)]; ok {
			return inst, true
		}
	}

	return nil, false
}

// pick an optimal replica for the index `defnID` under least load.
func (b *metadataClient) pickOptimal(defnID uint64, excludes map[uint64]bool) (*mclient.InstanceDefn, bool) {
	// gather active-replicas
	var actvReplicas [128]uint64
	var loadList [128]float64
	n := 0
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, replicaID := range currmeta.replicas[common.IndexDefnId(defnID)] {
		state, _ := b.indexInstState(uint64(replicaID))
		if state == common.INDEX_STATE_ACTIVE && (excludes == nil || !excludes[uint64(replicaID)]) {
			actvReplicas[n] = uint64(replicaID)
			load, ok := currmeta.loads[replicaID]
			if !ok {
				loadList[n] = 0.0
			} else {
				loadList[n] = load.getValue()
			}
			n++
		}
	}
	if n == 0 { // none are active
		if inst, ok := b.pickReplicaInRebalance(defnID); ok {
			return inst, true
		}
		return nil, false
	}
	// compute replica with least load.
	sort.Float64s(loadList[:n])
	leastLoad := loadList[0]

	var replicas [128]uint64
	// gather list of replicas with equivalent load
	m := 0
	for _, replicaID := range actvReplicas[:n] {
		load, ok := currmeta.loads[common.IndexInstId(replicaID)]
		if !ok || (load.getValue()*b.equivalenceFactor <= leastLoad) {
			replicas[m] = replicaID
			m++
		}
	}
	return b.pickRandom(replicas[:m], defnID, excludes)
}

func (b *metadataClient) pickReplicaInRebalance(defnID uint64) (*mclient.InstanceDefn, bool) {

	// Run out of active instance.   Try instance under rebalance, in case rebalance may have finished.
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	replicasInRebal := currmeta.replicasInRebal[common.IndexDefnId(defnID)]
	n := len(replicasInRebal)
	if n > 0 {
		return replicasInRebal[rand.Intn(n*10)%n], true
	}

	return nil, false
}

//----------------
// local functions
//----------------

func (b *metadataClient) logstats() {
	tick := time.NewTicker(b.logtick)
	defer func() {
		tick.Stop()
	}()

loop:
	for {
		<-tick.C
		s := make([]string, 0, 16)
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

		logging.Infof("connected with %v indexers\n", len(currmeta.topology))
		for id, replicas := range currmeta.replicas {
			logging.Infof("index %v has %v replicas\n", id, len(replicas))
		}
		// currmeta.loads is immutable
		for id, _ := range currmeta.insts {
			load := currmeta.loads[id]
			s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getValue()))
		}
		logging.Infof("client load stats {%v}", strings.Join(s, ","))

		select {
		case _, ok := <-b.finch:
			if !ok {
				break loop
			}
		default:
		}
	}
}

// unprotected access to shared structures.
func (b *metadataClient) indexState(defnID uint64) (common.IndexState, error) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		if index.Error != "" {
			return common.INDEX_STATE_ERROR, errors.New(index.Error)
		}

		return index.State, nil
	}

	return common.INDEX_STATE_ERROR, ErrorIndexNotFound
}

// unprotected access to shared structures.
func (b *metadataClient) indexInstState(instID uint64) (common.IndexState, error) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if inst, ok := currmeta.insts[common.IndexInstId(instID)]; ok {
		if inst.Error != "" {
			return common.INDEX_STATE_ERROR, errors.New(inst.Error)
		}

		return inst.State, nil
	}

	return common.INDEX_STATE_ERROR, ErrorInstanceNotFound
}

// update 2i cluster information,
func (b *metadataClient) updateIndexerList(discardExisting bool) error {

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		return err
	}
	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return err
	}
	if err := cinfo.Fetch(); err != nil {
		return err
	}

	// UpdateIndexerList is synchronous, except for async callback from WatchMetadata() -- when indexer is
	// not responding fast enough.
	// TopoChangeLock is to protect the updates to index topology made by async callack.   Index topology contains
	// the assignment between admniport and indexerId -- which is udpated by async callback.   Metadata version cannot
	// enforce serializability of such assigment update, since it is not part of metadata tracked by metadataProvider.
	//
	// The adminport-indexerId assignment is used to figure out difference in topology so that gsiClient can call
	// WatchMetadata and UnwatchMetadata properly.   Corruption of such assignment can cause a lot of issue.
	//
	// The lock is to protect race condition in such case
	// 1) make sure that updateIndexerList() is finished before any its callback is invoked.
	//    This is to ensure async callback does not lose its work.
	// 2) make sure that the async callback is called sequentially so their changes on adminport-indexerId are accumulative.
	//    This is to ensure async callback does not lose its work.
	// 3) if there are consecutive topology changes by ns-server, make sure that async callback will not save a stale
	//    adminport-indexerId assignment by overwriting the assignment created by second topology changes.
	b.topoChangeLock.Lock()
	defer b.topoChangeLock.Unlock()

	// populate indexers' adminport and queryport
	adminports, activeNode, failedNode, unhealthyNode, newNode, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}
	b.mdClient.SetClusterStatus(activeNode, failedNode, unhealthyNode, newNode)

	fmsg := "Refreshing indexer list due to cluster changes or auto-refresh."
	logging.Infof(fmsg)
	logging.Infof("Refreshed Indexer List: %v", adminports)

	var curradmns map[string]common.IndexerId
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if currmeta != nil {
		curradmns = make(map[string]common.IndexerId)
		for adminport, indexerId := range currmeta.adminports {
			curradmns[adminport] = indexerId
		}
	}

	if discardExisting {
		for _, indexerID := range curradmns {
			b.mdClient.UnwatchMetadata(indexerID, activeNode)
		}
		curradmns = make(map[string]common.IndexerId)
	}

	// watch all indexers
	m := make(map[string]common.IndexerId)
	for _, adminport := range adminports { // add new indexer-nodes if any
		if indexerID, ok := curradmns[adminport]; !ok {
			// This adminport is provided by cluster manager.  Meta client will
			// honor cluster manager to treat this adminport as a healthy node.
			// If the indexer is unavail during initialization, WatchMetadata()
			// will return afer timeout. A background watcher will keep
			// retrying, since it can be tranisent partitioning error.
			// If retry eventually successful, this callback will be invoked
			// to update meta_client. The metadata client has to rely on the
			// cluster manager to send a notification if this node is detected
			// to be down, such that the metadata client can stop the
			// background watcher.
			fn := func(ad string, n_id common.IndexerId, o_id common.IndexerId) {
				// following time.Sleep() ensures that bootstrap initialization
				// completes before other async activities into metadataClient{}
				currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
				for currmeta == nil {
					time.Sleep(10 * time.Millisecond)
					currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
				}
				b.updateIndexer(ad, n_id, o_id)
			}

			// WatchMetadata will "unwatch" an old metadata watcher which
			// shares the same indexer Id (but the adminport may be different).
			indexerID = b.mdClient.WatchMetadata(adminport, fn, activeNode)
			m[adminport] = indexerID
		} else {
			err = b.mdClient.UpdateServiceAddrForIndexer(indexerID, adminport)
			m[adminport] = indexerID
			delete(curradmns, adminport)
		}
	}
	// delete indexer-nodes that got removed from cluster.
	for _, indexerID := range curradmns {
		// check if the indexerId exists in var "m".  In case the
		// adminport changes for the same index node, there would
		// be two adminport mapping to the same indexerId, one
		// in b.adminport (old) and the other in "m" (new).  So
		// make sure not to accidently unwatch the indexer.
		found := false
		for _, id := range m {
			if indexerID == id {
				found = true
			}
		}
		if !found {
			b.mdClient.UnwatchMetadata(indexerID, activeNode)
		}
	}
	b.safeupdate(m, true /*force*/)
	return err
}

func (b *metadataClient) updateIndexer(
	adminport string, newIndexerId, oldIndexerId common.IndexerId) {

	func() {
		b.topoChangeLock.Lock()
		defer b.topoChangeLock.Unlock()

		adminports := make(map[string]common.IndexerId)
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		for admnport, indexerId := range currmeta.adminports {
			adminports[admnport] = indexerId
		}

		// UpdateIndexer is a async call.  When this is invoked, currmeta.adminports may be different.
		if _, ok := adminports[adminport]; ok {
			logging.Infof(
				"Acknowledged that new indexer is registered.  Indexer = %v, id = %v",
				adminport, newIndexerId)
			adminports[adminport] = newIndexerId
			b.safeupdate(adminports, true /*force*/)
		} else {
			logging.Infof(
				"New indexer registration is skipped.  Indexer may have been rebalanced out (unwatch).  Indexer = %v, id = %v",
				adminport, newIndexerId)
		}
	}()
}

// replicas:
//		Refresh
//		deleteIndex
// loads:
//		Refresh
//		deleteIndex
// topology:
//		Refresh
//		deleteIndex
//
//		Timeit for b.loads

func (b *metadataClient) updateTopology(
	adminports map[string]common.IndexerId, force bool) *indexTopology {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	// Refresh indexer version
	b.mdClient.RefreshIndexerVersion()

	mindexes, version := b.mdClient.ListIndex()
	// detect change in indexer cluster or indexes.
	if force == false && currmeta != nil &&
		(!b.hasIndexersChanged(adminports) &&
			!b.hasIndexesChanged(mindexes, version)) {
		return currmeta
	}

	// create a new topology.
	newmeta := &indexTopology{
		version:         version,
		allIndexes:      mindexes,
		adminports:      make(map[string]common.IndexerId),
		topology:        make(map[common.IndexerId][]*mclient.IndexMetadata),
		replicas:        make(map[common.IndexDefnId][]common.IndexInstId),
		queryports:      make(map[common.IndexerId]string),
		insts:           make(map[common.IndexInstId]*mclient.InstanceDefn),
		defns:           make(map[common.IndexDefnId]*mclient.IndexMetadata),
		replicasInRebal: make(map[common.IndexDefnId][]*mclient.InstanceDefn),
	}

	// adminport/queryport
	for adminport, indexerID := range adminports {
		newmeta.adminports[adminport] = indexerID
		newmeta.topology[indexerID] = make([]*mclient.IndexMetadata, 0, 16)

		_, qp, _, err := b.mdClient.FindServiceForIndexer(indexerID)
		if err == nil {
			// This excludes watcher that is not currently connected
			newmeta.queryports[indexerID] = qp
		}
	}

	// insts/defns
	topologyMap := make(map[common.IndexerId]map[common.IndexDefnId]*mclient.IndexMetadata)
	for _, mindex := range mindexes {
		newmeta.defns[mindex.Definition.DefnId] = mindex

		for _, instance := range mindex.Instances {
			if _, ok := topologyMap[instance.IndexerId]; !ok {
				topologyMap[instance.IndexerId] = make(map[common.IndexDefnId]*mclient.IndexMetadata)
			}
			if _, ok := topologyMap[instance.IndexerId][instance.DefnId]; !ok {
				topologyMap[instance.IndexerId][instance.DefnId] = mindex
			}
			newmeta.insts[instance.InstId] = instance
		}

		for _, instance := range mindex.InstsInRebalance {
			if _, ok := topologyMap[instance.IndexerId]; !ok {
				topologyMap[instance.IndexerId] = make(map[common.IndexDefnId]*mclient.IndexMetadata)
			}
			if _, ok := topologyMap[instance.IndexerId][instance.DefnId]; !ok {
				topologyMap[instance.IndexerId][instance.DefnId] = mindex
			}
			if _, ok := newmeta.insts[instance.InstId]; !ok {
				newmeta.insts[instance.InstId] = instance
			}
		}
	}

	// topology
	for indexerId, indexes := range topologyMap {
		for _, index := range indexes {
			indexes2, ok := newmeta.topology[indexerId]
			if !ok {
				fmsg := "indexer node %v not available"
				logging.Fatalf(fmsg, indexerId)
				continue
			}
			newmeta.topology[indexerId] = append(indexes2, index)
		}
	}

	// replicas/replicaInRebal
	newmeta.replicas = b.computeReplicas(newmeta.topology)
	newmeta.replicasInRebal = b.computeReplicasInRebalance(newmeta.topology)

	// loads - after creation, newmeta.loads is immutable, even though the
	// content (loadHeuristics) is mutable
	newmeta.loads = make(map[common.IndexInstId]*loadHeuristics)

	if currmeta != nil {
		// currmeta.loads is immutable
		for instId, _ := range currmeta.insts {
			if _, ok := newmeta.insts[instId]; ok {
				if load, ok := currmeta.loads[instId]; ok {
					newmeta.loads[instId] = load
				}
			}
		}
	}

	for instId, _ := range newmeta.insts {
		if _, ok := newmeta.loads[instId]; !ok {
			newmeta.loads[instId] = &loadHeuristics{avgLoad: 0}
		}
	}

	return newmeta
}

func (b *metadataClient) safeupdate(
	adminports map[string]common.IndexerId, force bool) {

	var currmeta, newmeta *indexTopology

	done := false
	for done == false {
		currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))

		// no need to update if cached metadata is already up to date
		if !force && currmeta != nil && b.mdClient.GetMetadataVersion() <= currmeta.version {
			return
		}

		// if adminport is nil, then safeupdate is not triggered by
		// topology change.  Get the adminports from currmeta.
		if currmeta != nil && adminports == nil {
			adminports = currmeta.adminports
		}

		newmeta = b.updateTopology(adminports, force)
		if currmeta == nil {
			// This should happen only during bootstrap
			atomic.StorePointer(&b.indexers, unsafe.Pointer(newmeta))
			logging.Infof("initialized currmeta %v force %v \n", newmeta.version, force)
			return
		} else if force {
			if newmeta.version < currmeta.version {
				// This should not happen.  But if it does force to increment metadata version.
				b.mdClient.IncrementMetadataVersion()
				continue
			}
		} else if newmeta.version <= currmeta.version {
			fmsg := "skip newmeta %v <= %v force %v \n"
			logging.Infof(fmsg, newmeta.version, currmeta.version, force)
			return
		}

		oldptr := unsafe.Pointer(currmeta)
		newptr := unsafe.Pointer(newmeta)
		done = atomic.CompareAndSwapPointer(&b.indexers, oldptr, newptr)

		// metaCh should never close
		if done && b.metaCh != nil {
			select {
			// update scan clients
			case b.metaCh <- true:
			default:
			}
		}
	}
	fmsg := "switched currmeta from %v -> %v force %v \n"
	logging.Infof(fmsg, currmeta.version, newmeta.version, force)
}

func (b *metadataClient) hasIndexersChanged(
	adminports map[string]common.IndexerId) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for adminport, indexerId := range adminports {
		x, ok := currmeta.adminports[adminport]
		if !ok || x != indexerId { // new indexer node detected.
			return true
		}
	}
	for adminport, indexerId := range currmeta.adminports {
		x, ok := adminports[adminport]
		if !ok || x != indexerId { // indexer node dropped out.
			return true
		}
	}
	return false
}

func (b *metadataClient) hasIndexesChanged(
	mindexes []*mclient.IndexMetadata, version uint64) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if currmeta.version < version {
		fmsg := "metadata provider version changed %v -> %v\n"
		logging.Infof(fmsg, currmeta.version, version)
		return true
	}

	for _, mindex := range mindexes {
		_, ok := currmeta.replicas[mindex.Definition.DefnId]
		if !ok { // new index detected.
			return true
		}
	}

	for _, iindexes := range currmeta.topology { //iindexes -> []*IndexMetadata
	loop:
		for _, index := range iindexes { // index -> *IndexMetadata
			for _, mindex := range mindexes {
				if mindex.Definition.DefnId == index.Definition.DefnId {
					for _, ix := range index.Instances {
						for _, iy := range mindex.Instances {
							if ix.InstId == iy.InstId && ix.State != iy.State {
								return true
							}
						}
					}
					continue loop
				}
			}
			return true
		}
	}
	return false
}

// return adminports for all known indexers.
func getIndexerAdminports(cinfo *common.ClusterInfoCache) ([]string, int, int, int, int, error) {
	iAdminports := make([]string, 0)
	unhealthyNodes := 0
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		status, err := cinfo.GetNodeStatus(node)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		}
		logging.Verbosef("node %v status: %q", node, status)
		if status == "healthy" || status == "active" || status == "warmup" {
			adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
			if err != nil {
				return nil, 0, 0, 0, 0, err
			}
			iAdminports = append(iAdminports, adminport)
		} else {
			unhealthyNodes++
			logging.Warnf("node %v status: %q", node, status)
		}
	}

	return iAdminports, // active, healthy indexer node with known ports
		len(cinfo.GetActiveIndexerNodes()), // all active indexer node (known ports + unknown ports)
		len(cinfo.GetFailedIndexerNodes()), // failover indexer node
		unhealthyNodes, // active, unhealthy indexer node with known ports
		len(cinfo.GetNewIndexerNodes()), // new indexer node
		nil
}

// FIXME/TODO: based on discussion with John-
//
//    if we cannot watch the metadata due to network partition we will
//    have an empty list of index and cannot query, in other words
//    the client will tolerate the partition and rejects scans until it
//    is healed.
//    i) alternatively, figure out a way to propagate error that happens
//       with watchClusterChanges() go-routine.
//
//    and while propating error back to the caller
//    1) we can encourage the caller to Refresh() the client hoping for
//       success, or,
//    2) Close() the client and re-create it.
//
//    side-effects of partitioning,
//    a) query cannot get indexes from the indexer node -- so n1ql has
//       to do bucket scan. It is a perf issue.
//    b) Network disconnected after watcher is up. We have the list of
//       indexes -- but we cannot query on it. N1QL should still degrade
//       to bucket scan.

func (b *metadataClient) watchClusterChanges() {
	selfRestart := func() {
		time.Sleep(time.Duration(b.servicesNotifierRetryTm) * time.Millisecond)
		go b.watchClusterChanges()
	}

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		logging.Errorf("common.ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}
	scn, err := common.NewServicesChangeNotifier(clusterURL, "default")
	if err != nil {
		logging.Errorf("common.NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	hasUnhealthyNode := false
	ticker := time.NewTicker(time.Duration(5) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else if err := b.updateIndexerList(false); err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}
		case _, ok := <-b.mdNotifyCh:
			if ok {
				if err := b.updateIndexerList(false); err != nil {
					logging.Errorf("updateIndexerList(): %v\n", err)
					selfRestart()
					return
				}
			}
		case <-ticker.C:
			// refresh indexer version
			b.mdClient.RefreshIndexerVersion()

			cinfo, err := common.FetchNewClusterInfoCache(b.cluster, common.DEFAULT_POOL)
			if err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}

			_, _, _, unhealthyNode, _, err := getIndexerAdminports(cinfo)
			if err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}

			if unhealthyNode != 0 {
				hasUnhealthyNode = true
				b.mdClient.SetClusterStatus(-1, -1, unhealthyNode, -1)

			} else if hasUnhealthyNode {
				// refresh indexer version when there is no more unhealthy node
				hasUnhealthyNode = false
				if err := b.updateIndexerList(false); err != nil {
					logging.Errorf("updateIndexerList(): %v\n", err)
					selfRestart()
					return
				}
			}
		case <-b.finch:
			return
		}
	}
}

func postWithAuth(url string, bodyType string, body io.Reader, timeout time.Duration) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		logging.Errorf("Error setting auth %v", err)
		return nil, err
	}

	client := http.Client{Timeout: timeout}
	return client.Do(req)
}
