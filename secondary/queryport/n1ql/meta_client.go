package n1ql

import "sync"
import "fmt"
import "strconv"

import c "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbaselabs/query/errors"

type metaBridgeClient struct {
	rw          sync.RWMutex // protects `mdClient` and `nextNode`
	gsi         *gsiKeyspace
	clusterURL  string
	serviceAddr string
	nodes       []c.NodeId
	mdClient    *mclient.MetadataProvider
	adminports  map[c.NodeId]string
	queryports  map[c.NodeId]string
	// sherlock topology management
	nextNode int
}

func newMetaBridgeClient(
	gsi *gsiKeyspace, cluster, serviceAddr string) (*metaBridgeClient, errors.Error) {

	var e error

	cinfo, err := getClusterInfo(cluster, "default" /*pool*/) // TODO:nomagic
	if err != nil {
		return nil, err
	}
	b := &metaBridgeClient{
		clusterURL:  cluster,
		serviceAddr: serviceAddr,
		nodes:       cinfo.GetNodesByServiceType("indexers"),
		nextNode:    0, /* start assigning from 0 */
	}
	// initialize meta-data-provide.
	b.mdClient, e = mclient.NewMetadataProvider(b.serviceAddr)
	if e != nil {
		msg := "metaBridgeClient NewMetadataProvider() failed"
		return nil, errors.NewError(e, fmt.Sprintf(msg))
	}
	// populate indexers' adminport and queryport
	b.adminports, err = getIndexerAdminports(cinfo, b.nodes)
	if err != nil {
		return nil, err
	}
	for _, adminport := range b.adminports {
		b.mdClient.WatchMetadata(adminport)
	}
	b.queryports, err = getIndexerQueryports(cinfo, b.nodes)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// get latest set of meta-data.
func (b *metaBridgeClient) refresh() ([]*secondaryIndex, errors.Error) {
	b.rw.RLock()
	if b.mdClient == nil {
		return nil, errors.NewError(nil, "metaBridgeClient closed")
	}
	mdClient := b.mdClient
	b.rw.RUnlock()

	mindexes := mdClient.ListIndex()
	indexes := make([]*secondaryIndex, len(mindexes))
	for i, mindex := range mindexes {
		if mindex.Definition.Bucket != b.gsi.keyspace {
			continue
		}
		// TODO: how to compute the topology of indexes from ListIndex ??
		nodeIds := []c.NodeId{}
		index, err := newSecondaryIndexFromMetaData(b.gsi, mindex, nodeIds)
		if err != nil {
			return nil, err
		}
		indexes[i] = index
	}
	return indexes, nil
}

// topology management for sherlock. returns list of node-ids
// that contains a single reference to indexer-node.
func (b *metaBridgeClient) topologyForIndex() []c.NodeId {
	b.rw.RLock()
	defer b.rw.RUnlock()
	if len(b.nodes) > 0 {
		nodeId := b.nodes[b.nextNode]
		b.nextNode = (b.nextNode + 1) % len(b.nodes)
		return []c.NodeId{nodeId}
	}
	return nil
}

// get adminport address for nodeId
func (b *metaBridgeClient) getAdminport(
	nodeId c.NodeId) (adminport string, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()
	if b.adminports != nil {
		adminport, ok := b.adminports[nodeId]
		return adminport, ok
	}
	return "", false
}

// get queryport address for nodeId
func (b *metaBridgeClient) getQueryport(
	nodeId c.NodeId) (queryport string, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()
	if b.queryports != nil {
		queryport, ok := b.queryports[nodeId]
		return queryport, ok
	}
	return "", false
}

// create a new index after picking necessary topology.
func (b *metaBridgeClient) createIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool) (*secondaryIndex, errors.Error) {

	nodeIds := b.topologyForIndex()
	if nodeIds == nil {
		err := errors.NewError(nil, " metaBridgeClient topologyForIndex() failed")
		return nil, err
	}
	adminport, ok := b.getAdminport(nodeIds[0])
	if !ok {
		err := errors.NewError(nil, " metaBridgeClient getAdminport() failed")
		return nil, err
	}

	b.rw.RLock()
	if b.mdClient == nil {
		return nil, errors.NewError(nil, "metaBridgeClient closed")
	}
	mdClient := b.mdClient
	b.rw.RUnlock()

	defnId, e := mdClient.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		adminport, secExprs, isPrimary)
	if e != nil {
		return nil, errors.NewError(e, " metaBridgeClient CreateIndex()")
	}
	for _, index := range mdClient.ListIndex() {
		if index.Definition.DefnId == defnId {
			return newSecondaryIndexFromMetaData(b.gsi, index, nodeIds)
		}
	}
	errmsg := fmt.Sprintf(" metaBridgeClient unknown index %v", defnId)
	err := errors.NewError(nil, errmsg)
	return nil, err
}

// drop index
func (b *metaBridgeClient) dropIndex(si *secondaryIndex) errors.Error {
	b.rw.RLock()
	if b.mdClient == nil {
		return errors.NewError(nil, "metaBridgeClient closed")
	}
	mdClient := b.mdClient
	b.rw.RUnlock()

	adminport, ok := b.getAdminport(b.nodes[0])
	if !ok {
		return errors.NewError(nil, " metaBridgeClient getAdminport()")
	}
	defnId, e := strconv.ParseUint(si.Id(), 16, 64)
	if e != nil {
		return errors.NewError(e, " metaBridgeClient ParseUint()")
	}
	if e := mdClient.DropIndex(c.IndexDefnId(defnId), adminport); e != nil {
		return errors.NewError(e, " metaBridgeClient DropIndex()")
	}
	return nil
}

// create a new scanner client.
func (b *metaBridgeClient) newScannerClient(
	si *secondaryIndex) (*qclient.Client, errors.Error) {

	if len(si.nodes) > 0 {
		config := c.SystemConfig.SectionConfig("queryport.client.", true)
		queryport, ok := b.getQueryport(b.nodes[0])
		if !ok {
			return nil, errors.NewError(nil, "metaBridgeClient no queryport")
		}

		client := qclient.NewClient(qclient.Remoteaddr(queryport), config)
		return client, nil
	}
	return nil, errors.NewError(nil, "metaBridgeClient empty indexers")
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metaBridgeClient) closeBridge() {
	b.rw.Lock()
	defer b.rw.Unlock()
	if b.mdClient != nil {
		b.mdClient.Close()
		b.mdClient = nil
		b.adminports, b.queryports = nil, nil
	}
}
