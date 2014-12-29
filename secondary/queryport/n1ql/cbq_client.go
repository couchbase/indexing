package n1ql

import "github.com/couchbaselabs/query/errors"
import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"

type cbqBridgeClient struct {
	gsi       *gsiKeyspace
	adminport string
	queryport string
}

func newCbqBridgeClient(gsi *gsiKeyspace) *cbqBridgeClient {
	b := &cbqBridgeClient{
		gsi:       gsi,
		adminport: "localhost:9100",
		queryport: "localhost:9101",
	}
	return b
}

// refresh with cbq-bridge.
func (b *cbqBridgeClient) refresh() ([]*secondaryIndex, errors.Error) {
	client := qclient.NewClusterClient(b.adminport)
	infos, err := client.List()
	if err != nil {
		errmsg := " GSI.cbqBridgeClient.NewClusterClient()"
		return nil, errors.NewError(err, errmsg)
	} else if infos == nil { // empty list of indexes
		return nil, errors.NewError(err, " GSI.cbqBridgeClient infos empty")
	}

	indexes := make([]*secondaryIndex, len(infos))
	for i, info := range infos {
		if info.Bucket != b.gsi.keyspace {
			continue
		}
		nodeIds := []c.NodeId{}
		index, err := newSecondaryIndexFromInfo(b.gsi, &info, nodeIds)
		if err != nil {
			return nil, err
		}
		indexes[i] = index
	}
	return indexes, nil
}

// create a new index after picking necessary topology.
func (b *cbqBridgeClient) createIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool) (*secondaryIndex, errors.Error) {

	client := qclient.NewClusterClient(b.adminport)
	// update meta-data.
	info, err := client.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary)
	if err != nil {
		return nil, errors.NewError(err, " cbqBridgeClient createIndex()")
	}
	return newSecondaryIndexFromInfo(b.gsi, info, []c.NodeId{})
}

// drop index.
func (b *cbqBridgeClient) dropIndex(si *secondaryIndex) errors.Error {
	client := qclient.NewClusterClient(b.adminport)
	err := client.DropIndex(si.defnID)
	if err != nil {
		return errors.NewError(err, " cbqBridgeClient dropIndex()")
	}
	return nil
}

// create a new scanner client.
func (b *cbqBridgeClient) newScannerClient(
	si *secondaryIndex) (*qclient.Client, errors.Error) {

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client := qclient.NewClient(qclient.Remoteaddr(b.queryport), config)
	return client, nil
}

// no-op
func (b *cbqBridgeClient) closeBridge() {
}
