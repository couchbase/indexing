package serverlesstests

import (
	"fmt"
	"testing"

	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"gopkg.in/couchbase/gocb.v1"
)

// When creating an index through N1QL, the index is expected
// to be created with a replica
func TestIndexPlacement(t *testing.T) {
	bucket := "bucket_1"
	scope := "_default"
	collection := "c1"
	index := "idx_1"

	// Create Bucket
	kvutility.CreateBucket(bucket, "sasl", "", clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress, "256", "11213")
	kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})

	kvutility.CreateCollection(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress)
	cid := kvutility.WaitForCollectionCreation(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})

	CreateDocsForCollection(bucket, cid, 1000)

	n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(company)", index, bucket, scope, collection)
	tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket,
		n1qlStatement, false, gocb.RequestPlus)

	// Get index statement after index creation
	waitForIndexActive(bucket, index, t)
	waitForIndexActive(bucket, index+" (replica 1)", t)

	waitForStatsUpdate()

	// Scan the index
	scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, 100, 1000, 1, t)
}
