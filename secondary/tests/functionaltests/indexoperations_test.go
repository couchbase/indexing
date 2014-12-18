package functionaltests

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"testing"
)

// Skipping right now as this causes further tests to fail
func SkipTestCreateIndexNonExistentBucket(t *testing.T) {
	fmt.Println("In TestCreateIndexNonExistentBucket()")
	var indexName = "index_BucketTest1"
	var bucketName = "test1"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, []string{"score"}, true)
	FailTestIfError(err, "Error in creating the index", t)
}
