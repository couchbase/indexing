package functionaltests

import (
	"fmt"
	"testing"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func SkipTestCreateIndexNonExistentBucket(t *testing.T) {
	fmt.Println("In TestCreateIndexNonExistentBucket()")
	var indexName = "index_BucketTest1"
	var bucketName = "test1"
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, []string {"score"}, true)
	FailTestIfError(err, "Error in creating the index", t)
}