package largedatatests

import (
	"fmt"
	"time"
	// tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"testing"
)

func TestPerfInitialIndexBuild(t *testing.T) {
	fmt.Println("In TestPerfInitialIndexBuild()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/users.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 10000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kvutility.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	fmt.Printf("Index build of %d user documents took %s\n", count, elapsed)
}