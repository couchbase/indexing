package functionaltests

import (
	"log"
	"testing"

	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"gopkg.in/couchbase/gocb.v1"
)

// Test cases
// 1. Test the basic missing leading key syntax
//	* Create an index with missing leading key
//	* Try querying without using "is not missing"
//	* Check partitioned index too
// 2. Check if index missing leading key can be used as primary index
//	* Create and index with missing leading key
//	* Check if Querying for non covering data is picking up this index
// 3. Check for equivalency of indexes
//	* Indexes with everything being similar and only missing value set
//	* should not be treated equivalent
// 4. Check aggregations and etc with this new index type
// 5. Check ordering along with missing.. DESC with MISSING

// Other tests
// 1. Mixed mode

func execN1QL(bucket, stmt string) ([]interface{}, error) {
	return tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket,
		stmt, false, gocb.RequestPlus)
}

func TestMissingLeadingKeyBasic(t *testing.T) {

	bucket := "default"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	myDocs := generateDocs(1000, "users.prod")
	c := 0
	for _, value := range myDocs {
		if c%2 == 0 {
			val := value.(map[string]interface{})
			val["vaccinated"] = 1
			val["num_doses"] = c % 3
		}
		c = c + 1
	}

	// Populate the bucket
	log.Printf("Populating the default bucket")
	kvutility.SetKeyValues(myDocs, bucket, "", clusterconfig.KVAddress)

	// Create index with missing key
	n1ql := "CREATE INDEX idx_vac ON default(vaccinated MISSING)"
	_, err := execN1QL(bucket, n1ql)
	FailTestIfError(err, "Error in creating idx_vac with missing leading key", t)

	n1ql = "SELECT age FROM default"
	output, err := execN1QL(bucket, n1ql)
	if err != nil {
		log.Printf("%+v", output)
	}
	FailTestIfError(err, "Error in scanning with idx_vac with missing leading key", t)

	if len(output) != 1000 {
		t.Fatalf("Output returned from index with missing on leading key is not complete %v", len(output))
	}
}

func TestMissingLeadingKeyPartitioned(t *testing.T) {
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestMissingLeadingKeyPartitioned for forestdb")
		return
	}

	bucket := "default"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create index with missing key
	n1ql := "CREATE INDEX idx_doses_partn ON default(num_doses MISSING, vaccinated) PARTITION BY HASH(num_doses)"
	_, err := execN1QL(bucket, n1ql)
	FailTestIfError(err, "Error in creating idx_vac with missing leading key", t)

	n1ql = "SELECT emailid FROM default"
	output, err := execN1QL(bucket, n1ql)
	if err != nil {
		log.Printf("%+v", output)
	}
	FailTestIfError(err, "Error in scanning with idx_vac with missing leading key", t)

	if len(output) != 1000 {
		t.Fatalf("Output returned from index with missing on leading key is not complete %v", len(output))
	}
}
