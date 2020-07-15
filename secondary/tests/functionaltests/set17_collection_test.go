package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func CreateDocsForCollection(bucketName, collectionID string, num int) tc.KeyValues {
	kvdocs := generateDocs(num, "users.prod")
	kv.SetKeyValuesForCollection(kvdocs, bucketName, collectionID, "", clusterconfig.KVAddress)
	return kvdocs
}

func DeleteDocsFromCollection(bucketName, collectionID string, kvdocs tc.KeyValues) {
	kv.DeleteKeysFromCollection(kvdocs, bucketName, collectionID, "", clusterconfig.KVAddress)
}

func createIndex(index, bucket, scope, coll string, field []string, t *testing.T) {
	err := secondaryindex.CreateSecondaryIndex3(index, bucket, scope, coll, indexManagementAddress,
		"", field, []bool{false}, false, nil, c.SINGLE, nil, true,
		defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

func createDeferIndex(index, bucket, scope, coll string, field []string, t *testing.T) {
	err := secondaryindex.CreateSecondaryIndex3(index, bucket, scope, coll, indexManagementAddress,
		"", field, []bool{false}, false, []byte("{\"defer_build\": true}"), c.SINGLE, nil, true,
		0, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

func createPrimaryIndex(index, bucket, scope, coll string, t *testing.T) {
	err := secondaryindex.CreateSecondaryIndex3(index, bucket, scope, coll, indexManagementAddress,
		"", nil, nil, true, nil, c.SINGLE, nil, true,
		defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

func dropIndex(index, bucket, scope, coll string, t *testing.T) {
	err := secondaryindex.DropSecondaryIndex2(index, bucket, scope, coll, indexManagementAddress)
	FailTestIfError(err, "Error in drop index", t)
}

func scanAllAndVerify(index, bucket, scope, collection, field string, masterDocs tc.KeyValues, t *testing.T) {
	docScanResults := datautility.ExpectedScanAllResponse(masterDocs, field)
	scanResults, err := secondaryindex.ScanAll2(index, bucket, scope,
		collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func scanAllAndVerifyCount(index, bucket, scope, collection string, masterDocs tc.KeyValues, t *testing.T) {
	scanResults, err := secondaryindex.ScanAll2(index, bucket, scope,
		collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan ", t)
	if len(scanResults) != len(masterDocs) {
		errMsg := fmt.Sprintf("Scan Count mismatch. Expected %v. Actual %v", len(masterDocs), len(scanResults))
		err = errors.New(errMsg)
	}
	FailTestIfError(err, "Error in scan result validation", t)
}

func updateMasterDocSet(masterDocs tc.KeyValues, incrDocs tc.KeyValues) {
	for k, v := range incrDocs {
		masterDocs[k] = v
	}
}

func TestCollectionSetup(t *testing.T) {

	log.Printf("In TestCollectionSetup()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	time.Sleep(10 * time.Second)

	kv.DropAllScopesAndCollections("default", clusterconfig.Username, clusterconfig.Password, kvaddress, false)
	time.Sleep(10 * time.Second)
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
}

var masterDocs_c1, masterDocs_default, masterDocs_c2, masterDocs_c3 tc.KeyValues

func TestCollectionDefault(t *testing.T) {

	log.Printf("In TestCollectionDefault()")

	//Initial build on index on default collection in default scope
	bucket := "default"
	scope := "_default"
	coll := "_default"
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)

	masterDocs_default = CreateDocsForCollection(bucket, cid, 2000)
	time.Sleep(5 * time.Second)

	//Initial build on index on default collection in default scope.
	//There are documents in default collection.
	index1 := scope + "_" + coll + "_" + "i1"
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_default, t)

	//Create 2nd index to check stream merge
	index2 := scope + "_" + coll + "_" + "i2"
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_default, t)

	//Drop one index and check scan results
	dropIndex(index1, bucket, scope, coll, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_default, t)

	//Create index again
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_default, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)

	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_default, t)

	//Create 2nd index to check stream merge
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_default, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)

	//Create multiple indexes with defer build
	createDeferIndex(index1, bucket, scope, coll, []string{"age"}, t)
	createDeferIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	secondaryindex.BuildIndexes([]string{index1, index2}, bucket, indexManagementAddress, defaultIndexActiveTimeout)

	//Load more docs and scan
	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_default, incrdocs)
	time.Sleep(5 * time.Second)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_default, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_default, t)

	//Drop one index
	dropIndex(index1, bucket, scope, coll, t)

	//Load more docs and scan
	incrdocs = CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_default, incrdocs)
	time.Sleep(5 * time.Second)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_default, t)

	dropIndex(index2, bucket, scope, coll, t)

}

func TestCollectionNonDefault(t *testing.T) {

	log.Printf("In TestCollectionNonDefault()")

	bucket := "default"
	scope := "s1"
	coll := "c1"
	kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)

	time.Sleep(10 * time.Second)
	masterDocs_c1 = CreateDocsForCollection(bucket, cid, 2000)

	//Initial build on index on non-default collection in non-default scope.
	//There are documents in default collection.
	index1 := scope + "_" + coll + "_" + "i1"
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//Create 2nd index to check stream merge
	index2 := scope + "_" + coll + "_" + "i2"
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Drop one index and check scan results
	dropIndex(index1, bucket, scope, coll, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Create index again
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)

	/*
		//Delete default collection
		time.Sleep(60 * time.Second)
		kv.DropCollection(bucket, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, clusterconfig.Username, clusterconfig.Password, kvaddress)
	*/

	//Initial build on index on non-default collection in non-default scope
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//Create 2nd index to check stream merge
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)

	//Create multiple indexes with defer build
	createDeferIndex(index1, bucket, scope, coll, []string{"age"}, t)
	createDeferIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	secondaryindex.BuildIndexes([]string{index1, index2}, bucket, indexManagementAddress, defaultIndexActiveTimeout)

	//Load more docs and scan
	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c1, incrdocs)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Drop one index
	dropIndex(index1, bucket, scope, coll, t)

	//Load more docs and scan
	incrdocs = CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c1, incrdocs)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	/*
		//Initial build on index on non-default collection in default scope
		scope = c.DEFAULT_SCOPE
		kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		time.Sleep(5 * time.Second)
		cid1 := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		createIndex(index1, bucket, scope, coll, []string{"age"}, t)
		kvdocs := CreateDocsForCollection(bucket, cid1, 1000)
		scanAllAndVerify(index1, bucket, "gender", kvdocs, t)
	*/

	//Drop both the index
	//dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)

}

func TestCollectionMetaAtSnapEnd(t *testing.T) {

	log.Printf("In TestCollectionMetaAtSnapEnd()")

	//create another collection
	bucket := "default"
	scope := "s2"
	coll := "c2"
	kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(10 * time.Second)
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)

	//SYSTEM_EVENT Test
	//create index on this collection
	index1 := scope + "_" + coll + "_" + "i1"
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)

	masterDocs_c2 = CreateDocsForCollection(bucket, cid, 2000)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)

	//create an unrelated collection
	coll1 := "c3"
	kvutility.CreateCollection(bucket, scope, coll1, clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(10 * time.Second)

	//verify scan when snapshot end is meta
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)

	//SEQNO_ADVANCED Test
	//create one more index on collection
	index2 := scope + "_" + coll + "_" + "i2"
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c2, t)

	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c2, incrdocs)
	time.Sleep(5 * time.Second)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c2, t)

	//do not drop indexes, used by the TestCollectionUpdateSeq

}

func TestCollectionUpdateSeq(t *testing.T) {

	log.Printf("In TestCollectionUpdateSeq()")

	//verify updateSeq message by loading docs in a collection which doesn't have any index and verify scan
	bucket := "default"
	scope := "s2"
	coll := "c3"

	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	masterDocs_c3 = CreateDocsForCollection(bucket, cid, 1000)

	coll = "c2"
	index1 := scope + "_" + coll + "_" + "i1"
	index2 := scope + "_" + coll + "_" + "i2"
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c2, t)

	//add more docs to the collection for the index
	cid = kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c2, incrdocs)
	time.Sleep(5 * time.Second)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c2, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c2, t)

	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)
}

func TestCollectionMultiple(t *testing.T) {

	log.Printf("In TestCollectionMultiple()")

	//create index on default collection
	bucket := "default"
	scope1 := "_default"
	coll1 := "_default"
	//cid := kvutility.GetCollectionID(bucket, scope1, coll1, clusterconfig.Username, clusterconfig.Password, kvaddress)

	index1 := scope1 + "_" + coll1 + "_" + "i3"
	createIndex(index1, bucket, scope1, coll1, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope1, coll1, "age", masterDocs_default, t)

	//create index on non-default collection and check stream merge
	scope2 := "s1"
	coll2 := "c1"
	//cid = kvutility.GetCollectionID(bucket, scope2, coll2, clusterconfig.Username, clusterconfig.Password, kvaddress)

	//create index on this collection
	index2 := scope2 + "_" + coll2 + "_" + "i4"
	createIndex(index2, bucket, scope2, coll2, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope2, coll2, "gender", masterDocs_c1, t)

	dropIndex(index1, bucket, scope1, coll1, t)
	dropIndex(index2, bucket, scope2, coll2, t)

	//create defer indexes on 2 different collections and build together

	//create multiple indexes in different collections and update docs such that only 1 index gets changed

	//create multiple indexes in same collection and update docs such that only 1 index gets changed

	//incremental build on multiple collections before/after stream merge

	//manual verification for upsertDelete message

}

func TestCollectionNoDocs(t *testing.T) {

	log.Printf("In TestCollectionNoDocs()")

	bucket := "default"
	scope := "s1"
	coll := "c1"

	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	DeleteDocsFromCollection(bucket, cid, masterDocs_c1)
	masterDocs_c1 = make(tc.KeyValues)

	//create index on empty collection
	index1 := scope + "_" + coll + "_" + "i1"
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//Create 2nd index to check stream merge
	index2 := scope + "_" + coll + "_" + "i2"
	createIndex(index2, bucket, scope, coll, []string{"gender"}, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Drop one index and check scan results
	dropIndex(index1, bucket, scope, coll, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Create index again
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//load data to check incremental build
	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c1, incrdocs)
	time.Sleep(5 * time.Second)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)
	scanAllAndVerify(index2, bucket, scope, coll, "gender", masterDocs_c1, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)
}

func TestCollectionPrimaryIndex(t *testing.T) {

	log.Printf("In TestCollectionPrimaryIndex()")

	bucket := "default"
	scope := "s1"
	coll := "c1"
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	DeleteDocsFromCollection(bucket, cid, masterDocs_c1)
	masterDocs_c1 = CreateDocsForCollection(bucket, cid, 2000)

	//create primary index
	index1 := scope + "_" + coll + "_" + "i1"
	createPrimaryIndex(index1, bucket, scope, coll, t)
	scanAllAndVerifyCount(index1, bucket, scope, coll, masterDocs_c1, t)

	//Create 2nd index to check stream merge
	index2 := scope + "_" + coll + "_" + "i2"
	createPrimaryIndex(index2, bucket, scope, coll, t)
	scanAllAndVerifyCount(index2, bucket, scope, coll, masterDocs_c1, t)

	//load data to check incremental build
	incrdocs := CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c1, incrdocs)
	scanAllAndVerifyCount(index1, bucket, scope, coll, masterDocs_c1, t)
	scanAllAndVerifyCount(index2, bucket, scope, coll, masterDocs_c1, t)

	//Drop one index and check scan results
	dropIndex(index1, bucket, scope, coll, t)

	//create regular index and check results
	createIndex(index1, bucket, scope, coll, []string{"age"}, t)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)

	//load data to check incremental build
	incrdocs = CreateDocsForCollection(bucket, cid, 1000)
	updateMasterDocSet(masterDocs_c1, incrdocs)
	scanAllAndVerify(index1, bucket, scope, coll, "age", masterDocs_c1, t)
	scanAllAndVerifyCount(index2, bucket, scope, coll, masterDocs_c1, t)

	//Drop both the index
	dropIndex(index1, bucket, scope, coll, t)
	dropIndex(index2, bucket, scope, coll, t)
}

func SkipTestCollectionWhereClause(t *testing.T) {

	log.Printf("In TestCollectionWhereClause()")

	//where clause and no docs qualify

	//where clause with some docs qualify

}

/*
	- restart projector
	- restart indexer(disk recovery) [for both maint/init stream] -- Add to test plan
	- drop index while build is in progress
*/

func TestCollectionDrop(t *testing.T) {

	log.Printf("In TestCollectionDrop()")

	//At this point following, scope/collection are available
	//s1.c1, s2.c2, s2.c3, default

	bucket := "default"

	//Create couple of indexes on all collections
	create2Indexes := func(scope, coll string) {
		index1 := scope + "_" + coll + "_" + "i1"
		createIndex(index1, bucket, scope, coll, []string{"age"}, t)
		index2 := scope + "_" + coll + "_" + "i2"
		createIndex(index2, bucket, scope, coll, []string{"age"}, t)
	}

	create2Indexes("s1", "c1")
	create2Indexes("s2", "c2")
	create2Indexes("s2", "c3")
	create2Indexes("_default", "_default")

	//drop one collection
	scope := "s1"
	coll := "c1"
	kvutility.DropCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(5 * time.Second)
	scanResults, err := secondaryindex.ScanAll2(scope+"_"+coll+"_"+"i1", bucket, scope,
		coll, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		t.Fatal("Error expected when scanning for dropped index but scan didn't fail \n")
		log.Printf("Length of scanResults = %v", len(scanResults))
	} else {
		log.Printf("Scan failed as expected with error: %v\n", err)
	}

	//drop one scope with 2 collections
	scope = "s2"
	coll = "c1"
	kvutility.DropScope(bucket, scope, clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(5 * time.Second)
	scanResults, err = secondaryindex.ScanAll2(scope+"_"+coll+"_"+"i1", bucket, scope,
		coll, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		t.Fatal("Error expected when scanning for dropped index but scan didn't fail \n")
		log.Printf("Length of scanResults = %v", len(scanResults))
	} else {
		log.Printf("Scan failed as expected with error: %v\n", err)
	}

}
