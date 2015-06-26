package functionaltests

import (
	"errors"
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"log"
	"path/filepath"
	"testing"
	"time"
)

// =====================================================
// Settings Tests
// =====================================================

func TestIndexerSettings(t *testing.T) {
	log.Printf("In TestIndexerSettings()")

	// Indexer CPU Cores: "indexer.settings.max_cpu_percent"
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.max_cpu_percent", float64(300), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// In Memory Snapshot Interval: "indexer.settings.inmemory_snapshot.interval"
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.inmemory_snapshot.interval", float64(300), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Stable Snapshot Interval: "indexer.settings.persisted_snapshot.interval"
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(20000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Max Rollback Points:  "indexer.settings.recovery.max_rollbacks",
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.recovery.max_rollbacks", float64(3), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Indexer Log Level: "indexer.settings.log_level" : "debug",
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.log_level", "error", clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
}

func TestRestoreDefaultSettings(t *testing.T) {
	log.Printf("In TestIndexerSettings_RestoreDefault()")

	// Indexer CPU Cores: "indexer.settings.max_cpu_percent"
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.max_cpu_percent", float64(400), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// In Memory Snapshot Interval: "indexer.settings.inmemory_snapshot.interval"
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.inmemory_snapshot.interval", float64(200), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Stable Snapshot Interval: "indexer.settings.persisted_snapshot.interval"
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(30000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Max Rollback Points:  "indexer.settings.recovery.max_rollbacks",
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.recovery.max_rollbacks", float64(5), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	// Indexer Log Level: "indexer.settings.log_level" : "debug",
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.log_level", "debug", clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
}

// =====================================================
// Stats Tests
// =====================================================
func TestStat_ItemsCount(t *testing.T) {
	log.Printf("In TestStat_ItemsCount()")

	// Stat Name: items_count
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	log.Printf("Emptying the default bucket")
	kv.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)
	time.Sleep(5 * time.Second)

	log.Printf("Generating JSON docs")
	docs = GenerateJsons(10000, seed, filepath.Join(proddir, "test.prod"), bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	indexName := "index_test1"
	bucketName := "default"
	indexField := "company"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexField}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(10 * time.Second)

	prefix := bucketName + ":" + indexName
	stats := secondaryindex.GetIndexStats(indexName, bucketName, clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount := stats[prefix+":items_count"].(float64)
	log.Printf("items_count stat is %v", itemsCount)

	if itemsCount != float64(len(docs)) {
		log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
		err = errors.New("items_count is incorrect for index " + prefix)
		FailTestIfError(err, "Error in TestStat_ItemsCount", t)
	}
}

// =====================================================
// Compaction Tests
// =====================================================

type compactionStats struct {
	frag_percent    float64
	num_compactions float64
	disk_size       float64
}

func TestDefaultCompactionBehavior(t *testing.T) {
	log.Printf("In TestDefaultCompactionBehavior()")
	compactionFragmentationTest(float64(30), false, "index_compactiontest1", "default", "company", 100, t)
}

func TestLowFragmentation(t *testing.T) {
	log.Printf("In TestLowFragmentation()")
	compactionFragmentationTest(float64(5), true, "index_compactiontest1", "default", "company", 100, t)
}

func TestHighFragmentation(t *testing.T) {
	log.Printf("In TestLowFragmentation()")
	compactionFragmentationTest(float64(70), true, "index_compactiontest1", "default", "company", 150, t)
}

func TestHighCompactionMinSize(t *testing.T) {
	log.Printf("In TestHighCompactionMinSize()")
	compactionFragmentationTest(float64(70), true, "index_compactiontest1", "default", "company", 150, t)
}

func TestCompactionDiskMinSize(t *testing.T) {
	log.Printf("In TestCompactionDiskMinSize()")

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	log.Printf("Emptying the default bucket")
	kv.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)
	time.Sleep(5 * time.Second)

	log.Printf("Generating JSON docs")
	docs = GenerateJsons(10000, seed, filepath.Join(proddir, "test.prod"), bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	indexName := "index_compactiontest1"
	bucketName := "default"
	indexField := "company"
	min_sizeValue := float64(320000000)

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.compaction.min_size", min_sizeValue, clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	log.Printf("Creating a 2i")
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexField}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	stats1 := getCompactionStats(indexName, bucketName)
	log.Printf("Current Compaction Stats: Fragmentation=%v, Num_Compactions=%v, Disk_Size=%v\n", stats1.frag_percent, stats1.num_compactions, stats1.disk_size)
	prev_compactionnumber := stats1.num_compactions

	for i := 0; i < 100; i++ {
		log.Printf("ITERATION %v", i)
		updateDocsFieldForFragmentation(indexField)
		kv.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

		stats2 := getCompactionStats(indexName, bucketName)
		log.Printf("Current Compaction Stats: Fragmentation=%v, Num_Compactions=%v, Disk_Size=%0.0f\n", stats2.frag_percent, stats2.num_compactions, stats2.disk_size)
		if stats2.num_compactions > prev_compactionnumber {
			time.Sleep(10 * time.Second)
			stats3 := getCompactionStats(indexName, bucketName)
			if stats3.disk_size < min_sizeValue {
				errstr := fmt.Sprintf("Index compaction occurred before disk size %0.0f reached the set min_size %0.0f", stats3.disk_size, min_sizeValue)
				log.Printf(errstr)
				err := errors.New(errstr)
				FailTestIfError(err, "Error in TestCompactionDiskMinSize", t)
			}
		}
		prev_compactionnumber = stats2.num_compactions
	}
	
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.compaction.min_size", float64(1048576), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
}

func compactionFragmentationTest(fragmentationValue float64, updateFragmentationValue bool, indexName, bucketName, indexField string, updateCount int, t *testing.T) {
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	log.Printf("Emptying the default bucket")
	kv.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)
	time.Sleep(5 * time.Second)

	log.Printf("Generating JSON docs")
	docs = GenerateJsons(10000, seed, filepath.Join(proddir, "test.prod"), bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	if updateFragmentationValue {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.compaction.min_frag", fragmentationValue, clusterconfig.Username, clusterconfig.Password, kvaddress)
		FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	}

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexField}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	stats1 := getCompactionStats(indexName, bucketName)
	log.Printf("Compaction Stats are: Fragmentation =  %v and Num of Compactions = %v\n", stats1.frag_percent, stats1.num_compactions)
	prev_compactionnumber := stats1.num_compactions

	for i := 0; i < updateCount; i++ {
		log.Printf("ITERATION %v", i)
		updateDocsFieldForFragmentation(indexField)
		kv.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

		stats2 := getCompactionStats(indexName, bucketName)
		log.Printf("Current Compaction Stats are: Fragmentation =  %v and Num of Compactions = %v\n", stats2.frag_percent, stats2.num_compactions)

		if stats2.frag_percent > fragmentationValue {
			time.Sleep(40 * time.Second)
			stats3 := getCompactionStats(indexName, bucketName)
			if stats3.num_compactions <= prev_compactionnumber {
				errorStr := fmt.Sprintf("Expected compaction to occur at %v but did not occur. Number of compactions = %v", fragmentationValue, stats3.num_compactions)
				log.Printf(errorStr)
				FailTestIfError(errors.New(errorStr), "Error in TestDefaultCompactionBehavior", t)
			}
			stats1 = getCompactionStats(indexName, bucketName)
			log.Printf("Compaction occured :: Compaction Stats are: Fragmentation =  %v and Num of Compactions = %v\n", stats1.frag_percent, stats1.num_compactions)
		}

		prev_compactionnumber = stats1.num_compactions
	}
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.compaction.min_frag", float64(30), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
}

func getCompactionStats(indexName, bucketName string) compactionStats {
	prefix := bucketName + ":" + indexName
	stats := secondaryindex.GetIndexStats(indexName, bucketName, clusterconfig.Username, clusterconfig.Password, kvaddress)
	//data_size := stats[prefix+":data_size"].(float64)
	disk_size := stats[prefix+":disk_size"].(float64)
	fragmentation := stats[prefix+":frag_percent"].(float64)
	num_compactions := stats[prefix+":num_compactions"].(float64)
	return compactionStats{fragmentation, num_compactions, disk_size}
}

func updateDocsFieldForFragmentation(field string) {
	for key, value := range docs {
		json := value.(map[string]interface{})
		strValue := randString(6)
		json[field] = strValue
		docs[key] = json
	}
}
