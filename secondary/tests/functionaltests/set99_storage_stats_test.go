package functionaltests

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/query/datastore"
)

const CORRUPT_DATA_SUBDIR = ".corruptData"

func waitTillIndexBecomesActive(indexName, bucketName, indexManagementAddress string, timeout int) error {
	ti := 0
	var err error
	var state string
	for {
		ti++
		time.Sleep(time.Second)
		if ti == timeout {
			return errors.New("Timeout in waitTillIndexBecomesActive")
		}

		state, err = secondaryindex.IndexState(indexName, bucketName, indexManagementAddress)
		if err != nil {
			continue
		}

		if state != "INDEX_STATE_ACTIVE" {
			continue
		}

		return nil
	}
}

func TestIdxCorruptBasicSanityMultipleIndices(t *testing.T) {
	// Due to pickEquivalent implementation in meta_client and retry in doScan,
	// Queries on corrupt indices may succeed. So, cleanup all indices before
	// starting corruption tests.
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	time.Sleep(10 * time.Second)

	// Step 1: Create 2 indices
	fmt.Println("Creating two indices ...")
	var err error
	err = secondaryindex.CreateSecondaryIndex("corrupt_idx1_age", "default", indexManagementAddress,
		"", []string{"age"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex("corrupt_idx2_company", "default", indexManagementAddress,
		"", []string{"company"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(hosts) > 1 {
		errHosts = errors.New("Unexpected number of hosts")
	}
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)
	fmt.Println("hosts =", hosts)

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	ok, _ := verifyPathExists(absIndexStorageDir)
	if !ok {
		return
	}

	var slicePath string
	slicePath, err = tc.GetIndexSlicePath("corrupt_idx1_age", "default", absIndexStorageDir, 0)
	FailTestIfError(err, "Error in GetIndexSlicePath", t)

	// Step 2: Corrupt one of them (corrupt_idx1_age)
	err = secondaryindex.CorruptIndex("corrupt_idx1_age", "default", absIndexStorageDir,
		clusterconfig.IndexUsing, 0)
	FailTestIfError(err, "Error while corrupting the index", t)

	// Step 3: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Step 4: Wait for indexes to become active
	err = waitTillIndexBecomesActive("corrupt_idx2_company", "default", indexManagementAddress, 30)
	FailTestIfError(err, "Error while waiting for index to be active", t)

	// Step 5: Query on corrupt_idx2_company should succeed, query on corrupt_idx1_age should fail

	_, err = secondaryindex.Range("corrupt_idx2_company", "default", indexScanAddress, []interface{}{"G"},
		[]interface{}{"M"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan", t)

	_, err = secondaryindex.Range("corrupt_idx1_age", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		err = errors.New("Unexpected success of scan as index was corrupted")
		FailTestIfError(err, "Error in scan of corrupt index", t)
	}

	err = verifyDeletedPath(slicePath)

	FailTestIfError(err, "Error in verifyDeletedPath", t)
}

func TestIdxCorruptPartitionedIndex(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		fmt.Println("Not running TestPartitionedIndex for forestdb")
		return
	}

	fmt.Println("Creating partitioned index ...")
	var err error
	err = secondaryindex.CreateSecondaryIndex2("corrupt_idx3_age", "default", indexManagementAddress,
		"", []string{"age"}, []bool{false}, false, nil, c.KEY, []string{"age"}, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(hosts) > 1 {
		errHosts = errors.New("Unexpected number of hosts")
	}
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)
	fmt.Println("hosts =", hosts)

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	ok, _ := verifyPathExists(absIndexStorageDir)
	if !ok {
		return
	}

	numPartnsIf, errGetNumPartn := tc.GetIndexerSetting(hosts[0], "indexer.numPartitions",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetNumPartn, "Error in errGetNumPartn", t)

	fmt.Println("indexer.numPartitions =", numPartnsIf)

	numPartnsStr := fmt.Sprintf("%v", numPartnsIf)
	numPartns, err := strconv.Atoi(numPartnsStr)
	FailTestIfError(err, "Error while Atoi", t)

	if int(numPartns) <= 1 {
		return
	}

	// Get slice paths for all partitions
	slicePaths := make(map[int]string)
	partnToCorrupt := c.PartitionId(rand.Intn(int(numPartns)) + 1)
	fmt.Println("Corrupting partn id", partnToCorrupt)
	for i := 1; i <= int(numPartns); i++ {
		fmt.Println("Getting slicepath for ", i)
		slicePaths[i], err = tc.GetIndexSlicePath("corrupt_idx3_age", "default",
			absIndexStorageDir, c.PartitionId(i))
		FailTestIfError(err, "Error in GetIndexSlicePath", t)
		fmt.Println("slicePath for partn", i, "=", slicePaths[i])
	}

	// Step 2: Corrupt the index corrupt_idx3_age partition 10
	err = secondaryindex.CorruptIndex("corrupt_idx3_age", "default", absIndexStorageDir,
		clusterconfig.IndexUsing, partnToCorrupt)
	FailTestIfError(err, "Error while corrupting the index", t)

	// Step 3: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Step 4: Ideally, point Query on non corrupted index partitions should succeed.
	//         But, as of now, the index is treated as unavailable if one partition is unavailable.
	//         So, the query will fail.
	//         Also, in case of n1ql client, Range won't fail. It will return zero results.
	_, err = secondaryindex.Range("corrupt_idx3_age", "default", indexScanAddress,
		[]interface{}{21}, []interface{}{21}, 3, false, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		err = errors.New("Unexpected success of scan as index was corrupted")
		FailTestIfError(err, "Error in scan of corrupt index", t)
	}

	fmt.Println("Verified single partition corruption")

	// Step 5: Restart indexer process again
	forceKillIndexer()

	// Step 6: Try the query again. Expect the same result.
	_, err = secondaryindex.Range("corrupt_idx3_age", "default", indexScanAddress,
		[]interface{}{21}, []interface{}{21}, 3, false, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		err = errors.New("Unexpected success of scan as index was corrupted")
		FailTestIfError(err, "Error in scan of corrupt index", t)
	}

	// Step 7: corrupt all remaining partitions
	for i := 1; i <= int(numPartns); i++ {
		if c.PartitionId(i) == partnToCorrupt {
			fmt.Println("Skip corrupting partition", i)
			continue
		}

		err = secondaryindex.CorruptIndex("corrupt_idx3_age", "default", absIndexStorageDir,
			clusterconfig.IndexUsing, c.PartitionId(i))
		FailTestIfError(err, "Error while corrupting the index", t)
	}

	// Step 8: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Step 9: Query on the index should return error
	_, err = secondaryindex.Range("corrupt_idx3_age", "default", indexScanAddress,
		[]interface{}{35}, []interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		err = errors.New("Unexpected success of scan as index was corrupted")
		FailTestIfError(err, "Error in scan of corrupt index", t)
	}

	// Step 10: Validate all slice paths are deleted.
	for i := 1; i <= int(numPartns); i++ {
		err = verifyDeletedPath(slicePaths[i])
		FailTestIfError(err, "Error in verifyDeletedPath", t)
	}
}

func TestIdxCorruptMOITwoSnapsOneCorrupt(t *testing.T) {
	if clusterconfig.IndexUsing != "memory_optimized" {
		fmt.Println("Not running TestMOITwoSnapsOneCorrupt for", clusterconfig.IndexUsing)
		return
	}

	// Step 0: Set MOI snapshot frequency to 20 seconds
	var err error
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval",
		float64(20000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Step 1: Create an index
	fmt.Println("Creating an index ...")
	err = secondaryindex.CreateSecondaryIndex("corrupt_idx4_age", "default", indexManagementAddress,
		"", []string{"age"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(hosts) > 1 {
		errHosts = errors.New("Unexpected number of hosts")
	}
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)
	fmt.Println("hosts =", hosts)

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	ok, _ := verifyPathExists(absIndexStorageDir)
	if !ok {
		return
	}

	var slicePath string
	slicePath, err = tc.GetIndexSlicePath("corrupt_idx4_age", "default", absIndexStorageDir, 0)
	FailTestIfError(err, "Error in GetIndexSlicePath", t)

	// Step 2: Add more data and wait for snapshot interval
	fmt.Printf("Populating the default bucket with more docs\n")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(20000 * time.Millisecond)

	// Verify if new snapshot is created
	pattern := "*"
	files, errGlob := filepath.Glob(filepath.Join(slicePath, pattern))
	FailTestIfError(errGlob, "Error in filepath.Glob", t)
	fmt.Println("Snapshots: ", files)
	if len(files) < 2 {
		err = errors.New("Less number of snapshots than expected")
		FailTestIfError(err, "Error", t)
	}

	var pth string
	pth, err = tc.GetMOILatestSnapshotPath("corrupt_idx4_age", "default", absIndexStorageDir, 0)
	FailTestIfError(err, "Error in GetMOILatestSnapshotPath", t)

	// Step 3: Corrupt latest snapshot for corrupt_idx4_age
	err = secondaryindex.CorruptMOIIndexLatestSnapshot("corrupt_idx4_age", "default", absIndexStorageDir,
		clusterconfig.IndexUsing, 0)
	FailTestIfError(err, "Error while corrupting the index", t)

	// Step 4: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Step 5: Query on the index should not fail.
	_, errRange := secondaryindex.Range("corrupt_idx4_age", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(errRange, "Error in scan of corrupt index", t)

	// Step 6: Verify if the snapshot directory is deleted.
	err = verifyDeletedPath(pth)
	FailTestIfError(err, "Error in verifyDeletedPath", t)

	// Step 7: Kill indexer
	forceKillIndexer()

	// Step 8: Same query on the index should not fail after restart.
	_, errRange1 := secondaryindex.Range("corrupt_idx4_age", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(errRange1, "Error in scan of corrupt index", t)
}

func TestIdxCorruptMOITwoSnapsBothCorrupt(t *testing.T) {
	if clusterconfig.IndexUsing != "memory_optimized" {
		fmt.Println("Not running TestMOITwoSnapsBothCorrupt for", clusterconfig.IndexUsing)
		return
	}

	// Step 0: Set MOI snapshot frequency to 20 seconds
	var err error
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval",
		float64(20000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Step 1: Create an index
	fmt.Println("Creating an index ...")
	err = secondaryindex.CreateSecondaryIndex("corrupt_idx5_name", "default", indexManagementAddress,
		"", []string{"name"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(hosts) > 1 {
		errHosts = errors.New("Unexpected number of hosts")
	}
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)
	fmt.Println("hosts =", hosts)

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	ok, _ := verifyPathExists(absIndexStorageDir)
	if !ok {
		return
	}

	var slicePath string
	slicePath, err = tc.GetIndexSlicePath("corrupt_idx5_name", "default", absIndexStorageDir, 0)
	FailTestIfError(err, "Error in GetIndexSlicePath", t)

	// Step 2: Add more data and wait for snapshot interval
	fmt.Printf("Populating the default bucket with more docs\n")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(20000 * time.Millisecond)

	// Verify if new snapshot is created
	pattern := "*"
	files, errGlob := filepath.Glob(filepath.Join(slicePath, pattern))
	FailTestIfError(errGlob, "Error in filepath.Glob", t)
	fmt.Println("Snapshots: ", files)
	if len(files) < 2 {
		err = errors.New("Less number of snapshots than expected")
		FailTestIfError(err, "Error", t)
	}

	// Step 3: Corrupt all snapshots
	infos, err := tc.GetMemDBSnapshots(slicePath, false)
	FailTestIfError(err, "Error in GetMemDBSnapshots", t)

	snapInfoContainer := tc.NewSnapshotInfoContainer(infos)
	allSnapshots := snapInfoContainer.List()

	if len(allSnapshots) <= 1 {
		err = errors.New("Less number of snapshots than expected")
		FailTestIfError(err, "No enough snapshots", t)
	}

	for i := 0; i < len(allSnapshots); i++ {
		secondaryindex.CorruptMOIIndexBySnapshotPath(allSnapshots[i].DataPath)
		FailTestIfError(err, "Error while corrupting the index", t)
	}

	// Step 4: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Verify both snapshot paths are deleted
	for i := 0; i < len(allSnapshots); i++ {
		err = verifyDeletedPath(allSnapshots[i].DataPath)
		FailTestIfError(err, "Error in verifyDeletedPath", t)
	}

	// Step 5: Query on the index should not fail.
	_, errRange := secondaryindex.Range("corrupt_idx5_name", "default", indexScanAddress, []interface{}{"A"},
		[]interface{}{"X"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if errRange == nil {
		err = errors.New("Unexpected success of scan as index was corrupted")
		FailTestIfError(err, "Error in scan of corrupt index", t)
	}
}

func TestIdxCorruptBackup(t *testing.T) {
	// Step 0: Enable backup
	var err error
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.enable_corrupt_index_backup",
		true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// snapshot after index build is done. The previous test sets snapshot persistance to 20 seconds
	// If indexer takes more than 20 seconds to process the index creation and build, then indexer
	// will create another disk snapshot (FORCE_COMMIT) and then this test will fail if the first
	// snapshot is corrupted while the second snapshot is being created.
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval",
		float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Step 1: Create index
	fmt.Println("Creating index ...")
	err = secondaryindex.CreateSecondaryIndex2("corrupt_idx6_age", "default", indexManagementAddress,
		"", []string{"age"}, []bool{false}, false, nil, c.SINGLE, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(hosts) > 1 {
		errHosts = errors.New("Unexpected number of hosts")
	}
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)
	fmt.Println("hosts =", hosts)

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	ok, _ := verifyPathExists(absIndexStorageDir)
	if !ok {
		return
	}

	var slicePath string
	slicePath, err = tc.GetIndexSlicePath("corrupt_idx6_age", "default",
		absIndexStorageDir, c.PartitionId(0))

	// Step 2: Corrupt the index corrupt_idx3_age partition 10
	err = secondaryindex.CorruptIndex("corrupt_idx6_age", "default", absIndexStorageDir,
		clusterconfig.IndexUsing, 0)
	FailTestIfError(err, "Error while corrupting the index", t)

	// Step 3: Restart indexer process and wait for some time.
	forceKillIndexer()

	// Step 4: Verify that partition is cleaned up
	err = verifyDeletedPath(slicePath)
	FailTestIfError(err, "Error in verifyDeletedPath", t)

	// Step 5: Verify that backup folder exists
	corruptDataDir := filepath.Join(absIndexStorageDir, CORRUPT_DATA_SUBDIR)
	files, err := ioutil.ReadDir(corruptDataDir)
	FailTestIfError(err, "Error while ioutil.ReadDir(corruptDataDir)", t)

	_, sliceP := filepath.Split(slicePath)

	found := false
	for _, f := range files {
		if strings.HasSuffix(f.Name(), sliceP) {
			found = true
		}
	}

	if !found {
		err = errors.New("Backup folder %v doesn't exist")
		FailTestIfError(err, "Error while verifying backup folder existance", t)
	}
}

func TestStatsPersistence(t *testing.T) {
	log.Printf("In TestStatsPersistence()")

	lastKnownScanTimeStat := "last_known_scan_time"

	bucketName := "default"
	indexNames := [...]string{"index_age", "index_gender", "index_city"}
	indexFields := [...]string{"age", "gender", "address.city"}

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvdocs := generateDocs(1000, "users.prod")
	UpdateKVDocs(kvdocs, docs)
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	for i := 0; i < 3; i++ {
		err := secondaryindex.CreateSecondaryIndex(indexNames[i], bucketName, indexManagementAddress, "", []string{indexFields[i]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
	}

	err := secondaryindex.CreateSecondaryIndex("p1", bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating primary index p1", t)

	testPersistence := func(enabled bool, persistenceInterval uint64) {
		log.Printf("=== Testing for persistence enabled = %v, with interval = %v  ===", enabled, persistenceInterval)
		err := secondaryindex.ChangeIndexerSettings("indexer.statsPersistenceInterval", persistenceInterval, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in ChangeIndexerSettings")
		tc.KillIndexer()
		time.Sleep(5 * time.Second)

		for i := 0; i < 3; i++ {
			_, err := secondaryindex.ScanAll(indexNames[i], bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
			FailTestIfError(err, "Error in ScanAll", t)
		}

		lqt1 := make([]float64, 3)
		stats := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
		for i := 0; i < 3; i++ {
			lqt1[i] = stats[bucketName+":"+indexNames[i]+":"+lastKnownScanTimeStat].(float64)
		}

		time.Sleep(time.Duration(persistenceInterval+2) * time.Second)
		tc.KillIndexer()
		time.Sleep(5 * time.Second)

		stats = secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
		for i := 0; i < 3; i++ {
			lqt := stats[bucketName+":"+indexNames[i]+":"+lastKnownScanTimeStat].(float64)
			if enabled {
				if lqt != lqt1[i] {
					err := errors.New(fmt.Sprintf("%v stat mistach after restart for index %v: Before: %v After %v", lastKnownScanTimeStat, indexNames[i], lqt1[i], lqt))
					FailTestIfError(err, "Error in TestStatsPersistence", t)
				}
			} else {
				if lqt == lqt1[i] {
					err := errors.New(fmt.Sprintf("%v stat unexpected match with stats persistence disabled for index %v", lastKnownScanTimeStat, indexNames[i]))
					FailTestIfError(err, "Error in TestStatsPersistence", t)
				}
			}
		}
	}

	testPersistence(true, 5)
	testPersistence(false, 0)
	testPersistence(true, 10)
	testPersistence(true, 5)
}

func TestStats_StorageStatistics(t *testing.T) {
	log.Printf("In TestStats_StorageStatistics()")

	bucketName := "default"

	err := secondaryindex.CreateSecondaryIndex("index_age", bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	stats, err := secondaryindex.N1QLStorageStatistics("index_age", bucketName, indexScanAddress)
	FailTestIfError(err, "Error from N1QLStorageStatistics", t)
	log.Printf("Stats from Index4 StorageStatistics for index %v are %v", "index_age", stats)

	err = secondaryindex.CreateSecondaryIndex("p1", bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating primary index", t)

	stats, err = secondaryindex.N1QLStorageStatistics("p1", bucketName, indexScanAddress)
	FailTestIfError(err, "Error from N1QLStorageStatistics", t)
	log.Printf("Stats from Index4 StorageStatistics for index %v are %v", "p1", stats)
}

func validateNonZeroLastResetTimeWithBaseTime(
	stats map[uint64][]map[string]interface{},
	beforeTime int64,
) error {
	if len(stats) == 0 {
		return fmt.Errorf("stats map is empty")
	}

	for instID, allPartnStats := range stats {
		for _, partnStats := range allPartnStats {
			if lastResetTimeIface, ok := partnStats[string(datastore.IX_STAT_LAST_RESET_TS)]; ok {
				var lastResetTime int64 = lastResetTimeIface.(int64)
				if lastResetTime == 0 {
					return fmt.Errorf("lastResetTime is 0 for instance %v", instID)
				} else if lastResetTime < beforeTime {
					return fmt.Errorf("lastResetTime is less than creation TS (%v) for instance %v (%v)",
						beforeTime, instID, lastResetTime)
				}
			} else if !ok {
				return fmt.Errorf("lastResetTime not found in stats for inst %v", instID)
			}
		}
	}
	return nil
}

func TestStats_DefnStorageStatistics(t *testing.T) {
	if clusterconfig.IndexUsing != "plasma" {
		t.Skipf("TestStats_DefnStorageStatistics is only applicable for plasma")
		return
	}

	log.Printf("In TestStats_DefnStorageStatistics()")

	bucketName := "default"

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllSecondaryIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	time.Sleep(30 * time.Second)

	beforeTime := time.Now().UnixMilli()

	log.Println("*********Create Indexes*********")
	err = secondaryindex.CreateSecondaryIndex("index_age", bucketName, indexManagementAddress, "", []string{"age"}, false, nil, false, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	stats, err := secondaryindex.N1QLDefnStorageStatistics("index_age", bucketName, indexScanAddress)
	FailTestIfError(err, "Error from N1QLStorageStatistics", t)
	log.Printf("Stats from Index6 StorageStatistics for index %v are %v", "index_age", stats)

	err = validateNonZeroLastResetTimeWithBaseTime(stats, beforeTime)
	FailTestIfError(err, "Error in TestStats_DefnStorageStatistics", t)

	beforeTime = time.Now().UnixMilli()
	err = secondaryindex.CreateSecondaryIndex("p1", bucketName, indexManagementAddress, "", nil, true, nil, false, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating primary index", t)

	stats, err = secondaryindex.N1QLDefnStorageStatistics("p1", bucketName, indexScanAddress)
	FailTestIfError(err, "Error from N1QLStorageStatistics", t)
	log.Printf("Stats from Index6 StorageStatistics for index %v are %v", "p1", stats)

	err = validateNonZeroLastResetTimeWithBaseTime(stats, beforeTime)
	FailTestIfError(err, "Error in TestStats_DefnStorageStatistics", t)

	getProcessPID := func(processCmd string) []int {
		pids := make([]int, 0, 4)
		procs, err := os.ReadDir("/proc")
		if err != nil {
			return pids
		}

		for _, proc := range procs {
			if !proc.IsDir() {
				continue
			}

			pid, err := strconv.Atoi(proc.Name())
			if err != nil {
				continue
			}

			cmdline, err := os.ReadFile(filepath.Join("/proc", proc.Name(), "cmdline"))
			if err != nil {
				continue
			}

			if strings.Contains(string(cmdline), processCmd) {
				indexerCmd := filepath.Base(string(cmdline))
				log.Printf("found process with %v in with pid %v cmd - '%v'", processCmd, pid, indexerCmd)
				pids = append(pids, pid)
			}
		}

		return pids
	}

	newBeforeTime := time.Now().UnixMilli()
	pids := getProcessPID("/indexer") // always search for /indexer else we will kill ns_server too
	if len(pids) == 0 {
		t.Fatalf("indexer process not found")
	}
	for _, pid := range pids {
		err = syscall.Kill(pid, syscall.SIGABRT)
		FailTestIfError(err, "failed to kill indexer process", t)
		log.Printf("%v killed pid %v", t.Name(), pid)
	}

	time.Sleep(10 * time.Second)

	rh := c.NewRetryHelper(10, 10*time.Millisecond, 2, func(a int, err error) error {
		log.Printf("Attempt %v: Check if stats are valid", a)
		stats, err = secondaryindex.N1QLDefnStorageStatistics("index_age", bucketName, indexScanAddress)
		log.Printf("Stats from Index6 StorageStatistics for index %v are %v", "index_age", stats)

		err = validateNonZeroLastResetTimeWithBaseTime(stats, newBeforeTime)
		if err != nil {
			log.Printf("Stats for index %v are invalid. err - %v", "index_age", err)
			return err
		}

		stats, err = secondaryindex.N1QLDefnStorageStatistics("p1", bucketName, indexScanAddress)
		log.Printf("Stats from Index6 StorageStatistics for index %v are %v", "p1", stats)

		err = validateNonZeroLastResetTimeWithBaseTime(stats, beforeTime)
		if err != nil {
			log.Printf("Stats for index %v are invalid. err - %v", "p1", err)
			return err
		}
		log.Printf("Stats are valid")
		return nil
	})

	err = rh.Run()
	FailTestIfError(err, "Error from N1QLStorageStatistics", t)
}
