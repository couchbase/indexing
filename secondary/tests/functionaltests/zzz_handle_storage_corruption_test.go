package functionaltests

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

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

func verifyDeletedPath(Pth string) error {
	var err error
	_, errStat := os.Stat(Pth)
	if errStat == nil {
		err = errors.New("os.Stat was successful for deleted directory")
		return err
	}

	if !os.IsNotExist(errStat) {
		errMsg := fmt.Sprintf("errStat = %v", errStat)
		err = errors.New("Directory may exists even if it was expected to be deleted." + errMsg)
		return err
	}

	return nil
}

func forceKillIndexer() {
	// restart the indexer
	fmt.Println("Restarting indexer process ...")
	tc.KillIndexer()
	time.Sleep(30 * time.Second)
}

/*func TestIdxCorruptBasicSanityMultipleIndices(t *testing.T) {
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

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir")
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

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
}*/

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

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir")
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	numPartnsIf, errGetNumPartn := tc.GetIndexerSetting(hosts[0], "indexer.numPartitions")
	FailTestIfError(errGetNumPartn, "Error in errGetNumPartn", t)

	fmt.Println("indexer.numPartitions =", numPartnsIf)

	numPartnsStr := fmt.Sprintf("%v", numPartnsIf)
	numPartns, err := strconv.Atoi(numPartnsStr)
	FailTestIfError(err, "Error while Atoi", t)

	if int(numPartns) != 16 {
		err = errors.New("Test case not implemeted for numPartns != 16")
		FailTestIfError(err, "Error : Unsupported number of partitions", t)
	}

	// Get slice paths for all partitions
	slicePaths := make(map[int]string)
	partnToCorrupt := c.PartitionId(10)
	for i := 1; i <= 16; i++ {
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
	for i := 1; i <= 16; i++ {
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
	for i := 1; i <= 16; i++ {
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

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir")
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

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

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir")
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

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
	infos, err := tc.GetMemDBSnapshots(slicePath)
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
