//go:build 2ici_test
// +build 2ici_test

package testcode

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

func TestActionAtTag(cfg common.Config, tag TestActionTag) {
	execTestAction := cfg["shardRebalance.execTestAction"].Bool()
	if !execTestAction {
		return
	}

	var option TestOptions
	// Read option from metaKV - The test is expected to persist the options
	// to metaKV before the start of the test
	_, err := common.MetakvGet(common.IndexingMetaDir+METAKV_TEST_PATH, &option)
	if err != nil {
		logging.Errorf("TestCode::TestActionAtTag: Error while retrieving options from metaKV, err: %v", err)
		return
	}

	clusterAddr := cfg["clusterAddr"].String()
	// For non-master nodes, take action only for the node specified in the action
	if !isMasterTag(option.ActionAtTag) {
		if option.ActionOnNode != clusterAddr {
			return
		}
	}

	logging.Infof("TestCode::TestActionAtTag: currTag: %v, actionAtTag: %v, actionOnNode: %v, currNode: %v, action: %v",
		tag, option.ActionAtTag, option.ActionOnNode, clusterAddr, option.Action)

	if tag == option.ActionAtTag {
		switch option.Action {
		case INDEXER_PANIC:
			panic(fmt.Errorf("TestCode::TestActionAtTag - Inducing artificial panic at tag: %v as wished", tag))
		case REBALANCE_CANCEL:
			resp, err := security.PostWithAuth(clusterAddr+"/controller/stopRebalance", "application/json", strings.NewReader(""), nil)
			if err != nil {
				logging.Errorf("TestCode::TestActionAtTag - Error observed while posting cancel message, err: %v", err)
			}
			if resp != nil {
				defer resp.Body.Close()
			}
			time.Sleep(3 * time.Second)
		case SLEEP:
			logging.Infof("TestCode::TestActionAtTag: Sleeping for %v milliseconds as wished", option.SleepTime)
			time.Sleep(time.Duration(option.SleepTime) * time.Millisecond)
			logging.Infof("TestCode::TestActionAtTag: Woke-up from sleep")

		case EXEC_N1QL_STATEMENT:
			// TODO: Add support for n1ql statement execution
		}
	}

	// No-op for other states
	return
}

func IgnoreAlternateShardIds(cfg common.Config, defn *common.IndexDefn) {
	logging.Infof("testcode::IgnoreAlternateShardIds (Before), defn.Name: %v, defnId: %v, replicaId: %v, alternateShardIds: %v",
		defn.Name, defn.DefnId, defn.ReplicaId, defn.AlternateShardIds)
	if val, ok := cfg["thisNodeOnly.ignoreAlternateShardIds"]; ok && val.Bool() {
		defn.AlternateShardIds = nil
	}
	logging.Infof("testcode::IgnoreAlternateShardIds (After), defn.Name: %v, defnId: %v, replicaId: %v, alternateShardIds: %v",
		defn.Name, defn.DefnId, defn.ReplicaId, defn.AlternateShardIds)
}

func CorruptIndex(cfg common.Config, index *common.IndexInst) {
	if val, ok := cfg["shardRebalance.corruptIndexOnRecovery"]; ok && val.Bool() && index != nil {
		storageDir := cfg["storage_dir"].String()

		if index.Defn.Using != common.PlasmaDB {
			logging.Warnf("testcode::CorruptIndex only for plasma instances but %v is of %v storage",
				index.Defn.Name, index.Defn.Using)
			return
		}
		logging.Infof("testcode::CorruptIndex corrupting index %v", index.Defn.Name)
		instId := index.InstId
		if index.IsProxy() {
			instId = index.RealInstId
		}
		for _, part := range index.Pc.GetAllPartitions() {

			indexDirName := fmt.Sprintf("%v_%v_%v_%v.index", index.Defn.Bucket, index.Defn.Name, instId, part.GetPartitionId())

			mainIndexFilePath := filepath.Join(storageDir, indexDirName, "mainIndex")

			if _, err := os.ReadDir(mainIndexFilePath); err != nil && os.IsNotExist(err) {
				// this could be a shared instance under a shard
				// we need to corrupt the shard
				shardIds := index.Defn.ShardIdsForDest[part.GetPartitionId()]
				if len(shardIds) == 0 {
					logging.Errorf("testcode:CorruptIndex no index files found and index does not have shards. cannot corrupt index %v",
						index.Defn.Name)
					return
				}

				mainIndexFilePath = filepath.Join(storageDir, "shards", fmt.Sprintf("shard%v", shardIds[0]))
			}
			// Corrupt only main index
			mainIndexErrFilePath := path.Join(mainIndexFilePath, "error")
			// log.Printf("Corrupting index %v mainIndexErrFilePath %v", indexName, mainIndexErrFilePath)
			// err = ioutil.WriteFile(mainIndexErrFilePath, []byte("Fatal error: Automation Induced Storage Corruption"), 0755)
			file, err := os.OpenFile(mainIndexErrFilePath, os.O_CREATE, 0755)
			if file != nil {
				file.WriteString("Fatal error: Automation Induced Storage Corruption")

				file.Close()

				logging.Infof("testcode::CorruptIndex corrupted index %v partition %v at %v", index.Defn.Name, part.GetPartitionId(), mainIndexFilePath)
			} else {
				logging.Errorf("testcode::CorruptIndex Failed to corrupt index %v partition %v at path %v with error %v",
					index.Defn.Name, part.GetPartitionId(), mainIndexErrFilePath, err)
			}

		}
	}
}
