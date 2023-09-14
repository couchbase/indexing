package functionaltests

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func getIndexStatusFromIndexer() (*tc.IndexStatusResponse, error) {
	url, err := makeurl("/getIndexStatus")
	if err != nil {
		return nil, err
	}

	var resp *http.Response
	resp, err = http.Get(url)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var st tc.IndexStatusResponse
	err = json.Unmarshal(respbody, &st)
	if err != nil {
		return nil, err
	}

	return &st, nil
}

func getShardGroupingFromLiveCluster() (tc.AlternateShardMap, error) {
	statuses, err := getIndexStatusFromIndexer()
	if err != nil {
		return nil, err
	}

	shardGrouping := make(tc.AlternateShardMap)
	for _, status := range statuses.Status {
		var replicaMap map[int]map[c.PartitionId][]string
		var partnMap map[c.PartitionId][]string

		var ok bool

		if defnStruct, ok := shardGrouping[status.DefnId]; !ok {
			replicaMap = make(map[int]map[c.PartitionId][]string)
			shardGrouping[status.DefnId] = &struct {
				Name         string
				NumReplica   int
				NumPartition int
				IsPrimary    bool
				ReplicaMap   map[int]map[c.PartitionId][]string
			}{
				Name:         status.Name,
				NumReplica:   status.NumReplica,
				NumPartition: status.NumPartition,
				IsPrimary:    status.IsPrimary,
				ReplicaMap:   replicaMap,
			}
		} else {
			replicaMap = defnStruct.ReplicaMap
		}

		if partnMap, ok = replicaMap[status.ReplicaId]; !ok {
			partnMap = make(map[c.PartitionId][]string)
			replicaMap[status.ReplicaId] = partnMap
		}

		for _, partShardMap := range status.AlternateShardIds {
			for partnId, shards := range partShardMap {
				partnMap[c.PartitionId(partnId)] = shards
			}
		}

	}

	// adjust partition count for replica indices (NumPartn will be actual value * index count)
	// for _, defnStruct := range shardGrouping {
	// 	defnStruct.NumPartition /= 1 + defnStruct.NumReplica
	// }
	return shardGrouping, nil
}

func performClusterStateValidation(t *testing.T, validations ...tc.InvalidClusterState) {
	shardGrouping, err := getShardGroupingFromLiveCluster()
	tc.HandleError(err, "Err in getting Index Status from live cluster")

	errMap := tc.ValidateClusterState(shardGrouping, len(validations) != 0)
	errStr := strings.Builder{}
	if len(validations) == 0 && len(errMap) != 0 {
		for violation, errs := range errMap {
			errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: %v\n", violation, errs))
		}
	} else if len(validations) > 0 && len(errMap) > 0 {
		for _, validation := range validations {
			if errs, ok := errMap[validation]; ok {
				errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: %v\n", validation, errs))
			}
		}
	}
	if errStr.Len() > 0 {
		t.Fatalf("%v:performClusterStateValidation validations failed - \n%v", t.Name(), errStr.String())
	}
}

func TestWithShardAffinity(t *testing.T) {

	if clusterconfig.IndexUsing != "plasma" {
		t.Skipf("Shard affinity tests only valid with plasma storage")
		return
	}

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enable_shard_affinity`")
	err = secondaryindex.ChangeIndexerSettings("indexer.planner.honourNodesInDefn", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Unable to change indexer setting `indexer.planner.honourNodesInDefn`")

	defer func() {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enableShardAffinity`")

		err = secondaryindex.ChangeIndexerSettings("indexer.planner.honourNodesInDefn", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.planner.honourNodesInDefn`")
	}()

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
	})
	defer t.Run("RebalanceResetCluster", func(subt *testing.T) {
		TestRebalanceResetCluster(subt)
	})

	t.Run("TestCreateDocsBeforeRebalance", func(subt *testing.T) {
		TestCreateDocsBeforeRebalance(subt)
	})

	t.Run("TestCreateIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateIndexesBeforeRebalance(subt)
	})

	t.Run("TestShardAffinityInInitialCluster", func(subt *testing.T) {
		performClusterStateValidation(subt)
	})

	t.Run("TestIndexNodeRebalanceIn", func(subt *testing.T) {
		TestIndexNodeRebalanceIn(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestCreateReplicatedIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateReplicatedIndexesBeforeRebalance(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(t)

		performClusterStateValidation(subt)
	})

	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		TestRebalanceReplicaRepair(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestFailureAndRebalanceDuringInitialIndexBuild", func(subt *testing.T) {
		TestFailureAndRebalanceDuringInitialIndexBuild(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestRedistributWhenNodeIsAddedForFalse", func(subt *testing.T) {
		TestRedistributeWhenNodeIsAddedForFalse(subt)

		performClusterStateValidation(subt)
	})

	t.Run("TestRedistributeWhenNodeInAddedForTrue", func(subt *testing.T) {
		TestRedistributeWhenNodeIsAddedForTrue(subt)

		performClusterStateValidation(subt)
	})

}
