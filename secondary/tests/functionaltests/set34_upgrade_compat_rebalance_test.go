package functionaltests

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

// set34 tests simulate a rebalance-based upgrade from ShardCompatVersion=1
// (old index path format: <bucket>_<name>_<instId>_<partnId>.index) to
// ShardCompatVersion=2 (new path format: <bucketUUID>_<instId>_<partnId>.index).
//
// Since CI cannot have old-version binaries, the
// indexer.thisNodeOnly.simulateShardCompatV1 config makes Nodes[1] behave like
// an old-version node: it creates index files at old-format paths AND reports
// ShardCompatVersion=1 in stats. The planner (always new-version) then populates
// TransferToken.InstRenameMap to rewrite paths during shard restore on the dest
// node. A successful rebalance + scan verifies the MB-71635 fix end-to-end.

const (
	set34IndexName      = "set34_idx_age"
	set34VecIndexName   = "set34_idx_sift"
	set34BhiveIndexName = "set34_idx_bhive_sift"
	// See set34CreateCoTenantIndex for what this one is for.
	set34CoTenantIndexName = "set34_idx_cotenant"
	// set34BhiveFailedIdxName  = "set34_idx_bhive_fail"
	// set34PlasmaFailedIdxName = "set34_idx_plasma_fail"
	set34Bucket = BUCKET // "default"
	set34Scope  = "_default"
	set34Coll   = "_default"
)

// set34GetStorageDir returns the abs path of the indexer storage dir on clusterNode.
func set34GetStorageDir(t *testing.T, clusterNode string) string {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(
		clusterconfig.Username, clusterconfig.Password, clusterNode)
	raw, err := tc.GetIndexerSetting(indexerAddr, "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(err, "set34GetStorageDir: GetIndexerSetting", t)
	abs, err := filepath.Abs(fmt.Sprintf("%v", raw))
	FailTestIfError(err, "set34GetStorageDir: filepath.Abs", t)
	return abs
}

// set34GetBhiveStorageDir returns the abs path of the bhive storage dir on clusterNode
// (storageDir/@bhive).
func set34GetBhiveStorageDir(t *testing.T, clusterNode string) string {
	return filepath.Join(set34GetStorageDir(t, clusterNode), c.BHIVE_DIR_PREFIX)
}

// set34WaitForIndexError polls clusterNode's /getIndexStatus until idxName reports
// Status=="Error", which indicates a training failure. Fails the test on timeout.
func set34WaitForIndexError(t *testing.T, caller, clusterNode, idxName string) {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(
		clusterconfig.Username, clusterconfig.Password, clusterNode)
	deadline := time.Now().Add(time.Duration(defaultIndexActiveTimeout) * time.Second)
	for time.Now().Before(deadline) {
		resp, err := tc.GetIndexStatusResponse(indexerAddr,
			clusterconfig.Username, clusterconfig.Password)
		if err == nil {
			for _, s := range resp.Status {
				if s.IndexName == idxName && s.Bucket == set34Bucket && s.Status == "Error" {
					log.Printf("%v index %v reached error state (training failure): %v",
						caller, idxName, s.Error)
					return
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%v index %v did not reach error state within timeout", caller, idxName)
}

// set34ValidateOldFormat verifies that every index in names has an old-format
// (<bucket>_<name>_<instId>_<partnId>.index) directory in the storage dir on clusterNode.
func set34ValidateOldFormat(t *testing.T, caller, clusterNode string, names []string) {
	storageDir := set34GetStorageDir(t, clusterNode)
	for _, name := range names {
		path, err := tc.GetIndexSlicePath(name, set34Bucket, storageDir, 0)
		if err != nil {
			t.Fatalf("%v old-format path missing for %v in %v: %v", caller, name, storageDir, err)
		}
		log.Printf("%v old-format path confirmed for %v: %v", caller, name, path)
	}
}

// set34ValidateNewFormat verifies that every index in names has at least one
// instance with a new-format (<bucketUUID>_<instId>_<partnId>.index) directory on
// clusterNode, and that the old-format directory is absent on that node.
//
// It uses PartitionMap from /getIndexStatus so it only inspects partitions that
// actually reside on clusterNode, which is correct both for swap-rebalance (all
// instances on the new node) and replica-repair (one of two instances on the new
// node).
func set34ValidateNewFormat(t *testing.T, caller, clusterNode string, names []string) {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(
		clusterconfig.Username, clusterconfig.Password, clusterNode)
	storageDir := set34GetStorageDir(t, clusterNode)

	bucketUUID, err := tc.GetBucketUUID(indexManagementAddress,
		clusterconfig.Username, clusterconfig.Password, set34Bucket)
	FailTestIfError(err, caller+" GetBucketUUID", t)

	resp, err := tc.GetIndexStatusResponse(indexerAddr,
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(err, caller+" GetIndexStatusResponse", t)

	for _, name := range names {
		var validated bool
		for _, status := range resp.Status {
			if status.IndexName != name || status.Bucket != set34Bucket {
				continue
			}
			// PartitionMap is keyed by the host string that also appears in Hosts
			// (same KV/REST-port format as clusterconfig.Nodes[N]).
			partnIds := status.PartitionMap[clusterNode]
			if len(partnIds) == 0 {
				continue
			}
			for _, partnId := range partnIds {
				pid := c.PartitionId(partnId)
				var newPath string
				var err error
				if clusterconfig.IndexUsing == "forestdb" {
					newPath, err = tc.GetIndexSlicePath(name, set34Bucket, storageDir, pid)
				} else {
					newPath, err = tc.GetIndexSlicePath2(bucketUUID, status.InstId, storageDir, pid)
				}
				if err != nil {
					t.Fatalf("%v new-format path missing for %v instId=%v partnId=%v: %v",
						caller, name, status.InstId, pid, err)
				}
				log.Printf("%v new-format path confirmed for %v instId=%v partnId=%v: %v",
					caller, name, status.InstId, pid, newPath)

				if clusterconfig.IndexUsing != "forestdb" {
					// Old-format must be absent on the destination node.
					oldPath, _ := tc.GetIndexSlicePath(name, set34Bucket, storageDir, pid)
					if oldPath != "" {
						t.Fatalf("%v old-format path unexpectedly present on dest node for %v: %v",
							caller, name, oldPath)
					}
				}
				validated = true
			}
		}
		if !validated {
			t.Fatalf("%v no instance of %v found on node %v", caller, name, clusterNode)
		}
	}
}

// set34ValidateOldFormatBhive verifies that every bhive index in names has an old-format
// directory under the @bhive storage subdir on clusterNode.
func set34ValidateOldFormatBhive(t *testing.T, caller, clusterNode string, names []string) {
	if len(names) == 0 {
		return
	}
	storageDir := set34GetBhiveStorageDir(t, clusterNode)
	for _, name := range names {
		path, err := tc.GetIndexSlicePath(name, set34Bucket, storageDir, 0)
		if err != nil {
			t.Fatalf("%v old-format bhive path missing for %v in %v: %v",
				caller, name, storageDir, err)
		}
		log.Printf("%v old-format bhive path confirmed for %v: %v", caller, name, path)
	}
}

// set34ValidateNewFormatBhive verifies that every bhive index in names has at least one
// instance with a new-format directory under the @bhive storage subdir on clusterNode,
// and that no old-format directory for that index exists there.
func set34ValidateNewFormatBhive(t *testing.T, caller, clusterNode string, names []string) {
	if len(names) == 0 {
		return
	}
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(
		clusterconfig.Username, clusterconfig.Password, clusterNode)
	storageDir := set34GetBhiveStorageDir(t, clusterNode)

	bucketUUID, err := tc.GetBucketUUID(indexManagementAddress,
		clusterconfig.Username, clusterconfig.Password, set34Bucket)
	FailTestIfError(err, caller+" GetBucketUUID (bhive)", t)

	resp, err := tc.GetIndexStatusResponse(indexerAddr,
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(err, caller+" GetIndexStatusResponse (bhive)", t)

	for _, name := range names {
		var validated bool
		for _, status := range resp.Status {
			if status.IndexName != name || status.Bucket != set34Bucket {
				continue
			}
			partnIds := status.PartitionMap[clusterNode]
			if len(partnIds) == 0 {
				continue
			}
			for _, partnId := range partnIds {
				pid := c.PartitionId(partnId)
				newPath, err := tc.GetIndexSlicePath2(bucketUUID, status.InstId, storageDir, pid)
				if err != nil {
					t.Fatalf("%v new-format bhive path missing for %v instId=%v partnId=%v: %v",
						caller, name, status.InstId, pid, err)
				}
				log.Printf("%v new-format bhive path confirmed for %v instId=%v partnId=%v: %v",
					caller, name, status.InstId, pid, newPath)

				oldPath, _ := tc.GetIndexSlicePath(name, set34Bucket, storageDir, pid)
				if oldPath != "" {
					t.Fatalf("%v old-format bhive path unexpectedly present on dest node for %v: %v",
						caller, name, oldPath)
				}
				validated = true
			}
		}
		if !validated {
			t.Fatalf("%v no bhive instance of %v found on node %v", caller, name, clusterNode)
		}
	}
}

// set34ConfigCompatV1 applies the compat-v1 simulation settings to clusterNode:
// enables shard affinity, sets simulateShardCompatV1=true, and optionally
// disables the shard dealer when shouldTestWithShardDealer is false.
func set34ConfigCompatV1(t *testing.T, caller, clusterNode string) {
	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":     true,
		"indexer.planner.honourNodesInDefn":          true,
		"indexer.thisNodeOnly.simulateShardCompatV1": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}
	err := secondaryindex.ChangeMultipleIndexerSettings(configChanges,
		clusterconfig.Username, clusterconfig.Password, clusterNode)
	FailTestIfError(err, fmt.Sprintf("%v Error applying compat-v1 config on %v", caller, clusterNode), t)
	log.Printf("%v simulateShardCompatV1=true on %v", caller, clusterNode)
}

// set34CreateIndexes creates the scalar (and optionally vector/bhive) set34 indexes.
// withReplica=true gives each index one replica; the scalar index gets its replica by
// expansion rather than at create time, for the placement reason described below.
func set34CreateIndexes(t *testing.T, caller string, withReplica bool) {
	if withReplica {
		// Create pinned and unreplicated, then expand. A pinned unreplicated create
		// puts index replica 0 on Nodes[1], and the shard dealer stamps the slot
		// replica id from the index replica id, so slot replica 0 lands on Nodes[1]
		// too. The expansion then places replica 1 on the only other index node.
		//
		// The order matters for the co-tenant in
		// TestPathUpgrade_ReplicaRepairCompatV1ToV2: reusing a slot requires the slot
		// replica id on the target node to equal the index replica id, so an
		// unreplicated co-tenant pinned to Nodes[1] can only join a slot whose
		// replica 0 is there. Creating both replicas at once lets the planner put
		// replica 0 on either node, which would send the co-tenant to a slot of its
		// own and stop the case covering anything.
		scalarStmt := fmt.Sprintf(
			`CREATE INDEX %v ON `+"`%v`.`%v`.`%v`"+`(age) WITH {"nodes":["%v"]}`,
			set34IndexName, set34Bucket, set34Scope, set34Coll, clusterconfig.Nodes[1],
		)
		executeN1qlStmt(scalarStmt, set34Bucket, caller, t)
		waitForIndexActiveWithTimeout(set34Bucket, set34IndexName, 15*time.Minute, t)

		// set34 indexes live on the default scope and collection, so the two part
		// bucket.index form resolves them.
		alterStmt := fmt.Sprintf(
			"alter index `%v`.%v with {\"action\":\"replica_count\", \"num_replica\":1}",
			set34Bucket, set34IndexName,
		)
		executeN1qlStmt(alterStmt, set34Bucket, caller, t)
		waitForIndexActiveWithTimeout(set34Bucket, fmt.Sprintf("%v (replica 1)", set34IndexName), 15*time.Minute, t)
		log.Printf("%v Scalar index %v created on %v with slot replica 0, then expanded to num_replica:1",
			caller, set34IndexName, clusterconfig.Nodes[1])
	} else {
		scalarStmt := fmt.Sprintf(
			`CREATE INDEX %v ON `+"`%v`.`%v`.`%v`"+`(age) WITH {}`,
			set34IndexName, set34Bucket, set34Scope, set34Coll,
		)
		executeN1qlStmt(scalarStmt, set34Bucket, caller, t)
		log.Printf("%v Scalar index %v created (no replica)", caller, set34IndexName)
	}

	if clusterconfig.IndexUsing == "plasma" {
		if err := loadVectorData(t, set34Bucket, set34Scope, set34Coll, 10000); err != nil {
			tc.HandleError(err, fmt.Sprintf("%v Error loading vector data", caller))
		}

		// Plasma composite vector index (IVF256) — trains successfully.
		vecWith := `"dimension":128,"description":"IVF256,PQ32x8","similarity":"L2_SQUARED","defer_build":true`
		if withReplica {
			vecWith += `,"num_replica":1`
		}
		vecStmt := fmt.Sprintf(
			`CREATE INDEX %v ON `+"`%v`.`%v`.`%v`"+`(sift VECTOR) WITH {%v}`,
			set34VecIndexName, set34Bucket, set34Scope, set34Coll, vecWith,
		)

		res, err := execN1QL(set34Bucket, vecStmt)
		FailTestIfError(err, fmt.Sprintf("%v Error creating vector index %v", caller, set34VecIndexName), t)
		log.Printf("%v Vector index %v created (replica=%v) res %v", caller, set34VecIndexName, withReplica, res)

		err = issueBuildStatement(set34Bucket, set34Scope, set34Coll, []string{set34VecIndexName})
		FailTestIfError(err, "Error in building vector indexes", t)

		var wg sync.WaitGroup

		idxes := []string{set34VecIndexName}
		if withReplica {
			idxes = append(idxes, fmt.Sprintf("%v (replica 1)", set34VecIndexName))
		}

		for _, name := range idxes {
			wg.Add(1)
			go func(name string) {
				defer func() {
					e := recover()
					wg.Done()
					if e != nil {
						t.Errorf("%v", e)
					}
				}()
				waitForIndexActiveWithTimeout(set34Bucket, name, 15*time.Minute, t)
			}(name)
		}

		wg.Wait()

		log.Printf("%v Done with building valid vector indexes %v", caller, set34VecIndexName)

		// Bhive vector index (IVF256) — trains successfully.
		bhiveWith := `"dimension":128,"description":"IVF256,PQ32x8","similarity":"L2_SQUARED","defer_build":true`
		if withReplica {
			bhiveWith += `,"num_replica":1`
		}
		bhiveStmt := fmt.Sprintf(
			`CREATE VECTOR INDEX %v ON `+"`%v`.`%v`.`%v`"+`(sift VECTOR) WITH {%v}`,
			set34BhiveIndexName, set34Bucket, set34Scope, set34Coll, bhiveWith,
		)

		res, err = execN1QL(set34Bucket, bhiveStmt)
		FailTestIfError(err, fmt.Sprintf("%v Error creating bhive index %v", caller, set34BhiveIndexName), t)
		log.Printf("%v Bhive index %v created (replica=%v) with res %v", caller, set34BhiveIndexName, withReplica, res)

		err = issueBuildStatement(set34Bucket, set34Scope, set34Coll, []string{set34BhiveIndexName})
		FailTestIfError(err, "Error in building vector indexes", t)

		idxes = []string{set34BhiveIndexName}
		if withReplica {
			idxes = append(idxes, fmt.Sprintf("%v (replica 1)", set34BhiveIndexName))
		}

		for _, name := range idxes {
			wg.Add(1)
			go func(name string) {
				defer func() {
					e := recover()
					wg.Done()
					if e != nil {
						t.Errorf("%v", e)
					}
				}()
				waitForIndexActiveWithTimeout(set34Bucket, name, 15*time.Minute, t)
			}(name)
		}

		wg.Wait()

		log.Printf("%v Done with building valid vector indexes %v", caller, set34BhiveIndexName)

		// *********** DISABLED DUE TO UNSTABLITY IN TESTS ****************************
		// // Bhive vector index with intentionally failed training (IVF100000 > 10000 docs).
		// bhiveFailWith := `"dimension":128,"description":"IVF100000,PQ32x8","similarity":"L2_SQUARED","defer_build":true`
		// if withReplica {
		// 	bhiveFailWith += `,"num_replica":1`
		// }
		// bhiveFailStmt := fmt.Sprintf(
		// 	`CREATE VECTOR INDEX %v ON `+"`%v`.`%v`.`%v`"+`(sift VECTOR) WITH {%v}`,
		// 	set34BhiveFailedIdxName, set34Bucket, set34Scope, set34Coll, bhiveFailWith,
		// )
		// _, err = execN1QL(set34Bucket, bhiveFailStmt)
		// FailTestIfError(err, fmt.Sprintf("%v Error creating bhive failed-training index %v", caller, set34BhiveFailedIdxName), t)
		// issueBuildStatement(set34Bucket, set34Scope, set34Coll, []string{set34BhiveFailedIdxName})
		// set34WaitForIndexError(t, caller, clusterconfig.Nodes[1], set34BhiveFailedIdxName)
		// log.Printf("%v Bhive failed-training index %v in error state (replica=%v)", caller, set34BhiveFailedIdxName, withReplica)

		// // Plasma composite vector index with intentionally failed training (IVF100000 > 10000 docs).
		// plasmaFailWith := `"dimension":128,"description":"IVF100000,PQ32x8","similarity":"L2_SQUARED","defer_build":true`
		// if withReplica {
		// 	plasmaFailWith += `,"num_replica":1`
		// }
		// plasmaFailStmt := fmt.Sprintf(
		// 	`CREATE INDEX %v ON `+"`%v`.`%v`.`%v`"+`(sift VECTOR) WITH {%v}`,
		// 	set34PlasmaFailedIdxName, set34Bucket, set34Scope, set34Coll, plasmaFailWith,
		// )
		// _, err = execN1QL(set34Bucket, plasmaFailStmt)
		// FailTestIfError(err, fmt.Sprintf("%v Error creating plasma failed-training index %v", caller, set34PlasmaFailedIdxName), t)
		// issueBuildStatement(set34Bucket, set34Scope, set34Coll, []string{set34PlasmaFailedIdxName})
		// set34WaitForIndexError(t, caller, clusterconfig.Nodes[1], set34PlasmaFailedIdxName)
		// log.Printf("%v Plasma failed-training index %v in error state (replica=%v)", caller, set34PlasmaFailedIdxName, withReplica)

		// clearCreateComandTokens()
		// clearBuildTokens()

		// log.Printf("%v sleeping to clear tokens across indexers", caller)
		// time.Sleep(10 * time.Second)
	}
}

// set34CreateCoTenantIndex creates a single-instance index pinned to clusterNode.
//
// Replica repair moves only the instances whose replica is missing but copies the
// shard whole, so this index arrives on the destination under its original v1 name
// with no InstRenameMap entry. Plasma recovers it anyway and asks for its key by
// path: that co-tenant instance is what MB-73417 hangs on.
//
// Needs at least planner.internal.min_shards_per_node (6) shards already on
// clusterNode, else GetSlot pass 0 opens a fresh slot rather than reusing one and the
// co-tenant lands in a shard of its own. The three non-primary set34 indexes supply
// exactly 6 (mainstore plus backstore each), so shrinking that set breaks this.
func set34CreateCoTenantIndex(t *testing.T, caller, clusterNode string) {
	stmt := fmt.Sprintf(
		`CREATE INDEX %v ON `+"`%v`.`%v`.`%v`"+`(company) WITH {"nodes":["%v"]}`,
		set34CoTenantIndexName, set34Bucket, set34Scope, set34Coll, clusterNode,
	)
	executeN1qlStmt(stmt, set34Bucket, caller, t)
	log.Printf("%v Co-tenant index %v created on %v (no replica, no token will cover it)",
		caller, set34CoTenantIndexName, clusterNode)
}

// set34DropIndexes drops all set34 indexes.
func set34DropIndexes(t *testing.T, caller string) {
	err := secondaryindex.DropSecondaryIndex(set34IndexName, set34Bucket, indexManagementAddress)
	FailTestIfError(err, fmt.Sprintf("%v Error dropping %v", caller, set34IndexName), t)
	if clusterconfig.IndexUsing == "plasma" {
		for _, name := range []string{
			set34VecIndexName,
			set34BhiveIndexName,
			// set34BhiveFailedIdxName,
			// set34PlasmaFailedIdxName,
		} {
			err = secondaryindex.DropSecondaryIndex(name, set34Bucket, indexManagementAddress)
			FailTestIfError(err, fmt.Sprintf("%v Error dropping %v", caller, name), t)
		}
	}
}

// set34PlasmaIndexesToCheck returns plasma (non-bhive) index names: the scalar index
// and, for plasma storage, the active plasma vector index and the plasma failed-training index.
func set34PlasmaIndexesToCheck() []string {
	names := []string{set34IndexName}
	if clusterconfig.IndexUsing == "plasma" {
		names = append(names, set34VecIndexName)
	}
	return names
}

// set34BhiveIndexesToCheck returns bhive index names for plasma storage: the active
// bhive vector index and the bhive failed-training index.
func set34BhiveIndexesToCheck() []string {
	if clusterconfig.IndexUsing != "plasma" {
		return nil
	}
	return []string{set34BhiveIndexName}
}

// set34ScanIndexes verifies the set34 indexes are queryable after a rebalance.
func set34ScanIndexes(t *testing.T, caller string) {
	scanResults, err := secondaryindex.ScanAll(set34IndexName, set34Bucket, indexScanAddress,
		defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, fmt.Sprintf("%v ScanAll failed", caller), t)
	log.Printf("%v Scalar scan returned %v rows", caller, len(scanResults))

	if clusterconfig.IndexUsing == "plasma" {
		var sb strings.Builder
		sb.WriteByte('[')
		for i, val := range indexVector.QueryVector {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "%v", val)
		}
		sb.WriteByte(']')
		annStmt := fmt.Sprintf(
			`with qvec as (%v) select meta().id from `+"`%v`"+
				` ORDER BY APPROX_VECTOR_DISTANCE(sift, qvec, "L2_SQUARED", %v) limit 5`,
			sb.String(), set34Bucket, indexVector.Probes)
		annResults, err := execN1QL(set34Bucket, annStmt)
		FailTestIfError(err, fmt.Sprintf("%v ANN scan failed", caller), t)
		log.Printf("%v ANN scan returned %v rows", caller, len(annResults))
	}
}

// ---------------------------------------------------------------------------
// Test set A: swap rebalance (compat-v1 node replaced by compat-v2 node)
// ---------------------------------------------------------------------------

// TestPathUpgrade_RebalanceSetup resets the cluster to Nodes[1] as the sole
// index node and enables simulateShardCompatV1 on Nodes[1], making it behave
// like an old-version node for the duration of this test set.
//
//	Starting config: any
//	Ending config:   [0: kv n1ql] [1: index]
func TestPathUpgrade_RebalanceSetup(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_RebalanceSetup:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	setupCluster(t) // resets to [0: kv n1ql] [1: index]
	set34ConfigCompatV1(t, caller, clusterconfig.Nodes[1])

	printClusterConfig(caller, "exit")
}

// TestPathUpgrade_CreateIndexOnCompatV1Node creates a scalar and (on plasma) vector index
// while Nodes[1] has simulateShardCompatV1=true. Confirms old-format paths on
// Nodes[1] before the swap rebalance.
//
//	Starting config: [0: kv n1ql] [1: index]
//	Ending config:   same
func TestPathUpgrade_CreateIndexOnCompatV1Node(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_CreateIndexOnCompatV1Node:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	set34CreateIndexes(t, caller, false /* no replica */)
	set34ValidateOldFormat(t, caller, clusterconfig.Nodes[1], set34PlasmaIndexesToCheck())
	set34ValidateOldFormatBhive(t, caller, clusterconfig.Nodes[1], set34BhiveIndexesToCheck())

	printClusterConfig(caller, "exit")
}

// TestPathUpgrade_SwapRebalanceCompatV1ToV2 adds Nodes[2] (new-version) and rebalances out
// Nodes[1] (simulated old-version). The planner detects the ShardCompatVersion
// mismatch (1→2) and populates InstRenameMap in the transfer token so the shard
// restore step rewrites old-format paths to new-format before recovery.
// Scan and path validation after rebalance verify the MB-71635 fix end-to-end.
//
//	Starting config: [0: kv n1ql] [1: index]
//	Ending config:   [0: kv n1ql] [2: index]
func TestPathUpgrade_SwapRebalanceCompatV1ToV2(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_SwapRebalanceCompatV1ToV2:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	swapRebalance(t, 2, 1)

	expectedStatus := map[string][]string{
		clusterconfig.Nodes[0]: {"kv", "n1ql"},
		clusterconfig.Nodes[2]: {"index"},
	}
	validateClusterStatus(expectedStatus, caller, t)
	waitForRebalanceCleanup()

	set34ScanIndexes(t, caller)
	set34ValidateNewFormat(t, caller, clusterconfig.Nodes[2], set34PlasmaIndexesToCheck())
	set34ValidateNewFormatBhive(t, caller, clusterconfig.Nodes[2], set34BhiveIndexesToCheck())

	printClusterConfig(caller, "exit")
}

// ---------------------------------------------------------------------------
// Test set B: replica repair (compat-v1 replica source → compat-v2 dest)
// ---------------------------------------------------------------------------
//
// Flow:
//   1. Setup: [0:kv][1:index(v1)] + [2:index(v2)] with num_replica:1 indexes.
//      Each index has one instance on Nodes[1] (old-format) and one on
//      Nodes[2] (new-format).
//   2. Failover Nodes[2] + rebalance → [0:kv][1:index(v1)], replica lost.
//   3. Add Nodes[3] (v2) + rebalance → planner repairs the missing replica on
//      Nodes[3] via shard transfer from Nodes[1] (compat-v1 source). The
//      transfer token carries InstRenameMap to rename old→new format paths.
//   4. Validate Nodes[1] still has old-format; Nodes[3] has new-format.

// TestPathUpgrade_ReplicaRepairCompatV1ToV2Setup sets up a 2-node cluster with Nodes[1]
// (compat-v1) and Nodes[2] (compat-v2), creates indexes with num_replica:1,
// and validates that old-format paths are present on Nodes[1] and new-format
// paths on Nodes[2].
//
//	Starting config: [0: kv n1ql] [2: index]
//	Ending config:   [0: kv n1ql] [1: index] [2: index]
func TestPathUpgrade_ReplicaRepairCompatV1ToV2Setup(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_ReplicaRepairCompatV1ToV2Setup:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	err := secondaryindex.DropAllSecondaryIndexes(clusterconfig.Nodes[2])
	tc.HandleError(err, "failed to drop indexes")

	addNodeAndRebalance(clusterconfig.Nodes[1], "index", t)
	set34ConfigCompatV1(t, caller, clusterconfig.Nodes[1])

	expectedStatus := map[string][]string{
		clusterconfig.Nodes[0]: {"kv", "n1ql"},
		clusterconfig.Nodes[1]: {"index"},
		clusterconfig.Nodes[2]: {"index"},
	}
	validateClusterStatus(expectedStatus, caller, t)

	// The co-tenant case is plasma only: CanMaintanShardAffinity requires plasma, so
	// on forestdb and memory_optimized replica repair runs over DCP, no shard is
	// copied, and nothing can carry a co-tenant to the new node.
	if clusterconfig.IndexUsing == "plasma" {
		// Set here, ahead of the creates below, so this case builds its indexes and its
		// co-tenant into shared shards: a slice decides shared vs dedicated when it is
		// built. set34 indexes live on _default._default, where IndexOnCollection() is
		// false and each slice would get a dedicated LSS, and shared LSS is what makes
		// a shard recover all of its instances in one pass.
		err = secondaryindex.ChangeMultipleIndexerSettings(
			map[string]interface{}{"indexer.plasma.useSharedLSS": true},
			clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1],
		)
		FailTestIfError(err, fmt.Sprintf("%v Error enabling plasma.useSharedLSS", caller), t)
		log.Printf("%v Enabled indexer.plasma.useSharedLSS", caller)
	}

	// Create indexes with one replica each. With two index nodes that puts one
	// instance on Nodes[1] (compat-v1 → old-format) and one on Nodes[2]
	// (compat-v2 → new-format).
	set34CreateIndexes(t, caller, true /* withReplica */)

	if clusterconfig.IndexUsing == "plasma" {
		// Rides along in the shard copy that replica repair makes, uncovered by any token.
		set34CreateCoTenantIndex(t, caller, clusterconfig.Nodes[1])
	}

	// Confirm the expected on-disk path formats before failover.
	set34ValidateOldFormat(t, caller, clusterconfig.Nodes[1], set34PlasmaIndexesToCheck())
	set34ValidateNewFormat(t, caller, clusterconfig.Nodes[2], set34PlasmaIndexesToCheck())
	set34ValidateOldFormatBhive(t, caller, clusterconfig.Nodes[1], set34BhiveIndexesToCheck())
	set34ValidateNewFormatBhive(t, caller, clusterconfig.Nodes[2], set34BhiveIndexesToCheck())

	printClusterConfig(caller, "exit")
}

// TestPathUpgrade_ReplicaRepairCompatV1ToV2 fails over Nodes[2] to lose the compat-v2
// replica, then adds Nodes[3] and rebalances. The planner performs replica
// repair by transferring shards from Nodes[1] (compat-v1, old-format paths) to
// Nodes[3] (compat-v2). InstRenameMap in the transfer token ensures the
// restored paths use the new format. Scan and path validation confirm success.
//
//	Starting config: [0: kv n1ql] [1: index] [2: index]
//	Ending config:   [0: kv n1ql] [1: index] [3: index]
func TestPathUpgrade_ReplicaRepairCompatV1ToV2(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_ReplicaRepairCompatV1ToV2:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	// Failover Nodes[2] (compat-v2 replica holder) and rebalance it out.
	log.Printf("%v Failing over Nodes[2] (%v)", caller, clusterconfig.Nodes[2])
	failoverNode(clusterconfig.Nodes[2], t)
	rebalance(t)

	expectedAfterFailover := map[string][]string{
		clusterconfig.Nodes[0]: {"kv", "n1ql"},
		clusterconfig.Nodes[1]: {"index"},
	}
	validateClusterStatus(expectedAfterFailover, caller, t)
	waitForRebalanceCleanup()
	log.Printf("%v Nodes[2] ejected; one replica is now missing", caller)

	// Add Nodes[3] (clean compat-v2) and rebalance.  The planner repairs the
	// missing replica by transferring shards from Nodes[1] (compat-v1,
	// old-format) to Nodes[3] (compat-v2).  InstRenameMap rewrites paths.
	log.Printf("%v Adding Nodes[3] (%v) for replica repair", caller, clusterconfig.Nodes[3])
	addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)

	expectedAfterRepair := map[string][]string{
		clusterconfig.Nodes[0]: {"kv", "n1ql"},
		clusterconfig.Nodes[1]: {"index"},
		clusterconfig.Nodes[3]: {"index"},
	}
	validateClusterStatus(expectedAfterRepair, caller, t)
	waitForRebalanceCleanup()

	// Verify indexes are queryable.
	set34ScanIndexes(t, caller)

	// Reaching this far is the MB-73417 regression check: the co-tenant's v1
	// directory used to block shard recovery on Nodes[3]. Nodes[1] is the copy that
	// never moved; Nodes[3] proves the shard carried it, and it survives there
	// because plasma removes a destroyed instance's directory only when dedicated.
	// Plasma only, for the same reason the co-tenant is created only there.
	if clusterconfig.IndexUsing == "plasma" {
		set34ValidateOldFormat(t, caller, clusterconfig.Nodes[1], []string{set34CoTenantIndexName})
		set34ValidateOldFormat(t, caller, clusterconfig.Nodes[3], []string{set34CoTenantIndexName})
	}

	// Validate path formats: Nodes[1] unchanged (old-format), Nodes[3] new-format.
	set34ValidateOldFormat(t, caller, clusterconfig.Nodes[1], set34PlasmaIndexesToCheck())
	set34ValidateNewFormat(t, caller, clusterconfig.Nodes[3], set34PlasmaIndexesToCheck())
	set34ValidateOldFormatBhive(t, caller, clusterconfig.Nodes[1], set34BhiveIndexesToCheck())
	set34ValidateNewFormatBhive(t, caller, clusterconfig.Nodes[3], set34BhiveIndexesToCheck())

	printClusterConfig(caller, "exit")
}

func TestPathUpgrade_RestartPathUpgrade(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_RestartPathUpgrade:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	err := secondaryindex.ChangeMultipleIndexerSettings(
		map[string]interface{}{"indexer.thisNodeOnly.simulateShardCompatV1": false},
		clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1],
	)
	tc.HandleError(err, fmt.Sprintf("%v Error disabling simulateShardCompatV1 on Nodes[1]", caller))

	log.Printf("%v Restarting indexer to test path upgrade", caller)

	forceKillIndexer()

	err = secondaryindex.WaitForIndexerActive(
		clusterconfig.Username, clusterconfig.Password,
		clusterconfig.Nodes[1],
	)

	tc.HandleError(err, "Node 1 not active after timeout")

	log.Printf("%v verifying if path upgrades is performed after restart or not", caller)

	set34ValidateNewFormat(t, caller, clusterconfig.Nodes[1], set34PlasmaIndexesToCheck())
	set34ValidateNewFormatBhive(t, caller, clusterconfig.Nodes[1], set34BhiveIndexesToCheck())

	printClusterConfig(caller, "exit")
}

// TestReplicaRepairCompatV1ToV2Cleanup drops the indexes, disables
// simulateShardCompatV1 on Nodes[1], and resets the cluster.
//
//	Starting config: [0: kv n1ql] [1: index] [3: index]
//	Ending config:   [0: kv n1ql] [1: index]
func TestPathUpgrade_ReplicaRepairCompatV1ToV2Cleanup(t *testing.T) {
	const caller = "set34_upgrade_compat_rebalance_test.go::TestPathUpgrade_ReplicaRepairCompatV1ToV2Cleanup:"
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", caller)
		return
	}

	// Deferred so a Fatal in the drops cannot leave either setting on for the rest of
	// the run; useSharedLSS is cluster-wide. The drops must still run first, since
	// the path a slice is destroyed at depends on simulateShardCompatV1.
	defer func() {
		err := secondaryindex.ChangeMultipleIndexerSettings(
			map[string]interface{}{"indexer.thisNodeOnly.simulateShardCompatV1": false},
			clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1],
		)
		tc.HandleError(err, fmt.Sprintf("%v Error disabling simulateShardCompatV1 on Nodes[1]", caller))

		err = secondaryindex.ChangeMultipleIndexerSettings(
			map[string]interface{}{"indexer.plasma.useSharedLSS": false},
			clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1],
		)
		tc.HandleError(err, fmt.Sprintf("%v Error disabling plasma.useSharedLSS", caller))
	}()

	set34DropIndexes(t, caller)

	// Created only by the replica-repair setup, and only on plasma, so tolerate a
	// missing index here rather than logging a drop failure on every other mode.
	if clusterconfig.IndexUsing == "plasma" {
		if err := secondaryindex.DropSecondaryIndex(
			set34CoTenantIndexName, set34Bucket, indexManagementAddress); err != nil {
			log.Printf("%v Ignoring drop error for %v: %v", caller, set34CoTenantIndexName, err)
		}
	}

	setupCluster(t) // reset back to [0: kv n1ql] [1: index]
	printClusterConfig(caller, "exit")
}
