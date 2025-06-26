//go:build weekly_plasma_test
// +build weekly_plasma_test

package functionaltests

import "testing"

// TestWithShardAffinity2 is a test that runs the shard affinity tests without the shard dealer;
// This is to ensure we have not regressed from the basic 7.6 shard affinity functionality;
func TestWithShardAffinity2(t *testing.T) {
	skipShardAffinityTests(t)
	t.Log("Running shard affinity tests without shard dealer")
	var oldShouldTestWithShardDealer = shouldTestWithShardDealer

	shouldTestWithShardDealer = false

	defer func() {
		shouldTestWithShardDealer = oldShouldTestWithShardDealer
	}()

	if !t.Run("Sanity", TestWithShardAffinity) {
		return
	}

	if !t.Run("RebalancePseudoOfflineUgradeWithShardAffinity", TestRebalancePseudoOfflineUgradeWithShardAffinity) {
		return
	}

	if !t.Run("CreateInSimulatedMixedMode", TestCreateInSimulatedMixedMode) {
		return
	}

	if !t.Run("SwapRebalanceMixedMode", TestSwapRebalanceMixedMode) {
		return
	}

	if !t.Run("FailoverAndRebalanceMixedMode", TestFailoverAndRebalanceMixedMode) {
		return
	}

	if !t.Run("RebalanceOutNewerNodeInMixedMode", TestRebalanceOutNewerNodeInMixedMode) {
		return
	}

	if !t.Run("ReplicaRepairInMixedModeRebalance", TestReplicaRepairInMixedModeRebalance) {
		return
	}

	if !t.Run("ShardRebalance_DropDuplicateIndexes", TestShardRebalance_DropDuplicateIndexes) {
		return
	}

	if !t.Run("ShardRebalanceSetupCluster", TestShardRebalanceSetupCluster) {
		return
	}

}
