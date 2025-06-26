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

	TestWithShardAffinity(t)
	TestRebalancePseudoOfflineUgradeWithShardAffinity(t)
	TestCreateInSimulatedMixedMode(t)
	TestSwapRebalanceMixedMode(t)
	TestFailoverAndRebalanceMixedMode(t)
	TestRebalanceOutNewerNodeInMixedMode(t)
	TestReplicaRepairInMixedModeRebalance(t)
	TestShardRebalance_DropDuplicateIndexes(t)
	TestShardRebalanceSetupCluster(t)

	shouldTestWithShardDealer = oldShouldTestWithShardDealer
}
