// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/planner"
)

//////////////////////////////////////////////////////////////
// Unit Test
/////////////////////////////////////////////////////////////

type initialPlacementTestCase struct {
	comment        string
	memQuotaFactor float64
	cpuQuotaFactor float64
	workloadSpec   string
	indexSpec      string
	memScore       float64
	cpuScore       float64
}

type incrPlacementTestCase struct {
	comment        string
	memQuotaFactor float64
	cpuQuotaFactor float64
	plan           string
	indexSpec      string
	memScore       float64
	cpuScore       float64
}

type rebalanceTestCase struct {
	comment        string
	memQuotaFactor float64
	cpuQuotaFactor float64
	plan           string
	shuffle        int
	addNode        int
	deleteNode     int
	memScore       float64
	cpuScore       float64
}

type heterogenousRebalTestCase struct {
	comment                   string
	memQuotaFactor            float64
	cpuQuotaFactor            float64
	plan                      string
	shuffle                   int
	addNode                   int
	deleteNode                int
	checkMovement             bool
	keepNodesByNodeID         []string
	excludeValueOnDeleteNodes string
}

type excludeInTestCase struct {
	comment        string
	memQuotaFactor float64
	cpuQuotaFactor float64
	plan           string
	shuffle        int
	addNode        int
	deleteNode     int
	checkMovement  bool
}

type iterationTestCase struct {
	comment   string
	topoSpec  string
	indexers  string
	plan      string
	minIter   int
	maxIter   int
	threshold float64
	success   bool
	action    string
	idxCount  int
}

type greedyPlannerFuncTestCase struct {
	comment     string
	topology    string
	index       string
	targetNodes map[string]bool
}

type greedyPlannerIdxDistTestCase struct {
	comment         string
	topology        string
	sampleIndex     string
	numIndexes      int
	minDistVariance float64
	maxDistVariance float64
}

type partitionedIdxHeterogenousTestCase struct {
	comment       string
	topology      string
	index         string
	numPartitions int
}

type scaleupAlterIndexTestCase struct {
	comment   string
	topology  string
	defnId    common.IndexDefnId
	increment int
}

var initialPlacementTestCases = []initialPlacementTestCase{
	{"initial placement - 20-50M, 10 index, 3 replica, 2x", 2.0, 2.0, "../testdata/planner/workload/uniform-small-10-3.json", "", 0.20, 0.20},
	{"initial placement - 20-50M, 30 index, 3 replica, 2x", 2.0, 2.0, "../testdata/planner/workload/uniform-small-30-3.json", "", 0.20, 0.20},
	{"initial placement - 20-50M, 30 index, 3 replica, 4x", 4.0, 4.0, "../testdata/planner/workload/uniform-small-30-3.json", "", 0.1, 0.1},
	{"initial placement - 200-500M, 10 index, 3 replica, 2x", 2.0, 2.0, "../testdata/planner/workload/uniform-medium-10-3.json", "", 0.20, 0.20},
	{"initial placement - 200-500M, 30 index, 3 replica, 2x", 2.0, 2.0, "../testdata/planner/workload/uniform-medium-30-3.json", "", 0.20, 0.20},
	{"initial placement - mixed small/medium, 30 index, 3 replica, 1.5/4x", 1.5, 4, "../testdata/planner/workload/mixed-small-medium-30-3.json", "", 0.20, 0.20},
	{"initial placement - mixed all, 30 index, 3 replica, 1.5/4x", 1.5, 4, "../testdata/planner/workload/mixed-all-30-3.json", "", 0.25, 0.25},
	{"initial placement - 6 2M index, 1 replica, 2x", 2, 2, "", "../testdata/planner/index/small-2M-6-1.json", 0, 0},
	{"initial placement - 5 20M primary index, 2 replica, 2x", 2, 2, "", "../testdata/planner/index/primary-small-5-2.json", 0, 0},
	{"initial placement - 5 20M array index, 2 replica, 2x", 2, 2, "", "../testdata/planner/index/array-small-5-2.json", 0, 0},
	{"initial placement - 3 replica constraint, 2 index, 2x", 2, 2, "", "../testdata/planner/index/replica-3-constraint.json", 0, 0},
}

var incrPlacementTestCases = []incrPlacementTestCase{
	{"incr placement - 20-50M, 5 2M index, 1 replica, 1x", 1, 1, "../testdata/planner/plan/uniform-small-10-3.json",
		"../testdata/planner/index/small-2M-5-1.json", 0.1, 0.1},
	{"incr placement - mixed small/medium, 6 2M index, 1 replica, 1x", 1, 1, "../testdata/planner/plan/mixed-small-medium-30-3.json",
		"../testdata/planner/index/small-2M-6-1.json", 0.1, 0.1},
	{"incr placement - 3 server group, 3 replica, 1x", 1, 1, "../testdata/planner/plan/empty-3-zone.json",
		"../testdata/planner/index/replica-3.json", 0, 0},
	{"incr placement - 2 server group, 3 replica, 1x", 1, 1, "../testdata/planner/plan/empty-2-zone.json",
		"../testdata/planner/index/replica-3.json", 0, 0},
}

var rebalanceTestCases = []rebalanceTestCase{
	{"rebalance - 20-50M, 90 index, 20%% shuffle, 1x, utilization 90%%+", 1, 1, "../testdata/planner/plan/uniform-small-30-3-90.json", 20, 0, 0, 0.15, 0.15},
	{"rebalance - mixed small/medium, 90 index, 20%% shuffle, 1x", 1, 1, "../testdata/planner/plan/mixed-small-medium-30-3.json", 20, 0, 0, 0.15, 0.15},
	{"rebalance - travel sample, 10%% shuffle, 1x", 1, 1, "../testdata/planner/plan/travel-sample-plan.json", 10, 0, 0, 0.50, 0.50},
	{"rebalance - 20-50M, 90 index, swap 2, 1x", 1, 1, "../testdata/planner/plan/uniform-small-30-3-90.json", 0, 2, 2, 0.1, 0.1},
	{"rebalance - mixed small/medium, 90 index, swap 2, 1x", 1, 1, "../testdata/planner/plan/mixed-small-medium-30-3.json", 0, 2, 2, 0.1, 0.1},
	{"rebalance - travel sample, swap 2, 1x", 1, 1, "../testdata/planner/plan/travel-sample-plan.json", 0, 2, 2, 0.50, 0.50},
	{"rebalance - 8 identical index, add 4, 1x", 1, 1, "../testdata/planner/plan/identical-8-0.json", 0, 4, 0, 0, 0},
	{"rebalance - 8 identical index, delete 2, 2x", 2, 2, "../testdata/planner/plan/identical-8-1.json", 0, 0, 2, 0, 0},
	{"rebalance - drop replcia - 3 replica, 3 zone, delete 1, 2x", 2, 2, "../testdata/planner/plan/replica-3-zone.json", 0, 0, 1, 0, 0},
	{"rebalance - rebuid replica - 3 replica, 3 zone, add 1, delete 1, 1x", 1, 1, "../testdata/planner/plan/replica-3-zone.json", 0, 1, 1, 0, 0},
}

var heterogenousRebalTestCases = []heterogenousRebalTestCase{
	{"heterogenous rebalance - keep vertically scaled Node, swap 1, 1x", 1, 1, "../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json", 0, 1, 1, true, []string{"127.0.0.1:9000"}, "in"},
}

var excludeInTestCases = []excludeInTestCase{
	{"scaling with excludeIn - 8 identical index, add 1, 1x", 1, 1, "../testdata/planner/plan/scale-identical-8-0.json", 0, 1, 0, true},
	{"swap with excludeIn - 8 identical index, add 1, 1x", 1, 1, "../testdata/planner/plan/scale-identical-8-0.json", 0, 1, 1, true},
	{"replica repair with excludeIn - 3 replica, 4 zone, delete 1, 1x", 1, 1, "../testdata/planner/plan/scale-replica-3-zone.json", 0, 0, 1, false},
}

var iterationTestCases = []iterationTestCase{
	{
		"Remove one node - failure",
		"../testdata/planner/workload/uniform-small-10-1.json",
		"../testdata/planner/plan/empty-1-zone.json",
		"",
		1,
		2,
		0.35,
		false,
		"Remove 1",
		0,
	},
	{
		"Remove one node - success",
		"../testdata/planner/workload/uniform-small-10-1.json",
		"../testdata/planner/plan/empty-1-zone.json",
		"",
		1,
		15,
		0.25,
		true,
		"Remove 1",
		0,
	},
	/*
		// This test is not needed due to deterministic initial placement
		// of the lost replicas
		{
			"Index rebuild - failure",
			"",
			"",
			"../testdata/planner/plan/replica-repair-2-zone.json",
			1,
			1,
			0.80,
			false,
			"Lost index",
			30,
		},
	*/
	{
		"Index rebuild - success",
		"",
		"",
		"../testdata/planner/plan/replica-repair-2-zone.json",
		1,
		50,
		0.80,
		true,
		"Lost index",
		60,
	},
	{
		"Index rebuild - initial placement - success",
		"",
		"",
		"../testdata/planner/plan/replica-repair-unbalanced-sg.json",
		0,
		0,
		0.99,
		true,
		"Lost index",
		6,
	},
	{
		"Index rebuild - initial placement - numReplica > numSG - success",
		"",
		"",
		"../testdata/planner/plan/replica-repair-unbalanced-sg-1.json",
		0,
		0,
		0.99,
		true,
		"Lost index",
		9,
	},
}

var greedyPlannerFuncTestCases = []greedyPlannerFuncTestCase{
	// Place single index instace
	{
		"Place Single Index Instance - 3 empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true},
	},
	{
		"Place Single Index Instance - 2 empty nodes, 1 non-empty node - 1 SG",
		"../testdata/planner/greedy/topologies/2_empty_1_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	{
		"Place Single Index Instance - 1 empty node, 2 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_2_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		map[string]bool{"127.0.0.1:9002": true},
	},
	{
		"Place Single Index Instance - 3 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		map[string]bool{"127.0.0.1:9003": true},
	},
	// Place index with 1 replica
	{
		"Place Index With 1 Replica - 3 empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true},
	},
	{
		"Place Index With 1 Replica - 2 empty nodes, 1 non-empty node - 1 SG",
		"../testdata/planner/greedy/topologies/2_empty_1_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	{
		"Place Index With 1 Replica - 1 empty node, 2 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_2_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9002": true, "127.0.0.1:9003": true},
	},
	{
		"Place Index With 1 Replica - 3 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9001": true},
	},
	// Place index with 2 replicas
	{
		"Place Index With 2 Replica - 3 empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_2_replicas.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true},
	},
	{
		"Place Index With 2 Replica - 3 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_2_replicas.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	// Place index with 1 replica across server groups
	{
		"Place Index With 1 Replica - 2 empty nodes, 1 non-empty node - 2 SG",
		"../testdata/planner/greedy/topologies/2_empty_1_non_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	{
		"Place Index With 1 Replica - 1 empty node, 2 non-empty nodes - 2 SG",
		"../testdata/planner/greedy/topologies/1_empty_2_non_empty_nodes_2_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	{
		"Place Index With 1 Replica - 3 non-empty nodes - 2 SG",
		"../testdata/planner/greedy/topologies/3_non_empty_nodes_2_sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9002": true},
	},
	// Place equivalent indexes
	{
		"Place Equivalent Index Without any replica - 3 non-empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_nodes_equiv_index.json",
		"../testdata/planner/greedy/new_equiv_index.json",
		map[string]bool{"127.0.0.1:9002": true},
	},
	{
		"Place Equivalent Index With 1 Replica - 3 non-empty nodes - 1 SG - Skip least loaded node",
		"../testdata/planner/greedy/topologies/3_nodes_equiv_index.json",
		"../testdata/planner/greedy/new_equiv_index_1_replica.json",
		map[string]bool{"127.0.0.1:9000": true, "127.0.0.1:9002": true},
	},
	{
		"Place Equivalent Index With 1 Replica - 3 non-empty nodes - 1 SG - Use least loaded node",
		"../testdata/planner/greedy/topologies/3_nodes_equiv_index_1.json",
		"../testdata/planner/greedy/new_equiv_index_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
}

var greedyPlannerIdxDistTestCases = []greedyPlannerIdxDistTestCase{
	{
		"Place 60 index instaces on 3 empty nodes - 1 SG",
		"../testdata/planner/greedy/topologies/3_empty_nodes_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		60,
		0.0, // Deferred index distribution 30 - 30
		0.0, // Deferred index distribution 30 - 30
	},
	{
		"Place 60 index instaces on 1 empty and 1 10 percent filled node - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_1_10pct_filled_node_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		60,
		8.0,  // Deferred index distribution 28 - 32
		32.0, // Deferred index distribution 26 - 34
	},
	{
		"Place 60 index instaces on 1 empty and 1 30 percent filled node - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_1_30pct_filled_node_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		60,
		50.0,  // Deferred index distribution 25 - 35
		128.0, // Deferred index distribution 22 - 38
	},
	{
		"Place 5 index instaces on 1 empty and 1 60 percent filled node - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_1_60pct_filled_node_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		5,
		4.5, // Deferred index distribution 1 - 4
		4.5, // Deferred index distribution 1 - 4
	},
	{
		"Place 60 index instaces on 1 empty and 1 60 percent filled node - 1 SG",
		"../testdata/planner/greedy/topologies/1_empty_1_60pct_filled_node_1_sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		60,
		450.0, // Deferred index distribution 15 - 45
		800.0, // Deferred index distribution 10 - 50
	},
}

var ddlHeterogenousPlannerFuncTestCases = []greedyPlannerFuncTestCase{
	// Place single index instace
	{
		"Place Single Index Instance - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true},
	},
	// Place index with 1 replica
	{
		"Place Index with 1 replica - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	// Place index with 2 replica
	{
		"Place Index with 2 replicas - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_2_replicas.json",
		map[string]bool{"127.0.0.1:9000": true, "127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	// Place index with 1 replica across server groups
	{
		"Place Index With 1 Replica - 3 non-empty heterogenous nodes with 1 Scaled up - 2 SG - Placed on Scaled up node",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_2-sg_scaled-alone.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9000": true, "127.0.0.1:9001": true},
	},
	{
		"Place Index With 1 Replica - 3 non-empty heterogenous nodes with 1 Scaled up - 2 SG - Placed on older nodes",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_2-sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	// Place equivalent indexes
	{
		"Place Equivalent Index Without any replica - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG",
		"../testdata/planner/scaleup/heterogenous_3_nodes_equiv_index.json",
		"../testdata/planner/greedy/new_equiv_index.json",
		map[string]bool{"127.0.0.1:9002": true},
	},
	{
		"Place Equivalent Index With 1 Replica - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG - Use least loaded node",
		"../testdata/planner/scaleup/heterogenous_3_nodes_equiv_index.json",
		"../testdata/planner/greedy/new_equiv_index_1_replica.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
	},
	{
		"Place Equivalent Index With 1 Replica - 3 non-empty heterogenous nodes with 1 Scaled up - 1 SG - Use ScaleUp Node ",
		"../testdata/planner/scaleup/heterogenous_3_nodes_equiv_index_1.json",
		"../testdata/planner/greedy/new_equiv_index_1_replica.json",
		map[string]bool{"127.0.0.1:9000": true, "127.0.0.1:9002": true},
	},
}

var ddlHeterogenousPartitionedIndexTestCases = []partitionedIdxHeterogenousTestCase{
	// Place single partitioned(8) index instace - 1 Scaled Node
	{
		"Place Single Partitioned Index Instance - 3 non-empty heterogenous nodes with 1 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		8,
	},
	// Place single partitioned(8) index instace - 2 Scaled Node
	{
		"Place Single Partitioned Index Instance - 3 non-empty heterogenous nodes with 2 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-2-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_1.json",
		8,
	},

	// Place partitioned(8) index with 1 replica - 1 Scaled Node
	{
		"Place Paritioned Index with 1 replica - 3 non-empty heterogenous nodes with 1 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		8,
	},
	// Place partitioned(8) index with 1 replica - 2 Scaled Node
	{
		"Place Paritioned Index with 1 replica - 3 non-empty heterogenous nodes with 2 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-2-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_1_replica.json",
		8,
	},

	// Place partitioned(8) index with 2 replica - 1 Scaled Node
	{
		"Place Paritioned Index with 2 replica - 3 non-empty heterogenous nodes with 1 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-1-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_2_replicas.json",
		8,
	},
	// Place partitioned(8) index with 2 replica - 2 Scaled Node
	{
		"Place Paritioned Index with 2 replica - 3 non-empty heterogenous nodes with 2 Scaled up",
		"../testdata/planner/scaleup/heterogenous-6-2_3-nodes-2-scaled_1-sg.json",
		"../testdata/planner/greedy/new_index_with_2_replicas.json",
		8,
	},
}

var scaleupAlterIndexTestCases = []scaleupAlterIndexTestCase{
	// Increment Replica count by 1 - 1 Scaled Node
	{
		"Increment Replica count by 1 - 3 non-empty heterogenous nodes with 1 Scaled up",
		"../testdata/planner/scaleup/heterogenous_3-nodes-1-scaled_replica.json",
		common.IndexDefnId(4242),
		1,
	},
	// Increment Replica count by 2 - 1 Scaled Node
	{
		"Increment Replica count by 2 - 3 non-empty heterogenous nodes with 1 Scaled up",
		"../testdata/planner/scaleup/heterogenous_3-nodes-1-scaled_replica.json",
		common.IndexDefnId(1337),
		2,
	},
	// Increment Replica count by 1 - 2 Scaled Node
	{
		"Increment Replica count by 1 - 3 non-empty heterogenous nodes with 2 Scaled up",
		"../testdata/planner/scaleup/heterogenous_3-nodes-2-scaled_replica.json",
		common.IndexDefnId(4242),
		1,
	},
	// Increment Replica count by 2 - 2 Scaled Node
	{
		"Increment Replica count by 2 - 3 non-empty heterogenous nodes with 2 Scaled up",
		"../testdata/planner/scaleup/heterogenous_3-nodes-2-scaled_replica.json",
		common.IndexDefnId(1337),
		2,
	},
}

func TestPlanner(t *testing.T) {
	log.Printf("In TestPlanner()")

	logging.SetLogLevel(logging.Info)
	defer logging.SetLogLevel(logging.Warn)

	initialPlacementTest(t)
	incrPlacementTest(t)
	rebalanceTest(t)
	minMemoryTest(t)
	iterationTest(t)
	excludeInTest(t)
	heterogenousRebalanceTest(t)
}

func TestGreedyPlanner(t *testing.T) {
	log.Printf("In TestGreedyPlanner()")

	logging.SetLogLevel(logging.Info)
	defer logging.SetLogLevel(logging.Warn)

	greedyPlannerTests(t)
}

func TestTenantAwarePlanner(t *testing.T) {
	log.Printf("In TestTenantAwarePlanner()")

	logging.SetLogLevel(logging.Info)
	defer logging.SetLogLevel(logging.Warn)

	tenantAwarePlannerTests(t)
}

func TestPlanDuringHeterogenousScaleup(t *testing.T) {
	log.Printf("In TestPlanDuringHeterogenousScaleup()")

	logging.SetLogLevel(logging.Info)
	defer logging.SetLogLevel(logging.Warn)

	t.Run("GreedyPlanner", func(t *testing.T) {
		scaleupGreedyPlannerTests(t)
	})
	t.Run("SAPlanner", func(t *testing.T) {
		scaleupSAPlannerTests(t)
	})
	t.Run("CommandRepair", func(t *testing.T) {
		scaleupCommandRepairTests(t)
	})

}

// This test randomly generated a set of index and place them on a single indexer node.
// The placement algorithm will expand the cluster (by adding node) until every node
// is under cpu and memory quota.   This test will check if the indexer cpu and
// memory deviation is less than 10%.
func initialPlacementTest(t *testing.T) {

	for _, testcase := range initialPlacementTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.MemQuotaFactor = testcase.memQuotaFactor
		config.CpuQuotaFactor = testcase.cpuQuotaFactor

		s := planner.NewSimulator()

		spec, err := s.ReadWorkloadSpec(testcase.workloadSpec)
		FailTestIfError(err, "Fail to read workload spec", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.indexSpec)
		FailTestIfError(err, "Fail to read index spec", t)

		p, _, err := s.RunSingleTestPlan(config, spec, nil, indexSpecs)
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.GetResult().ComputeMemUsage()
		cpuMean, cpuDev := p.GetResult().ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.GetResult().PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.GetResult()); err != nil {
			t.Fatal(err)
		}
	}
}

// This test starts with an initial index layout with a fixed number of nodes (plan).
// It then places a set of same size index onto these indexer nodes where number
// of new index is equal to the number of indexer nodes.  There should be one index
// on each indexer node.
func incrPlacementTest(t *testing.T) {

	for _, testcase := range incrPlacementTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.MemQuotaFactor = testcase.memQuotaFactor
		config.CpuQuotaFactor = testcase.cpuQuotaFactor
		config.Resize = false

		s := planner.NewSimulator()

		plan, err := planner.ReadPlan(testcase.plan)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.indexSpec)
		FailTestIfError(err, "Fail to read index spec", t)

		p, _, err := s.RunSingleTestPlan(config, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.GetResult().ComputeMemUsage()
		cpuMean, cpuDev := p.GetResult().ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.GetResult().PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.GetResult()); err != nil {
			t.Fatal(err)
		}
	}
}

// This test planner to rebalance indexer nodes:
// 1) rebalance after randomly shuffle a certain percentage of indexes
// 2) rebalance after swap in/out of indexer nodes
func rebalanceTest(t *testing.T) {

	for _, testcase := range rebalanceTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.MemQuotaFactor = testcase.memQuotaFactor
		config.CpuQuotaFactor = testcase.cpuQuotaFactor
		config.Shuffle = testcase.shuffle
		config.AddNode = testcase.addNode
		config.DeleteNode = testcase.deleteNode
		config.Resize = false

		s := planner.NewSimulator()

		plan, err := planner.ReadPlan(testcase.plan)
		FailTestIfError(err, "Fail to read plan", t)

		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.GetResult().ComputeMemUsage()
		cpuMean, cpuDev := p.GetResult().ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.GetResult().PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.GetResult()); err != nil {
			t.Fatal(err)
		}
	}
}

// This test planner to rebalance indexer nodes with excludeIn nodes:
// 1) rebalance after randomly shuffle a certain percentage of indexes
// 2) rebalance after swap in/out of indexer nodes
func excludeInTest(t *testing.T) {

	for _, testcase := range excludeInTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.MemQuotaFactor = testcase.memQuotaFactor
		config.CpuQuotaFactor = testcase.cpuQuotaFactor
		config.Shuffle = testcase.shuffle
		config.AddNode = testcase.addNode
		config.DeleteNode = testcase.deleteNode
		config.Resize = false

		s := planner.NewSimulator()

		plan, err := planner.ReadPlan(testcase.plan)
		FailTestIfError(err, "Fail to read plan", t)

		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		solution := p.GetResult()

		// Check there is no index movement
		if testcase.checkMovement {
			for _, indexer1 := range solution.Placement { // indexers after rebalance
				for _, indexer2 := range plan.Placement { // indexers before rebalance
					if indexer1.NodeId == indexer2.NodeId {
						for _, index1 := range indexer1.Indexes { // check each index in indexer after rebalance
							found := false
							for _, index2 := range indexer2.Indexes {
								if index1.DefnId == index2.DefnId &&
									index1.InstId == index2.InstId &&
									index1.PartnId == index2.PartnId { // no new index added
									found = true
									break
								}
							}

							if !found {
								t.Fatalf("new index (%v,%v,%v) found in node %v after rebalance",
									index1.DefnId, index1.InstId, index1.PartnId, indexer1.NodeId)
							}
						}
					}
				}
			}
		}

		// check number of indexes are the same after rebalancing
		count1 := 0
		for _, indexer := range solution.Placement {
			count1 += len(indexer.Indexes)
		}

		count2 := 0
		for _, indexer := range plan.Placement {
			count2 += len(indexer.Indexes)
		}

		if count1 != count2 {
			t.Fatalf("number of indexes are different before (%v) and after (%v) rebalance",
				count2, count1)
		}

		if err := planner.ValidateSolution(p.GetResult()); err != nil {
			t.Fatal(err)
		}
	}
}

// This test planner to rebalance indexer nodes in a heterogenous configuration:
// 1) Some nodes are vertically scaled up and its actual memory consumption will be much higher
// 2) swap-only rebalance is performed by removing 1 old node and adding 1 new. Memory violations are ommitted
//
// NOTE: Omitting the memScore and cpuScore in these tests, as reaching a valid solution is important,
// where no movement except for index movement from outNode to addNode happens.
// The scores migh not be optimal due to the vertically scaled nodes.
func heterogenousRebalanceTest(t *testing.T) {

	for _, testcase := range heterogenousRebalTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.MemQuotaFactor = testcase.memQuotaFactor
		config.CpuQuotaFactor = testcase.cpuQuotaFactor
		config.Shuffle = testcase.shuffle
		config.AddNode = testcase.addNode
		config.DeleteNode = testcase.deleteNode
		config.Resize = false

		s := planner.NewSimulator()

		plan, err := planner.ReadPlan(testcase.plan)
		FailTestIfError(err, "Fail to read plan", t)

		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, testcase.keepNodesByNodeID, testcase.excludeValueOnDeleteNodes)
		FailTestIfError(err, "Error in planner test", t)

		solution := p.GetResult()

		// Check there is no index movement
		if testcase.checkMovement {
			for _, indexer1 := range solution.Placement { // indexers after rebalance
				for _, indexer2 := range plan.Placement { // indexers before rebalance
					if indexer1.NodeId == indexer2.NodeId {
						for _, index1 := range indexer1.Indexes { // check each index in indexer after rebalance
							found := false
							for _, index2 := range indexer2.Indexes {
								if index1.DefnId == index2.DefnId &&
									index1.InstId == index2.InstId &&
									index1.PartnId == index2.PartnId { // no new index added
									found = true
									break
								}
							}

							if !found {
								t.Fatalf("new index (%v,%v,%v) found in node %v after rebalance",
									index1.DefnId, index1.InstId, index1.PartnId, indexer1.NodeId)
							}
						}
					}
				}
			}
		}

		// check number of indexes are the same after rebalancing
		count1 := 0
		for _, indexer := range solution.Placement {
			count1 += len(indexer.Indexes)
		}

		count2 := 0
		for _, indexer := range plan.Placement {
			count2 += len(indexer.Indexes)
		}

		if count1 != count2 {
			t.Fatalf("number of indexes are different before (%v) and after (%v) rebalance",
				count2, count1)
		}

		if err := planner.ValidateSolution(p.GetResult()); err != nil {
			t.Fatal(err)
		}
	}
}

func scaleupGreedyPlannerTests(t *testing.T) {

	ddlScaleUpGreedyFuncTestCase(t)
	// TO-DO: Replica Repair and Alter Index
}

func ddlScaleUpGreedyFuncTestCase(t *testing.T) {

	for _, testcase := range ddlHeterogenousPlannerFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseLive = true
		config.UseGreedyPlanner = true
		config.AllowDDLDuringScaleup = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in RunSingleTestPlan", t)

		if _, ok := p.(*planner.GreedyPlanner); !ok {
			t.Fatalf("Greedy planner was not chosen for index placement.")
			continue
		}

		validateGreedyPlacementFunc(t, p, indexSpecs, testcase.targetNodes)
	}
}

func scaleupSAPlannerTests(t *testing.T) {

	t.Run("FunctionalDDLTests", func(t *testing.T) {
		ddlScaleUpSAPlannerFuncTestCase(t)
	})
	t.Run("CreatePartitionedIndex", func(t *testing.T) {
		ddlScaleUpPartitionedIndexSAPlannerTestCase(t)
	})
	// TO-DO: Replica Repair and Alter Index(CommandRepair)
}

// Currently no cost based optimisation check is done for heterogenous
// cluster, if planner is able to place indexes without any error it is
// consider as a successful run
func ddlScaleUpSAPlannerFuncTestCase(t *testing.T) {

	for _, testcase := range ddlHeterogenousPlannerFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseLive = true
		config.UseGreedyPlanner = false
		config.AllowDDLDuringScaleup = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in RunSingleTestPlan", t)

		p.Print()

		if _, ok := p.(*planner.SAPlanner); !ok {
			t.Fatalf("SAPlanner was not chosen for index placement.")
			continue
		}
	}
}

func ddlScaleUpPartitionedIndexSAPlannerTestCase(t *testing.T) {
	for _, testcase := range ddlHeterogenousPartitionedIndexTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseLive = true
		config.AllowDDLDuringScaleup = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		// update the partition config in indexSpecs
		for _, spec := range indexSpecs {
			spec.Deferred = false
			spec.Immutable = false
			spec.Desc = []bool{false}
			spec.NumPartition = uint64(testcase.numPartitions)
			spec.PartitionScheme = string(common.SINGLE)
			spec.HashScheme = uint64(common.CRC32)
			spec.PartitionKeys = []string(nil)
			spec.RetainDeletedXATTR = false
			spec.ExprType = string(common.N1QL)
			spec.Using = string(common.PlasmaDB)
		}

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in RunSingleTestPlan", t)

		p.Print()

		if _, ok := p.(*planner.SAPlanner); !ok {
			t.Fatalf("SAPlanner was not chosen for index placement.")
			continue
		}
	}
}

func scaleupCommandRepairTests(t *testing.T) {
	scaleupAlterIndexTest(t)
}

func scaleupAlterIndexTest(t *testing.T) {

	for _, testcase := range scaleupAlterIndexTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseLive = true
		config.AllowDDLDuringScaleup = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		earlierCount, err := checkForExcessiveIncrement(plan, 0, testcase.defnId, testcase.increment)
		FailTestIfError(err, "checkForExcessiveIncrement failed", t)

		s := planner.NewSimulator()
		p, err := s.RunSingleTestCommandRepair(config, plan, testcase.defnId, testcase.increment)
		FailTestIfError(err, "Error in RunSingleTestCommandRepair", t)

		p.Print()

		validateAlterIndexFunc(t, p, earlierCount+testcase.increment, testcase.defnId)
	}
}

func minMemoryTest(t *testing.T) {

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 1: min memory = 0")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		if len(plan.Placement) != 2 {
			t.Fatal("plan does not have 2 indexer nodes")
		}

		// set min memory to 0 for disabling resource check
		for _, indexer := range plan.Placement {
			indexer.ActualMemMin = 0
			for _, index := range indexer.Indexes {
				index.ActualMemMin = 0
			}
		}

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		// check if the indexers have equal number of indexes
		if len(p.GetResult().Placement[0].Indexes) != len(p.GetResult().Placement[1].Indexes) {
			t.Fatal("rebalance does not spread indexes equally")
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 2: min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		memQuota := plan.MemQuota

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		// check if index violates memory constraint
		for _, indexer := range p.GetResult().Placement {
			minMemory := uint64(0)
			for _, index := range indexer.Indexes {
				minMemory += index.ActualMemMin
			}
			if minMemory > memQuota {
				t.Fatal("min memory is over memory quota")
			}
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 3: min memory < quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		if len(plan.Placement) != 2 {
			t.Fatal("plan does not have 2 indexer nodes")
		}
		plan.MemQuota = 800

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		// check if the indexers have equal number of indexes
		if len(p.GetResult().Placement[0].Indexes) != len(p.GetResult().Placement[1].Indexes) {
			t.Fatal("rebalance does not spread indexes equally")
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 4: replica repair with min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-replica-plan.json")
		FailTestIfError(err, "Fail to read plan", t)

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		// check the total number of indexes
		for _, indexer := range p.GetResult().Placement {
			if len(indexer.Indexes) != 1 {
				t.Fatal("There is more than 1 index per node")
			}
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 5: replica repair with min memory < quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-replica-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 800

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		// check the total number of indexes
		total := 0
		for _, indexer := range p.GetResult().Placement {
			total += len(indexer.Indexes)
		}
		if total != 4 {
			t.Fatal(fmt.Sprintf("Replica is not repaired. num replica = %v", total))
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 6: rebalance with min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 10
		count1 := len(plan.Placement[0].Indexes)
		count2 := len(plan.Placement[1].Indexes)

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		if count1 != len(p.GetResult().Placement[0].Indexes) {
			t.Fatal(fmt.Sprintf("Index count for indexer1 has changed %v != %v", count1, len(p.GetResult().Placement[0].Indexes)))
		}

		if count2 != len(p.GetResult().Placement[1].Indexes) {
			t.Fatal(fmt.Sprintf("Index count for indexer2 has changed %v != %v", count2, len(p.GetResult().Placement[1].Indexes)))
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 7: rebalance-out with min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 10
		plan.Placement[0].MarkDeleted()

		count1 := 0
		for _, indexer := range plan.Placement {
			count1 += len(indexer.Indexes)
		}

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)

		if len(p.GetResult().Placement) != 1 {
			t.Fatal("There is more than 1 node after rebalance-out")
		}

		count2 := 0
		for _, indexer := range p.GetResult().Placement {
			count2 += len(indexer.Indexes)
		}

		if count1 != count2 {
			t.Fatal(fmt.Sprintf("Indexes are dropped after rebalance-out %v != %v", count1, count2))
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 8: plan with min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 10

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		var spec planner.IndexSpec
		spec.DefnId = common.IndexDefnId(time.Now().UnixNano())
		spec.Name = "test8"
		spec.Bucket = "test8"
		spec.IsPrimary = false
		spec.SecExprs = []string{"test8"}
		spec.WhereExpr = ""
		spec.Deferred = false
		spec.Immutable = false
		spec.IsArrayIndex = false
		spec.Desc = []bool{false}
		spec.NumPartition = 1
		spec.PartitionScheme = string(common.SINGLE)
		spec.HashScheme = uint64(common.CRC32)
		spec.PartitionKeys = []string(nil)
		spec.Replica = 1
		spec.RetainDeletedXATTR = false
		spec.ExprType = string(common.N1QL)
		spec.Using = string(common.PlasmaDB)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, []*planner.IndexSpec{&spec})
		FailTestIfError(err, "Error in planner test", t)

		found := false
		for _, indexer := range p.GetResult().Placement {
			for _, index := range indexer.Indexes {
				if index.DefnId == spec.DefnId {
					found = true
					break
				}
			}
		}

		if !found {
			t.Fatal("Fail to find index after placement")
		}
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 9: single node rebalance with min memory > quota")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 10
		plan.Placement = plan.Placement[0:1]

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		s := planner.NewSimulator()
		_, _, err = s.RunSingleTestRebal(config, planner.CommandRebalance, nil, plan, nil, nil, "")
		FailTestIfError(err, "Error in planner test", t)
	}()

	func() {
		log.Printf("-------------------------------------------")
		log.Printf("Minimum memory test 10: plan with partitioned index on empty cluster")

		plan, err := planner.ReadPlan("../testdata/planner/plan/min-memory-empty-plan.json")
		FailTestIfError(err, "Fail to read plan", t)
		plan.MemQuota = 512000000

		config := planner.DefaultRunConfig()
		config.UseLive = true
		config.Resize = false

		var spec planner.IndexSpec
		spec.DefnId = common.IndexDefnId(time.Now().UnixNano())
		spec.Name = "test10"
		spec.Bucket = "test10"
		spec.IsPrimary = false
		spec.SecExprs = []string{"test10"}
		spec.WhereExpr = ""
		spec.Deferred = false
		spec.Immutable = false
		spec.IsArrayIndex = false
		spec.Desc = []bool{false}
		spec.NumPartition = 4
		spec.PartitionScheme = string(common.HASH)
		spec.HashScheme = uint64(common.CRC32)
		spec.PartitionKeys = []string{"test10"}
		spec.Replica = 1
		spec.RetainDeletedXATTR = false
		spec.ExprType = string(common.N1QL)
		spec.Using = string(common.PlasmaDB)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, []*planner.IndexSpec{&spec})
		FailTestIfError(err, "Error in planner test", t)

		success := true
		for _, indexer := range p.GetResult().Placement {
			if len(indexer.Indexes) != 1 {
				success = false
				p.Print()
				break
			}
		}

		if !success {
			t.Fatal("fail to evently distribute index across noodes")
			return
		}

	}()
}

// The SA Planner runs a certain number of iterations per temperature in an
// attempt to move the indexes. Ideally the number of iterations should be
// enuogh to evaulate a large number of index movements to find an optimal
// solution. But as the number of iterations increase, the number of index
// movements also increase - which can lead to too much data movement
// in the cluster. One way to redue the data movement in the cluster is to
// reduce the planner iterations. But that may lead to avoiding necessary
// index movements as well. For example, when number of indexes moved per
// iteration are less than total indexes to be moved out of a deleted
// node.
//
// With higher allowable variance in the cluster, planner may choose to
// move only upto "iterations per temperature" number of indexes. To ensure
// functional correctness, iterations per temperature are defined as a range
// of minimum to maximum.
//
// This test varifies different functional scenarios where minimuum number
// of iterations are not enough to ensure:
// 1. Replica/partition repair with HA
// 2. Node removal
// 3. Node swap
func iterationTest(t *testing.T) {
	for _, testcase := range iterationTestCases {
		var p planner.Planner

		log.Printf("-------------------------------------------")
		log.Printf("iterationTest :: %v", testcase.comment)

		if testcase.action == "Remove 1" {
			config := planner.DefaultRunConfig()

			s := planner.NewSimulator()
			spec, err := s.ReadWorkloadSpec(testcase.topoSpec)
			FailTestIfError(err, "Fail to read workload spec", t)

			plan, err := planner.ReadPlan(testcase.indexers)
			FailTestIfError(err, "Fail to read plan", t)

			p, _, err = s.RunSingleTestPlan(config, spec, plan, nil)
			FailTestIfError(err, "Error in planner test", t)

			if err := planner.ValidateSolution(p.GetResult()); err != nil {
				t.Fatal(err)
			}
		}

		// p.Print()

		var newPlan *planner.Plan

		switch testcase.action {

		case "Remove 1":
			p.GetResult().Placement[0].MarkDeleted()
			newPlan = &planner.Plan{
				Placement: p.GetResult().Placement,
				MemQuota:  5302940000000,
			}

		case "Lost index":
			var err error
			newPlan, err = planner.ReadPlan(testcase.plan)
			FailTestIfError(err, "Fail to read plan", t)
			newPlan.MemQuota = 5302940000000

		default:
			t.Fatal("Unupported testcase action")
		}

		config1 := planner.DefaultRunConfig()
		config1.UseLive = true
		config1.Resize = false
		config1.MinIterPerTemp = testcase.minIter
		config1.MaxIterPerTemp = testcase.maxIter
		config1.Threshold = testcase.threshold

		s1 := planner.NewSimulator()
		p1, _, err := s1.RunSingleTestRebal(config1, planner.CommandRebalance, nil, newPlan, nil, nil, "")

		// p1.Print()

		if testcase.action == "Remove 1" {
			if testcase.success {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if len(p1.Result.Placement) == 2 {
					t.Fatal(fmt.Errorf("Unexpected success for testcase %v", testcase.comment))
				}
			}
		}

		if testcase.action == "Lost index" {
			expCount := testcase.idxCount
			count := 0
			for _, node := range p1.GetResult().Placement {
				count += len(node.Indexes)
			}

			if count != expCount {
				t.Fatal(fmt.Errorf("Error: expected count %v, actual count %v", expCount, count))
			}
		}
	}
}

// Greedy planner tests
func greedyPlannerTests(t *testing.T) {

	greedyPlannerFuncTests(t)

	greedyPlannerIdxDistTests(t)

}

// Greedy planner functional tests.
// Each test case takes following inputs:
// 1. Initial topology
// 2. Set of index specs to be placed
// 3. A set of target nodes, on which the input indexes are to be placed.
//
// During verification, the index placement decided by greedy planner is
// validated against the set of target nodes.
func greedyPlannerFuncTests(t *testing.T) {

	for _, testcase := range greedyPlannerFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseGreedyPlanner = true
		// config.UseLive = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestPlan(config, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in RunSingleTestPlan", t)

		// p.Print()

		if _, ok := p.(*planner.GreedyPlanner); !ok {
			t.Fatalf("Greedy planner was not chosen for index placement.")
			continue
		}

		validateGreedyPlacementFunc(t, p, indexSpecs, testcase.targetNodes)
	}
}

// Greedy planner index distibution tests
//
// The purpose of these test cases is to verify the overall good index
// distribution across the nodes. With the existance of deferred indexes,
// the size estimation needs to run to estimate the size of the existing
// deferred indexes in the cluster. These tests verify the overall index
// distribution in the cluster has controlled variance.
//
// Note: these tests primarily focus on the distribution of the deferred
//
//	indexes, given the initial topology.
//
// Each test takes following inputs
// 1. Initial topology
// 2. A sample index to be placed
// 3. Total number of indexes to be placed
// 4. Allowed variance in the number of new indexes placed across the nodes
func greedyPlannerIdxDistTests(t *testing.T) {

	for _, testcase := range greedyPlannerIdxDistTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		config := planner.DefaultRunConfig()
		config.Resize = false
		config.AddNode = -1
		config.AllowSwap = false
		config.AllowMove = false
		config.UseGreedyPlanner = true
		// config.UseLive = true

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.sampleIndex)
		FailTestIfError(err, "Fail to read index spec", t)

		defnId := indexSpecs[0].DefnId
		name := indexSpecs[0].Name
		secExpr := indexSpecs[0].SecExprs[0]

		var p planner.Planner

		for i := 0; i < testcase.numIndexes; i++ {

			// Update index spec i.e. name, DefnId and secExprs
			newDefnId := int64(defnId) + int64(1000000000) + int64(i)*int64(1000000)
			indexSpecs[0].DefnId = common.IndexDefnId(newDefnId)

			indexSpecs[0].Name = name + "_" + fmt.Sprintf("%v", i)

			indexSpecs[0].SecExprs = []string{secExpr + "_" + fmt.Sprintf("%v", i)}

			s := planner.NewSimulator()

			var err error
			p, _, err = s.RunSingleTestPlan(config, nil, plan, indexSpecs)
			FailTestIfError(err, "Error in RunSingleTestPlan", t)

			// p.Print()

			if _, ok := p.(*planner.GreedyPlanner); !ok {
				t.Fatalf("Greedy planner was not chosen for index placement.")
			}

			result := p.GetResult()
			if i < testcase.numIndexes-1 {
				cleanupEstimation(result)
				plan.Placement = result.Placement
			}
		}

		// p.Print()

		validateGreedyPlacementIdxDist(t, p, testcase.numIndexes,
			testcase.minDistVariance, testcase.maxDistVariance)

	}
}

func validateGreedyPlacementFunc(t *testing.T, p planner.Planner,
	indexSpecs []*planner.IndexSpec, targetNodes map[string]bool) {

	defnId := indexSpecs[0].DefnId
	count := 0

	targets := make(map[string]bool)
	for nid, ok := range targetNodes {
		targets[nid] = ok
	}

	result := p.GetResult()
	for _, indexer := range result.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defnId {
				count++

				if _, ok := targets[indexer.NodeId]; !ok {
					log.Printf("Unexpected index placement by greedy planner. Target Nodes = %v", targetNodes)
					p.Print()
					t.Fatalf("Unexpected index placement by greedy planner %v, %v", targets, indexer.NodeId)
				}

				delete(targets, indexer.NodeId)
			}
		}
	}

	if count != int(indexSpecs[0].Replica) {
		p.Print()
		t.Fatalf("Some indexes are not found in the result")
	}
}

func calcVariance(dist []int) float64 {
	if len(dist) <= 0 {
		return 0.0
	}

	if len(dist) == 1 {
		return float64(dist[0])
	}

	sum := 0
	for _, count := range dist {
		sum += count
	}

	mean := float64(sum) / float64(len(dist))

	ss := 0.0
	for _, count := range dist {
		sqr := (float64(count) - mean) * (float64(count) - mean)
		ss += sqr
	}

	return ss / float64(len(dist)-1)
}

func validateGreedyPlacementIdxDist(t *testing.T, p planner.Planner,
	numIndexes int, minDistVariance float64, maxDistVariance float64) {

	result := p.GetResult()

	deferredIdxCount := make([]int, 0, len(result.Placement))
	total := 0
	for _, indexer := range result.Placement {
		count := 0
		for _, index := range indexer.Indexes {
			if index.NoUsageInfo {
				count++
				total++
			}
		}

		deferredIdxCount = append(deferredIdxCount, count)
	}

	if total != numIndexes {
		p.Print()
		t.Fatalf("Total number of deferred indexes don't match expected %v actual %v", numIndexes, total)
	}

	actVariance := calcVariance(deferredIdxCount)
	logging.Infof("Actual variance of deferred index count across nodes is %v", actVariance)

	if actVariance < minDistVariance || actVariance > maxDistVariance {
		p.Print()
		t.Fatalf("Deferred index distribution variace (%v) doesn't fall in allowed variance range (%v, %v)",
			actVariance, minDistVariance, maxDistVariance)
	}
}

func cleanupEstimation(s *planner.Solution) {
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.NeedsEstimation() {
				indexer.SubtractMemUsageOverhead(s, index.EstimatedMemUsage, 0, index.EstimatedMemUsage)
				indexer.SubtractDataSize(s, index.EstimatedDataSize)
				index.EstimatedMemUsage = 0
				index.EstimatedDataSize = 0
			}
		}
	}
}

// returns the current replica count before alter index and any error if the placement is not able to hold the
// new value
func checkForExcessiveIncrement(p *planner.Plan, addNodes int, defnId common.IndexDefnId, increment int) (int, error) {

	if increment <= 0 {
		return -1, errors.New(fmt.Sprintf("increment value has to be greater than 1"))
	}

	idxReplicaMap := planner.GenerateReplicaMap(p.Placement)
	currCount := len(idxReplicaMap[defnId])

	if currCount+increment > len(p.Placement)+addNodes {
		return currCount, errors.New(fmt.Sprintf("placement can't accomodate the number of replica increase"))
	}

	return currCount, nil
}

func validateAlterIndexFunc(t *testing.T, p planner.Planner,
	expectedNumReplica int, defnId common.IndexDefnId) {

	result := p.GetResult()
	idxReplicaMap := planner.GenerateReplicaMap(result.Placement)

	if len(idxReplicaMap[defnId]) != expectedNumReplica {
		p.Print()
		t.Fatalf("The expected num replica did not match the resultant num replica. Expected: %v Got: %v",
			expectedNumReplica, idxReplicaMap[defnId])
	}
}

type tenantAwarePlannerFuncTestCase struct {
	comment       string
	topology      string
	index         string
	targetNodes   map[string]bool
	ignoreReplica bool
	errStr        string
}

type tenantAwarePlannerRebalFuncTestCase struct {
	comment      string
	topology     string
	result       string
	errStr       string
	ignoreInstId bool
}

var tenantAwarePlannerFuncTestCases = []tenantAwarePlannerFuncTestCase{
	// Place single index instace
	{
		"Place Single Index Instance - 1 empty node - 1 SG",
		"../testdata/planner/tenantaware/topology/1_empty_node_1_sg.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true},
		true,
		"",
	},
	{
		"Place Single Index Instance - 4 empty nodes - 2 SG",
		"../testdata/planner/tenantaware/topology/4_empty_nodes_2_sg.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 1 node - 1 SG",
		"../testdata/planner/tenantaware/topology/1_non_empty_node_1_sg.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true},
		true,
		"",
	},
	{
		"Place Single Index Instance - 2 nodes - 1 SG",
		"../testdata/planner/tenantaware/topology/2_non_empty_node_1_sg.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true},
		true,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes - 2 SG - Tenant Affinity(a)",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_a.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes - 2 SG - Tenant Affinity(b)",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_b.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes - 2 SG - Tenant Affinity(c)",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_c.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - Tenant Affinity Memory Above LWM",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_a.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9002": true, "127.0.0.1:9005": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - Tenant Affinity Units Above LWM",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_b.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9002": true, "127.0.0.1:9005": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - Tenant Affinity New Tenant(a)",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_a.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9006": true, "127.0.0.1:9003": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - Tenant Affinity New Tenant(b)",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_c.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9006": true, "127.0.0.1:9003": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes - 2 SG - Tenant Affinity Above Units HWM",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_e.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - New Tenant Units Above LWM",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_e.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9006": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(inst version) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_e_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(rstate) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_f_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(rstate+inst version) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_g_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance pending cleanup",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_h_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9004": true},
		false,
		"",
	},
	/*
		{
			"Place Single Index Instance - 4 nodes - 2 SG - Failed Over Node",
			"../testdata/planner/tenantaware/topology/4_empty_nodes_2_sg.json",
			"../testdata/planner/tenantaware/new_index_1.json",
			map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true, "127.0.0.1:9004": true},
		},
		{
			"Place Single Index Instance - 4 nodes - 2 SG - Failed Swap Rebalance",
			"../testdata/planner/tenantaware/topology/4_empty_nodes_2_sg.json",
			"../testdata/planner/tenantaware/new_index_1.json",
			map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true, "127.0.0.1:9003": true, "127.0.0.1:9004": true},
		},
	*/
}

var tenantAwarePlannerFuncTestCasesNegative = []tenantAwarePlannerFuncTestCase{
	{
		"Place Single Index Instance - 2 empty nodes - 1 SG",
		"../testdata/planner/tenantaware/topology/2_empty_nodes_1_sg.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		true,
		"Planner not able to find any node for placement - Unable to find any valid SubCluster",
	},
	{
		"Place Single Index Instance - 4 nodes - 2 SG - Tenant Affinity Above Memory HWM",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_d.json",
		"../testdata/planner/tenantaware/new_index_1.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9004": true},
		false,
		"Tenant SubCluster Above High Usage Threshold",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - New Tenant Memory Above LWM",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_d.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9002": true, "127.0.0.1:9005": true},
		false,
		"No SubCluster Below Low Usage Threshold",
	},
	{
		"Place Single Index Instance - 6 nodes - 3 SG - New Tenant Units Above LWM",
		"../testdata/planner/tenantaware/topology/6_non_empty_nodes_3_sg_f.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9003": true, "127.0.0.1:9006": true},
		false,
		"Planner Constraint Violation",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(inst version) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_a_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"Rebalance in progress or cleanup pending from previous rebalance",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(rstate) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_b_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"Rebalance in progress or cleanup pending from previous rebalance",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance in progress(rstate+inst version) ",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_c_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"Rebalance in progress or cleanup pending from previous rebalance",
	},
	{
		"Place Single Index Instance - 4 nodes, rebalance pending cleanup",
		"../testdata/planner/tenantaware/topology/4_non_empty_nodes_2_sg_d_rebal.json",
		"../testdata/planner/tenantaware/new_index_2.json",
		map[string]bool{"127.0.0.1:9001": true, "127.0.0.1:9002": true},
		false,
		"Rebalance in progress or cleanup pending from previous rebalance",
	},
}

// Tenant Aware planner tests
func tenantAwarePlannerTests(t *testing.T) {

	tenantAwarePlannerFuncTests(t)
	tenantAwarePlannerRebalanceTests(t)
	tenantAwarePlannerReplicaRepairTests(t)
	tenantAwarePlannerSwapRebalanceTests(t)
	tenantAwarePlannerScaleDownTests(t)
	tenantAwarePlannerDefragTests(t)
	tenantAwarePlannerResumeTests(t)

}

// Tenant Aware planner functional tests.
// Each test case takes following inputs:
// 1. Initial topology
// 2. Set of index specs to be placed
//
// During verification, the index placement decided by tenant aware planner is
// validated against the a pre-defined set of constraints.
func tenantAwarePlannerFuncTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		p, _, err := s.RunSingleTestTenantAwarePlan(plan, indexSpecs[0])
		FailTestIfError(err, "Error in RunSingleTestPlan", t)

		if _, ok := p.(*planner.TenantAwarePlanner); !ok {
			t.Fatalf("TenantAware planner was not chosen for index placement.")
			continue
		}

		validateTenantAwarePlacement(t, p, indexSpecs[0], testcase.targetNodes, testcase.ignoreReplica)
	}

	for _, testcase := range tenantAwarePlannerFuncTestCasesNegative {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		indexSpecs, err := planner.ReadIndexSpecs(testcase.index)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		_, _, err = s.RunSingleTestTenantAwarePlan(plan, indexSpecs[0])
		FailTestIfNoError(err, "Error in RunSingleTestPlan", t)
		if strings.Contains(err.Error(), testcase.errStr) {
			log.Printf("Expected error %v", err)
		} else {
			t.Fatalf("Unexpected error %v. Expected %v\n", err, testcase.errStr)
		}

	}
}

func validateTenantAwarePlacement(t *testing.T, p planner.Planner,
	indexSpec *planner.IndexSpec, targetNodes map[string]bool, ignoreReplica bool) {

	defnId := indexSpec.DefnId
	count := 0

	targets := make(map[string]bool)
	for nid, ok := range targetNodes {
		targets[nid] = ok
	}

	result := p.GetResult()
	for _, indexer := range result.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defnId {
				count++

				if _, ok := targets[indexer.NodeId]; !ok {
					log.Printf("Unexpected index placement by tenant aware planner. Target Nodes = %v", targetNodes)
					p.Print()
					t.Fatalf("Unexpected index placement by tenant aware planner %v, %v", targets, indexer.NodeId)
				}

				delete(targets, indexer.NodeId)
			}
		}
	}

	if ignoreReplica {
		if count != 1 {
			p.Print()
			t.Fatalf("Some indexes are not found in the result")
		}

	} else {
		if count != int(indexSpec.Replica) {
			p.Print()
			t.Fatalf("Some indexes are not found in the result")
		}
	}
}

var tenantAwarePlannerRebalFuncTestCases = []tenantAwarePlannerRebalFuncTestCase{

	{
		"Rebalance - 3 SG, 1 empty, 1 Memory Above HWM",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_a.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_a_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, 1 empty, 1 Units Above HWM",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_b.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_b_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, 1 empty, Both Memory/Units Above HWM",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_c.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_c_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, Multiple tenants to move, single source, multiple destination",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_d.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_d_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, Multiple tenants to move, no nodes below LWM",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_e.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_e_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Multiple tenants to move, multiple source, multiple destination(non-uniform memory/units usage)",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_f.json",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_f_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Multiple tenants to move, multiple source, multiple destination(non-uniform memory/units usage)",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_g.json",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_g_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, Single Large Tenant, Nothing to move",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_h.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_h_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Multiple tenants to move, multiple source, multiple destination(zero usage tenants)",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_h.json",
		"../testdata/planner/tenantaware/topology/rebalance/8_non_empty_nodes_4_sg_h_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 SG, 1 Partial Subcluster",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_i.json",
		"../testdata/planner/tenantaware/topology/rebalance/6_non_empty_nodes_3_sg_i_out.json",
		"",
		false,
	},
}

func tenantAwarePlannerRebalanceTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerRebalFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		result, err := planner.ReadPlan(testcase.result)
		s := planner.NewSimulator()
		solution, err := s.RunSingleTestTenantAwareRebal(plan, nil)
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)

		err = validateTenantAwareRebalanceSolution(t, solution, result, plan.DeletedNodes, testcase.ignoreInstId)
		if err != nil {
			log.Printf("Actual Solution \n")
			solution.PrintLayout()
			log.Printf("Expected Result %v\n", result)
		}
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)

	}
}

func validateTenantAwareRebalanceSolution(t *testing.T, solution *planner.Solution,
	result *planner.Plan, deletedNodes []string, ignoreInstId bool) error {

	if len(solution.Placement) != len(result.Placement) {
		return errors.New(fmt.Sprintf("Mismatch in indexer node count."+
			"Solution %v Expected %v", len(solution.Placement), len(result.Placement)))
	}
	for _, indexer := range solution.Placement {

		if checkIfDeletedNode(indexer, deletedNodes) {

			_, err := findIndexerNodeInResult(result, indexer)
			if err == nil {
				return errors.New(fmt.Sprintf("Deleted Node Found in Result %v", indexer))
			}

		} else {
			indexer1, err := findIndexerNodeInResult(result, indexer)
			if err != nil {
				return err
			}

			err = compareIndexesOnNodes(indexer, indexer1, ignoreInstId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func checkIfDeletedNode(indexer *planner.IndexerNode, deletedNodes []string) bool {

	for _, nodeuuid := range deletedNodes {
		if nodeuuid == indexer.NodeUUID {
			return true
		}
	}
	return false
}

func compareIndexesOnNodes(indexer *planner.IndexerNode,
	indexer1 *planner.IndexerNode, ignoreInstId bool) error {

	if len(indexer.Indexes) != len(indexer1.Indexes) {
		return errors.New(fmt.Sprintf("Mismatch in index count on "+
			"node %v. Solution %v. Expected %v", indexer.NodeUUID,
			len(indexer.Indexes), len(indexer1.Indexes)))
	}

	for _, index := range indexer.Indexes {

		err := findIndexOnNode(index, indexer1, ignoreInstId)
		if err != nil {
			return err
		}

	}

	return nil

}

func findIndexOnNode(index *planner.IndexUsage, indexer1 *planner.IndexerNode, ignoreInstId bool) error {

	found := false
	for _, index1 := range indexer1.Indexes {

		if index.DefnId == index1.DefnId &&
			(index.InstId == index1.InstId || ignoreInstId) &&
			index.PartnId == index1.PartnId &&
			index.Name == index1.Name &&
			index.Bucket == index1.Bucket &&
			index.Instance.ReplicaId == index1.Instance.ReplicaId {
			found = true
			break
		}
	}

	if !found {
		return errors.New(fmt.Sprintf("Index %v not found on Node %v in expected result.", index, indexer1.NodeUUID))
	}

	return nil
}

func findIndexerNodeInResult(result *planner.Plan, indexer *planner.IndexerNode) (*planner.IndexerNode, error) {

	for _, indexer1 := range result.Placement {
		if indexer.NodeUUID == indexer1.NodeUUID {
			return indexer1, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("Indexer Node %v not found in expected result.", indexer))

}

var tenantAwarePlannerReplicaRepairFuncTestCases = []tenantAwarePlannerRebalFuncTestCase{

	{
		"Replica Repair - 4 SG, Missing Replicas for multiple tenants in SG",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_a.json",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_a_out.json",
		"",
		true,
	},
	{
		"Replica Repair - 4 SG, Missing Replicas, Buddy Node Failed over",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_b.json",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_b_out.json",
		"",
		true,
	},
	{
		"Replica Repair - 4 SG, Missing Replicas, Buddy Node Failed over, No replacement",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_c.json",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_c_out.json",
		"",
		true,
	},
	{
		"Replica Repair - 4 SG, Missing Replicas, one replica missing with pendingDelete true ",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_d.json",
		"../testdata/planner/tenantaware/topology/replica_repair/8_non_empty_nodes_4_sg_d_out.json",
		"",
		true,
	},
	{
		"Replica Repair - 2 SG, Missing Replicas with Nodes over HWM",
		"../testdata/planner/tenantaware/topology/replica_repair/4_non_empty_nodes_2_sg_e.json",
		"../testdata/planner/tenantaware/topology/replica_repair/4_non_empty_nodes_2_sg_e_out.json",
		"",
		true,
	},
}

func tenantAwarePlannerReplicaRepairTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerReplicaRepairFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		result, err := planner.ReadPlan(testcase.result)
		s := planner.NewSimulator()
		solution, err := s.RunSingleTestTenantAwareRebal(plan, nil)
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)

		err = validateTenantAwareRebalanceSolution(t, solution, result, nil, testcase.ignoreInstId)
		if err != nil {
			log.Printf("Actual Solution \n")
			solution.PrintLayout()
			log.Printf("Expected Result %v\n", result)
		}
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)

	}
}

var tenantAwarePlannerSwapRebalFuncTestCases = []tenantAwarePlannerRebalFuncTestCase{

	{
		"Swap Rebalance - 4 SG, Swap 1 node each from 2 SG with 2 new nodes",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_a.json",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_a_out.json",
		"",
		false,
	},
	{
		"Swap Rebalance - 4 SG, Swap 1 node each from 2 SG with 2 new nodes(different SG)",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_b.json",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_b_out.json",
		"",
		false,
	},
	{
		"Swap Rebalance - 4 SG, Swap 1 SG with 2 new nodes",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_c.json",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_c_out.json",
		"",
		false,
	},
	{
		"Swap Rebalance - 4 SG, Swap 1 node with 2 new nodes",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_d.json",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_d_out.json",
		"",
		false,
	},
	{
		"Swap Rebalance - 4 SG, Swap 1 empty node with 2 new nodes",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_e.json",
		"../testdata/planner/tenantaware/topology/swap/8_non_empty_nodes_4_sg_e_out.json",
		"",
		true,
	},
	{
		"Swap Rebalance - 1 SG, Swap 1 node - Failed swap rebalance",
		"../testdata/planner/tenantaware/topology/swap/2_non_empty_nodes_1_sg_f.json",
		"../testdata/planner/tenantaware/topology/swap/2_non_empty_nodes_1_sg_f_out.json",
		"",
		false,
	},
	{
		"Swap Rebalance - 1 SG, Swap 2 node - server group mismatch",
		"../testdata/planner/tenantaware/topology/swap/2_non_empty_nodes_1_sg_g.json",
		"../testdata/planner/tenantaware/topology/swap/2_non_empty_nodes_1_sg_g_out.json",
		"Planner - Unable to satisfy server group constraint while replacing removed nodes with new nodes.",
		false,
	},
}

func tenantAwarePlannerSwapRebalanceTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerSwapRebalFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		result, err := planner.ReadPlan(testcase.result)
		s := planner.NewSimulator()
		solution, err := s.RunSingleTestTenantAwareRebal(plan, plan.DeletedNodes)

		if testcase.errStr == "" {
			FailTestIfError(err, "Error in RunSingleTestRebalance", t)
			err = validateTenantAwareRebalanceSolution(t, solution, result, plan.DeletedNodes, testcase.ignoreInstId)
			if err != nil {
				log.Printf("Actual Solution \n")
				solution.PrintLayout()
				log.Printf("Expected Result %v\n", result)
			}
			FailTestIfError(err, "Error in RunSingleTestRebalance", t)
		} else {

			FailTestIfNoError(err, "Error in RunSingleTestRebalance", t)
			if strings.Contains(err.Error(), testcase.errStr) {
				log.Printf("Expected error %v", err)
			} else {
				t.Fatalf("Unexpected error %v. Expected %v\n", err, testcase.errStr)
			}
		}
	}
}

var tenantAwarePlannerScaleDownFuncTestCases = []tenantAwarePlannerRebalFuncTestCase{

	{
		"Rebalance - 4 SG, Move out 1 subcluster, Enough Capacity",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_a.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_a_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 subcluster, Not Enough Capacity",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_b.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_b_out.json",
		"Planner - Not enough capacity to place indexes of deleted nodes.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 node, Pair node not deleted(Failed swap rebalance of 1 node)",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_c.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_c_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 node, Pair node already deleted",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_d.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_d_out.json",
		"Provide additional node as replacement.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 subcluster, empty nodes",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_e.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_e_out.json",
		"",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 subcluster, Deleted Nodes more than added nodes",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_f.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_f_out.json",
		"Planner - Number of non-empty deleted nodes cannot be greater than number of added nodes.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 node, Add 1 node in, server group mismatch",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_g.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_g_out.json",
		"Planner - Unable to satisfy server group constraint while replacing removed nodes with new nodes.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 node, Pair node exists",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_h.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_h_out.json",
		"Provide additional node as replacement.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 subcluster, No nodes under LWM",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_i.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_i_out.json",
		"Planner - Not enough capacity to place indexes of deleted nodes.",
		false,
	},
	{
		"Rebalance - 4 SG, Move out 1 subcluster, Not Enough Capacity, Partial Subcluster",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_j.json",
		"../testdata/planner/tenantaware/topology/scaledown/8_non_empty_nodes_4_sg_j_out.json",
		"Planner - Not enough capacity to place indexes of deleted nodes.",
		false,
	},
	{
		"Rebalance - 2 SG, Move out 1 non-empty and 1 empty  node",
		"../testdata/planner/tenantaware/topology/scaledown/4_non_empty_nodes_2_sg_k.json",
		"../testdata/planner/tenantaware/topology/scaledown/4_non_empty_nodes_2_sg_k_out.json",
		"",
		false,
	},
}

func tenantAwarePlannerScaleDownTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerScaleDownFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		result, err := planner.ReadPlan(testcase.result)
		s := planner.NewSimulator()
		solution, err := s.RunSingleTestTenantAwareRebal(plan, plan.DeletedNodes)

		if testcase.errStr == "" {
			FailTestIfError(err, "Error in RunSingleTestRebalance", t)
			err = validateTenantAwareRebalanceSolution(t, solution, result, plan.DeletedNodes, testcase.ignoreInstId)
			if err != nil {
				log.Printf("Actual Solution \n")
				solution.PrintLayout()
				log.Printf("Expected Result %v\n", result)
			}
			FailTestIfError(err, "Error in RunSingleTestRebalance", t)
		} else {

			FailTestIfNoError(err, "Error in RunSingleTestPlan", t)
			if strings.Contains(err.Error(), testcase.errStr) {
				log.Printf("Expected error %v", err)
			} else {
				t.Fatalf("Unexpected error %v. Expected %v\n", err, testcase.errStr)
			}
		}

	}
}

var tenantAwarePlannerDefragFuncTestCases = []tenantAwarePlannerRebalFuncTestCase{

	{
		"Rebalance - 2 Subclusters, 1 empty, 1 Above HWM",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_a.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_a_out.json",
		"",
		false,
	},
	{
		"Rebalance - 2 Subclusters, 1 below LWM, 1 above HWM",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_b.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_b_out.json",
		"",
		false,
	},
	{
		"Rebalance - 3 Subclusters, 1 empty, 1 Above HWM, 1 below LWM",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_c.json",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_c_out.json",
		"",
		false,
	},
	{
		"Rebalance - 2 Subclusters, 1 above LWM/below HWM, 1 empty",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_d.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_d_out.json",
		"",
		false,
	},
	{
		"Rebalance - 2 Subclusters, Both above LWM/below HWM",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_e.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_e_out.json",
		"",
		false,
	},
	{
		"Rebalance - 2 Subclusters, 1 empty, 1 Above HWM (partial replica repair)",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_f.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_f_out.json",
		"",
		false,
	},
	{
		"Rebalance - 2 Subclusters, 1 empty, 1 Above HWM(full replica repair)",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_g.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_g_out.json",
		"",
		false,
	},
	{
		"ScaleIn- 2 Subclusters, Both below LWM, Positive Case",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_h.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_h_out.json",
		"",
		false,
	},
	{
		"ScaleIn- 2 Subclusters, One below LWM/ 1 Empty",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_i.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_i_out.json",
		"",
		false,
	},
	{
		"ScaleIn- 3 Subclusters, One above HWM, one below LWM and 1 Empty. No ScaleIn.",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_j.json",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_j_out.json",
		"",
		false,
	},
	{
		"ScaleIn- 3 Subclusters, One above HWM, one below LWM and 1 Empty. ScaleIn. ",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_k.json",
		"../testdata/planner/tenantaware/topology/defrag/6_non_empty_nodes_3_sg_k_out.json",
		"",
		false,
	},
	{
		"Rebalance - 1 Subcluster, Below HWM (partial replica repair)",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_l.json",
		"../testdata/planner/tenantaware/topology/defrag/4_non_empty_nodes_3_sg_l_out.json",
		"",
		false,
	},
}

func tenantAwarePlannerDefragTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerDefragFuncTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		result, err := planner.ReadDefragUtilStats(testcase.result)
		s := planner.NewSimulator()
		defragUtilStats, err := s.RunSingleTestDefragUtil(plan)
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)

		err = validateDefragUtilStats(t, defragUtilStats, result)
		if err != nil {
			log.Printf("Actual Stats %v\n", defragUtilStats)
			log.Printf("Expected Stats %v\n", result)
		}
		FailTestIfError(err, "Error in RunSingleTestRebalance", t)
	}
}

func validateDefragUtilStats(t *testing.T, defragUtilStats map[string]map[string]interface{},
	result map[string]map[string]interface{}) error {

	if len(defragUtilStats) != len(result) {
		return errors.New(fmt.Sprintf("Mismatch in indexer node count."+
			"Solution %v Expected %v", len(defragUtilStats), len(result)))
	}

	for node, stats := range defragUtilStats {

		var rstats map[string]interface{}
		var ok bool
		if rstats, ok = result[node]; !ok {
			return errors.New(fmt.Sprintf("Missing node %v stats in result", node))
		}
		for sname, sval := range stats {
			var rval interface{}
			if rval, ok = rstats[sname]; !ok {
				return errors.New(fmt.Sprintf("Missing stat %v for node %v in result", sname, node))
			}
			if int64(sval.(uint64)) != int64(rval.(float64)) {
				return errors.New(fmt.Sprintf("Mismatch in node %v stat %v. Expected %v. Actual %v", node, sname, rval, sval))
			}
		}
	}
	return nil

}

type tenantAwarePlannerResumeTestCase struct {
	comment  string
	topology string
	tenant   string
	result   string
	errStr   string
}

var tenantAwarePlannerResumeTestCases = []tenantAwarePlannerResumeTestCase{

	{
		"Resume - 1 tenant, Empty node in cluster.",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_a.json",
		"../testdata/planner/tenantaware/topology/resume/resume_tenant_a.json",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_a_out.json",
		"",
	},
	{
		"Resume - 1 tenant, No empty node in cluster.",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_b.json",
		"../testdata/planner/tenantaware/topology/resume/resume_tenant_b.json",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_b_out.json",
		"",
	},
	{
		"Resume - 1 tenant, No node below LWM in the cluster.",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_c.json",
		"../testdata/planner/tenantaware/topology/resume/resume_tenant_c.json",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_c_out.json",
		"No SubCluster Below Low Usage Threshold",
	},
	{
		"Resume - 1 tenant, Not enough capacity in the cluster.",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_d.json",
		"../testdata/planner/tenantaware/topology/resume/resume_tenant_d.json",
		"../testdata/planner/tenantaware/topology/resume/resume_cluster_d_out.json",
		"Not Enough Capacity To Place Tenant",
	},
}

func tenantAwarePlannerResumeTests(t *testing.T) {

	for _, testcase := range tenantAwarePlannerResumeTestCases {
		log.Printf("-------------------------------------------")
		log.Printf(testcase.comment)

		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)

		tenant, err := planner.ReadPlan(testcase.tenant)
		FailTestIfError(err, "Fail to read index spec", t)

		s := planner.NewSimulator()
		solution, err := s.RunSingleTestTenantAwarePlanForResume(plan, tenant.Placement)

		if testcase.errStr == "" {

			FailTestIfError(err, "Error in tenantAwarePlannerResumeTests ", t)
			result, err := planner.ReadPlan(testcase.result)
			err = validateTenantAwareRebalanceSolution(t, solution, result, plan.DeletedNodes, false)
			if err != nil {
				log.Printf("Actual Solution \n")
				solution.PrintLayout()
				log.Printf("Expected Result %v\n", result)
			}
			FailTestIfError(err, "Error in RunSingleTestRebalance", t)
		} else {

			FailTestIfNoError(err, "Error in RunSingleTestRebalance", t)
			if strings.Contains(err.Error(), testcase.errStr) {
				log.Printf("Expected error %v", err)
			} else {
				t.Fatalf("Unexpected error %v. Expected %v\n", err, testcase.errStr)
			}
		}
	}

}

type shardAssignmentTestCase struct {
	testId   string
	comment  string
	topology string
}

var shardAssignmentTestCases = []shardAssignmentTestCase{
	// Place single index instace
	{
		"2_nodes_1_new_index",
		"\t Place single new instance on a node.\n " +
			"\t\t\t New alternate shard Ids are to be generated for the index",
		"../testdata/planner/shard_assignment/2_nodes_1_new_index.json",
	},
	{
		"2_nodes_2_new_index",
		"\t Place two new instance on a node.\n " +
			"\t\t\t New alternate shard Ids has to be generated for 2 indexes ",
		"../testdata/planner/shard_assignment/2_nodes_2_new_index.json",
	},
	{
		"2_nodes_3_new_index",
		"\t Place three new instance on a node.\n" +
			"\t\t\t New alternate shard Ids has to be generated for 2 indexes\n" +
			"\t\t\t as the minShardCapacity of the node is 4. For third index,\n" +
			"\t\t\t the alternate shardIds of existing indexes are to be re-used ",
		"../testdata/planner/shard_assignment/2_nodes_3_new_index.json",
	},
	{
		"2_nodes_1_new_index_node_full",
		"\t Place an index on a node which has reached it's minimum shard capacity\n" +
			"\t\t\t New index has to re-use the shardIds of 'index1' as its disk size is less",
		"../testdata/planner/shard_assignment/2_nodes_1_new_index_node_full.json",
	},
	{
		"2_nodes_1_new_index_2_replicas",
		"\t Place one index with two replica instance on two nodes.\n " +
			"\t\t\t New alternate shard Ids are to be generated for both instances.\n" +
			"\t\t\t The slotIds should be the same for replica instances but\n" +
			"\t\t\t slot replica numbers should be different ",
		"../testdata/planner/shard_assignment/2_nodes_1_new_index_2_replicas.json",
	},
	{
		"2_nodes_2_new_index_2_replicas",
		"\t Place two indexes with 2 replica instances on two nodes.\n" +
			"\t\t\t New alternate shard Ids are to be generated for both instances\n." +
			"\t\t\t The slotIds should be the same for replica instances but\n" +
			"\t\t\t slot replica numbers should be different. Different indexes will have \n" +
			"\t\t\t different slot Ids",
		"../testdata/planner/shard_assignment/2_nodes_2_new_index_2_replicas.json",
	},
	{
		"2_nodes_3_new_index_2_replicas",
		"\t Place three indexes with 2 replica instances on two nodes.\n" +
			"\t\t\t New alternate shard Ids are to be generated for two indexes.\n" +
			"\t\t\t For third index, slotIds should be reused from existing index\n" +
			"\t\t\t The replicaIds of the new index should match the shard replica Ids.",
		"../testdata/planner/shard_assignment/2_nodes_3_new_index_2_replicas.json",
	},
	{
		"5_nodes_1_new_index_3_replicas_2_full_nodes",
		"\t Place one index with 3 replica instances on three nodes.\n" +
			"\t\t\t Of these 3 nodes, 2 nodes have reached their shard capacity.\n" +
			"\t\t\t Planner should use the shardId present on both nodes and create\n" +
			"\t\t\t the same slotId on third node with a different replicaId",
		"../testdata/planner/shard_assignment/5_nodes_1_new_index_3_replicas_2_full_nodes.json",
	},
	{
		"5_nodes_1_new_index_3_replicas_prune_replica_shard",
		"\t Place one index with 3 replica instances on three nodes.\n" +
			"\t\t\t Of these 3 nodes, 2 nodes have 2 shard pairs each\n" +
			"\t\t\t Of these, planner can not use one as it has a replica 2 on a \n" +
			"\t\t\t that is beyond the initially decided placement\n " +
			"\t\t\t (even though it has less disk usage)",
		"../testdata/planner/shard_assignment/5_nodes_1_new_index_3_replicas_prune_replica_shard.json",
	},
	{
		"5_nodes_1_new_index_3_replicas_different_replica_order",
		"\tPlace one index with 3 replica instances on three nodes.\n" +
			"\t\t\t Node-1 has a shard with replica:2, node-1 has same shard with\n" +
			"\t\t\t replicaId:0, node-3 with replicaId: 1. Planner has decided to \n" +
			"\t\t\t place a new index with replicaId: 0 on node-1 and replicaId: 1\n" +
			"\t\t\t on node-2. The replica ordering of planner should not matter and the\n" +
			"\t\t\t final placement for new index should match the shard replica placement",
		"../testdata/planner/shard_assignment/5_nodes_1_new_index_3_replicas_different_replica_order.json",
	},
	{"5_nodes_1_new_index_more_shard_replicas",
		"\t A shard exists on 3 nodes and a new index is being placed on a node which has\n" +
			"\t\t\t 2 of the replicas of the shard. The other 2 indexers have maxed out on capacity\n" +
			"\t\t\t In such case, planner will re-use the shard even though it has more replicas outside\n" +
			"\t\t\t the planned nodes",
		"../testdata/planner/shard_assignment/5_nodes_1_new_index_more_shard_replicas.json",
	},
	{"5_nodes_1_new_partn_index_slot_grouping",
		"\t A slot (1234) exists on nodes n1, n2, n3, n4 with replica Ids: 0, 1, 2, 3 respectively. \n" +
			"\t\t\t A new partitioned index with 2 replicas and 3 partitions is being placed on these nodes\n" +
			"\t\t\t The partn distribution is: partnId: 1 -> n2, n3. partnId 2 -> n3, n4. partnId 3 -> n1, n4 \n" +
			"\t\t\t During slot assignment, the 1234 slot should not be used for the new partitioned index \n" +
			"\t\t\t as there is no placement on nodes n1, n2 for any partition",
		"../testdata/planner/shard_assignment/5_nodes_1_new_partn_index_slot_grouping.json",
	},
}

func validateShardIds(solution *planner.Solution, expectedShards int, t *testing.T) {
	shardMap := make(map[string]bool)
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if len(index.AlternateShardIds) == 0 {
				t.Fatalf("Alternate shardIds are empty for index: %v, node: %v", index.Name, indexer.NodeId)
			}
			var msAltId, bsAltId *common.AlternateShardId
			var err error
			msAltId, err = common.ParseAlternateId(index.AlternateShardIds[0])
			FailTestIfError(err, fmt.Sprintf("Error parsing mainstore alternateId: %v", index.AlternateShardIds[0]), t)
			if !index.IsPrimary {
				bsAltId, err = common.ParseAlternateId(index.AlternateShardIds[1])
				FailTestIfError(err, fmt.Sprintf("Error parsing backstore alternateId: %v", index.AlternateShardIds[1]), t)
				if msAltId.SlotId != bsAltId.SlotId || msAltId.ReplicaId != bsAltId.ReplicaId {
					t.Fatalf("Mismatch in slot or replicaId between mainstore and backstore. "+
						"Mainstore altId: %v, backstore altId: %v", msAltId.String(), bsAltId.String())
				}
			}
			for _, altId := range index.AlternateShardIds {
				shardMap[altId] = true
			}
		}
	}
	if len(shardMap) != expectedShards {
		t.Fatalf("Expected %v alternateShardIds, seeing only: %v. Actual: %v",
			expectedShards, len(shardMap), shardMap)
	}
}
func validateSlotMapping(newIndexes []*planner.IndexUsage, expectedSlots, expectedReplicas int, expectedGroups map[int]bool, t *testing.T) {
	slotMap := make(map[uint64]map[int]int) // slot to replica mapping
	for _, indexUsage := range newIndexes {
		if len(indexUsage.AlternateShardIds) == 0 {
			t.Fatalf("Alternate shardIds are empty for index: %v, replicaId: %v",
				indexUsage.Name, indexUsage.Instance.ReplicaId)
		}
		var msAltId, bsAltId *common.AlternateShardId
		var err error
		msAltId, err = common.ParseAlternateId(indexUsage.AlternateShardIds[0])
		FailTestIfError(err, fmt.Sprintf("Error parsing mainstore alternateId: %v", indexUsage.AlternateShardIds[0]), t)
		if _, ok := slotMap[msAltId.SlotId]; !ok {
			slotMap[msAltId.SlotId] = make(map[int]int)
		}
		slotMap[msAltId.SlotId][int(msAltId.ReplicaId)]++
		bsAltId, err = common.ParseAlternateId(indexUsage.AlternateShardIds[1])
		FailTestIfError(err, fmt.Sprintf("Error parsing backstore alternateId: %v", indexUsage.AlternateShardIds[1]), t)
		slotMap[bsAltId.SlotId][int(msAltId.ReplicaId)]++
	}
	if len(slotMap) != expectedSlots {
		t.Fatalf("Expected %v slots but found a mismatch. SlotMap: %v", expectedSlots, slotMap)
	}
	for slotId, replicaCount := range slotMap {
		if len(replicaCount) != expectedReplicas {
			t.Fatalf("Expected %v replicas found a mismatch. SlotId: %v, ReplicaCount: %v",
				expectedReplicas, slotId, expectedReplicas)
		}
		for replicaId, groupCount := range replicaCount {
			if _, ok := expectedGroups[groupCount]; !ok {
				t.Fatalf("Expected %v groups but found a mismatch. SlotMap: %v, slotId: %v, replicaId: %v",
					expectedGroups, slotMap, slotId, replicaId)
			}
		}
	}
}
func TestShardAssignmentFuncTestCases(t *testing.T) {

	for _, testcase := range shardAssignmentTestCases {
		log.Printf("---------------------------------------------------")
		log.Printf("===========%v===========", testcase.testId)
		log.Printf(testcase.comment)
		plan, err := planner.ReadPlan(testcase.topology)
		FailTestIfError(err, "Fail to read plan", t)
		var newIndexes []*planner.IndexUsage
		for _, indexer := range plan.Placement {
			if indexer.NodeVersion < common.INDEXER_76_VERSION {
				continue
			}
			for _, index := range indexer.Indexes {
				if len(index.AlternateShardIds) == 0 { // For a newly created index, alternate shardIds will be empty
					newIndexes = append(newIndexes, index)
				}
			}
		}
		if len(newIndexes) == 0 {
			t.Fatalf("New indexes are empty - Expected non-zero indexes")
		}
		for _, indexer := range plan.Placement {
			indexer.Indexes, indexer.NumShards, _ = planner.GroupIndexes(indexer.Indexes, indexer, true)
		}
		// Test the index-shard assigment function
		solution := planner.SolutionFromPlan2(plan)
		planner.PopulateAlternateShardIds(solution, newIndexes, 2560*1024*1024)
		// Test validation
		switch testcase.testId {
		// For this test, the new index that got created should have non-empty shardIds
		case "2_nodes_1_new_index":
			validateShardIds(solution, 2, t)
			break
		// For these tests, there should be 4 different alterante shardIds in total
		// as minShardCapacity is defined as 4
		case "2_nodes_2_new_index", "2_nodes_3_new_index":
			// For this test, both the new index that got created should have non-empty shardIds
			// Also, there should be 4 alternate shardIds
			validateShardIds(solution, 4, t)
			break
		// For this test, the newly created index should re-use the shardIds of index1
		// as it has less disk size
		case "2_nodes_1_new_index_node_full":
			validateShardIds(solution, 4, t)
			var index3AltIds []string
			expectedShardIds := []string{"1234-0-0", "1234-0-1"}
			for _, indexUsage := range newIndexes {
				if indexUsage.Name == "index3" {
					index3AltIds = indexUsage.AlternateShardIds
					break
				}
			}
			if len(index3AltIds) == 0 || expectedShardIds[0] != index3AltIds[0] || expectedShardIds[1] != index3AltIds[1] {
				t.Fatalf("Mismatch in alternateID assignment. Expected: %v, Actual: %v", expectedShardIds, index3AltIds)
			}
			break
		case "2_nodes_1_new_index_2_replicas":
			// 1 slot with 2 replica numbers and 2 group count
			validateSlotMapping(newIndexes, 1, 2, map[int]bool{2: true}, t)
			break
		case "2_nodes_2_new_index_2_replicas":
			// 2 slot with 2 replica numbers and 4 group count
			validateSlotMapping(newIndexes, 2, 2, map[int]bool{2: true}, t)
			break
		case "2_nodes_3_new_index_2_replicas":
			// 2 slot with 2 replica numbers and 6 group count
			validateSlotMapping(newIndexes, 2, 2, map[int]bool{2: true, 4: true}, t)
			break

		case "5_nodes_1_new_index_3_replicas_2_full_nodes",
			"5_nodes_1_new_index_3_replicas_prune_replica_shard",
			"5_nodes_1_new_index_3_replicas_different_replica_order",
			"5_nodes_1_new_index_more_shard_replicas":
			expectedPlacement := make(map[string][]string)
			if testcase.testId == "5_nodes_1_new_index_3_replicas_2_full_nodes" {
				validateShardIds(solution, 6, t)
				expectedPlacement["127.0.0.1:9001"] = []string{"1234-0-0", "1234-0-1"}
				expectedPlacement["127.0.0.1:9002"] = []string{"1234-1-0", "1234-1-1"}
				expectedPlacement["127.0.0.1:9003"] = []string{"1234-2-0", "1234-2-1"}
			} else if testcase.testId == "5_nodes_1_new_index_3_replicas_prune_replica_shard" {
				validateShardIds(solution, 12, t)
				expectedPlacement["127.0.0.1:9001"] = []string{"1234-0-0", "1234-0-1"}
				expectedPlacement["127.0.0.1:9002"] = []string{"1234-1-0", "1234-1-1"}
				expectedPlacement["127.0.0.1:9003"] = []string{"1234-2-0", "1234-2-1"}
			} else if testcase.testId == "5_nodes_1_new_index_3_replicas_different_replica_order" {
				validateShardIds(solution, 6, t)
				expectedPlacement["127.0.0.1:9001"] = []string{"1234-2-0", "1234-2-1"}
				expectedPlacement["127.0.0.1:9002"] = []string{"1234-0-0", "1234-0-1"}
				expectedPlacement["127.0.0.1:9003"] = []string{"1234-1-0", "1234-1-1"}
			} else if testcase.testId == "5_nodes_1_new_index_more_shard_replicas" {
				expectedPlacement["127.0.0.1:9002"] = []string{"1234-0-0", "1234-0-1"}
				expectedPlacement["127.0.0.1:9003"] = []string{"1234-1-0", "1234-1-1"}
			}

			for _, indexer := range solution.Placement {
				for _, indexUsage := range indexer.Indexes {
					if indexUsage.Name == "index2" {
						nodeId := indexer.NodeId
						if nodeId != "127.0.0.1:9001" && nodeId != "127.0.0.1:9002" && nodeId != "127.0.0.1:9003" {
							continue
						}
						if indexUsage.AlternateShardIds[0] != expectedPlacement[nodeId][0] || indexUsage.AlternateShardIds[1] != expectedPlacement[nodeId][1] {
							t.Fatalf("Mismatch in alternateID assignment for node: %v. Expected: %v, Actual: %v",
								indexer.NodeId, expectedPlacement[nodeId], indexUsage.AlternateShardIds)
						}
					}
				}
			}

			break

		case "5_nodes_1_new_partn_index_slot_grouping":
			for _, indexer := range solution.Placement {
				for _, indexUsage := range indexer.Indexes {
					if indexUsage.Name == "index2" {

						msAltId, err := common.ParseAlternateId(indexUsage.AlternateShardIds[0])
						FailTestIfError(err, "Error observed when parsing mainstore alternate shardId", t)

						if msAltId.SlotId == 1234 {
							t.Fatalf("Mismatch in mainstore alternateID assignment for node: %v. Actual: %v can not have 1234 slot",
								indexer.NodeId, indexUsage.AlternateShardIds)
						}

						bsAltId, err := common.ParseAlternateId(indexUsage.AlternateShardIds[0])
						FailTestIfError(err, "Error observed when parsing backstore alternate shardId", t)

						if bsAltId.SlotId == 1234 {
							t.Fatalf("Mismatch in backstore alternateID assignment for node: %v. Actual: %v can not have 1234 slot",
								indexer.NodeId, indexUsage.AlternateShardIds)
						}
					}
				}
			}
			break
		}
	}
}
