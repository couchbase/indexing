// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package functionaltests

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/planner"
	"log"
	"math"
	"testing"
	"time"
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
	{"rebalance - 8 identical index, delete 2, 2x", 2, 2, "../testdata/planner/plan/identical-8-0.json", 0, 0, 2, 0, 0},
	{"rebalance - drop replcia - 3 replica, 3 zone, delete 1, 2x", 2, 2, "../testdata/planner/plan/replica-3-zone.json", 0, 0, 1, 0, 0},
	{"rebalance - rebuid replica - 3 replica, 3 zone, add 1, delete 1, 1x", 1, 1, "../testdata/planner/plan/replica-3-zone.json", 0, 1, 1, 0, 0},
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
	},
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
	},
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
}

//
// This test randomly generated a set of index and place them on a single indexer node.
// The placement algorithm will expand the cluster (by adding node) until every node
// is under cpu and memory quota.   This test will check if the indexer cpu and
// memory deviation is less than 10%.
//
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

		p, _, err := s.RunSingleTest(config, planner.CommandPlan, spec, nil, indexSpecs)
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.Result.ComputeMemUsage()
		cpuMean, cpuDev := p.Result.ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.Result.PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.Result); err != nil {
			t.Fatal(err)
		}
	}
}

//
// This test starts with an initial index layout with a fixed number of nodes (plan).
// It then places a set of same size index onto these indexer nodes where number
// of new index is equal to the number of indexer nodes.  There should be one index
// on each indexer node.
//
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

		p, _, err := s.RunSingleTest(config, planner.CommandPlan, nil, plan, indexSpecs)
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.Result.ComputeMemUsage()
		cpuMean, cpuDev := p.Result.ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.Result.PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.Result); err != nil {
			t.Fatal(err)
		}
	}
}

//
// This test planner to rebalance indexer nodes:
// 1) rebalance after randomly shuffle a certain percentage of indexes
// 2) rebalance after swap in/out of indexer nodes
//
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

		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		p.PrintCost()

		memMean, memDev := p.Result.ComputeMemUsage()
		cpuMean, cpuDev := p.Result.ComputeCpuUsage()

		if memDev/memMean > testcase.memScore || math.Floor(cpuDev/cpuMean) > testcase.cpuScore {
			p.Result.PrintLayout()
			t.Fatal("Score exceed acceptance threshold")
		}

		if err := planner.ValidateSolution(p.Result); err != nil {
			t.Fatal(err)
		}
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		// check if the indexers have equal number of indexes
		if len(p.Result.Placement[0].Indexes) != len(p.Result.Placement[1].Indexes) {
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		// check if index violates memory constraint
		for _, indexer := range p.Result.Placement {
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		// check if the indexers have equal number of indexes
		if len(p.Result.Placement[0].Indexes) != len(p.Result.Placement[1].Indexes) {
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		// check the total number of indexes
		for _, indexer := range p.Result.Placement {
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		// check the total number of indexes
		total := 0
		for _, indexer := range p.Result.Placement {
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		if count1 != len(p.Result.Placement[0].Indexes) {
			t.Fatal(fmt.Sprintf("Index count for indexer1 has changed %v != %v", count1, len(p.Result.Placement[0].Indexes)))
		}

		if count2 != len(p.Result.Placement[1].Indexes) {
			t.Fatal(fmt.Sprintf("Index count for indexer2 has changed %v != %v", count2, len(p.Result.Placement[1].Indexes)))
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
		p, _, err := s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
		FailTestIfError(err, "Error in planner test", t)

		if len(p.Result.Placement) != 1 {
			t.Fatal("There is more than 1 node after rebalance-out")
		}

		count2 := 0
		for _, indexer := range p.Result.Placement {
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
		p, _, err := s.RunSingleTest(config, planner.CommandPlan, nil, plan, []*planner.IndexSpec{&spec})
		FailTestIfError(err, "Error in planner test", t)

		found := false
		for _, indexer := range p.Result.Placement {
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
		_, _, err = s.RunSingleTest(config, planner.CommandRebalance, nil, plan, nil)
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
		p, _, err := s.RunSingleTest(config, planner.CommandPlan, nil, plan, []*planner.IndexSpec{&spec})
		FailTestIfError(err, "Error in planner test", t)

		success := true
		for _, indexer := range p.Result.Placement {
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

//
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
//
func iterationTest(t *testing.T) {
	for _, testcase := range iterationTestCases {
		var p *planner.SAPlanner

		log.Printf("-------------------------------------------")
		log.Printf("iterationTest :: %v", testcase.comment)

		if testcase.action == "Remove 1" {
			config := planner.DefaultRunConfig()

			s := planner.NewSimulator()
			spec, err := s.ReadWorkloadSpec(testcase.topoSpec)
			FailTestIfError(err, "Fail to read workload spec", t)

			plan, err := planner.ReadPlan(testcase.indexers)
			FailTestIfError(err, "Fail to read plan", t)

			p, _, err = s.RunSingleTest(config, planner.CommandPlan, spec, plan, nil)
			FailTestIfError(err, "Error in planner test", t)

			if err := planner.ValidateSolution(p.Result); err != nil {
				t.Fatal(err)
			}
		}

		// p.Print()

		var newPlan *planner.Plan

		switch testcase.action {

		case "Remove 1":
			p.Result.Placement[0].MarkDeleted()
			newPlan = &planner.Plan{
				Placement: p.Result.Placement,
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
		p1, _, err := s1.RunSingleTest(config1, planner.CommandRebalance, nil, newPlan, nil)

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
			count := 0
			for _, node := range p1.Result.Placement {
				count += len(node.Indexes)
			}

			if testcase.success {
				if count != 60 {
					t.Fatal(fmt.Errorf("Error: expected ccounut %v, actual count %v", 60, count))
				}
			} else {
				if count != 30 {
					t.Fatal(fmt.Errorf("Error: expected ccounut %v, actual count %v", 30, count))
				}
			}
		}
	}
}
