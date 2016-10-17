// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package planner

import (
	"flag"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"strings"
	"testing"
)

//////////////////////////////////////////////////////////////
// Global Variable
/////////////////////////////////////////////////////////////

var gIteration int
var gDetail bool
var gMemQuotaFactor float64
var gCpuQuotaFactor float64
var gWorkloadSpec string
var gPlan string
var gIndexSpecs string
var gClusterUrl string
var gUsername string
var gPassword string
var gOutput string
var gResize bool
var gLogLevel string
var gMaxNumNode int
var gShuffle int
var gAllowMove bool
var gAllowSwap bool
var gAllowUnpin bool
var gCommand string
var gAddNode int
var gDeleteNode int
var gMaxCpuUse int
var gMaxMemUse int
var gMemQuota string
var gCpuQuota int
var gDataCostWeight float64
var gCpuCostWeight float64
var gMemCostWeight float64

//////////////////////////////////////////////////////////////
// Manual Simulation Test
/////////////////////////////////////////////////////////////

func init() {
	flag.IntVar(&gIteration, "iteration", 100, "num of iteration per simulation")
	flag.BoolVar(&gDetail, "layout", false, "print simulation result with index layout plan")
	flag.StringVar(&gLogLevel, "logLevel", "INFO", "log level")
	flag.StringVar(&gOutput, "output", "", "file for saving simultation result as index layout plan")

	// command + index specification
	flag.StringVar(&gCommand, "command", "", "command = plan, rebalance")
	flag.StringVar(&gWorkloadSpec, "workload", "", "file for workload specification")
	flag.StringVar(&gPlan, "plan", "", "file for initial index layout in the cluster")
	flag.StringVar(&gClusterUrl, "cluster", "", "url for the cluster")
	flag.StringVar(&gIndexSpecs, "indexes", "", "file for list of indexes for placement")
	flag.StringVar(&gUsername, "username", "", "admin user for the cluster")
	flag.StringVar(&gPassword, "password", "", "admin password for the cluster")

	// quota
	flag.Float64Var(&gMemQuotaFactor, "memCapacity", 1.0, "adjust memory quota by multipling capacity")
	flag.Float64Var(&gCpuQuotaFactor, "cpuCapacity", 1.0, "adjust cpu quota by multiplying capacity")
	flag.IntVar(&gMaxCpuUse, "maxCpuUse", -1, "maximum cpu utilization (as percentage) per indexer node")
	flag.IntVar(&gMaxMemUse, "maxMemUse", -1, "maximum memory utilization (as percentage) per indexer node")
	flag.StringVar(&gMemQuota, "memQuota", "", "memory quota per indexer node")
	flag.IntVar(&gCpuQuota, "cpuQuota", -1, "cpu quota per indexer node")

	// cluster size
	flag.BoolVar(&gResize, "resize", true, "allow new node to be dynamcially added to cluster while running the planner")
	flag.IntVar(&gMaxNumNode, "maxNumNode", int(math.MaxInt16), "max number of indexer node to use during simulation")
	flag.IntVar(&gAddNode, "addNode", 0, "number of indexer to add before running the planner")
	flag.IntVar(&gDeleteNode, "deleteNode", 0, "number of indexer to delete before running the planner")

	// rebalance
	flag.IntVar(&gShuffle, "shuffle", 0, "percentage of index to shuffle in the initial index layout. Use with arugment 'plan'.")
	flag.BoolVar(&gAllowSwap, "allowSwap", true, "flag to tell if planner can swap index between nodes during planning.")
	flag.BoolVar(&gAllowUnpin, "allowUnpin", false, "flag to tell if planner should ignore pinned index during rebalancing.")

	// placement
	flag.BoolVar(&gAllowMove, "allowMove", false, "flag to tell if planner can move existing index (on initial layout) when placing new index.")

	// cost
	flag.Float64Var(&gDataCostWeight, "dataCostWeight", 1, "Adjusted weight for data movement cost.")
	flag.Float64Var(&gCpuCostWeight, "cpuCostWeight", 1, "Adjusted weight for cpu usage cost.")
	flag.Float64Var(&gMemCostWeight, "memCostWeight", 1, "Adjusted weight for mem usage cost.")
}

func TestSimulation(t *testing.T) {
	flag.Parse()

	logging.SetLogLevel(logging.Level(strings.ToUpper(gLogLevel)))

	logging.Infof("TestSimulation: start")

	s := NewSimulator()

	spec, err := s.ReadWorkloadSpec(gWorkloadSpec)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := ReadPlan(gPlan)
	if err != nil {
		t.Fatal(err)
	}

	indexSpecs, err := ReadIndexSpecs(gIndexSpecs)
	if err != nil {
		t.Fatal(err)
	}

	if gClusterUrl != "" {

		_, err := cbauth.InternalRetryDefaultInit(gClusterUrl, gUsername, gPassword)
		if err != nil {
			logging.Fatalf("cbauth initialization fails. err = %s", gClusterUrl, err)
			return
		}

		plan, err = RetrievePlanFromCluster(gClusterUrl)
		if err != nil {
			t.Fatal("Unable to read index layout from cluster %v. err = %s", gClusterUrl, err)
		}
	}

	if spec == nil && plan == nil && indexSpecs == nil {
		spec = s.defaultWorkloadSpec()
	}

	if spec != nil {
		logging.Infof("******* WORKLOAD SPEC *****")
		s.printWorkloadSpec(spec)
	}

	if plan != nil {
		logging.Infof("******* PLAN SUMMARY *****")
		printPlanSummary(plan)
	}

	if gCommand == CommandRebalance {
		gResize = false
	}

	config := &RunConfig{
		Detail:         gDetail,
		MemQuotaFactor: gMemQuotaFactor,
		CpuQuotaFactor: gCpuQuotaFactor,
		Resize:         gResize,
		MaxNumNode:     gMaxNumNode,
		Output:         gOutput,
		Shuffle:        gShuffle,
		AllowMove:      gAllowMove,
		AllowSwap:      gAllowSwap,
		AddNode:        gAddNode,
		DeleteNode:     gDeleteNode,
		MaxMemUse:      gMaxMemUse,
		MaxCpuUse:      gMaxCpuUse,
		MemQuota:       parseMemoryStr(t, gMemQuota),
		CpuQuota:       gCpuQuota,
		DataCostWeight: gDataCostWeight,
		CpuCostWeight:  gCpuCostWeight,
		MemCostWeight:  gMemCostWeight,
		AllowUnpin:     gAllowUnpin,
	}

	if err := s.RunSimulation(gIteration, config, CommandType(gCommand), spec, plan, indexSpecs); err != nil {
		t.Fatal(err)
	}
}

//////////////////////////////////////////////////////////////
// Utility
/////////////////////////////////////////////////////////////

func parseMemoryStr(t *testing.T, mem string) int64 {

	memQ, err := ParseMemoryStr(mem)
	if err != nil {
		t.Fatal(err)
	}

	return memQ
}
