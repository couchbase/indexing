// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

/******************* SAMPLE WORKLOAD *****************

{
"name"     	: "Planner Simulation 1",
"comment"   : "Sample",
"workload" 	: [
    {
        "name"      : "bucket1",
        "replica"   : 3,
        "workload"  : [
        {
            "name"              : "small",
            "minNumDoc"         : 500000,
            "maxNumDoc"         : 100000000,
            "minDocKeySize"     : 20,
            "maxDocKeySize"     : 200,
            "minSecKeySize"     : 20,
            "maxSecKeySize"     : 1000,
            "minArrKeySize"     : 20,
            "maxArrKeySize"     : 200,
            "minArrSize"        : 10,
            "maxArrSize"        : 100,
            "minMutationRate"   : 10000,
            "maxMutationRate"   : 300000,
            "minScanRate"       : 1000,
            "maxScanRate"       : 50000
        }],
        "distribution"   : [100]
    }],
"distribution" 	: [100],
"minNumIndex"   : 5,
"maxNumIndex"   : 100
}

******************* SAMPLE WORKLOAD *****************/

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
/////////////////////////////////////////////////////////////

type simulator struct {
	rs *rand.Rand
}

type WorkloadSpec struct {
	Name         string        `json:"name,omitempty"`
	Comment      string        `json:"comment,omitempty"`
	Workload     []*BucketSpec `json:"workload,omitempty"`
	Distribution []int64       `json:"distribution,omitempty"`
	MinNumIndex  int64         `json:"minNumIndex,omitempty"`
	MaxNumIndex  int64         `json:"maxNumIndex,omitempty"`
}

type BucketSpec struct {
	Name         string            `json:"name,omitempty"`
	Replica      int64             `json:"replica,omitempty"`
	Workload     []*CollectionSpec `json:"workload,omitempty"`
	Distribution []int64           `json:"distribution,omitempty"`
}

type CollectionSpec struct {
	Name            string `json:"name,omitempty"`
	MinNumDoc       int64  `json:"minNumDoc,omitempty"`
	MaxNumDoc       int64  `json:"maxNumDoc,omitempty"`
	MinDocKeySize   int64  `json:"minDocKeySize,omitempty"`
	MaxDocKeySize   int64  `json:"maxDocKeySize,omitempty"`
	MinSecKeySize   int64  `json:"minSecKeySize,omitempty"`
	MaxSecKeySize   int64  `json:"maxSecKeySize,omitempty"`
	MinArrKeySize   int64  `json:"minArrKeySize,omitempty"`
	MaxArrKeySize   int64  `json:"maxArrKeySize,omitempty"`
	MinArrSize      int64  `json:"minArrSize,omitempty"`
	MaxArrSize      int64  `json:"maxArrSize,omitempty"`
	MinMutationRate int64  `json:"minMutationRate,omitempty"`
	MaxMutationRate int64  `json:"maxMutationRate,omitempty"`
	MinScanRate     int64  `json:"minScanRate,omitempty"`
	MaxScanRate     int64  `json:"maxScanRate,omitempty"`
}

//////////////////////////////////////////////////////////////
// Simulation
/////////////////////////////////////////////////////////////

func NewSimulator() *simulator {

	s := &simulator{
		rs: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	return s
}

func (t *simulator) RunSimulation(count int, config *RunConfig, command CommandType, spec *WorkloadSpec, plan *Plan, indexSpecs []*IndexSpec) error {

	var cost float64
	var duration uint64
	var elapsedTime uint64
	var convergenceTime uint64
	var iteration uint64
	var move uint64
	var positiveMove uint64
	var indexSize float64
	var indexSizeDev float64
	var indexCpu float64
	var indexCpuDev float64
	var indexCount uint64
	var memoryQuota uint64
	var cpuQuota uint64
	var indexerSize float64
	var indexerSizeDev float64
	var indexerCpu float64
	var indexerCpuDev float64
	var numOfIndexers float64
	var indexerMemUtil float64
	var indexerCpuUtil float64
	var totalData uint64
	var dataMoved uint64
	var indexMoved uint64
	var indexCanBeMoved uint64
	var initial_score float64
	var initial_indexCount uint64
	var initial_indexerCount uint64
	var initial_avgIndexSize float64
	var initial_stdDevIndexSize float64
	var initial_avgIndexCpu float64
	var initial_stdDevIndexCpu float64
	var initial_avgIndexerSize float64
	var initial_stdDevIndexerSize float64
	var initial_avgIndexerCpu float64
	var initial_stdDevIndexerCpu float64
	var initial_movedIndex uint64
	var initial_movedData uint64
	var startTemp float64
	var startScore float64
	var try uint64
	var needRetry uint64

	detail := config.Detail
	scores := make([]float64, count)

	for i := 0; i < count; i++ {
		p, s, err := t.RunSingleTest(config, command, spec, plan, indexSpecs)
		if err != nil {
			if _, ok := err.(*Violations); ok {
				logging.Infof("Cluster Violations: number of retry %v", p.Try)
				logging.Infof("************ Result *************", i)
				p.Print()
			}
			return err
		}

		if err := ValidateSolution(p.Result); err != nil {
			if detail {
				p.Result.PrintLayout()
			}
			return err
		}

		numOfIndexers += float64(len(p.Result.Placement))

		scores[i] = p.Score
		cost += p.Score
		duration += p.ElapseTime
		elapsedTime += p.ElapseTime
		convergenceTime += p.ConvergenceTime
		iteration += p.Iteration
		move += p.Move
		positiveMove += p.PositiveMove
		startTemp += p.StartTemp
		startScore += p.StartScore
		try += p.Try

		if p.Try != 1 {
			needRetry++
		}

		t1, t2, t3, t4 := p.Result.computeIndexMovement(true)
		totalData += t1
		dataMoved += t2
		indexCanBeMoved += t3
		indexMoved += t4

		sa, sd := p.Result.ComputeMemUsage()
		indexerSize += sa
		indexerSizeDev += sd

		ca, cd := p.Result.ComputeCpuUsage()
		indexerCpu += ca
		indexerCpuDev += cd

		indexSize += s.AvgIndexSize
		indexSizeDev += s.StdDevIndexSize
		indexCpu += s.AvgIndexCpu
		indexCpuDev += s.StdDevIndexCpu
		memoryQuota += s.MemoryQuota
		cpuQuota += s.CpuQuota
		indexCount += s.IndexCount

		initial_score += s.Initial_score
		initial_indexCount += s.Initial_indexCount
		initial_indexerCount += s.Initial_indexerCount
		initial_avgIndexSize += s.Initial_avgIndexSize
		initial_stdDevIndexSize += s.Initial_stdDevIndexSize
		initial_avgIndexCpu += s.Initial_avgIndexCpu
		initial_stdDevIndexCpu += s.Initial_stdDevIndexCpu
		initial_avgIndexerSize += s.Initial_avgIndexerSize
		initial_stdDevIndexerSize += s.Initial_stdDevIndexerSize
		initial_avgIndexerCpu += s.Initial_avgIndexerCpu
		initial_stdDevIndexerCpu += s.Initial_stdDevIndexerCpu
		initial_movedIndex += s.Initial_movedIndex
		initial_movedData += s.Initial_movedData

		indexerMemUtil += sa / float64(s.MemoryQuota)
		indexerCpuUtil += ca / float64(s.CpuQuota)

		if detail {
			logging.Infof("************ Result for Simulation %v *************", i)
			p.Print()
			logging.Infof("****************************************")
		}
	}

	// calculate score variance
	scorev := float64(0)
	scorem := cost / float64(count)
	for i := 0; i < count; i++ {
		v := scores[i] - scorem
		scorev += v * v
	}
	scorev = scorev / float64(count)
	scorev = math.Sqrt(scorev)

	logging.Infof("Aggregated Simulation Result for with No. of Run = %v", count)
	logging.Infof("\taverage score: %v", cost/float64(count))
	logging.Infof("\tstd dev score: %v (%.2f%%)", scorev, scorev/cost*100)
	logging.Infof("\taverage duration: %v", formatTimeStr(duration/uint64(count)))
	logging.Infof("\taverage convergence time: %v", formatTimeStr(convergenceTime/uint64(count)))
	logging.Infof("\taverage no. of moves: %v", move/uint64(count))
	logging.Infof("\taverage no. of positive moves: %v", positiveMove/uint64(count))
	logging.Infof("\taverage no. of iterations: %v", iteration/uint64(count))
	logging.Infof("\taverage no. of try per run: %.2f", float64(try)/float64(count))
	logging.Infof("\tpercentage no. of run needs retry: %.2f%%", float64(needRetry)/float64(count)*100)
	logging.Infof("\taverage start temperature: %v", startTemp/float64(count))
	logging.Infof("\taverage start score: %v", startScore/float64(count))
	logging.Infof("\taverage total index data : %v", formatMemoryStr(totalData/uint64(count)))
	logging.Infof("\taverage index data moved : %v", formatMemoryStr(dataMoved/uint64(count)))
	logging.Infof("\taverage no. index can be moved : %v", formatMemoryStr(indexCanBeMoved/uint64(count)))
	logging.Infof("\taverage no. index moved : %v", formatMemoryStr(indexMoved/uint64(count)))
	logging.Infof("\t--- quota ")
	logging.Infof("\taverage memory quota: %v", formatMemoryStr(memoryQuota/uint64(count)))
	logging.Infof("\taverage cpu quota: %v", cpuQuota/uint64(count))
	logging.Infof("\t--- final layout : indexers stats")
	logging.Infof("\taverage no. of indexers: %v", numOfIndexers/float64(count))
	logging.Infof("\taverage indexer memory: %v", formatMemoryStr(uint64(indexerSize/float64(count))))
	logging.Infof("\taverage indexer memory deviation: %v", formatMemoryStr(uint64(indexerSizeDev/float64(count))))
	logging.Infof("\taverage indexer memory score : %v", indexerSizeDev/indexerSize)
	logging.Infof("\taverage indexer memory utilization : %v%s", formatMemoryStr(uint64(indexerMemUtil/float64(count)*100)), "%")
	logging.Infof("\taverage indexer cpu (core) : %v", indexerCpu/float64(count))
	logging.Infof("\taverage indexer cpu deviation (core) : %v", indexerCpuDev/float64(count))
	logging.Infof("\taverage indexer cpu score : %v", indexerCpuDev/indexerCpu)
	logging.Infof("\taverage indexer cpu utilization (core) : %v%s", uint64(indexerCpuUtil/float64(count)*100), "%")

	if command == CommandPlan {
		logging.Infof("\t--- placement : index stats")
		logging.Infof("\taverage index count: %v", indexCount/uint64(count))
		logging.Infof("\taverage index size: %v", formatMemoryStr(uint64(indexSize/float64(count))))
		logging.Infof("\taverage index size deviation: %v", formatMemoryStr(uint64(indexSizeDev/float64(count))))
		logging.Infof("\taverage index cpu : %v", indexCpu/float64(count))
		logging.Infof("\taverage index cpu deviation: %v", indexCpuDev/float64(count))
	}

	if command == CommandRebalance || (command == CommandPlan && plan != nil) {
		logging.Infof("\t--- initial layout : index stats")
		logging.Infof("\taverage initial score: %v", initial_score/float64(count))
		logging.Infof("\taverage initial index count: %v", initial_indexCount/uint64(count))
		logging.Infof("\taverage initial index size: %v", formatMemoryStr(uint64(initial_avgIndexSize/float64(count))))
		logging.Infof("\taverage initial index size deviation: %v", formatMemoryStr(uint64(initial_stdDevIndexSize/float64(count))))
		logging.Infof("\taverage initial index cpu : %v", initial_avgIndexCpu/float64(count))
		logging.Infof("\taverage initial index cpu deviation: %v", initial_stdDevIndexCpu/float64(count))
		logging.Infof("\t--- initial layout : indexer stats")
		logging.Infof("\taverage initial indexer count: %v", initial_indexerCount/uint64(count))
		logging.Infof("\taverage initial indexer memory: %v", formatMemoryStr(uint64(initial_avgIndexerSize/float64(count))))
		logging.Infof("\taverage initial indexer memory deviation: %v", formatMemoryStr(uint64(initial_stdDevIndexerSize/float64(count))))
		logging.Infof("\taverage initial indexer memory score : %v", initial_stdDevIndexerSize/initial_avgIndexerSize)
		logging.Infof("\taverage initial indexer cpu (core) : %v", initial_avgIndexerCpu/float64(count))
		logging.Infof("\taverage initial indexer cpu deviation (core) : %v", initial_stdDevIndexerCpu/float64(count))
		logging.Infof("\taverage initial indexer cpu score : %v", initial_stdDevIndexerCpu/initial_avgIndexerCpu)
	}

	if command == CommandRebalance && plan != nil {
		logging.Infof("\t--- rebalance stats")
		logging.Infof("\taverage index being shuffled on initial layout: %v", initial_movedIndex/uint64(count))
		logging.Infof("\taverage index size being shuffled on initial layout: %v", formatMemoryStr(initial_movedData/uint64(count)))
		logging.Infof("\taverage no. index moved : %v", formatMemoryStr(indexMoved/uint64(count)))
		logging.Infof("\taverage index data moved : %v", formatMemoryStr(dataMoved/uint64(count)))

		if config.Shuffle != 0 {
			logging.Infof("\taverage rebalance score: %v", float64(dataMoved)/float64(initial_movedData))
		} else {
			logging.Infof("\taverage rebalance score: N/A")
		}
	}

	return nil
}

func (t *simulator) RunSingleTest(config *RunConfig, command CommandType, spec *WorkloadSpec, p *Plan, indexSpecs []*IndexSpec) (*SAPlanner, *RunStats, error) {

	var indexes []*IndexUsage
	var err error

	sizing := newGeneralSizingMethod()

	if command == CommandPlan {
		if spec != nil {
			indexes, err = t.indexUsages(sizing, spec)
			if err != nil {
				return nil, nil, err
			}

		} else if indexSpecs != nil {
			indexes, err = IndexUsagesFromSpec(sizing, indexSpecs)
			if err != nil {
				return nil, nil, err
			}

		} else {
			return nil, nil, errors.New("missing argument:  workload or indexes must be present")
		}

		if p != nil {
			t.setStorageType(p)
		}

		return plan(config, p, indexes)

	} else if command == CommandRebalance || command == CommandSwap {
		if spec != nil {
			indexes, err = t.indexUsages(sizing, spec)
			if err != nil {
				return nil, nil, err
			}

		} else if p == nil {
			return nil, nil, errors.New("missing argument: either workload or plan must be present")
		}

		deletedNodes, err := t.findNodesToDelete(config, p)
		if err != nil {
			return nil, nil, err
		}

		if p != nil {
			t.setStorageType(p)
		}

		return rebalance(command, config, p, indexes, deletedNodes)

	} else {
		return nil, nil, errors.New(fmt.Sprintf("unknown command: %v", command))
	}

	return nil, nil, nil
}

//////////////////////////////////////////////////////////////
// Topology Change
/////////////////////////////////////////////////////////////

func (t *simulator) findNodesToDelete(config *RunConfig, plan *Plan) ([]string, error) {

	outNodeIds := ([]string)(nil)
	outNodes := ([]*IndexerNode)(nil)

	if config.DeleteNode != 0 {

		if config.DeleteNode > len(plan.Placement) {
			return nil, errors.New("The number of node in cluster is smaller than the number of node to be deleted.")
		}

		for config.DeleteNode > len(outNodeIds) {

			candidate := getRandomNode(t.rs, plan.Placement)
			if !hasMatchingNode(candidate.NodeId, outNodes) {
				outNodes = append(outNodes, candidate)
				outNodeIds = append(outNodeIds, candidate.String())
			}
		}
	}

	return outNodeIds, nil
}

//////////////////////////////////////////////////////////////
// Index Usage Generation
/////////////////////////////////////////////////////////////

func (t *simulator) indexUsages(s SizingMethod, spec *WorkloadSpec) ([]*IndexUsage, error) {

	count := t.rs.Int63n(spec.MaxNumIndex + 1 - spec.MinNumIndex)
	count += spec.MinNumIndex
	var result []*IndexUsage

	for i := int64(0); i < count; i++ {
		bucket, err := t.bucket(spec)
		if err != nil {
			return nil, err
		}
		collection, err := t.collection(bucket)
		if err != nil {
			return nil, err
		}
		indexes, err := t.indexUsage(s, bucket.Name, collection, bucket.Replica)
		if err != nil {
			return nil, err
		}
		result = append(result, indexes...)
	}

	return result, nil
}

func (t *simulator) indexUsage(s SizingMethod, bucket string, spec *CollectionSpec, replica int64) ([]*IndexUsage, error) {

	result := make([]*IndexUsage, replica)

	uuid, err := common.NewUUID()
	if err != nil {
		return nil, errors.New("unable to generate UUID")
	}

	defnId := common.IndexDefnId(uuid.Uint64())

	for i := 0; i < int(replica); i++ {

		index := &IndexUsage{}
		index.DefnId = defnId
		index.InstId = common.IndexInstId(i)
		index.Name = strconv.FormatUint(uint64(index.DefnId), 10)
		index.Bucket = bucket

		// TODO
		//index.IsPrimary = t.isPrimary(spec)
		index.IsPrimary = false
		index.StorageMode = common.MemoryOptimized
		index.AvgSecKeySize = t.avgSecKeySize(spec)
		index.AvgDocKeySize = t.avgDocKeySize(spec)
		//TODO
		//index.AvgArrKeySize = t.avgArrKeySize(spec)
		//index.AvgArrSize = t.avgArrSize(spec)
		index.AvgArrKeySize = 0
		index.AvgArrSize = 0
		index.NumOfDocs = t.numOfDocs(spec)
		index.ResidentRatio = 100
		index.MutationRate = t.mutationRate(spec)
		index.ScanRate = t.scanRate(spec)

		// This is need to compute stats for new indexes
		// The index size will be recomputed later on in plan/rebalance
		s.ComputeIndexSize(index)

		result[i] = index
	}

	return result, nil
}

func (t *simulator) isPrimary() bool {
	// primary index: 1 out of 5
	if t.rs.Int63n(5) == 0 {
		return true
	}

	return false
}

func (t *simulator) avgSecKeySize(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxSecKeySize + 1 - spec.MinSecKeySize)
	return uint64(v + spec.MinSecKeySize)
}

func (t *simulator) avgDocKeySize(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxDocKeySize + 1 - spec.MinDocKeySize)
	return uint64(v + spec.MinDocKeySize)
}

func (t *simulator) avgArrSize(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxArrSize + 1 - spec.MinArrSize)
	return uint64(v + spec.MinArrSize)
}

func (t *simulator) avgArrKeySize(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxArrKeySize + 1 - spec.MinArrKeySize)
	return uint64(v + spec.MinArrKeySize)
}

func (t *simulator) numOfDocs(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxNumDoc + 1 - spec.MinNumDoc)
	return uint64(v + spec.MinNumDoc)
}

func (t *simulator) mutationRate(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxMutationRate + 1 - spec.MinMutationRate)
	return uint64(v + spec.MinMutationRate)
}

func (t *simulator) scanRate(spec *CollectionSpec) uint64 {
	v := t.rs.Int63n(spec.MaxScanRate + 1 - spec.MinScanRate)
	return uint64(v + spec.MinScanRate)
}

func (t *simulator) bucket(spec *WorkloadSpec) (*BucketSpec, error) {

	b := t.rs.Int63n(100) + 1

	// spec.Workload is sorted based on spec.Distribution
	for i, distribution := range spec.Distribution {
		if distribution >= b {
			return spec.Workload[i], nil
		}
		b = b - distribution
	}

	return nil, errors.New("bucket workload does not sum up to 100")
}

func (t *simulator) collection(spec *BucketSpec) (*CollectionSpec, error) {

	b := t.rs.Int63n(100) + 1

	// spec.Workload is sorted based on spec.Distribution
	for i, distribution := range spec.Distribution {
		if distribution >= b {
			return spec.Workload[i], nil
		}
		b = b - distribution
	}

	return nil, errors.New("collection workload does not sum up to 100")
}

func (t *simulator) setStorageType(plan *Plan) {

	for _, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			if len(index.StorageMode) == 0 {
				index.StorageMode = common.MemoryOptimized
			}
		}
	}
}

//////////////////////////////////////////////////////////////
// Workload
/////////////////////////////////////////////////////////////

func (t *simulator) sortBucketSpec(spec *WorkloadSpec) error {

	if len(spec.Workload) != len(spec.Distribution) {
		return errors.New("Invalid workload spec. Bucket workload and distribution array length mismatch")
	}

	num := len(spec.Workload)

	newSpec := make([]*BucketSpec, num)
	copy(newSpec, spec.Workload)

	newPercent := make([]int64, num)
	copy(newPercent, spec.Distribution)

	for i, _ := range newSpec {
		max := i
		for j := i + 1; j < num; j++ {
			if newPercent[j] > newPercent[max] {
				max = j
			}
		}

		if max != i {
			tmp1 := newSpec[i]
			newSpec[i] = newSpec[max]
			newSpec[max] = tmp1

			tmp2 := newPercent[i]
			newPercent[i] = newPercent[max]
			newPercent[max] = tmp2
		}
	}

	for _, bucket := range newSpec {
		t.sortCollectionSpec(bucket)
	}

	spec.Workload = newSpec
	spec.Distribution = newPercent

	return nil
}

func (t *simulator) sortCollectionSpec(spec *BucketSpec) error {

	if len(spec.Workload) != len(spec.Distribution) {
		return errors.New("Invalid bucket spec. Collection workload and distribution array length mismatch")
	}

	num := len(spec.Workload)

	newSpec := make([]*CollectionSpec, num)
	copy(newSpec, spec.Workload)

	newPercent := make([]int64, num)
	copy(newPercent, spec.Distribution)

	for i, _ := range newSpec {
		max := i
		for j := i + 1; j < num; j++ {
			if newPercent[j] > newPercent[max] {
				max = j
			}
		}

		if max != i {
			tmp1 := newSpec[i]
			newSpec[i] = newSpec[max]
			newSpec[max] = tmp1

			tmp2 := newPercent[i]
			newPercent[i] = newPercent[max]
			newPercent[max] = tmp2
		}
	}

	spec.Workload = newSpec
	spec.Distribution = newPercent

	return nil
}

func (t *simulator) printWorkloadSpec(spec *WorkloadSpec) {

	if spec == nil {
		return
	}

	logging.Infof("--------------------------------------")
	logging.Infof("Workload Spec: 		%v", spec.Name)
	logging.Infof("Number of Indexes : 	[%v - %v]", spec.MinNumIndex, spec.MaxNumIndex)
	for i, bucket := range spec.Workload {
		logging.Infof("--------------------------------------")
		logging.Infof("\t Bucket: 		%v", bucket.Name)
		logging.Infof("\t Distribution: %v", spec.Distribution[i])

		for j, collection := range bucket.Workload {
			logging.Infof("\t\t Workload Name:  	%v", collection.Name)
			logging.Infof("\t\t Number of Docs: 	[%v - %v]", collection.MinNumDoc, collection.MaxNumDoc)
			logging.Infof("\t\t Doc Key Size :  	[%v - %v]", collection.MinDocKeySize, collection.MaxDocKeySize)
			logging.Infof("\t\t Sec Key Size :  	[%v - %v]", collection.MinSecKeySize, collection.MaxSecKeySize)
			logging.Infof("\t\t Arr Key Size :  	[%v - %v]", collection.MinArrKeySize, collection.MaxArrKeySize)
			logging.Infof("\t\t Arr Size :      	[%v - %v]", collection.MinArrSize, collection.MaxArrSize)
			logging.Infof("\t\t Mutation Rate : 	[%v - %v]", collection.MinMutationRate, collection.MaxMutationRate)
			logging.Infof("\t\t Scan Rate :     	[%v - %v]", collection.MinScanRate, collection.MaxScanRate)
			logging.Infof("\t\t Distribution :    	%v", bucket.Distribution[j])
			logging.Infof("--------------------------------------")
		}
	}
}

func (t *simulator) defaultWorkloadSpec() *WorkloadSpec {

	spec := &WorkloadSpec{}
	spec.Name = "Default Simulation Workload"

	spec.Distribution = make([]int64, 1)
	spec.Distribution[0] = 100

	spec.Workload = make([]*BucketSpec, 1)
	spec.Workload[0] = t.defaultBucketSpec()

	spec.MinNumIndex = 5
	spec.MaxNumIndex = 100

	return spec
}

func (t *simulator) defaultBucketSpec() *BucketSpec {

	spec := &BucketSpec{}
	spec.Name = "bucket1"

	spec.Distribution = make([]int64, 1)
	spec.Distribution[0] = 100

	spec.Workload = make([]*CollectionSpec, 1)
	spec.Workload[0] = t.defaultCollectionSpec()

	return spec
}

func (t *simulator) defaultCollectionSpec() *CollectionSpec {

	spec := &CollectionSpec{}
	spec.Name = "small"
	spec.MinNumDoc = 500000
	spec.MaxNumDoc = 20000000
	spec.MinDocKeySize = 20
	spec.MaxDocKeySize = 200
	spec.MinSecKeySize = 20
	spec.MaxSecKeySize = 1000
	spec.MinArrKeySize = 0
	spec.MaxArrKeySize = 0
	spec.MinArrSize = 0
	spec.MaxArrSize = 0
	spec.MinMutationRate = 10000
	spec.MaxMutationRate = 100000
	spec.MinScanRate = 1000
	spec.MaxScanRate = 10000

	return spec
}

func (s *simulator) ReadWorkloadSpec(specFile string) (*WorkloadSpec, error) {

	if specFile != "" {

		spec := &WorkloadSpec{}

		buf, err := ioutil.ReadFile(specFile)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to read workload spec from %v. err = %s", specFile, err))
		}

		if err := json.Unmarshal(buf, spec); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse workload spec from %v. err = %s", specFile, err))
		}

		s.sortBucketSpec(spec)
		return spec, nil
	}

	return nil, nil
}
