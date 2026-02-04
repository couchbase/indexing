//go:build nolint

// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"sync"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/mschoch/smat"
)

// Fuzz using state machine driven by byte stream
func Fuzz(data []byte) int {
	return smat.Fuzz(&smatContext{}, smat.ActionID('S'), smat.ActionID('T'), actionMap, data)
}

var numItems int
var keys [][]byte
var vals [][]byte
var wg sync.WaitGroup
var smatDebug = false
var dummyChan chan bool

func smatLog(prefix, format string, args ...interface{}) {
	if smatDebug {
		fmt.Print(prefix)
		fmt.Printf(format, args...)
	}
}

type smatContext struct {
	plasmaDir string

	slice        indexer.Slice
	infos        []indexer.SnapshotInfo
	snap         indexer.Snapshot
	mutationMeta *indexer.MutationMeta

	actions int
}

// Action map
var actionMap = smat.ActionMap{
	smat.ActionID('a'): action("  createSlice", createSliceFunc),
	smat.ActionID('b'): action("  insert", insertFunc),
	smat.ActionID('c'): action("  delete", deleteFunc),
	smat.ActionID('d'): action("  storageStatistics", storageStatisticsFunc),
	smat.ActionID('e'): action("  createSnapshot", createSnapshotFunc),
	smat.ActionID('f'): action("  scan", scanFunc),
	smat.ActionID('g'): action("  rollback", rollbackFunc),
	smat.ActionID('h'): action("  close", closeFunc),
}

var runningPercentActions []smat.PercentAction

// Init function
func init() {
	var ids []int
	for actionId := range actionMap {
		ids = append(ids, int(actionId))
	}
	sort.Ints(ids)

	pct := 100 / len(actionMap)
	for _, actionId := range ids {
		runningPercentActions = append(runningPercentActions,
			smat.PercentAction{Percent: pct, Action: smat.ActionID(actionId)})
	}
	actionMap[smat.ActionID('S')] = action("SETUP", setupFunc)
	actionMap[smat.ActionID('T')] = action("TEARDOWN", teardownFunc)
	numItems = 5000000
	keys = make([][]byte, numItems)
	vals = make([][]byte, numItems)
}

func running(next byte) smat.ActionID {
	return smat.PercentExecute(next, runningPercentActions...)
}

func action(name string, f func(ctx smat.Context) (smat.State, error)) func(ctx smat.Context) (smat.State, error) {
	return func(ctx smat.Context) (smat.State, error) {
		c := ctx.(*smatContext)
		c.actions++

		smatLog("  ", "%s\n", name)

		return f(ctx)
	}
}

// setup function
func setupFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	c.plasmaDir, err = ioutil.TempDir("/tmp/", "plasma")
	if err != nil {
		return nil, err
	}
	return running, nil
}

// Teardown function
func teardownFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	if c.plasmaDir != "" {
		os.RemoveAll(c.plasmaDir)
	}
	return nil, err
}

// Helper functions
// TODO: Parameterize these functions to do only plasma slice or both plasma and MOI slices and reuse in go tests
// Create the plasma slice
func createSliceFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	config := common.SystemConfig.SectionConfig(
		"indexer.", true)
	config.SetValue("settings.moi.debug", true)
	indexerStats := &indexer.IndexerStats{}
	indexerStats.Init()
	stats := &indexer.IndexStats{}
	stats.Init()
	idxDefn := common.IndexDefn{
		DefnId:          common.IndexDefnId(200),
		Name:            "plasma_slice_test",
		Using:           common.PlasmaDB,
		Bucket:          "default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	instID, err := common.NewIndexInstId()
	if err != nil {
		return nil, err
	}
	c.slice, err = indexer.NewPlasmaSlice(c.plasmaDir, c.plasmaDir, "", 0, idxDefn, instID, common.PartitionId(0), false, 1, config, stats, int64(config.GetIndexerMemoryQuota()), false, true, nil, 64, 0, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	fmt.Print(c.slice, "\n")
	c.mutationMeta = indexer.NewMutationMeta()
	for i := 0; i < numItems; i++ {
		keys[i] = []byte(fmt.Sprintf("perf%v", rand.Intn(10000000000)))
		vals[i] = []byte(fmt.Sprintf("body%v", rand.Intn(10000000000)))
	}
	return running, nil
}

// Data insert function
func insertFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	for i := 0; i < numItems; i++ {
		c.mutationMeta.SetVBId(int(crc32.ChecksumIEEE(vals[i])) % 1024)
		c.slice.Insert(keys[i], vals[i], c.mutationMeta)
	}

	return running, nil
}

// Data delete function
func deleteFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	for i := 0; i < rand.Intn(numItems/4); i++ {
		c.mutationMeta.SetVBId(int(crc32.ChecksumIEEE(vals[i])) % 1024)
		c.slice.Delete(vals[i], c.mutationMeta)
	}

	return running, nil
}

// Printing storage statistics for logging purposes only
func storageStatisticsFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	storageStatistics, err := c.slice.Statistics()
	if err != nil {
		return nil, err
	}
	fmt.Print("Statistics \n")
	fmt.Print(storageStatistics)

	return running, nil
}

// Create snapshot, Open snapshot and Get snapshots
func createSnapshotFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)
	var info indexer.SnapshotInfo

	ts := common.NewTsVbuuid("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts.Seqnos[i] = uint64(1000000 + i)
		ts.Vbuuids[i] = uint64(2000000 + i)
		ts.Snapshots[i] = [2]uint64{i, i + 10}
	}
	info, err = c.slice.NewSnapshot(ts, true)
	if err != nil {
		return nil, err
	}
	fmt.Print(info, "\n")

	c.snap, err = c.slice.OpenSnapshot(info, nil)
	if err != nil {
		return nil, err
	}
	fmt.Print(c.snap, "\n")

	c.infos, err = c.slice.GetSnapshots()
	if err != nil {
		return nil, err
	}

	return running, nil
}

// Do Scans
// TODO: Add Range queries
func scanFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	reader := c.slice.GetReaderContext()
	reader.Init(dummyChan)
	scanReq := new(indexer.ScanRequest)
	scanReq.Ctxs = make([]indexer.IndexReaderContext, 1)
	scanReq.Ctxs[0] = reader
	stopch := make(indexer.StopChannel)
	for i := 1; i < 5; i++ {
		if scanReq.Low == nil && scanReq.High == nil {
			var statsCountTotal uint64
			statsCountTotal, err7 := c.snap.StatCountTotal()
			if err7 != nil {
				return nil, err7
			}
			fmt.Print("Stats Count Total : ", statsCountTotal, "\n")
		} else {
			var countRange uint64
			countRange, err11 := c.snap.CountRange(scanReq.Ctxs[0], scanReq.Low, scanReq.High, scanReq.Incl, stopch)
			if err11 != nil {
				return nil, err11
			}
			fmt.Print("Count Range : ", countRange, "\n")
		}

		var countTotal uint64
		countTotal, err10 := c.snap.CountTotal(scanReq.Ctxs[0], stopch)
		if err10 != nil {
			return nil, err10
		}
		fmt.Print("Count Total : ", countTotal, "\n")

		var countLookup uint64
		countLookup, err12 := c.snap.CountLookup(scanReq.Ctxs[0], scanReq.Keys, stopch)
		if err12 != nil {
			return nil, err12
		}
		fmt.Print("Count Lookup : ", countLookup, "\n")
	}
	return running, nil
}

// Function to do a rollback
func rollbackFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	err = c.slice.Rollback(c.infos[0])
	if err != nil {
		return nil, err
	}
	err = c.slice.RollbackToZero()
	if err != nil {
		return nil, err
	}

	return running, nil
}

// Cleanup function to close snapshots , clsose and destroy slices
func closeFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*smatContext)

	for i := 1; i < 5; i++ {
		c.snap.Close()
	}
	c.slice.Close()
	c.slice.Destroy()

	err = os.RemoveAll(c.plasmaDir)
	if err != nil {
		return nil, err
	}
	return running, nil
}
