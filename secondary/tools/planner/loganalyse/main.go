package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/planner"
)

//
// This tool can be used to create indexer layout which can be consumed by
// planner simulator, by looking at the planner logs dumped in the log
// files.
//
// Input:
// Indexer layout from logs in a single / separate file
// First line:
//    ************ Indexer Layout *************
// Last line:
//       ****************************************
//
// Output:
//    Json marshalled planner.Plan
//
// Caveats:
//    1. For indexer nodes: isDelete, isNew and exclude will not be captured.
//    2. For indexes: sizing info will not be populated as it doesn't get logged.
//    3. If useLive is true, "Actual" information will be populated. Sizing should
//       be explicitly disabled.
//    4. For indexes: pending build, pending delete won't be captured.
//    5. Equivalent index check is forcefully turned off, due to lack of index info.
//

var ErrMalformedLayout = fmt.Errorf("Malformed Layout")

type layout struct {
	p *planner.Plan
}

func NewLayout() *layout {
	l := &layout{
		p: &planner.Plan{},
	}

	l.p.Placement = make([]*planner.IndexerNode, 0)

	return l
}

func (l *layout) addIndexToCurrIndexer(index *planner.IndexUsage) {
	idx := len(l.p.Placement) - 1
	l.p.Placement[idx].Indexes = append(l.p.Placement[idx].Indexes, index)
}

func (l *layout) addNewIndexer(indexer *planner.IndexerNode) {
	l.p.Placement = append(l.p.Placement, indexer)
}

func (l *layout) setMemoryQuota(memQuota uint64) {
	l.p.MemQuota = memQuota
}

func (l *layout) setCpuQuota(cpuQuota uint64) {
	l.p.CpuQuota = cpuQuota
}

func (l *layout) setUseLive(useLive bool) {
	l.p.IsLive = useLive
}

func setMemoryQuota(l *layout, scanner *bufio.Scanner) error {
	for scanner.Scan() {
		txt := scanner.Text()
		s := strings.Split(txt, "Memory Quota ")
		if len(s) != 2 {
			continue
		}

		ss := strings.Split(s[1], " ")
		if len(s) != 2 {
			continue
		}

		u, err := strconv.ParseUint(ss[0], 10, 64)
		if err != nil {
			return err
		}

		l.setMemoryQuota(u)
		return nil
	}

	return fmt.Errorf("Memory Quota Not Found")
}

func setCpuQuota(l *layout, scanner *bufio.Scanner) error {
	for scanner.Scan() {
		txt := scanner.Text()
		s := strings.Split(txt, "CPU Quota ")
		if len(s) != 2 {
			continue
		}

		u, err := strconv.ParseUint(s[1], 10, 64)
		if err != nil {
			return err
		}

		l.setCpuQuota(u)
		return nil
	}

	return fmt.Errorf("Cpu Quota Not Found")
}

func getIndexerInfo1(txt string) (string, string, string, bool, error) {
	s := strings.Split(txt, "Indexer serverGroup:")
	if len(s) != 2 {
		return "", "", "", false, ErrMalformedLayout
	}

	ss := strings.Split(s[1], ", ")
	if len(ss) != 4 {
		return "", "", "", false, ErrMalformedLayout
	}

	sg := ss[0]
	nodeId := ss[1][len("nodeId:"):]
	nodeUUID := ss[2][len("nodeUUID:"):]

	var useLive bool
	strUseLive := ss[3][len("useLiveData:"):]
	if strUseLive == "true" {
		useLive = true
	} else if strUseLive == "false" {
		useLive = false
	} else {
		return "", "", "", false, ErrMalformedLayout
	}

	return sg, nodeId, nodeUUID, useLive, nil
}

func getIndexerInfo2(txt string) (uint64, uint64, uint64, float64, uint64, uint64, uint64, error) {
	s := strings.Split(txt, "Indexer total memory:")
	if len(s) != 2 {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), ErrMalformedLayout
	}

	ss := strings.Split(s[1], ", ")
	if len(ss) != 7 {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), ErrMalformedLayout
	}

	ss1 := strings.Split(ss[1], " ")
	mem, err := strconv.ParseUint(ss1[0][len("mem:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss2 := strings.Split(ss[2], " ")
	overhead, err := strconv.ParseUint(ss2[0][len("overhead:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss3 := strings.Split(ss[4], " ")
	data, err := strconv.ParseUint(ss3[0][len("data:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	cpu, err := strconv.ParseFloat(ss3[2][len("cpu:"):], 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss4 := strings.Split(ss[5], " ")
	io, err := strconv.ParseUint(ss4[0][len("io:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss5 := strings.Split(ss[6], " ")
	scan, err := strconv.ParseUint(ss5[0][len("scan:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	drain, err := strconv.ParseUint(ss5[1][len("drain:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	return mem, overhead, data, cpu, io, scan, drain, nil
}

func populateIndexerWarnings(txt string, n *planner.IndexerNode, warnings []string) error {

	// TODO: Need to implement this.

	return nil
}

func getIndexer(scanner *bufio.Scanner, warnings []string, storageMode string, txt1 string) (*planner.IndexerNode, bool, error) {

	if txt1 == "" {
		for {
			if scanner.Scan() {
				txt1 = scanner.Text()
				if strings.Contains(txt1, "****************************************") {
					return nil, false, nil
				}

				if strings.Contains(txt1, "Indexer serverGroup") {
					break
				}
			} else {
				// eof
				return nil, false, nil
			}
		}
	}

	if len(txt1) == 0 {
		return nil, false, ErrMalformedLayout
	}

	if !scanner.Scan() {
		return nil, false, ErrMalformedLayout
	}

	txt2 := scanner.Text()

	if !scanner.Scan() {
		return nil, false, ErrMalformedLayout
	}

	txt3 := scanner.Text()

	sg, nodeId, nodeUUID, useLive, err := getIndexerInfo1(txt1)
	if err != nil {
		return nil, false, err
	}

	mem, overhead, data, cpu, io, scan, drain, err := getIndexerInfo2(txt2)
	if err != nil {
		return nil, false, err
	}

	n := &planner.IndexerNode{}
	n.ServerGroup = sg
	n.NodeId = nodeId
	n.NodeUUID = nodeUUID
	n.StorageMode = storageMode

	if useLive {
		n.ActualMemUsage = mem
		n.ActualMemOverhead = overhead
		n.ActualDataSize = data
		n.ActualCpuUsage = cpu
		n.ActualDiskUsage = io
		n.ActualDrainRate = drain
		n.ActualScanRate = scan
	} else {
		n.MemUsage = mem
		n.MemOverhead = overhead
		n.DataSize = data
		n.CpuUsage = cpu
		n.DiskUsage = io
	}

	err = populateIndexerWarnings(txt3, n, warnings)
	if err != nil {
		return nil, false, err
	}

	return n, useLive, nil
}

func getIndexInfo1(txt string, skipNew bool) (string, int, string, string, string, common.IndexDefnId, common.IndexInstId, common.PartitionId, bool, error) {
	s := strings.Split(txt, "Index name:")
	if len(s) != 2 {
		return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, ErrMalformedLayout
	}

	ss := strings.Split(s[1], ", ")
	if len(ss) != 8 {
		return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, ErrMalformedLayout
	}

	if skipNew {
		if ss[7][len("new/moved:"):] != "true" && ss[7][len("new/moved:"):] != "false" {
			return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, ErrMalformedLayout
		}

		if ss[7][len("new/moved:"):] == "true" {
			return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), true, nil
		}
	}

	ss1 := strings.Split(ss[0], " ")
	name := ss1[0]
	bucket := ss[1][len("bucket:"):]
	scope := ss[2][len("scope:"):]
	collection := ss[3][len("collection:"):]

	replicaId := 0
	if len(ss1) == 4 {
		rid, err := strconv.ParseUint(ss1[3][:len(ss1[3])-1], 10, 64)
		if err != nil {
			return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, err
		}

		replicaId = int(rid)
	}

	defnId, err := strconv.ParseUint(ss[4][len("defnId:"):], 10, 64)
	if err != nil {
		return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, err
	}

	instId, err := strconv.ParseUint(ss[5][len("instId:"):], 10, 64)
	if err != nil {
		return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, err
	}

	partnId, err := strconv.ParseUint(ss[6][len("Partition: "):], 10, 64)
	if err != nil {
		return "", 0, "", "", "", common.IndexDefnId(0), common.IndexInstId(0), common.PartitionId(0), false, err
	}

	return name, replicaId, bucket, scope, collection, common.IndexDefnId(defnId), common.IndexInstId(instId), common.PartitionId(partnId), false, nil
}

// Most of getIndexInfo2 is same as getIndexerInfo2
func getIndexInfo2(txt string) (uint64, uint64, uint64, float64, uint64, uint64, uint64, error) {
	s := strings.Split(txt, "Index total memory:")
	if len(s) != 2 {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), ErrMalformedLayout
	}

	ss := strings.Split(s[1], ", ")
	if len(ss) != 5 {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), ErrMalformedLayout
	}

	ss1 := strings.Split(ss[1], " ")
	mem, err := strconv.ParseUint(ss1[0][len("mem:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss2 := strings.Split(ss[2], " ")
	overhead, err := strconv.ParseUint(ss2[0][len("overhead:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	ss3 := strings.Split(ss[4], " ")
	data, err := strconv.ParseUint(ss3[0][len("data:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	cpu, err := strconv.ParseFloat(ss3[2][len("cpu:"):], 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	io, err := strconv.ParseUint(ss3[3][len("io:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	scan, err := strconv.ParseUint(ss3[5][len("scan:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	drain, err := strconv.ParseUint(ss3[6][len("drain:"):], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), uint64(0), float64(0), uint64(0), uint64(0), uint64(0), err
	}

	return mem, overhead, data, cpu, io, scan, drain, nil
}

func getIndexInfo3(txt string, warnings []string) (uint64, uint64, error) {
	s := strings.Split(txt, "Index resident:")
	if len(s) != 2 {
		return uint64(0), uint64(0), ErrMalformedLayout
	}

	ss := strings.Split(s[1], " ")
	if len(ss) != 6 {
		return uint64(0), uint64(0), ErrMalformedLayout
	}

	rr, err := strconv.ParseUint(ss[0][:len(ss[0])-1], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), ErrMalformedLayout
	}

	build, err := strconv.ParseUint(ss[1][len("build:"):len(ss[1])-1], 10, 64)
	if err != nil {
		return uint64(0), uint64(0), ErrMalformedLayout
	}

	// TODO: Populate warnings.
	return rr, build, nil
}

func addInstance(index *planner.IndexUsage, replicaId int, storageMode string) {
	defn := common.IndexDefn{}
	defn.DefnId = index.DefnId
	defn.Name = index.Name
	defn.Bucket = index.Bucket
	defn.Scope = index.Scope
	defn.Collection = index.Collection

	// Force secExpr to be index name. This will fix the equivalent check.
	defn.SecExprs = []string{index.Name}

	// TODO: Need to populate PartitionScheme and NumReplica

	inst := &common.IndexInst{}
	inst.InstId = index.InstId
	inst.Defn = defn
	inst.ReplicaId = replicaId
	inst.StorageMode = storageMode
	// TODO: Need to set Pc

	index.Instance = inst
}

func getIndex(scanner *bufio.Scanner, skipNew bool, warnings []string, useLive bool, storageMode string) (*planner.IndexUsage, bool, string, error) {

	var txt1 string

	for {
		if scanner.Scan() {
			txt1 = scanner.Text()
			if strings.Contains(txt1, "****************************************") {
				return nil, false, "", nil
			}

			if strings.Contains(txt1, "Indexer serverGroup") {
				return nil, false, txt1, nil
			}

			if strings.Contains(txt1, "Index name:") {
				break
			}

		} else {
			// eof
			return nil, false, "", nil
		}
	}

	if len(txt1) == 0 {
		return nil, false, "", ErrMalformedLayout
	}

	if !scanner.Scan() {
		return nil, false, "", ErrMalformedLayout
	}

	txt2 := scanner.Text()

	if !scanner.Scan() {
		return nil, false, "", ErrMalformedLayout
	}

	txt3 := scanner.Text()

	name, replicaId, bucket, scope, collection, defnId, instId, partnId, skipped, err := getIndexInfo1(txt1, skipNew)
	if err != nil {
		return nil, false, "", err
	}

	if skipped {
		return nil, skipped, "", err
	}

	mem, overhead, data, cpu, io, scan, drain, err := getIndexInfo2(txt2)
	if err != nil {
		return nil, false, "", err
	}

	rr, build, err := getIndexInfo3(txt3, warnings)
	if err != nil {
		return nil, false, "", err
	}

	index := &planner.IndexUsage{}
	if useLive {
		index.ActualMemUsage = mem
		index.ActualMemOverhead = overhead
		index.ActualCpuUsage = cpu
		index.ActualDataSize = data
		index.ActualDiskUsage = io
		index.ActualDrainRate = drain
		index.ActualScanRate = scan
		index.ActualResidentPercent = float64(rr)
		index.ActualBuildPercent = build
	} else {
		index.MemUsage = mem
		index.MemOverhead = overhead
		index.CpuUsage = cpu
		index.DataSize = data
		index.DiskUsage = io
		index.DrainRate = drain
		index.ScanRate = scan
		index.ResidentRatio = float64(rr)
	}

	index.DefnId = defnId
	index.InstId = instId
	index.PartnId = partnId
	index.Name = name
	index.Bucket = bucket
	index.Scope = scope
	index.Collection = collection
	index.StorageMode = storageMode

	addInstance(index, replicaId, storageMode)

	// TODO: Set Hosts ? Check all fields

	return index, skipped, "", nil
}

func setIndexes(l *layout, scanner *bufio.Scanner, skipNew bool, warnings []string, useLive bool, storageMode string) (string, error) {

	var txt1 string

	for {
		index, skipped, txt, err := getIndex(scanner, skipNew, warnings, useLive, storageMode)
		if err != nil {
			return "", err
		}

		if skipped {
			continue
		}

		if txt != "" {
			txt1 = txt
		}

		if index == nil {
			break
		}

		l.addIndexToCurrIndexer(index)
	}

	return txt1, nil
}

func setIndexers(l *layout, scanner *bufio.Scanner, skipNew bool, warnings []string, storageMode string) error {

	var txt1 string

	for {
		indexer, useLive, err := getIndexer(scanner, warnings, storageMode, txt1)
		if err != nil {
			return err
		}

		if indexer == nil {
			break
		}

		l.addNewIndexer(indexer)

		txt1, err = setIndexes(l, scanner, skipNew, warnings, useLive, storageMode)
		if err != nil {
			return err
		}

		l.setUseLive(useLive)
	}

	return nil
}

func main() {
	help := flag.Bool("help", false, "Help")
	command := flag.String("command", "plan", "Planner command")
	action := flag.String("action", "genlayout", "Action to be performed")
	infile := flag.String("infile", "", "Input log file")
	outfile := flag.String("outfile", "", "Output json file")
	skipNew := flag.Bool("skipNew", true, "Skip New/Moved indexes")
	storageMode := flag.String("storageMode", "plasma", "Storage mode")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return
	}

	if *action != "genlayout" {
		logging.Errorf("The action %v is not supported", *action)
		return
	}

	if *command != "plan" && *command != "rebalance" {
		logging.Errorf("The command %v is not supported", *command)
		return
	}

	if len(*infile) == 0 || len(*outfile) == 0 {
		logging.Errorf("Both input file and output file need to be specified")
		return
	}

	f, err := os.Open(*infile)
	if err != nil {
		logging.Errorf("Error in infile open %v", err)
		return
	}

	defer f.Close()

	warnings := make([]string, 0)

	if *command == "rebalance" {
		warnings = append(warnings, "Command rebalance is suppored with indexer layout before rebalance.")
	}

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	l := NewLayout()

	err = setMemoryQuota(l, scanner)
	if err != nil {
		logging.Errorf("Error in setMemoryQuota %v", err)
		return
	}

	err = setCpuQuota(l, scanner)
	if err != nil {
		logging.Errorf("Error in setCpuQuota %v", err)
		return
	}

	err = setIndexers(l, scanner, *skipNew, warnings, *storageMode)
	if err != nil {
		logging.Errorf("Error in setIndexes %v", err)
		return
	}

	data, err := json.MarshalIndent(l.p, "", "	")
	if err != nil {
		logging.Errorf("Error %v in json marshal", err)
		return
	}

	err = ioutil.WriteFile(*outfile, data, os.ModePerm)
	if err != nil {
		logging.Errorf("Error %v in WriteFile", err)
		return
	}

	logging.Infof("The output is written to the outfile successfully.")
}
