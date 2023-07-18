package planner

import "github.com/couchbase/indexing/secondary/logging"

type UsageBasedCostMethod struct {
	MemMean        float64 `json:"memMean,omitempty"`
	MemStdDev      float64 `json:"memStdDev,omitempty"`
	CpuMean        float64 `json:"cpuMean,omitempty"`
	CpuStdDev      float64 `json:"cpuStdDev,omitempty"`
	DiskMean       float64 `json:"diskMean,omitempty"`
	DiskStdDev     float64 `json:"diskStdDev,omitempty"`
	DrainMean      float64 `json:"drainMean,omitempty"`
	DrainStdDev    float64 `json:"drainStdDev,omitempty"`
	ScanMean       float64 `json:"scanMean,omitempty"`
	ScanStdDev     float64 `json:"scanStdDev,omitempty"`
	DataSizeMean   float64 `json:"dataSizeMean,omitempty"`
	DataSizeStdDev float64 `json:"dataSizeStdDev,omitempty"`
	TotalData      uint64  `json:"totalData,omitempty"`
	DataMoved      uint64  `json:"dataMoved,omitempty"`
	TotalIndex     uint64  `json:"totalIndex,omitempty"`
	IndexMoved     uint64  `json:"indexMoved,omitempty"`
	constraint     ConstraintMethod
	dataCostWeight float64
	cpuCostWeight  float64
	memCostWeight  float64
}

// Constructor
func newUsageBasedCostMethod(constraint ConstraintMethod,
	dataCostWeight float64,
	cpuCostWeight float64,
	memCostWeight float64) *UsageBasedCostMethod {

	return &UsageBasedCostMethod{
		constraint:     constraint,
		dataCostWeight: dataCostWeight,
		memCostWeight:  memCostWeight,
		cpuCostWeight:  cpuCostWeight,
	}
}

// Get mem mean
func (c *UsageBasedCostMethod) GetMemMean() float64 {
	return c.MemMean
}

// Get cpu mean
func (c *UsageBasedCostMethod) GetCpuMean() float64 {
	return c.CpuMean
}

// Get data mean
func (c *UsageBasedCostMethod) GetDataMean() float64 {
	return c.DataSizeMean
}

// Get disk mean
func (c *UsageBasedCostMethod) GetDiskMean() float64 {
	return c.DiskMean
}

// Get scan mean
func (c *UsageBasedCostMethod) GetScanMean() float64 {
	return c.ScanMean
}

// Get drain mean
func (c *UsageBasedCostMethod) GetDrainMean() float64 {
	return c.DrainMean
}

// Compute average resource variation across 4 dimensions:
// 1) memory
// 2) cpu
// 3) disk
// 4) data size
// This computes a "score" from 0 to 4.  The lowest the score,
// the lower the variation.
func (c *UsageBasedCostMethod) ComputeResourceVariation() float64 {

	memCost := float64(0)
	//cpuCost := float64(0)
	dataSizeCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)
	count := 0

	if c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean
		count++
	}

	/*
		if c.CpuMean != 0 {
			cpuCost = c.CpuStdDev / c.CpuMean
			count++
		}
	*/

	if c.DataSizeMean != 0 {
		dataSizeCost = c.DataSizeStdDev / c.DataSizeMean
		count++
	}

	if c.DiskMean != 0 {
		diskCost = c.DiskStdDev / c.DiskMean
		count++
	}

	if c.DrainMean != 0 {
		drainCost = c.DrainStdDev / c.DrainMean
		count++
	}

	if c.ScanMean != 0 {
		scanCost = c.ScanStdDev / c.ScanMean
		count++
	}

	// return (memCost + cpuCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
	return (memCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
}

// Compute cost based on variance on memory and cpu usage across indexers
func (c *UsageBasedCostMethod) Cost(s *Solution) float64 {

	// compute usage statistics
	c.MemMean, c.MemStdDev = s.ComputeMemUsage()
	c.CpuMean, c.CpuStdDev = s.ComputeCpuUsage()
	c.DiskMean, c.DiskStdDev = s.ComputeDiskUsage()
	c.ScanMean, c.ScanStdDev = s.ComputeScanRate()
	c.DrainMean, c.DrainStdDev = s.ComputeDrainRate()
	c.TotalData, c.DataMoved, c.TotalIndex, c.IndexMoved = s.computeIndexMovement(false)
	c.DataSizeMean, c.DataSizeStdDev = s.ComputeDataSize()

	memCost := float64(0)
	//cpuCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)
	movementCost := float64(0)
	indexCost := float64(0)
	emptyIdxCost := float64(0)
	dataSizeCost := float64(0)
	count := 0

	if c.memCostWeight > 0 && c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean * c.memCostWeight
	}
	count++

	/*
		if c.cpuCostWeight > 0 && c.CpuMean != 0 {
			cpuCost = c.CpuStdDev / c.CpuMean * c.cpuCostWeight
			count++
		}
	*/

	if c.DataSizeMean != 0 {
		dataSizeCost = c.DataSizeStdDev / c.DataSizeMean
	}
	count++

	if c.DiskMean != 0 {
		diskCost = c.DiskStdDev / c.DiskMean
		count++
	}

	if c.ScanMean != 0 {
		scanCost = c.ScanStdDev / c.ScanMean
		count++
	}

	if c.DrainMean != 0 {
		drainCost = c.DrainStdDev / c.DrainMean
		count++
	}

	// Empty index is index with no recorded memory or cpu usage (excluding mem overhead).
	// It could be index without stats or sizing information.
	// The cost function minimize the residual memory after subtracting the estimated empty
	// index usage.
	//
	emptyIdxCost = s.ComputeCapacityAfterEmptyIndex()
	if emptyIdxCost != 0 {
		count++
	}

	// UsageCost is used as a weight to scale the impact of
	// moving data during rebalance.  Usage cost is affected by:
	// 1) relative ratio of memory deviation and memory mean
	// 2) relative ratio of data size deviation and data size mean
	usageCost := (memCost + dataSizeCost) / float64(2)

	if c.dataCostWeight > 0 && c.TotalData != 0 {
		// The cost of moving data is inversely adjust by the usage cost.
		// If the cluster resource usage is highly unbalanced (high
		// usage cost), the cost of data movement has less hinderance
		// for balancing resource consumption.
		weight := c.dataCostWeight * (1 - usageCost)
		movementCost = float64(c.DataMoved) / float64(c.TotalData) * weight
	}

	if c.dataCostWeight > 0 && c.TotalIndex != 0 {
		weight := c.dataCostWeight * (1 - usageCost)
		indexCost = float64(c.IndexMoved) / float64(c.TotalIndex) * weight
	}

	avgIndexMovementCost := (indexCost + movementCost) / 2
	avgResourceCost := (memCost + emptyIdxCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)

	//logging.Tracef("Planner::cost: mem cost %v cpu cost %v data moved %v index moved %v emptyIdx cost %v dataSize cost %v disk cost %v drain %v scan %v count %v",
	//	memCost, cpuCost, movementCost, indexCost, emptyIdxCost, dataSizeCost, diskCost, drainCost, scanCost, count)
	logging.Tracef("Planner::cost: mem cost %v data moved %v index moved %v emptyIdx cost %v dataSize cost %v disk cost %v drain %v scan %v count %v",
		memCost, movementCost, indexCost, emptyIdxCost, dataSizeCost, diskCost, drainCost, scanCost, count)

	//return (memCost + cpuCost + emptyIdxCost + movementCost + indexCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
	return (avgResourceCost + avgIndexMovementCost) / 2
}

// Print statistics
func (s *UsageBasedCostMethod) Print() {

	var memUtil float64
	var cpuUtil float64
	var diskUtil float64
	var drainUtil float64
	var scanUtil float64
	var dataSizeUtil float64
	var dataMoved float64
	var indexMoved float64

	if s.MemMean != 0 {
		memUtil = float64(s.MemStdDev) / float64(s.MemMean) * 100
	}

	if s.CpuMean != 0 {
		cpuUtil = float64(s.CpuStdDev) / float64(s.CpuMean) * 100
	}

	if s.DataSizeMean != 0 {
		dataSizeUtil = float64(s.DataSizeStdDev) / float64(s.DataSizeMean) * 100
	}

	if s.DiskMean != 0 {
		diskUtil = float64(s.DiskStdDev) / float64(s.DiskMean) * 100
	}

	if s.DrainMean != 0 {
		drainUtil = float64(s.DrainStdDev) / float64(s.DrainMean) * 100
	}

	if s.ScanMean != 0 {
		scanUtil = float64(s.ScanStdDev) / float64(s.ScanMean) * 100
	}

	if s.TotalData != 0 {
		dataMoved = float64(s.DataMoved) / float64(s.TotalData) * 100
	}

	if s.TotalIndex != 0 {
		indexMoved = float64(s.IndexMoved) / float64(s.TotalIndex) * 100
	}

	logging.Infof("Indexer Memory Mean %v (%s)", uint64(s.MemMean), formatMemoryStr(uint64(s.MemMean)))
	logging.Infof("Indexer Memory Deviation %v (%s) (%.2f%%)", uint64(s.MemStdDev), formatMemoryStr(uint64(s.MemStdDev)), memUtil)
	logging.Infof("Indexer Memory Utilization %.4f", float64(s.MemMean)/float64(s.constraint.GetMemQuota()))
	logging.Infof("Indexer CPU Mean %.4f", s.CpuMean)
	logging.Infof("Indexer CPU Deviation %.2f (%.2f%%)", s.CpuStdDev, cpuUtil)
	logging.Infof("Indexer CPU Utilization %.4f", float64(s.CpuMean)/float64(s.constraint.GetCpuQuota()))
	logging.Infof("Indexer IO Mean %.4f", s.DiskMean)
	logging.Infof("Indexer IO Deviation %.2f (%.2f%%)", s.DiskStdDev, diskUtil)
	logging.Infof("Indexer Drain Rate Mean %.4f", s.DrainMean)
	logging.Infof("Indexer Drain Rate Deviation %.2f (%.2f%%)", s.DrainStdDev, drainUtil)
	logging.Infof("Indexer Scan Rate Mean %.4f", s.ScanMean)
	logging.Infof("Indexer Scan Rate Deviation %.2f (%.2f%%)", s.ScanStdDev, scanUtil)
	logging.Infof("Indexer Data Size Mean %v (%s)", uint64(s.DataSizeMean), formatMemoryStr(uint64(s.DataSizeMean)))
	logging.Infof("Indexer Data Size Deviation %v (%s) (%.2f%%)", uint64(s.DataSizeStdDev), formatMemoryStr(uint64(s.DataSizeStdDev)), dataSizeUtil)
	logging.Infof("Total Index Data (from non-deleted node) %v", formatMemoryStr(s.TotalData))
	logging.Infof("Index Data Moved (exclude new node) %v (%.2f%%)", formatMemoryStr(s.DataMoved), dataMoved)
	logging.Infof("No. Index (from non-deleted node) %v", formatMemoryStr(s.TotalIndex))
	logging.Infof("No. Index Moved (exclude new node) %v (%.2f%%)", formatMemoryStr(s.IndexMoved), indexMoved)
}

// Validate the solution
func (c *UsageBasedCostMethod) Validate(s *Solution) error {

	return nil
}
