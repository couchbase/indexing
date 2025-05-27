// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !community
// +build !community

package indexer

import (
	"fmt"
	"github.com/couchbase/bhive"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/plasma"
)

var PLASMA_METRICS_PREFIX = METRICS_PREFIX + "storage_"
var BHIVE_METRICS_PREFIX = PLASMA_METRICS_PREFIX + "bhive_"
var PLASMA_TENANT_METRICS_PREFIX = PLASMA_METRICS_PREFIX + "tenant_"

func populateAggregatedStorageMetrics(st []byte) []byte {

	if common.GetStorageMode() == common.PLASMA {
		aggregatedPlasmaStats := plasma.GetAggregatedStats(plasma.ListShards())

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcurrent_quota gauge\n", PLASMA_METRICS_PREFIX))...)
		//TODO: Get Bhive memory quota
		st = append(st, []byte(fmt.Sprintf("%vcurrent_quota %v\n", PLASMA_METRICS_PREFIX, plasma.GetMemQuota()))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vheap_limit gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vheap_limit %v\n", PLASMA_METRICS_PREFIX, plasma.GetHeapLimit()))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vmemory_stats_size_page gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vmemory_stats_size_page %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.MemSz))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vreclaim_pending_global gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vreclaim_pending_global %v\n", PLASMA_METRICS_PREFIX, plasma.GetGlobalReclaimPending()))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_pages gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_pages %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumPages))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vitems_count gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vitems_count %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.ItemsCount))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vavg_item_size gauge\n", PLASMA_METRICS_PREFIX))...)
		if aggregatedPlasmaStats.TotalRecords != 0 {
			st = append(st, []byte(fmt.Sprintf("%vavg_item_size %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.TotalItemSize/aggregatedPlasmaStats.TotalRecords))...)
		} else {
			st = append(st, []byte(fmt.Sprintf("%vavg_item_size %v\n", PLASMA_METRICS_PREFIX, 0))...)
		}

		st = append(st, []byte(fmt.Sprintf("# TYPE %vpurges gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vpurges %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.Purges))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlss_used_space gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlss_used_space %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.LSSUsedSpace+aggregatedPlasmaStats.RecoveryUsedSpace))...)

		usedSpace := aggregatedPlasmaStats.LSSUsedSpace + aggregatedPlasmaStats.RecoveryUsedSpace
		dataSize := aggregatedPlasmaStats.LSSDataSize + aggregatedPlasmaStats.RecoveryDataSize
		var LSSFrag int64
		if usedSpace > 0 && usedSpace > dataSize {
			LSSFrag = int64(float64(usedSpace-dataSize) / float64(usedSpace) * 100)
		} else {
			LSSFrag = 0
		}

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlss_fragmentation gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlss_fragmentation %v\n", PLASMA_METRICS_PREFIX, LSSFrag))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlss_num_reads gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlss_num_reads %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumLSSReads+aggregatedPlasmaStats.NumRecoveryReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlss_blk_read_bs gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlss_blk_read_bs %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.LSSBlkReadBytes+aggregatedPlasmaStats.RecoveryBlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vrlss_num_reads gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vrlss_num_reads %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.ReaderNumLSSReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlss_blk_rdr_reads_bs gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlss_blk_rdr_reads_bs %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.ReaderLSSBlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlookup_num_reads gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlookup_num_reads %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.LookupNumLSSReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vlookup_blk_reads_bs gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vlookup_blk_reads_bs %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.LookupLSSBlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vbytes_written gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vbytes_written %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.BytesWritten+aggregatedPlasmaStats.RecoveryBytesWritten))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vbytes_incoming gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vbytes_incoming %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.BytesIncoming))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vresident_ratio gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vresident_ratio %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.ResidentRatio))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcompression_ratio gauge\n", PLASMA_METRICS_PREFIX))...)
		if aggregatedPlasmaStats.PageBytesCompressed != 0 {
			st = append(st, []byte(fmt.Sprintf("%vcompression_ratio %v\n", PLASMA_METRICS_PREFIX, float64(aggregatedPlasmaStats.PageBytesMarshalled)/float64(aggregatedPlasmaStats.PageBytesCompressed)))...)
		} else {
			st = append(st, []byte(fmt.Sprintf("%vcompression_ratio %v\n", PLASMA_METRICS_PREFIX, 0))...)
		}

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_burst_visits gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_burst_visits %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumBurstVisits))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_periodic_visits gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_periodic_visits %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumPeriodicVisits))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_evicted gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_evicted %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumEvicted))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_evictable gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_evictable %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumEvictable))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcleaner_num_reads gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vcleaner_num_reads %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.NumLSSCleanerReads+aggregatedPlasmaStats.NumRecoveryCleanerReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcleaner_blk_read_bs gauge\n", PLASMA_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vcleaner_blk_read_bs %v\n", PLASMA_METRICS_PREFIX, aggregatedPlasmaStats.LSSCleanerBlkReadBytes+aggregatedPlasmaStats.RecoveryCleanerBlkReadBytes))...)

		aggregatedBhiveStats := bhive.GetAggregatedStats()
		vSts := aggregatedBhiveStats.VSStats.(*bhive.MagmaStats)
		pSts := aggregatedBhiveStats.PSStats.(*bhive.LssStats)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vmemory_used gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vmemory_used %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.MemUsed))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vbuf_memused gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vbuf_memused %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.BufMemUsed))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_reads gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_reads %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.NumReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vblk_read_bs gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vblk_read_bs %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.BlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_reads_get gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_reads_get %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.ReaderNumReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vblk_reads_bs_get gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vblk_reads_bs_get %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.ReaderBlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vnum_reads_lookup gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vnum_reads_lookup %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.LookupNumReads))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vblk_reads_bs_lookup gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vblk_reads_bs_lookup %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.LookupBlkReadBytes))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vbytes_written gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vbytes_written %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.BytesWritten))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vbytes_incoming gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vbytes_incoming %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.BytesIncoming))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vtotal_used_size gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vtotal_used_size %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.DiskUsedSpace))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vtotal_disk_size gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vtotal_disk_size %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.DiskUsage))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vfragmentation gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vfragmentation %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.Frag))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vresident_ratio gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vresident_ratio %v\n", BHIVE_METRICS_PREFIX, pSts.ResidentRatio))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcompacts gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vcompacts %v\n", BHIVE_METRICS_PREFIX, vSts.NCompacts+pSts.NCompacts))...)

		st = append(st, []byte(fmt.Sprintf("# TYPE %vcompression_ratio_avg gauge\n", BHIVE_METRICS_PREFIX))...)
		st = append(st, []byte(fmt.Sprintf("%vcompression_ratio_avg %v\n", BHIVE_METRICS_PREFIX, aggregatedBhiveStats.AvgCompressionRatio))...)
	}

	return st
}

func populateStorageTenantMetrics(st []byte) []byte {

	if common.GetDeploymentModel() != common.SERVERLESS_DEPLOYMENT {
		return st
	}

	if common.GetStorageMode() == common.PLASMA {
		fmtStr := "%v%v{tenant=\"%v\"} %v\n"
		plasmaTenantStats := plasma.GetTenantStats()

		for tenant, stats := range plasmaTenantStats {

			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "quota", tenant, stats.Quota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "mandatory_quota", tenant, stats.MandatoryQuota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "discretionary_quota", tenant, stats.DiscretionaryQuota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "resident_quota", tenant, stats.ResidentQuota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "mutation_quota", tenant, stats.MutationQuota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "idle_quota", tenant, stats.IdleQuota))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "working_set", tenant, stats.WorkingSet))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "mem_in_use", tenant, stats.MemInUse))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "mem_index", tenant, stats.MemIndex))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "total_records", tenant, stats.TotalRecord))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "avg_key_size", tenant, stats.AvgKeySize))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "resident_ratio", tenant, stats.ResidentRatio))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "total_ops", tenant, stats.Ops))...)
			st = append(st, []byte(fmt.Sprintf(fmtStr, PLASMA_TENANT_METRICS_PREFIX, "compacts", tenant, stats.Compacts))...)
		}
	}

	return st
}
