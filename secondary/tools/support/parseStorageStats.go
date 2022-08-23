package main

import "bufio"
import "fmt"
import "encoding/json"
import "os"
import "strings"

////////////////////////////////////////////////////////////////////////////////////////////////////
// parseStorageStats.go reads an indexer.log file and outputs the StorageStats that sometimes
// follow the PeriodicStats to stdout in a more greppable format.
////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////
// STORAGE STATS STRUCTS FOR JSON UNMARSHAL
////////////////////////////////////////////////////////////////////////////////////////////////////

type IndexInfo struct {
	Index string // b:s:c:index or just b:index
	Id uint64
	PartitionId int
	Stats StatsInfo
}

type StatsInfo struct {
	MainStore StoreStats
	BackStore StoreStats
}

type StoreStats struct {
	// All field names need to start with upper-case else json.Unmarshal silently can't use them.
	Memory_quota         int64
	Punch_hole_support   bool
	Count                int64
	Compacts             int64
	Purges               int64
	Splits               int64
	Merges               int64
	Inserts              int64
	Deletes              int64
	Compact_conflicts    int64
	Split_conflicts      int64
	Merge_conflicts      int64
	Insert_conflicts     int64
	Delete_conflicts     int64
	Swapin_conflicts     int64
	Persist_conflicts    int64
	Memory_size          int64 // number of bytes consumed by ALL records allocated by plasma - includes keys, values, versions (MVCC) and other metadata [DOES NOT include Memory_size_index or Memory_size_bloom_filter]
	Memory_size_index    int64 // number of bytes consumed by skiplist layer (or index layer) - includes nodes and per node key. Always 100% resident -- does not depend on Resident_ratio.
	Allocated            int64
	Freed                int64
	Reclaimed            int64
	Reclaim_pending      int64
	Reclaim_list_size    int64
	Reclaim_list_count   int64
	Reclaim_threshold    int64
	Allocated_index      int64
	Freed_index          int64
	Reclaimed_index      int64
	Num_pages            int64
	Items_count          int64
	Total_records        int64
	Num_rec_allocs       int64
	Num_rec_frees        int64
	Num_rec_swapout      int64
	Num_rec_swapin       int64
	Bytes_incoming       int64
	Bytes_written        int64
	Write_amp            float64
	Write_amp_avg        float64
	Lss_gc_status        string // "frag 96, data: 420540, used: 13577357, relocated: 98704, retries: 0, skipped: 389 log:(16541730195 - 16555683840)"
	Lss_fragmentation    int64
	Lss_data_size        int64
	Lss_recoverypt_size  int64
	Lss_maxsn_size       int64
	Lss_used_space       int64
	Checkpoint_used_space int64
	Lss_num_reads        int64
	Lss_read_bs          int64
	Lss_blk_read_bs      int64 // number of backstore blocks read from disk (bs get evicted forever)
	Lss_gc_num_reads     int64
	Lss_gc_reads_bs      int64
	Lss_blk_gc_reads_bs  int64
	Lss_rdr_reads_bs     int64
	Lss_blk_rdr_reads_bs int64
	Cache_hits           int64
	Cache_misses         int64
	Cache_hit_ratio      float64
	Rlss_num_reads       int64
	Rcache_hits          int64
	Rcache_misses        int64
	Rcache_hit_ratio     float64
	Resident_ratio       float64
	Mvcc_purge_ratio     float64
	CurrSn               int64
	GcSn                 int64
	GcSnIntervals        string // "[0 1810020789 1810076265 1810121381]"
	Purger_running       bool
	Mem_throttled        bool
	Lss_throttled        bool
	Lss_head_offset      int64
	Lss_tail_offset      int64
	Num_wctxs            int64
	Num_readers          int64
	Num_writers          int64
	Buf_memused          int64
	Page_bytes           int64
	Page_cnt             int64
	Page_itemcnt         int64
	Avg_item_size        int64
	Avg_page_size        int64
	Act_max_page_items   int64
	Act_min_page_items   int64
	Act_max_delta_len    int64
	Est_resident_mem     int64 // Plasma's best estimate for bytes of mem this index would consume if it were 100% resident. Use this instead of trying to calculate it from other pieces.
	Page_bytes_marshalled int64
	Page_bytes_compressed int64
	Compression_ratio    float64
	Bloom_tests          int64
	Bloom_negatives      int64
	Bloom_false_positives int64
	Bloom_fp_rate        float64
	Memory_size_bloom_filter int64 // bytes of mem consumed by bloom filter (always 100% resident; does not depend on Resident_ratio)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// FUNCTIONS
////////////////////////////////////////////////////////////////////////////////////////////////////

// isPeriodicStats detects whether a scanned line is the "PeriodicStats" message and if so returns
// the timestamp as a string. PeriodicStats messages are of the form
//   2022-06-13T17:50:11.194+09:00 [Info] PeriodicStats = {...}
// where ... is a HUGE JSON object all on the same line. This message is only sometimes
// (every 17 min) followed by a StorageStats block which is not all one line.
func isPeriodicStats(line string) (result string) {
	if strings.Contains(line, "PeriodicStats") {
		return line[0:29] // timestamp is first 29 chars
	}
	return result
}

// isStorageStats detects whether a scanned line is the marker string "==== StorageStats ===="
// indicating a JSON array of periodic (every 17 min) storage stats immediately follows.
func isStorageStats(line string) bool {
	if strings.Contains(line, "==== StorageStats ====") {
		return true
	}
	return false
}

// getStorageStatsAsBytes reads the StorageStats JSON array starting at the next line of logScanner
// and concatenates it into a byte slice that it returns. If the log ends before the JSON is
// complete, it returns nil instead. This function depends on the fact that the closing bracket will
// be the first character of its own line, and no closing brackets appear in the first character of
// any line before that.
func getStorageStatsAsBytes(logScanner *bufio.Scanner) (result []byte) {
	var line string
	done := false
	for !done {
		if logScanner.Scan() { // got a line
			line = logScanner.Text()
			if line[0] == ']' { // last line of the JSON array
				done = true
			}
		} else { // no more lines
			break
		}
		result = append(result, logScanner.Bytes()...)
	}

	if !done { // indexer.log ended in the middle of the JSON array
		return nil
	}
	return result
}

// findIndexInfo finds the IndexInfo object in the storageStats slice that matches the supplied
// id and partitionId and returns a pointer to it. Returns nil if not found.
func findIndexInfo(id uint64, partitionId int, stoStatsPrev []IndexInfo) *IndexInfo {
	for _, indexInfo := range stoStatsPrev {
		if indexInfo.Id == id && indexInfo.PartitionId == partitionId {
			return &indexInfo
		}
	}
	return nil
}

// formatStatFloat64 formats one float64 stat to stdout for both main and back indexes
func formatStatFloat64(statName, prefixMain, prefixBack string, valMain, valBack float64) {
	fmt.Printf("%v %v: %v\n", prefixMain, statName, valMain)
	fmt.Printf("%v %v: %v\n", prefixBack, statName, valBack)
}

// formatStatInt64 formats one int64 stat to stdout for both main and back indexes
func formatStatInt64(statName, prefixMain, prefixBack string, valMain, valBack int64) {
	fmt.Printf("%v %v: %v\n", prefixMain, statName, valMain)
	fmt.Printf("%v %v: %v\n", prefixBack, statName, valBack)
}

// formatStorageStats prints the periodic StorageStats to stdout in a more greppable format than
// the original log.
//   timestamp - log timestamp of the StorageStats message containing stoStats
//   stoStatsPrev - prior StorageStats (for deltas); will be nil on first call
//   stoStats - current StorageStats being formatted
// kjc Currently this only prints a few fields as needed for the CBSE I am investigating.
func formatStorageStats(timestamp string, stoStatsPrev, stoStats []IndexInfo) {
	printSeparator()
	for _, indexInfo := range stoStats {
		//
		// Prefix for this index and partition
		//
		preMain := fmt.Sprintf("%v %v:%v (%v) main", timestamp, indexInfo.Index,
			indexInfo.PartitionId, indexInfo.Id)
		preBack := fmt.Sprintf("%v %v:%v (%v) back", timestamp, indexInfo.Index,
			indexInfo.PartitionId, indexInfo.Id)

		//
		// Non-Delta StorageStats stats
		//

		// Plasma's estimate for memory this index would consume if it were 100% resident
		formatStatFloat64("est_resident_mem MB", preMain, preBack,
			float64(indexInfo.Stats.MainStore.Est_resident_mem) / (1024.0 * 1024.0),
			float64(indexInfo.Stats.BackStore.Est_resident_mem) / (1024.0 * 1024.0))

		// Memory-resident "leaves"
		//formatStatInt64("memory_size B", preMain, preBack,
		//	indexInfo.Stats.MainStore.Memory_size, indexInfo.Stats.BackStore.Memory_size)
		formatStatFloat64("memory_size MB", preMain, preBack,
			float64(indexInfo.Stats.MainStore.Memory_size) / (1024.0 * 1024.0),
			float64(indexInfo.Stats.BackStore.Memory_size) / (1024.0 * 1024.0))

		// Skiplist parts (always 100% memory-resident: does not depend on resident_ratio)
		//formatStatInt64("memory_size_index B", preMain, preBack,
		//	indexInfo.Stats.MainStore.Memory_size_index,
		//	indexInfo.Stats.BackStore.Memory_size_index)
		formatStatFloat64("memory_size_index MB", preMain, preBack,
			float64(indexInfo.Stats.MainStore.Memory_size_index) / (1024.0 * 1024.0),
			float64(indexInfo.Stats.BackStore.Memory_size_index) / (1024.0 * 1024.0))




		formatStatFloat64("resident_ratio", preMain, preBack,
			indexInfo.Stats.MainStore.Resident_ratio, indexInfo.Stats.BackStore.Resident_ratio)

		////////////////////////////////////////////////////////////////////////////////////////////
		// Non-delta stats must preceed this point since we now continue the loop if
		// stoStatsPrev == nil to avoid an extra level of indentation for delta stats.
		////////////////////////////////////////////////////////////////////////////////////////////
		if stoStatsPrev == nil {
			continue
		}
		indexInfoPrev := findIndexInfo(indexInfo.Id, indexInfo.PartitionId, stoStatsPrev)
		if indexInfoPrev == nil {
			continue
		}

		//
		// Delta StorageStats stats
		//
		formatStatInt64("delta inserts", preMain, preBack,
			indexInfo.Stats.MainStore.Inserts - indexInfoPrev.Stats.MainStore.Inserts,
			indexInfo.Stats.BackStore.Inserts - indexInfoPrev.Stats.BackStore.Inserts)
		formatStatInt64("delta compact_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Compact_conflicts - indexInfoPrev.Stats.MainStore.Compact_conflicts,
			indexInfo.Stats.BackStore.Compact_conflicts - indexInfoPrev.Stats.BackStore.Compact_conflicts)
		formatStatInt64("delta split_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Split_conflicts - indexInfoPrev.Stats.MainStore.Split_conflicts,
			indexInfo.Stats.BackStore.Split_conflicts - indexInfoPrev.Stats.BackStore.Split_conflicts)
		formatStatInt64("delta merge_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Merge_conflicts - indexInfoPrev.Stats.MainStore.Merge_conflicts,
			indexInfo.Stats.BackStore.Merge_conflicts - indexInfoPrev.Stats.BackStore.Merge_conflicts)
		formatStatInt64("delta insert_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Insert_conflicts - indexInfoPrev.Stats.MainStore.Insert_conflicts,
			indexInfo.Stats.BackStore.Insert_conflicts - indexInfoPrev.Stats.BackStore.Insert_conflicts)
		formatStatInt64("delta delete_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Delete_conflicts - indexInfoPrev.Stats.MainStore.Delete_conflicts,
			indexInfo.Stats.BackStore.Delete_conflicts - indexInfoPrev.Stats.BackStore.Delete_conflicts)
		formatStatInt64("delta swapin_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Swapin_conflicts - indexInfoPrev.Stats.MainStore.Swapin_conflicts,
			indexInfo.Stats.BackStore.Swapin_conflicts - indexInfoPrev.Stats.BackStore.Swapin_conflicts)
		formatStatInt64("delta persist_conflicts", preMain, preBack,
			indexInfo.Stats.MainStore.Persist_conflicts - indexInfoPrev.Stats.MainStore.Persist_conflicts,
			indexInfo.Stats.BackStore.Persist_conflicts - indexInfoPrev.Stats.BackStore.Persist_conflicts)

		// lss_blk_read_bs counts the number of bytes block-read from disk for indexes. Often much
		// higher for back index than main index, as once a back index page is evicted, it NEVER
		// gets cached again, plus Plasma has an eager, preemptive periodic eviction algorithm that
		// tries to keep *50%* of its memory quota *free* at all times, *PLUS* it preferentially
		// targets back indexes for eviction because they are not serving scans, whereas mainstore
		// indexes are.
		formatStatInt64("delta lss_blk_read_bs", preMain, preBack,
			indexInfo.Stats.MainStore.Lss_blk_read_bs - indexInfoPrev.Stats.MainStore.Lss_blk_read_bs,
			indexInfo.Stats.BackStore.Lss_blk_read_bs - indexInfoPrev.Stats.BackStore.Lss_blk_read_bs)
	}
}

// scanLog parses an indexer.log file for StorageStats JSON objects and write them to stdout in a
// more greppable form.
func scanLog(logFname string) {
	const _method = "scanLog:"

	logFile, err := os.Open(logFname)
	if err != nil {
		fmt.Printf("%v os.Open returned error: %v\n", _method, err)
		return
	}
	defer logFile.Close()

	var stoStatsPrev []IndexInfo // saved previous stoStats
	logScanner := bufio.NewScanner(logFile)
	logScanner.Buffer(nil, 1024*1024) // set huge max buffer size for PeriodicStats line
	for logScanner.Scan() {
		line := logScanner.Text()
		if timestamp := isPeriodicStats(line); timestamp != "" {
			if !logScanner.Scan() { // no more input
				break
			}
			line = logScanner.Text() // next line will indicate StorageStats start if they are here
			if isStorageStats(line) {
				// Next line is opening bracket of a huge JSON array of StorageStats that has been
				// pretty-printed to the logs so spans many lines.
				stoStatsBytes := getStorageStatsAsBytes(logScanner)
				if stoStatsBytes != nil {
					var stoStats []IndexInfo
					err := json.Unmarshal(stoStatsBytes, &stoStats)
					if err != nil {
						stoStatsBytes256 := stoStatsBytes[0:256]
						fmt.Printf("%v json.Unmarshall returned error: %v, bytes[256]: %v",
							_method, stoStatsBytes256)
					} else {
						formatStorageStats(timestamp, stoStatsPrev, stoStats)
						stoStatsPrev = stoStats
					}
				}
			}
		}
	}
	err = logScanner.Err()
	if err != nil {
		fmt.Printf("ERROR %v %v\n", _method, err)
	}
}

// printSeparator prints a line of dashes to the console.
func printSeparator() {
	fmt.Printf("-----------------------------------------------------------------------\n")
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// MAIN
////////////////////////////////////////////////////////////////////////////////////////////////////

// main for parseStorageStats.
func main() {
	if len(os.Args) != 2 {
		fmt.Printf("\n    Usage: %v path/to/indexer.log\n\n", os.Args[0])
		os.Exit(1)
	}

	logFname := os.Args[1]
	printSeparator()
	fmt.Printf("parseStorageStats: Scanning log: %v\n", logFname)

	scanLog(logFname)

	printSeparator()
	fmt.Printf("parseStorageStats: Scanned log: %v\n", logFname)
	printSeparator()
}