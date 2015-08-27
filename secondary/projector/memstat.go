package projector

import "time"
import "runtime"
import "strings"
import "strconv"

import "github.com/couchbase/indexing/secondary/logging"

var memstatch = make(chan int64, 16)
var fmemsg = strings.Replace(`memstats {
"Alloc":%v, "TotalAlloc":%v, "Sys":%v, "Lookups":%v, "Mallocs":%v,
"Frees":%v, "HeapAlloc":%v, "HeapSys":%v, "HeapIdle":%v, "HeapInuse":%v,
"HeapReleased":%v, "HeapObjects":%v,
"GCSys":%v, "LastGC":%v,
"PauseTotalNs":%v, "PauseNs":%v, "NumGC":%v
}`, "\n", "", -1)

func memstatLogger(tick int64) {
	var ms runtime.MemStats
	var tickTm <-chan time.Time

	if tick > 0 {
		tickTm = time.Tick(time.Duration(tick) * time.Millisecond)
	}
	logging.Infof("MSAT starting with %v tick ...", tick)

	var oldNumGC uint32
	for {
		oldNumGC = ms.NumGC
		select {
		case <-tickTm:
			runtime.ReadMemStats(&ms)
			logging.Infof(
				fmemsg,
				ms.Alloc, ms.TotalAlloc, ms.Sys, ms.Lookups, ms.Mallocs,
				ms.Frees, ms.HeapAlloc, ms.HeapSys, ms.HeapIdle, ms.HeapInuse,
				ms.HeapReleased, ms.HeapObjects,
				ms.GCSys, ms.LastGC,
				ms.PauseTotalNs,
				reprList(newPauseNs(ms.PauseNs[:], oldNumGC, ms.NumGC)),
				ms.NumGC)

		case tick = <-memstatch:
			if tick > 0 {
				tickTm = time.Tick(time.Duration(tick) * time.Millisecond)
			}
		}
	}
}

func newPauseNs(pauseNs []uint64, oldcount, newcount uint32) []uint64 {
	diff := (newcount - oldcount)
	if diff >= 256 {
		return pauseNs[:]
	}
	for i := oldcount + 1; i <= newcount; i++ {
		pauseNs[i] = pauseNs[(i+255)%256]
	}
	return pauseNs[:diff]
}

func reprList(pauseNs []uint64) string {
	strs := make([]string, len(pauseNs))
	for i, x := range pauseNs {
		strs[i] = strconv.Itoa(int(x))
	}
	return "[" + strings.Join(strs, ", ") + "]"
}
