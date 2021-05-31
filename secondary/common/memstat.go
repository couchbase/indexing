package common

import (
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

var Memstatch = make(chan int64, 16)
var fmemsg = strings.Replace(`memstats {
"Alloc":%v, "TotalAlloc":%v, "Sys":%v, "Lookups":%v, "Mallocs":%v,
"Frees":%v, "HeapAlloc":%v, "HeapSys":%v, "HeapIdle":%v, "HeapInuse":%v,
"HeapReleased":%v, "HeapObjects":%v,
"MSpanInuse":%v, "MSpanSys": %v, "StackInuse": %v,
"GCSys":%v, "LastGC":%v,
"PauseTotalNs":%v, "PauseNs":%v, "NumGC":%v
}`, "\n", "", -1)

func MemstatLogger(tick int64) {
	var ms runtime.MemStats
	var tickTm *time.Ticker

	if tick > 0 {
		tickTm = time.NewTicker(time.Duration(tick) * time.Millisecond)
	}
	logging.Infof("MSAT starting with %v tick ...", tick)

	var oldNumGC uint32
	var PauseNs [256]uint64
	for {
		oldNumGC = ms.NumGC
		select {
		case <-tickTm.C:
			runtime.ReadMemStats(&ms)
			PrintMemstats(&ms, PauseNs[:], oldNumGC)

		case tick = <-Memstatch:
			tickTm.Stop()
			if tick > 0 {
				tickTm = time.NewTicker(time.Duration(tick) * time.Millisecond)
			}
		}
	}
}

func newPauseNs(pad, pauseNs []uint64, oldcount, newcount uint32) []uint64 {
	diff := (newcount - oldcount)
	if diff >= 256 {
		return pauseNs[:]
	}
	for i, j := 0, oldcount+1; j <= newcount; i, j = i+1, j+1 {
		pad[i] = pauseNs[(j+255)%256]
	}
	return pad[:diff]
}

func reprList(pauseNs []uint64) string {
	strs := make([]string, len(pauseNs))
	for i, x := range pauseNs {
		strs[i] = strconv.Itoa(int(x))
	}
	return "[" + strings.Join(strs, ", ") + "]"
}

func PrintMemstats(ms *runtime.MemStats, PauseNs []uint64, oldNumGC uint32) {

	logging.Infof(
		fmemsg,
		ms.Alloc, ms.TotalAlloc, ms.Sys, ms.Lookups, ms.Mallocs,
		ms.Frees, ms.HeapAlloc, ms.HeapSys, ms.HeapIdle, ms.HeapInuse,
		ms.HeapReleased, ms.HeapObjects,
		ms.MSpanInuse, ms.MSpanSys, ms.StackInuse,
		ms.GCSys, ms.LastGC,
		ms.PauseTotalNs,
		reprList(newPauseNs(PauseNs[:], ms.PauseNs[:], oldNumGC, ms.NumGC)),
		ms.NumGC)

}
