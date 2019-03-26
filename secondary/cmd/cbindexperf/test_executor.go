package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

func SeedInit() {
	rand.Seed(time.Now().UnixNano())
}

type ScanRange interface {
	GetLow(spec *ScanConfig) []interface{}
	GetHigh(spec *ScanConfig) []interface{}
	GetScans(spec *ScanConfig) qclient.Scans
}

type DefaultScanRange struct {
}

func (d DefaultScanRange) GetLow(spec *ScanConfig) []interface{} {
	return spec.Low
}

func (d DefaultScanRange) GetHigh(spec *ScanConfig) []interface{} {
	return spec.High
}

func (d DefaultScanRange) GetScans(spec *ScanConfig) qclient.Scans {
	return spec.Scans
}

type RandomScanRange struct {
	RandomKeyLen uint32
}

func (r RandomScanRange) GetLow(spec *ScanConfig) []interface{} {
	return randomString(r.RandomKeyLen, spec.Low[0].(string), spec.High[0].(string))
}

func (r RandomScanRange) GetHigh(spec *ScanConfig) []interface{} {
	return spec.High
}

func (r RandomScanRange) GetScans(spec *ScanConfig) qclient.Scans {
	return randomScans(r.RandomKeyLen, spec.Scans)
}

func ScanRangeFactory(spec *ScanConfig) ScanRange {
	if spec.TestSpec != nil && spec.TestSpec.RandomKeyLen > 0 {
		return RandomScanRange{RandomKeyLen: spec.TestSpec.RandomKeyLen}
	}
	return DefaultScanRange{}
}

func randomString(n uint32, low string, high string) []interface{} {
	if lowInt, err := strconv.ParseInt(low, 16, 64); err == nil {
		if highInt, err := strconv.ParseInt(high, 16, 64); err == nil {
			randomInt := rand.Intn(int(highInt - lowInt - 2))
			lowInt2 := int(lowInt) + randomInt
			format := fmt.Sprintf("%%%03dx", n)
			low = fmt.Sprintf(format, lowInt2)
		}
	}
	return []interface{}{low}
}

func randomScans(n uint32, scans qclient.Scans) qclient.Scans {
	scans_cpy := make(qclient.Scans, len(scans))
	for scan_index, scan := range scans {
		var scan_cpy qclient.Scan
		scan_cpy = *scan
		filters := make([]*qclient.CompositeElementFilter, len(scan_cpy.Filter))

		for index, filter := range scan_cpy.Filter {
			low := randomString(n, filter.Low.(string), filter.High.(string))
			temp_filter := qclient.CompositeElementFilter{}
			temp_filter = *filter
			temp_filter.Low = low[0]
			filters[index] = &temp_filter
		}
		scan_cpy.Filter = filters
		scans_cpy[scan_index] = &scan_cpy
	}
	return scans_cpy
}
