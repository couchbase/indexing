package main

import (
	crand "crypto/rand"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/goutils/logging"
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
	low_str, ok1 := spec.Low[0].(string)
	high_str, ok2 := spec.High[0].(string)
	if ok1 && ok2 {
		return randomString(r.RandomKeyLen, low_str, high_str)
	}

	low_float, ok1 := spec.Low[0].(float64)
	high_float, ok2 := spec.High[0].(float64)
	if ok1 && ok2 {
		return randomStringFloat(r.RandomKeyLen, low_float, high_float)
	}

	logging.Warnf("GetLow: low & high keys are not of the same type in json spec, returning low key for scan")
	return spec.Low
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
	lowLen, highLen := len(low), len(high)

	if lowLen <= 16 && highLen <= 16 {
		if lowInt, err := strconv.ParseInt(low, 16, 64); err == nil {
			if highInt, err := strconv.ParseInt(high, 16, 64); err == nil {
				if (highInt < lowInt ){
					highInt, lowInt = lowInt, highInt
				}
				randomInt := rand.Intn(int(highInt - lowInt + 1)) // diff between lowInt and highInt inclusive of both values
				lowInt2 := int(lowInt) + randomInt
				format := fmt.Sprintf("%%%03dx", n)
				low = fmt.Sprintf(format, lowInt2)
			}
		}
	} else {
		var bigLow, bigHigh, bigDiff, bigRandLow big.Int
		var success bool

		if _, success = bigLow.SetString(low, 16); !success {
			return []interface{}{low}
		}

		if _, success = bigHigh.SetString(high, 16); !success {
			return []interface{}{low}
		}

		bigDiff.Sub(&bigHigh, &bigLow)

		bigRandDiff, err := crand.Int(crand.Reader, &bigDiff)
		if err != nil {
			return []interface{}{low}
		}

		bigRandLow.Add(&bigLow, bigRandDiff)

		format := fmt.Sprintf("%%0%ds", n)
		return []interface{}{fmt.Sprintf(format, bigRandLow.Text(16))}
	}

	return []interface{}{low}
}

func randomStringFloat(keyLen uint32, low float64, high float64) []interface{} {
	var i []interface{}
	n := int(keyLen)

	if low >= high {
		logging.Errorf("randomStringFloat: invalid input: Low key >= High key")
		lowStr := strconv.FormatFloat(high, 'f', -1, 64)
		i = append(i, lowStr)
		return i
	}

	// Create random float64 in range [low, high)
	floatValue := low + rand.Float64()*(high-low)
	floatValueStr := strconv.FormatFloat(floatValue, 'f', -1, 64)

	// Split whole integer & decimal as strings, ex:- 123.456 to "123" & "456"
	split := strings.Split(floatValueStr, ".")
	intPartStr := split[0]
	if len(split) < 2 {
		logging.Errorf("randomStringFloat: invalid input for Low key")
		lowStr := strconv.FormatFloat(low, 'f', -1, 64)
		i = append(i, lowStr)
		return i
	}
	floatPartStr := split[1]
	lenIntPartStr := len(intPartStr)

	// Log error when lenIntPartStr >= KeyLen + 1 constraint
	if n <= lenIntPartStr+1 {
		logging.Errorf("randomStringFloat: KeyLen must be > length of Low key's integer part + 1")
		lowStr := strconv.FormatFloat(low, 'f', -1, 64)
		i = append(i, lowStr)
		return i
	}

	// Format for float string to limit decimals to not exceed keyLen or to increase decimal precision when length of floatKeyStr is lesser than keyLen
	// ex:- If float = 12.3456, keyLen=8 then str = 12.34560
	// ex:- If float = 12.3456, keyLen=6 then str = 12.346
	format := "%" + strconv.Itoa(lenIntPartStr) + "." + strconv.Itoa(n-lenIntPartStr-1) + "f"
	floatStr := intPartStr + "." + floatPartStr

	// Convert string representation of random float using format
	f, _ := strconv.ParseFloat(floatStr, 64)
	str := fmt.Sprintf(format, f)
	i = append(i, str)
	return i
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
