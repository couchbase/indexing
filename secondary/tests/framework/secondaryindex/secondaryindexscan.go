package secondaryindex

import (
	"errors"
	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/value"
	"log"
	"time"
)

var CheckCollation = false
var UseClient = "gsi"
var DescCollation = false

func RangeWithClient(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency, client *qc.GsiClient) (tc.ScanResponse, error) {
	var scanErr error
	scanErr = nil
	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)

	start := time.Now()
	connErr := client.Range(
		defnID, "", c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						tc.HandleError(err, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("Range", elapsed)
	return scanResults, nil
}

func Range(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	distinct = false
	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLRange(indexName, bucketName, server, low, high, inclusion,
			distinct, limit, consistency, vector)
	}

	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)
	start := time.Now()
	connErr := client.Range(
		defnID, "", c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						tc.HandleError(err, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						// Test collation only if CheckCollation is true
						if CheckCollation == true && len(skey) > 0 {
							secVal := skey2Values(skey)[0]
							if previousSecKey == nil {
								previousSecKey = secVal
							} else {
								if DescCollation {
									if secVal.Collate(previousSecKey) > 0 {
										errMsg := "Collation check failed. Previous Sec key < Current Sec key"
										scanErr = errors.New(errMsg)
										return false
									}
								} else {
									if secVal.Collate(previousSecKey) < 0 {
										errMsg := "Collation check failed. Previous Sec key > Current Sec key"
										scanErr = errors.New(errMsg)
										return false
									}
								}
							}
						}

						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("Range", elapsed)
	return scanResults, nil
}

func Lookup(indexName, bucketName, server string, values []interface{}, distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	distinct = false
	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLLookup(indexName, bucketName, server, values,
			distinct, limit, consistency, vector)

	}

	var scanErr error
	scanErr = nil
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)

	start := time.Now()
	connErr := client.Lookup(
		defnID, "", []c.SecondaryKey{values}, distinct, limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						tc.HandleError(err, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("Lookup", elapsed)
	return scanResults, nil
}

func ScanAll(indexName, bucketName, server string, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLScanAll(indexName, bucketName, server, limit, consistency, vector)
	}

	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)

	start := time.Now()
	connErr := client.ScanAll(
		defnID, "", limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						tc.HandleError(err, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						// Test collation only if CheckCollation is true
						if CheckCollation == true && len(skey) > 0 {
							secVal := skey2Values(skey)[0]
							if previousSecKey == nil {
								previousSecKey = secVal
							} else {
								if secVal.Collate(previousSecKey) < 0 {
									errMsg := "Collation check failed. Previous Sec key > Current Sec key"
									scanErr = errors.New(errMsg)
									return false
								}
							}
						}

						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("ScanAll", elapsed)
	return scanResults, nil
}

func CountRange(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	consistency c.Consistency, vector *qc.TsConsistency) (int64, error) {
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return 0, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	count, err := client.CountRange(defnID, "", c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), consistency, vector)
	if err != nil {
		return 0, err
	} else {
		return count, nil
	}
}

func CountLookup(indexName, bucketName, server string, values []interface{},
	consistency c.Consistency, vector *qc.TsConsistency) (int64, error) {
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return 0, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	count, err := client.CountLookup(defnID, "", []c.SecondaryKey{values}, consistency, vector)
	if err != nil {
		return 0, err
	} else {
		return count, nil
	}
}

func RangeStatistics(indexName, bucketName, server string, low, high []interface{}, inclusion uint32) error {
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	statistics, err := client.RangeStatistics(defnID, "", c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion))
	if err != nil {
		return err
	} else {
		log.Print("Statistics: %v\n\n", statistics)
		return nil
	}
}

func Scans(indexName, bucketName, server string, scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLScans(indexName, bucketName, server, scans, reverse, distinct,
			projection, offset, limit, consistency, vector)
	}

	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)

	count := 0
	start := time.Now()
	connErr := client.MultiScan(
		defnID, "", scans, reverse, distinct, projection, offset, limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				log.Printf("ScanError = %v ", scanErr)
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				log.Printf("ScanError = %v ", scanErr)
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					//log.Printf("Scanresult count = %v: %v  : %v ", count, skey, primaryKey)
					count++
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						dupError := errors.New("Duplicate primary key found")
						tc.HandleError(dupError, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						// Test collation only if CheckCollation is true
						if CheckCollation == true && len(skey) > 0 {
							secVal := skey2Values(skey)[0]
							if previousSecKey == nil {
								previousSecKey = secVal
							} else {
								if secVal.Collate(previousSecKey) < 0 {
									errMsg := "Collation check failed. Previous Sec key > Current Sec key"
									scanErr = errors.New(errMsg)
									return false
								}
							}
						}

						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("MultiScan", elapsed)
	log.Printf("Total Scanresults = %v", count)
	return scanResults, nil
}

func MultiScanCount(indexName, bucketName, server string, scans qc.Scans, distinct bool,
	consistency c.Consistency, vector *qc.TsConsistency) (int64, error) {

	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLMultiScanCount(indexName, bucketName, server, scans, distinct,
			consistency, vector)
	}

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return 0, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	count, err := client.MultiScanCount(defnID, "", scans, distinct, consistency, vector)
	if err != nil {
		return 0, err
	} else {
		return count, nil
	}
}

func Scan3(indexName, bucketName, server string, scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64, groupAggr *qc.GroupAggr,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	if UseClient == "n1ql" {
		log.Printf("Using n1ql client")
		return N1QLScan3(indexName, bucketName, server, scans, reverse, distinct,
			projection, offset, limit, groupAggr, consistency, vector)
	}

	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)

	count := 0
	start := time.Now()
	connErr := client.Scan3(
		defnID, "", scans, reverse, distinct, projection, offset, limit, groupAggr,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			if err := response.Error(); err != nil {
				scanErr = err
				log.Printf("ScanError = %v ", scanErr)
				return false
			} else if skeys, pkeys, err := response.GetEntries(); err != nil {
				scanErr = err
				log.Printf("ScanError = %v ", scanErr)
				return false
			} else {
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					//log.Printf("Scanresult count = %v: %v  : %v ", count, skey, primaryKey)
					count++
					if _, keyPresent := scanResults[primaryKey]; keyPresent {
						// Duplicate primary key found
						dupError := errors.New("Duplicate primary key found")
						tc.HandleError(dupError, "Duplicate primary key found in the scan results: "+primaryKey)
					} else {
						// Test collation only if CheckCollation is true
						if CheckCollation == true && len(skey) > 0 {
							secVal := skey2Values(skey)[0]
							if previousSecKey == nil {
								previousSecKey = secVal
							} else {
								if secVal.Collate(previousSecKey) < 0 {
									errMsg := "Collation check failed. Previous Sec key > Current Sec key"
									scanErr = errors.New(errMsg)
									return false
								}
							}
						}

						scanResults[primaryKey] = skey
					}
				}
				return true
			}
			return false
		})
	elapsed := time.Since(start)

	if connErr != nil {
		log.Printf("Connection error in Scan occured: %v", connErr)
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}

	tc.LogPerfStat("MultiScan", elapsed)
	log.Printf("Total Scanresults = %v", count)
	return scanResults, nil
}

func skey2Values(skey c.SecondaryKey) []value.Value {
	vals := make([]value.Value, len(skey))
	for i := 0; i < len(skey); i++ {
		if s, ok := skey[i].(string); ok && collatejson.MissingLiteral.Equal(s) {
			vals[i] = value.NewMissingValue()
		} else {
			vals[i] = value.NewValue(skey[i])
		}
	}
	return vals
}
