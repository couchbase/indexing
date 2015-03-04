package secondaryindex

import (
	"errors"
	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/value"
	"log"
)

var CheckCollation = false

func RangeWithClient(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64, client *qc.GsiClient) (tc.ScanResponse, error) {
	var scanErr error
	scanErr = nil
	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)
	connErr := client.Range(
		uint64(defnID), c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit,
		c.AnyConsistency, nil,
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

	if connErr != nil {
		tc.HandleError(connErr, "Connection error in Scan")
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}
	return scanResults, nil
}

func Range(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64) (tc.ScanResponse, error) {
	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)
	connErr := client.Range(
		uint64(defnID), c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit,
		c.AnyConsistency, nil,
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

	client.Close()
	if connErr != nil {
		tc.HandleError(connErr, "Connection error in Scan")
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}
	return scanResults, nil
}

func Lookup(indexName, bucketName, server string, values []interface{}, distinct bool, limit int64) (tc.ScanResponse, error) {
	var scanErr error
	scanErr = nil
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)
	connErr := client.Lookup(
		uint64(defnID), []c.SecondaryKey{values}, distinct, limit,
		c.AnyConsistency, nil,
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

	client.Close()
	if connErr != nil {
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}
	return scanResults, nil
}

func ScanAll(indexName, bucketName, server string, limit int64) (tc.ScanResponse, error) {
	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ScanResponse)
	connErr := client.ScanAll(
		uint64(defnID), limit,
		c.AnyConsistency, nil,
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

	client.Close()
	if connErr != nil {
		return scanResults, connErr
	} else if scanErr != nil {
		return scanResults, scanErr
	}
	return scanResults, nil
}

func CountRange(indexName, bucketName, server string, low, high []interface{}, inclusion uint32) (int64, error) {
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return 0, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	count, err := client.CountRange(uint64(defnID), c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion))
	if err != nil {
		return 0, err
	} else {
		return count, nil
	}
}

func CountLookup(indexName, bucketName, server string, values []interface{}) (int64, error) {
	// ToDo: Create a client pool
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return 0, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	count, err := client.CountLookup(uint64(defnID), []c.SecondaryKey{values})
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

	defnID, _ := GetDefnID(client, bucketName, indexName)
	statistics, err := client.RangeStatistics(uint64(defnID), c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion))
	if err != nil {
		return err
	} else {
		log.Print("Statistics: %v\n\n", statistics)
		return nil
	}
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
