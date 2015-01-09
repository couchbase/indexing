package secondaryindex

import (
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

var scanResults tc.ScanResponse

func Range(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64) (tc.ScanResponse, error) {
	c.LogIgnore()
	var scanErr error
	scanErr = nil
	// ToDo: Create a client pool
	client := CreateClient(server)
	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults = make(tc.ScanResponse)
	connErr := client.Range(uint64(defnID), c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit, func(response qc.ResponseReader) bool {
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

func Lookup(indexName, bucketName, server string, values []interface{}, distinct bool, limit int64) (tc.ScanResponse, error) {
	c.LogIgnore()
	var scanErr error
	scanErr = nil
	// ToDo: Create a client pool
	client := CreateClient(server)
	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults = make(tc.ScanResponse)
	connErr := client.Lookup(uint64(defnID), []c.SecondaryKey{values}, distinct, limit, func(response qc.ResponseReader) bool {
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

func scanCallback(response qc.ResponseReader) bool {
	if err := response.Error(); err != nil {
		// panic(err.Error())
		return false
	} else if skeys, pkeys, err := response.GetEntries(); err != nil {
		// panic(err.Error())
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
}
