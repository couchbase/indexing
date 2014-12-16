package secondaryindex

import (
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
)

var scanResults tc.ScanResponse

func Range(indexName, bucketName string, low, high []interface{}, inclusion uint32,
			distinct bool, limit int64) (tc.ScanResponse, error) {
	c.LogIgnore()
	addr := "localhost:9101" // Get from config

	// ToDo: Create a client pool
	client := qc.NewClient(qc.Remoteaddr(addr), c.SystemConfig.SectionConfig("queryport.client.", true))
	scanResults = make(tc.ScanResponse)
	
	err := client.Range(indexName, bucketName, c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit, scanCallback)
	client.Close()
	return scanResults, err
}

func Lookup(indexName, bucketName string, values []interface{}, distinct bool, limit int64) (tc.ScanResponse, error) {
	c.LogIgnore()
	addr := "localhost:9101" // Get from config

	// ToDo: Create a client pool
	client := qc.NewClient(qc.Remoteaddr(addr), c.SystemConfig.SectionConfig("queryport.client.", true))
	scanResults = make(tc.ScanResponse)
	err := client.Lookup(indexName, bucketName, []c.SecondaryKey { values }, distinct, limit, scanCallback)
	client.Close()
	return scanResults, err
}

func scanCallback(response qc.ResponseReader) bool {
	if err := response.Error(); err != nil {
			panic(err.Error())
			return false
	} else if skeys, pkeys, err := response.GetEntries(); err != nil {
			panic(err.Error())
			return false
	} else {
			for i, skey := range skeys {
				primaryKey := string(pkeys[i])
				if _, keyPresent := scanResults[primaryKey]; keyPresent {
    				// Duplicate primary key found
					tc.HandleError(err, "Duplicate primary key found in the scan results: " + primaryKey)
				} else {
    				scanResults[primaryKey] = skey
				}
			}
			return true
		}
	return false
}