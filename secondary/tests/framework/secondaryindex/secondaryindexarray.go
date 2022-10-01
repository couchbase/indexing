package secondaryindex

import (
	"errors"
	"log"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/value"
)

func ArrayIndex_Range(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ArrayIndexScanResponseActual, error) {
	var scanErr error
	scanErr = nil
	var previousSecKey value.Value

	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return nil, e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	scanResults := make(tc.ArrayIndexScanResponseActual)

	tmpbuf, tmpbufPoolIdx := qc.GetFromPools()
	defer func() {
		qc.PutInPools(tmpbuf, tmpbufPoolIdx)
	}()

	start := time.Now()
	connErr := client.Range(
		defnID, "", c.SecondaryKey(low), c.SecondaryKey(high), qc.Inclusion(inclusion), distinct, limit,
		consistency, vector,
		func(response qc.ResponseReader) bool {
			j := 0
			if err := response.Error(); err != nil {
				scanErr = err
				return false
			} else if keys, pkeys, err := response.GetEntries(client.GetDataEncodingFormat()); err != nil {
				scanErr = err
				return false
			} else {
				// scanResults[primaryKey] = make([][]interface{}, len(skeys))

				skeys, err1, retBuf := keys.Get(tmpbuf)
				if err1 != nil {
					tc.HandleError(err1, "err in keys.Get")
					return false
				}
				if retBuf != nil {
					tmpbuf = retBuf
				}
				for i, skey := range skeys {
					primaryKey := string(pkeys[i])
					// Test collation only if CheckCollation is true
					if CheckCollation == true && len(skey) > 0 {
						secVal := skey[0]
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

					scanResults[primaryKey] = append(scanResults[primaryKey], skey)
					j++
				}
				return true
			}
			return false
		}, false)
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
