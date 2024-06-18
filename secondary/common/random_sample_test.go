package common

import (
	"testing"

	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

func SkipTestFetchRandomKVSample(t *testing.T) {

	cluster := "127.0.0.1:9000"
	pool := "default"
	bucket := "default"
	scope := "_default"
	collection := "_default"
	cid := "0"

	donech := make(chan bool)
	datach, errch, err := FetchRandomKVSample(cluster, pool, bucket, scope, collection, cid, 800, donech)
	if err != nil {
		t.Errorf("FetchRandomKVSample error %v", err)
	}

	processResponse(datach, errch)
	close(donech)

}

func processResponse(datach chan *memcached.DcpEvent,
	errch chan error) {

	numDocs := 0
	numErrs := 0
sample:
	for {
		select {
		case _, ok := <-datach:
			if ok {
				numDocs++
			} else {
				break sample
			}

		case err := <-errch:
			//forward to the caller
			if err != nil {
				numErrs++
			}

		}
	}

	logging.Infof("Total Docs Fetched %v, Err Count %v", numDocs, numErrs)
}
