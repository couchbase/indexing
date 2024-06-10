package common

import (
	"fmt"

	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

const S_RESULT_CHANNEL_SIZE = 100

//FetchRandomKVSample fetches a given sampleSize number of documents
//from a collection. It returns "datach" on which sampled documents are
//returned in DcpEvent format. Any error during sampling is sent on
//the err channel. datach is closed by the sender when all the sample
//docs have been sent.
//
//Caller must close the donech to indicate it no longer wants to read from
//datach. If any scan is in progress and donech is closed, the scan will be aborted.

func FetchRandomKVSample(cluster, pooln, bucketn,
	scope, collection, cid string, sampleSize int64,
	donech chan bool) (datach chan *memcached.DcpEvent, errch chan error, ret error) {

	//panic safe
	defer func() {
		if r := recover(); r != nil {
			ret = fmt.Errorf("%v", r)
			logging.Warnf("FetchRandomKVSample failed : %v", ret)
		}
	}()

	logging.Infof("FetchRandomKVSample %v:%v:%v SampleSize %v", bucketn, scope, collection, sampleSize)

	bucket, err := ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Warnf("FetchRandomKVSample failed : %v", err)
		return nil, nil, err
	}

	defer func() {
		if err != nil {
			bucket.Close()
		}
	}()

	rs, err := bucket.NewRandomScanner(cid, sampleSize)
	if err != nil {
		logging.Warnf("FetchRandomKVSample failed : %v", err)
		return nil, nil, err
	}

	go func() {

		defer bucket.Close()
		select {

		case <-donech:
			rs.StopScan()

		}
	}()

	return rs.StartRandomScan()

}
