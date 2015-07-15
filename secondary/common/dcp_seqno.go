package common

import "sync"
import "time"
import "fmt"
import "sort"

import "github.com/couchbase/indexing/secondary/dcp"
import "github.com/couchbase/indexing/secondary/logging"

// cache Bucket{} and DcpFeed{} objects, its underlying connections
// to make Stats-Seqnos fast.
var dcp_buckets_seqnos struct {
	rw        sync.RWMutex
	numVbs    int
	buckets   map[string]*couchbase.Bucket
	readerMap map[string]*vbSeqnosReader // bucket->kvaddr->feed
}

func init() {
	dcp_buckets_seqnos.buckets = make(map[string]*couchbase.Bucket)
	dcp_buckets_seqnos.readerMap = make(map[string]*vbSeqnosReader)
	go pollForDeletedBuckets()
}

type vbSeqnosResponse struct {
	seqnos []uint64
	err    error
}

type vbSeqnosRequest chan *vbSeqnosResponse

func (ch *vbSeqnosRequest) Reply(response *vbSeqnosResponse) {
	*ch <- response
}

func (ch *vbSeqnosRequest) Response() ([]uint64, error) {
	response := <-*ch
	return response.seqnos, response.err
}

// Bucket level seqnos reader for the cluster
type vbSeqnosReader struct {
	kvfeeds   map[string]*couchbase.DcpFeed
	requestCh chan vbSeqnosRequest
}

func newVbSeqnosReader(kvfeeds map[string]*couchbase.DcpFeed) *vbSeqnosReader {
	r := &vbSeqnosReader{
		kvfeeds:   kvfeeds,
		requestCh: make(chan vbSeqnosRequest, 5000),
	}

	go r.Routine()
	return r
}

func (r *vbSeqnosReader) Close() {
	close(r.requestCh)
}

func (r *vbSeqnosReader) GetSeqnos() ([]uint64, error) {
	req := make(vbSeqnosRequest, 1)
	r.requestCh <- req
	return req.Response()
}

// This routine is responsible for computing request batches on the fly
// and issue single 'dcp seqno' per batch.
func (r *vbSeqnosReader) Routine() {
	for req := range r.requestCh {
		l := len(r.requestCh)
		seqnos, err := CollectSeqnos(r.kvfeeds)
		response := &vbSeqnosResponse{
			seqnos: seqnos,
			err:    err,
		}
		req.Reply(response)

		// Read outstanding requests that can be served by
		// using the same response
		for i := 0; i < l; i++ {
			req := <-r.requestCh
			req.Reply(response)
		}
	}

	// Cleanup all feeds
	for _, kvfeed := range r.kvfeeds {
		kvfeed.Close()
	}
}

func addDBSbucket(cluster, pooln, bucketn string) (err error) {
	var bucket *couchbase.Bucket

	bucket, err = ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return err
	}
	dcp_buckets_seqnos.buckets[bucketn] = bucket

	// get all kv-nodes
	if err = bucket.Refresh(); err != nil {
		logging.Errorf("bucket.Refresh(): %v\n", err)
		return err
	}

	// get current list of kv-nodes
	var m map[string][]uint16
	m, err = bucket.GetVBmap(nil)
	if err != nil {
		logging.Errorf("GetVBmap() failed: %v\n", err)
		return err
	}
	// calculate and cache the number of vbuckets.
	if dcp_buckets_seqnos.numVbs == 0 { // to happen only first time.
		for _, vbnos := range m {
			dcp_buckets_seqnos.numVbs += len(vbnos)
		}
	}

	// make sure a feed is available for all kv-nodes
	var kvfeed *couchbase.DcpFeed

	kvfeeds := make(map[string]*couchbase.DcpFeed)
	config := map[string]interface{}{"genChanSize": 1000, "dataChanSize": 10}
	for kvaddr := range m {
		uuid, _ := NewUUID()
		name := uuid.Str()
		if name == "" {
			err = fmt.Errorf("invalid uuid")
			logging.Errorf("NewUUID() failed: %v\n", err)
			return err
		}
		name = "dcp-get-seqnos:" + name
		kvfeed, err = bucket.StartDcpFeedOver(
			name, uint32(0), []string{kvaddr}, uint16(0xABBA), config)
		if err != nil {
			logging.Errorf("StartDcpFeedOver(): %v\n", err)
			return err
		}
		kvfeeds[kvaddr] = kvfeed
	}
	dcp_buckets_seqnos.readerMap[bucketn] = newVbSeqnosReader(kvfeeds)

	logging.Infof("{bucket,feeds} %q created for dcp_seqno cache...\n", bucketn)
	return nil
}

func delDBSbucket(bucketn string) {
	dcp_buckets_seqnos.rw.Lock()
	defer dcp_buckets_seqnos.rw.Unlock()

	bucket, ok := dcp_buckets_seqnos.buckets[bucketn]
	if ok && bucket != nil {
		bucket.Close()
	}
	delete(dcp_buckets_seqnos.buckets, bucketn)

	reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
	if ok && reader != nil {
		reader.Close()
	}
	delete(dcp_buckets_seqnos.readerMap, bucketn)
}

// BucketSeqnos return list of {{vbno,seqno}..} for all vbuckets.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
func BucketSeqnos(cluster, pooln, bucketn string) (l_seqnos []uint64, err error) {
	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.Lock()
		defer dcp_buckets_seqnos.rw.Unlock()

		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		if !ok { // no {bucket,kvfeeds} found, create!
			if err = addDBSbucket(cluster, pooln, bucketn); err != nil {
				return nil, err
			}
			reader = dcp_buckets_seqnos.readerMap[bucketn]
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	return reader.GetSeqnos()
}

func CollectSeqnos(kvfeeds map[string]*couchbase.DcpFeed) (l_seqnos []uint64, err error) {
	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([]map[uint16]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *couchbase.DcpFeed) {
			defer wg.Done()
			kv_seqnos_node[index], errors[index] = feed.DcpGetSeqnos()
		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := make(map[uint16]uint64)
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.DcpGetSeqnos(): %v\n", err)
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			if prev, ok := seqnos[vbno]; !ok || prev < seqno {
				seqnos[vbno] = seqno
			}
		}
	}
	// The following code is to detect rebalance or recovery !!
	// this is not yet supported in KV, GET_SEQNOS returns all
	// seqnos.
	if len(seqnos) < dcp_buckets_seqnos.numVbs {
		fmsg := "unable to get seqnos ts for all vbuckets (%v out of %v)"
		fmt.Errorf(fmsg, len(seqnos), dcp_buckets_seqnos.numVbs)
		logging.Errorf("%v\n", err)
		return nil, err
	}
	// sort them
	vbnos := make([]int, 0, dcp_buckets_seqnos.numVbs)
	for vbno := range seqnos {
		vbnos = append(vbnos, int(vbno))
	}
	sort.Ints(vbnos)
	// gather seqnos.
	l_seqnos = make([]uint64, 0, dcp_buckets_seqnos.numVbs)
	for _, vbno := range vbnos {
		l_seqnos = append(l_seqnos, seqnos[uint16(vbno)])
	}
	return l_seqnos, nil
}

func pollForDeletedBuckets() {
	for {
		time.Sleep(10 * time.Second)
		todels := []string{}
		func() {
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()
			for bucketn, bucket := range dcp_buckets_seqnos.buckets {
				if bucket.Refresh() != nil {
					// lazy detect bucket deletes
					todels = append(todels, bucketn)
				}
			}
		}()
		func() {
			dcp_buckets_seqnos.rw.RLock()
			defer dcp_buckets_seqnos.rw.RUnlock()
			for bucketn, bucket := range dcp_buckets_seqnos.buckets {
				if m, err := bucket.GetVBmap(nil); err != nil {
					// idle detect failures.
					todels = append(todels, bucketn)
				} else if len(m) != len(dcp_buckets_seqnos.readerMap[bucketn].kvfeeds) {
					// lazy detect kv-rebalance
					todels = append(todels, bucketn)
				}
			}
		}()
		for _, bucketn := range todels {
			delDBSbucket(bucketn)
		}
	}
}
