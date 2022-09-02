package projector

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/stats"

	"github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"

	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

const seqsReqChanSize = 20000
const seqsBufSize = 64 * 1024

var errConnClosed = errors.New("dcpSeqnos - conn closed already")
var errCollectSeqnosPanic = errors.New("Recovered from an error in CollectSeqnos")
var errBucketSeqnosPanic = errors.New("Recovered from an error in BucketSeqnosLocal")

// cache Bucket{} and DcpFeed{} objects, its underlying connections
// to make Stats-Seqnos fast.
var dcp_buckets_seqnos struct {
	rw        sync.RWMutex
	buckets   map[string]*couchbase.Bucket // bucket ->*couchbase.Bucket
	errors    map[string]error             // bucket -> error
	readerMap map[string]*vbSeqnosReader   // bucket->*vbSeqnosReader
	kvaddr    string
}

func init() {
	dcp_buckets_seqnos.buckets = make(map[string]*couchbase.Bucket)
	dcp_buckets_seqnos.errors = make(map[string]error)
	dcp_buckets_seqnos.readerMap = make(map[string]*vbSeqnosReader)
}

type vbSeqnosResponse struct {
	seqnos []uint64
	err    error
}

type kvConn struct {
	mc      *memcached.Client
	seqsbuf []uint64
	tmpbuf  []byte
}

func newKVConn(mc *memcached.Client) *kvConn {
	return &kvConn{mc: mc, seqsbuf: make([]uint64, 1024), tmpbuf: make([]byte, seqsBufSize)}
}

type vbSeqnosRequest struct {
	cid        string // Collection ID
	respCh     chan *vbSeqnosResponse
	bucketName string
}

func (req *vbSeqnosRequest) Reply(response *vbSeqnosResponse) {
	req.respCh <- response
}

func (req *vbSeqnosRequest) Response() ([]uint64, error) {
	response := <-req.respCh
	return response.seqnos, response.err
}

// Bucket level seqnos reader for the cluster
type vbSeqnosReader struct {
	bucket     string
	kvfeeds    map[string]*kvConn
	requestCh  chan vbSeqnosRequest
	seqsTiming stats.TimingStat
}

func newVbSeqnosReader(bucket string, kvfeeds map[string]*kvConn) *vbSeqnosReader {
	r := &vbSeqnosReader{
		bucket:    bucket,
		kvfeeds:   kvfeeds,
		requestCh: make(chan vbSeqnosRequest, seqsReqChanSize),
	}

	r.seqsTiming.Init()

	go r.Routine()
	return r
}

func (r *vbSeqnosReader) Close() {
	close(r.requestCh)
}

func (r *vbSeqnosReader) GetSeqnos(cid string) (seqs []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errConnClosed
		}
	}()

	req := vbSeqnosRequest{
		cid:        cid,
		respCh:     make(chan *vbSeqnosResponse),
		bucketName: r.bucket,
	}
	r.requestCh <- req
	seqs, err = req.Response()
	return
}

// This routine is responsible for computing request batches on the fly
// and issue single 'dcp seqno' per batch.
func (r *vbSeqnosReader) Routine() {
	for req := range r.requestCh {
		l := len(r.requestCh)
		t0 := time.Now()
		seqnos, err := CollectSeqnos(r.kvfeeds, req.cid, req.bucketName)
		response := &vbSeqnosResponse{
			seqnos: seqnos,
			err:    err,
		}
		r.seqsTiming.Put(time.Since(t0))
		if err != nil {
			dcp_buckets_seqnos.rw.Lock()
			dcp_buckets_seqnos.errors[r.bucket] = err
			dcp_buckets_seqnos.rw.Unlock()
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
		kvfeed.mc.Close()
	}
}

func addDBSbucket(cluster, pooln, bucketn, kvaddr string) (err error) {
	var bucket *couchbase.Bucket

	bucket, err = common.ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		logging.Errorf("Unable to connect with bucket %q\n", bucketn)
		return err
	}

	kvfeeds := make(map[string]*kvConn)

	defer func() {
		if err == nil {
			dcp_buckets_seqnos.buckets[bucketn] = bucket
			dcp_buckets_seqnos.readerMap[bucketn] = newVbSeqnosReader(bucketn, kvfeeds)
		} else {
			for _, kvfeed := range kvfeeds {
				kvfeed.mc.Close()
			}
		}
	}()

	// get all kv-nodes
	if err = bucket.Refresh(); err != nil {
		logging.Errorf("bucket.Refresh(): %v\n", err)
		return err
	}

	// get current list of kv-nodes
	var m map[string][]uint16
	m, err = bucket.GetVBmap([]string{kvaddr})
	if err != nil {
		logging.Errorf("GetVBmap() failed: %v\n", err)
		return err
	}

	// Empty kv-nodes list without error should never happen.
	// Return an error and caller can retry on error if needed.
	if len(m) == 0 {
		err = fmt.Errorf("Empty kv-nodes list")
		logging.Errorf("addDBSbucket:: Error %v for bucket %v", err, bucketn)
		return err
	}

	// make sure a feed is available for all kv-nodes
	var conn *memcached.Client

	for kvaddr := range m {
		uuid, _ := common.NewUUID()
		name := uuid.Str()
		if name == "" {
			err = fmt.Errorf("invalid uuid")
			logging.Errorf("NewUUID() failed: %v\n", err)
			return err
		}
		fname := couchbase.NewDcpFeedName("getseqnos-" + name)
		conn, err = bucket.GetDcpConn(fname, kvaddr)
		if err != nil {
			logging.Errorf("StartDcpFeedOver(): %v\n", err)
			return err
		}
		kvfeeds[kvaddr] = newKVConn(conn)
	}

	logging.Infof("{bucket,feeds} %q created for dcp_seqno cache...\n", bucketn)
	return nil
}

func delDBSbucket(bucketn string, checkErr bool) {
	dcp_buckets_seqnos.rw.Lock()
	defer dcp_buckets_seqnos.rw.Unlock()

	if !checkErr || dcp_buckets_seqnos.errors[bucketn] != nil {
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

		delete(dcp_buckets_seqnos.errors, bucketn)
	}
}

func BucketSeqsTiming(bucket string) *stats.TimingStat {
	dcp_buckets_seqnos.rw.RLock()
	defer dcp_buckets_seqnos.rw.RUnlock()
	if reader, ok := dcp_buckets_seqnos.readerMap[bucket]; ok {
		return &reader.seqsTiming
	}

	return nil
}

// BucketSeqnosLocal return list of {{vbno,seqno}..} for vbuckets
// belonging to the KV node where this projector is running.
// this call might fail due to,
// - concurrent access that can preserve a deleted/failed bucket object.
// - pollForDeletedBuckets() did not get a chance to cleanup
//   a deleted bucket.
// in both the cases if the call is retried it should get fixed, provided
// a valid bucket exists.
func SeqnosLocal(cluster, pooln, bucketn, cid, kvaddr string) (l_seqnos []uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			if cid == "" {
				logging.Warnf("Error encountered while retrieving bucket seqnos"+
					" for bucket: %v from kvaddr: %v, err: %v. Ignored", bucketn, kvaddr, r)
			} else {
				logging.Warnf("Error encountered while retrieving collection seqnos"+
					" for bucket: %v, for collection id: %v from kvaddr: %v, err: %v. Ignored", bucketn, cid, kvaddr, r)
			}
			l_seqnos = nil
			err = errBucketSeqnosPanic
		}
	}()

	// any type of error will cleanup the bucket and its kvfeeds.
	defer func() {
		if err != nil {
			delDBSbucket(bucketn, true)
		}
	}()

	var reader *vbSeqnosReader

	reader, err = func() (*vbSeqnosReader, error) {
		dcp_buckets_seqnos.rw.RLock()
		reader, ok := dcp_buckets_seqnos.readerMap[bucketn]
		dcp_buckets_seqnos.rw.RUnlock()
		if !ok { // no {bucket,kvfeeds} found, create!
			dcp_buckets_seqnos.rw.Lock()
			defer dcp_buckets_seqnos.rw.Unlock()

			dcp_buckets_seqnos.kvaddr = kvaddr
			// Recheck if reader is still not present since we acquired write lock
			// after releasing the read lock.
			if reader, ok = dcp_buckets_seqnos.readerMap[bucketn]; !ok {
				if err = addDBSbucket(cluster, pooln, bucketn, kvaddr); err != nil {
					return nil, err
				}
				// addDBSbucket has populated the reader
				reader = dcp_buckets_seqnos.readerMap[bucketn]
			}
		}
		return reader, nil
	}()
	if err != nil {
		return nil, err
	}

	l_seqnos, err = reader.GetSeqnos(cid) // For a bucket, cid is empty
	return
}

func ResetBucketSeqnos() error {
	dcp_buckets_seqnos.rw.Lock()

	bucketns := make([]string, 0, len(dcp_buckets_seqnos.buckets))

	for bucketn, _ := range dcp_buckets_seqnos.buckets {
		bucketns = append(bucketns, bucketn)
	}

	dcp_buckets_seqnos.rw.Unlock()

	for _, bucketn := range bucketns {
		delDBSbucket(bucketn, false)
	}

	return nil
}

func CollectSeqnos(kvfeeds map[string]*kvConn, cid string, bucketName string) (l_seqnos []uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Return error as callers take care of retry.
			logging.Errorf("%v: number of kvfeeds is %d", errCollectSeqnosPanic, len(kvfeeds))
			l_seqnos = nil
			err = errCollectSeqnosPanic
		}
	}()

	var wg sync.WaitGroup

	// Buffer for storing kv_seqs from each node
	kv_seqnos_node := make([][]uint64, len(kvfeeds))
	errors := make([]error, len(kvfeeds))

	if len(kvfeeds) == 0 {
		err = fmt.Errorf("Empty kvfeeds")
		logging.Errorf("CollectSeqnos:: %v", err)
		return nil, err
	}

	i := 0
	for _, feed := range kvfeeds {
		wg.Add(1)
		go func(index int, feed *kvConn) {
			defer wg.Done()
			kv_seqnos_node[index] = feed.seqsbuf
			if cid == "" {
				errors[index] = couchbase.GetSeqs(feed.mc, kv_seqnos_node[index], feed.tmpbuf)
			} else {
				// A call to CollectionSeqnos implies cluster is fully upgraded to 7.0
				err := tryEnableCollection(feed.mc)
				if err != nil {
					errors[index] = err
					return
				}
				errors[index] = couchbase.GetCollectionSeqs(feed.mc, kv_seqnos_node[index], feed.tmpbuf, cid)
			}

		}(i, feed)
		i++
	}

	wg.Wait()

	seqnos := kv_seqnos_node[0]
	for i, kv_seqnos := range kv_seqnos_node {
		err := errors[i]
		if err != nil {
			logging.Errorf("feed.DcpGetSeqnos(): %v\n", err)
			return nil, err
		}

		for vbno, seqno := range kv_seqnos {
			prev := seqnos[vbno]
			if prev < seqno {
				seqnos[vbno] = seqno
			}
		}
	}
	// The following code is to detect rebalance or recovery !!
	// this is not yet supported in KV, GET_SEQNOS returns all
	// seqnos.
	numVBuckets, ok := func() (int, bool) {
		dcp_buckets_seqnos.rw.RLock()
		defer dcp_buckets_seqnos.rw.RUnlock()

		nvb, nvbok := dcp_buckets_seqnos.buckets[bucketName]
		if !nvbok {
			return 0, nvbok
		}

		return nvb.NumVBuckets, true
	}()

	if !ok {
		fmsg := "unable to get numVBuckets for bucket: %v len(seqnos): %v numVBuckets: %v)"
		err = fmt.Errorf(fmsg, bucketName, len(seqnos), numVBuckets)
		logging.Errorf("%v\n", err)
		return nil, err
	}

	if len(seqnos) < numVBuckets {
		fmsg := "unable to get seqnos ts for all vbuckets (%v out of %v)"
		err = fmt.Errorf(fmsg, len(seqnos), numVBuckets)
		logging.Errorf("%v\n", err)
		return nil, err
	}

	// Since projector only gets the seqnos from local node,
	// and `seqnos` return sequence number of all vbuckets,
	// initialize the `l_seqnos` for all vbuckets even though
	// some of the values would be empty
	l_seqnos = make([]uint64, len(seqnos))
	copy(l_seqnos, seqnos)

	return l_seqnos, nil
}
func PollForDeletedBucketsV2(clusterUrl string, pool string, config common.Config) {

	// If using cinfo lite is disabled, use old way of polling ns_server
	if val, ok := config["projector.use_cinfo_lite"]; !ok || val.Bool() == false {
		logging.Warnf("PollForDeletedBucketsV2: Falling back to pollForDeletedBuckets() as use_cinfo_lite is false or not present")
		go pollForDeletedBuckets()
		return
	}

	retryCount := 0
loop:
	cicl, err := common.NewClusterInfoCacheLiteClient(clusterUrl, pool, "DeletedBucketsPoll",
		config.SectionConfig("projector.cinfo_lite.", true))
	if err != nil {
		retryCount++
		logging.Errorf("PollForDeletedBucktesV2: Error while initiliasing cinfo client, err: %v", err)
		if retryCount < 5 {
			time.Sleep(100 * time.Millisecond)
			goto loop
		} else {
			logging.Warnf("PollForDeletedBucketsV2: Falling back to pollForDeletedBuckets() due to errors while " +
				"initialising cinfo lite")
			go pollForDeletedBuckets() // Fall back to the old way of monitoring buckets
			return
		}
	}

	for {
		time.Sleep(10 * time.Second)
		todels := []string{}
		var bucketn string
		var bucket *couchbase.Bucket

		func() {
			dcp_buckets_seqnos.rw.RLock()
			defer func() {
				if r := recover(); r != nil {
					logging.Warnf("PollForDeletedBucketsV2: failover race in bucket: %v", r)
					todels = append(todels, bucketn)
				}
				dcp_buckets_seqnos.rw.RUnlock()
			}()

			for bucketn, bucket = range dcp_buckets_seqnos.buckets {
				bucketInfo, err := cicl.GetBucketInfo(bucketn)
				if err != nil {
					logging.Infof("pollForDeletedBucketsV2: Deleting bucket: %v from book-keeping "+
						"due to error while retrieving bucket info, err: %v", bucketn, err)
					todels = append(todels, bucketn)
					continue
				}

				bucketUUID := bucketInfo.GetBucketUUID(bucketn)
				if bucketUUID != bucket.UUID {
					logging.Infof("pollForDeletedBucketsV2: Deleting bucket: %v from book-keeping "+
						"due to UUID mismatch, currUUID: %v, uuid in book-keeping: %v ", bucketn, bucketUUID, bucket.UUID)
					todels = append(todels, bucketn)
					continue
				}

				kvaddr := dcp_buckets_seqnos.kvaddr
				if len(kvaddr) > 0 {
					if m, err := bucket.GetVBmap([]string{kvaddr}); err != nil {
						logging.Infof("pollForDeletedBucketsV2: Deleting bucket: %v from book-keeping "+
							"as some vbuckets are not active due to failed KV nodes, kvaddr: %v, err: %v", bucketn, kvaddr, err)
						// idle detect failures.
						todels = append(todels, bucketn)
					} else if len(m) != len(dcp_buckets_seqnos.readerMap[bucketn].kvfeeds) {
						logging.Infof("pollForDeletedBucketsV2: Deleting bucket: %v from book-keeping "+
							"due to mismatch in number of KV feeds and KV nodes, kvfeeds: %v, kvnodes: %v",
							bucketn, dcp_buckets_seqnos.readerMap[bucketn].kvfeeds, m)
						// lazy detect kv-rebalance
						todels = append(todels, bucketn)
					}
				}
			}
		}()
		for _, bucketn := range todels {
			delDBSbucket(bucketn, false)
		}
	}
}

func pollForDeletedBuckets() {
	for {
		time.Sleep(10 * time.Minute)
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
			var bucketn string
			var bucket *couchbase.Bucket

			dcp_buckets_seqnos.rw.RLock()
			defer func() {
				if r := recover(); r != nil {
					logging.Warnf("failover race in bucket: %v", r)
					todels = append(todels, bucketn)
				}
				dcp_buckets_seqnos.rw.RUnlock()
			}()
			for bucketn, bucket = range dcp_buckets_seqnos.buckets {
				kvaddr := dcp_buckets_seqnos.kvaddr
				if len(kvaddr) > 0 {
					if m, err := bucket.GetVBmap([]string{kvaddr}); err != nil {
						// idle detect failures.
						todels = append(todels, bucketn)
					} else if len(m) != len(dcp_buckets_seqnos.readerMap[bucketn].kvfeeds) {
						// lazy detect kv-rebalance
						todels = append(todels, bucketn)
					}
				}
			}
		}()
		for _, bucketn := range todels {
			delDBSbucket(bucketn, false)
		}
	}
}

func getConnName() (string, error) {
	uuid, _ := common.NewUUID()
	name := uuid.Str()
	if name == "" {
		err := fmt.Errorf("getConnName: invalid uuid.")

		// probably not a good idea to fail if uuid
		// based name fails. Can return const string
		return "", err
	}
	connName := "secidx:getseqnos" + name
	return connName, nil
}

func tryEnableCollection(conn *memcached.Client) error {
	if !conn.IsCollectionsEnabled() {
		connName, err := getConnName()
		if err != nil {
			return err
		}
		err = conn.EnableCollections(connName)
		if err != nil {
			return err
		}
	}
	return nil
}
