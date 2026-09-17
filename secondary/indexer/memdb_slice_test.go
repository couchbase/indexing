package indexer

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"golang.org/x/sync/semaphore"
)

var N *int
var isPrimary *bool
var lockThreads *bool

func TestMain(m *testing.M) {
	N = flag.Int("n", 10000000, "total number of docs")
	isPrimary = flag.Bool("primary", false, "Is primary index")
	lockThreads = flag.Bool("lockThreads", false, "Lock worker goroutines to a thread")
	flag.Parse()
	logging.SetLogLevel(logging.Error)
	os.Exit(m.Run())
}

const keySize = 25
const snapIncrInterval = time.Millisecond * 10
const snapInitInterval = time.Millisecond * 10

type ientry struct {
	e     []byte
	docid []byte
	m     *MutationMeta
}

func randString(r *rand.Rand, n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = alphanum[r.Intn(len(alphanum))]
	}
	return string(bytes)
}

func mutationProducer(wg *sync.WaitGroup, s Slice, offset, n, id int, isRand bool, stream chan *ientry) {
	defer wg.Done()

	if *lockThreads {
		runtime.LockOSThread()
	}

	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		docN := i + offset
		if isRand {
			docN = rnd.Int()%n + offset
		}

		docid := []byte(fmt.Sprintf("docid-%d", docN))
		key := []byte("[\"" + randString(rnd, keySize) + "\"]")
		meta := NewMutationMeta()
		meta.vbucket = Vbucket(id)

		stream <- &ientry{e: key, m: meta, docid: docid}

	}
}

func flushWorker(wg *sync.WaitGroup, stream chan *ientry, n int, slice Slice) {
	defer wg.Done()

	if *lockThreads {
		runtime.LockOSThread()
	}

	for i := 0; i < n; i++ {
		entry := <-stream
		slice.Insert(entry.e, entry.docid, nil, nil, nil, entry.m)
		entry.m.Free()
	}
}

func runFlusher(interval time.Duration, streams []chan *ientry, slice Slice, finch chan bool) {
	var snap Snapshot
	var wg sync.WaitGroup

	for {
		for _, ch := range streams {
			n := len(ch)
			wg.Add(1)
			go flushWorker(&wg, ch, n, slice)
		}

		wg.Wait()

		info, err := slice.NewSnapshot(nil, false)
		common.CrashOnError(err)
		if snap != nil {
			snap.Close()
		}
		snap, err = slice.OpenSnapshot(info, nil)
		common.CrashOnError(err)

		select {
		case <-time.After(interval):
		case <-finch:
			return
		}
	}
}

func TestMemDBInsertionPerf(t *testing.T) {
	var wg sync.WaitGroup
	finch := make(chan bool)
	nw := runtime.GOMAXPROCS(0)
	nPerWriter := *N / nw
	streams := make([]chan *ientry, nw)
	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", nw)
	idxDefn := common.IndexDefn{
		DefnId:       common.IndexDefnId(0),
		IsArrayIndex: false}
	slice, err := NewMemDBSlice("/tmp/mdbslice",
		SliceId(0), idxDefn, common.IndexInstId(0), common.PartitionId(0), *isPrimary, true, 1,
		cfg, stats, 1024, EncrCbsTest)
	common.CrashOnError(err)

	// Initial build
	t1 := time.Now()
	for i := 0; i < nw; i++ {
		wg.Add(1)
		streams[i] = make(chan *ientry, 500000)
		if i == nw-1 {
			nPerWriter = *N - nPerWriter*i
		}

		go mutationProducer(&wg, slice, i*nPerWriter, nPerWriter, i, false, streams[i])
	}

	go func() {
		wg.Wait()
		finch <- true
	}()

	runFlusher(snapInitInterval, streams, slice, finch)
	dur1 := time.Since(t1)

	// Incremental update
	t2 := time.Now()
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go mutationProducer(&wg, slice, i*nPerWriter, nPerWriter, i, false, streams[i])
	}

	go func() {
		wg.Wait()
		finch <- true
	}()

	runFlusher(snapIncrInterval, streams, slice, finch)
	dur2 := time.Since(t2)
	fmt.Printf("Initial build: %d items took %v -> %v items/s\n", *N, dur1, float64(*N)/dur1.Seconds())
	fmt.Printf("Incr build: %d items took %v -> %v items/s\n", *N, dur2, float64(*N)/dur2.Seconds())
	fmt.Println("Main Index:", slice.mainstore.DumpStats())
	if !*isPrimary {
		for i := 0; i < slice.numWriters; i++ {
			fmt.Println("Back Index", i, ":", slice.back[i].Stats())
		}
	}
}

const persistTimeout = 10 * time.Second

func resetMOIWriters(t *testing.T, capacity int) {
	oldSem, oldMax, oldAllowed := moiWriterSemaphore, moiMaxWritersAllowed, moiWritersAllowed
	oldTotal := atomic.SwapInt64(&totalMemDBItems, 0)
	moiWriterSemaphore = semaphore.NewWeighted(int64(capacity))
	moiMaxWritersAllowed, moiWritersAllowed = capacity, capacity
	t.Cleanup(func() {
		moiWriterSemaphore, moiMaxWritersAllowed, moiWritersAllowed = oldSem, oldMax, oldAllowed
		atomic.StoreInt64(&totalMemDBItems, oldTotal)
	})
}

func newCommittedSlice(t *testing.T, persistenceThreads, n int) (*memdbSlice, SnapshotInfo) {
	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", 1)
	cfg.SetValue("settings.moi.persistence_threads", persistenceThreads)
	slice, err := NewMemDBSlice(t.TempDir(), SliceId(0), common.IndexDefn{}, common.IndexInstId(0),
		common.PartitionId(0), *isPrimary, true, 1, cfg, stats, 1024, EncrCbsTest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(slice.Close)

	// load n docs
	var wg sync.WaitGroup
	stream := make(chan *ientry, n)
	wg.Add(2)
	go mutationProducer(&wg, slice, 0, n, 0, false, stream)
	go flushWorker(&wg, stream, n, slice)
	wg.Wait()

	// OSO snapshot, so cleanupOldSnapshotFiles skips the cluster seqno lookup
	ts := common.NewTsVbuuid("default", slice.numVbuckets)
	ts.SetSnapType(common.DISK_SNAP_OSO)
	info, err := slice.NewSnapshot(ts, true)
	if err != nil {
		t.Fatal(err)
	}
	return slice, info
}

func startPersist(t *testing.T, slice *memdbSlice, info SnapshotInfo) {
	snap, err := slice.OpenSnapshot(info, &sync.Once{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { snap.Close() })
	if atomic.LoadInt32(&slice.isPersistorActive) != 1 {
		t.Fatal("persistence did not start")
	}
}

// a persister at the semaphore holds moiWriterSemaphoreLk as reader
func waitAtSemaphore(t *testing.T) {
	for deadline := time.Now().Add(persistTimeout); moiWriterSemaphoreLk.TryLock(); {
		moiWriterSemaphoreLk.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no persister reached the semaphore")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitPersisted(t *testing.T, slice *memdbSlice) {
	for deadline := time.Now().Add(persistTimeout); atomic.LoadInt32(&slice.isPersistorActive) != 0; {
		if time.Now().After(deadline) {
			t.Fatalf("persistence did not finish within %v: deadlock", persistTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if infos, err := slice.GetSnapshots(); err != nil || len(infos) == 0 {
		t.Fatalf("no disk snapshot after persistence (err %v)", err)
	}
}

// TestMemDBPersisterSemaphore verifies that persisters waiting on moiWriterSemaphore
// cannot deadlock by each taking part of the other's share (MB-69408). The test
// holds tokens itself in place of the slice that is mid-persistence.
func TestMemDBPersisterSemaphore(t *testing.T) {
	ctx := context.Background()

	t.Run("HeavyIngest", func(t *testing.T) {
		resetMOIWriters(t, 4)

		// A is persisting with all 4 threads
		moiWriterSemaphore.Acquire(ctx, 4)

		// B: 100 of 100 docs, share 4
		sliceB, infoB := newCommittedSlice(t, 4, 100)
		startPersist(t, sliceB, infoB)
		waitAtSemaphore(t)

		// C: 400 of 500 docs, share 4; B already holds the read lock, so give C time to queue
		sliceC, infoC := newCommittedSlice(t, 4, 400)
		startPersist(t, sliceC, infoC)
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadInt32(&sliceB.isPersistorActive) == 0 || atomic.LoadInt32(&sliceC.isPersistorActive) == 0 {
			t.Fatal("persisted while A holds all threads")
		}

		// A finishes; with per token acquires B and C would each get 2 and hang
		moiWriterSemaphore.Release(4)
		waitPersisted(t, sliceB)
		waitPersisted(t, sliceC)
	})

	t.Run("ConfigChange", func(t *testing.T) {
		resetMOIWriters(t, 8)
		updateMOIWriters(4)

		// X: 100 docs, persisting with 2 of the 4 threads
		newCommittedSlice(t, 4, 100)
		moiWriterSemaphore.Acquire(ctx, 2)

		// persistence_threads raised to 8 in the slice config, semaphore not yet updated
		// Y: 100 of 200 docs, share 4
		sliceY, infoY := newCommittedSlice(t, 8, 100)
		startPersist(t, sliceY, infoY)
		waitAtSemaphore(t)

		// Z: 2000 of 2200 docs, share 8 > allowed, retries till capacity grows
		sliceZ, infoZ := newCommittedSlice(t, 8, 2000)
		startPersist(t, sliceZ, infoZ)

		updated := make(chan struct{})
		go func() {
			updateMOIWriters(8)
			close(updated)
		}()

		// X finishes
		moiWriterSemaphore.Release(2)
		waitPersisted(t, sliceY)
		waitPersisted(t, sliceZ)
		select {
		case <-updated:
		case <-time.After(persistTimeout):
			t.Fatal("updateMOIWriters did not finish")
		}
		if moiWritersAllowed != 8 {
			t.Fatalf("moiWritersAllowed %d, want 8", moiWritersAllowed)
		}
	})
}
