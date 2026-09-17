package indexer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/plasma"
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

func snapshotDirs(t *testing.T, slice *memdbSlice) []string {
	t.Helper()
	dirs, err := slice.getSnapshotDirs()
	if err != nil {
		t.Fatalf("getSnapshotDirs: %v", err)
	}
	return dirs
}

// TestMemDBSliceDropKeysWithConcurrentRollback verifies that Rollback and DropKeys
// remain mutually exclusive.
// Without the barrier the two MemDB instances would work on the same
// snapshot directories at once, one writing other reading.
//
// test uses Rollback rather than through RollbackToZero as latter calls
// cleanupAllOldSnapshotFiles, whose RemoveSnapshot takes the blocking
// dirGuard.Acquire and would stall on the parked rotation due to test artifact.
//
// This also checks that DropKey is not concurrently writing to a snapshot being
// removed else RemoveSnapshot can fail (Windows) which is treated as Rollback failure.
func TestMemDBSliceDropKeysWithConcurrentRollback(t *testing.T) {
	path := filepath.Join(os.TempDir(), "mdbslice-dropkeys")
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	var (
		keyMu       sync.Mutex
		activeKeyId = "keyA"
	)

	key := make([]byte, 32)
	cbs := SliceEncryptionCallbacks{
		getActiveKeyIdCipher: func(_, _ string) ([]byte, string, string) {
			keyMu.Lock()
			defer keyMu.Unlock()
			return key, activeKeyId, CipherNameAES256GCM
		},
		getKeyCipherById: func(_ string) ([]byte, string) { return key, CipherNameAES256GCM },
		setInUseKeys:     func(_ KeyDataType, _ string) {},
	}

	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", 1)

	slice, err := NewMemDBSlice(path, SliceId(0), common.IndexDefn{}, common.IndexInstId(0),
		common.PartitionId(0), false, true, 1, cfg, stats, 1024, cbs)
	if err != nil {
		t.Fatalf("NewMemDBSlice: %v", err)
	}
	defer slice.Close()

	for i := 0; i < 500; i++ {
		meta := NewMutationMeta()
		meta.vbucket = Vbucket(0)
		if err := slice.Insert([]byte(fmt.Sprintf("[\"key-%d\"]", i)),
			[]byte(fmt.Sprintf("docid-%d", i)), nil, nil, nil, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		meta.Free()
	}

	// two committed snapshots are persisted to disk under keyA
	persist := func(want int) {
		info, err := slice.NewSnapshot(nil, true)
		if err != nil {
			t.Fatalf("NewSnapshot: %v", err)
		}
		snap, err := slice.OpenSnapshot(info, nil)
		if err != nil {
			t.Fatalf("OpenSnapshot: %v", err)
		}

		// the next persist is skipped unless this one has fully finished:
		// doPersistSnapshot CAS-guards on isPersistorActive
		for i := 0; len(snapshotDirs(t, slice)) < want ||
			atomic.LoadInt32(&slice.isPersistorActive) != 0; i++ {
			if i == 300 {
				t.Fatalf("timed out waiting for disk snapshot %d", want)
			}
			time.Sleep(1 * time.Second)
		}
		snap.Close()
	}
	persist(1)

	for i := 500; i < 1000; i++ {
		meta := NewMutationMeta()
		meta.vbucket = Vbucket(0)
		if err := slice.Insert([]byte(fmt.Sprintf("[\"key-%d\"]", i)),
			[]byte(fmt.Sprintf("docid-%d", i)), nil, nil, nil, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		meta.Free()
	}
	persist(2)

	// GetSnapshots lists newest first: roll back to the earlier snapshot, so the
	// later one is purged by Rollback itself rather than by resetStores
	infos, err := slice.GetSnapshots()
	if err != nil || len(infos) != 2 {
		t.Fatalf("GetSnapshots: %v (n=%d)", err, len(infos))
	}
	laterSnap := infos[0].(*memdbSnapshotInfo).dataPath
	target := infos[1]
	targetPath := infos[1].(*memdbSnapshotInfo).dataPath

	keyMu.Lock()
	activeKeyId = "keyB"
	keyMu.Unlock()

	if err := slice.SetCurrentEncryptionKey(key, []byte("keyB"), CipherNameAES256GCM); err != nil {
		t.Fatalf("SetCurrentEncryptionKey: %v", err)
	}

	// Park the DropKeys in the keyA lookup of this instance.
	started := make(chan struct{})
	release := make(chan struct{})
	unpark := sync.OnceFunc(func() { close(release) })
	defer unpark()

	var parked int32
	store := slice.mainstore
	orig := store.GetKeyById
	store.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		// Only the first lookup parks, and that one is the rotation's: the hook is
		// installed after persistence, and DropKeys reaches ReadFileKeyId before
		// Rollback runs. Later lookups must pass through, because Rollback's own
		// GetSnapshots decrypts the manifest under keyA.
		if string(id) == "keyA" && atomic.CompareAndSwapInt32(&parked, 0, 1) {
			close(started)
			<-release
		}
		return orig(id)
	}

	dropCh := make(chan error, 1)
	slice.DropKeys([][]byte{[]byte("keyA")}, dropCh)

	select {
	case <-started:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the key rotation to start")
	}

	resetCh := make(chan error, 1)
	go func() { resetCh <- slice.Rollback(target) }()

	// the rotation holds dropKeyMu, so resetStores must not reach the swap
	select {
	case err := <-resetCh:
		t.Fatalf("Rollback completed (err=%v) while DropKeys was still in flight", err)
	case <-time.After(10 * time.Second):
	}

	if slice.mainstore != store {
		t.Fatal("mainstore was swapped while DropKeys was in flight")
	}

	unpark() // Rollback has cancelled the DropKeys by now

	if err := <-resetCh; err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := <-dropCh; err == nil {
		t.Fatal("a cancelled DropKeys reported success")
	}
	if slice.mainstore == store {
		t.Fatal("Rollback did not install a new mainstore instance")
	}

	// the later snapshot is purged and only the rollback target is left
	if _, err := os.Stat(laterSnap); !os.IsNotExist(err) {
		t.Fatalf("snapshot %v newer than the rollback target survived (stat err=%v)",
			laterSnap, err)
	}

	if dirs := snapshotDirs(t, slice); len(dirs) != 1 || dirs[0] != targetPath {
		t.Fatalf("expected only the rollback target %v to remain, got %v", targetPath, dirs)
	}

	// the target survives the cancelled rotation and still loads with the items it
	// held: the new instance cleans up whatever the rotation left behind, and a disk
	// snapshot reads back uncommitted, so OpenSnapshot goes through loadSnapshot
	infos, err = slice.GetSnapshots()
	if err != nil {
		t.Fatalf("GetSnapshots after rollback: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected one snapshot after rollback, got %v", len(infos))
	}

	sinfo := infos[0].(*memdbSnapshotInfo)
	if sinfo.dataPath != targetPath {
		t.Fatalf("expected snapshot %v after rollback, got %v", targetPath, sinfo.dataPath)
	}

	// A second DropKeys, this time on the instance the rollback installed, parked
	// mid-rotation. loadSnapshot must stay out of the snapshot dir until it drains:
	// the rotation renames every file it rewrites, so a reader that walks the dir
	// alongside it can miss a file or read a half-swapped one.
	store2 := slice.mainstore

	// the instance the rollback installed restored its current key from
	// getActiveKeyIdCipher, which hands back keyA: point it at keyB again, so the
	// rotation below has somewhere to rotate to
	if err := slice.SetCurrentEncryptionKey(key, []byte("keyB"), CipherNameAES256GCM); err != nil {
		t.Fatalf("SetCurrentEncryptionKey after rollback: %v", err)
	}

	started2 := make(chan struct{})
	release2 := make(chan struct{})
	unpark2 := sync.OnceFunc(func() { close(release2) })
	defer unpark2()

	// Same one-shot park as above: the rotation is the first keyA lookup on this
	// instance, because initStores already read the snapshot keyIds during Rollback.
	// Later lookups pass through, so the load can decrypt once it is let in.
	var parked2 int32
	orig2 := store2.GetKeyById
	store2.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		if string(id) == "keyA" && atomic.CompareAndSwapInt32(&parked2, 0, 1) {
			close(started2)
			<-release2
		}
		return orig2(id)
	}

	dropCh2 := make(chan error, 1)
	slice.DropKeys([][]byte{[]byte("keyA")}, dropCh2)

	select {
	case <-started2:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the second key rotation to start")
	}

	// the parked rotation holds dropKeyMu, so loadSnapshot must not reach LoadFromDisk
	var snap Snapshot
	openCh := make(chan error, 1)
	go func() {
		var er error
		snap, er = slice.OpenSnapshot(infos[0], nil)
		openCh <- er
	}()

	select {
	case er := <-openCh:
		t.Fatalf("OpenSnapshot completed (err=%v) while DropKeys was still in flight", er)
	case <-time.After(10 * time.Second):
	}

	unpark2()

	if err := <-dropCh2; err != nil {
		t.Fatalf("DropKeys after rollback: %v", err)
	}

	// the load runs once the rotation drains, and reads what the rotation left
	if err := <-openCh; err != nil {
		t.Fatalf("OpenSnapshot(%v) after rollback: %v", sinfo.dataPath, err)
	}
	defer snap.Close()

	if n := sinfo.MainSnap.Count(); n != 500 {
		t.Fatalf("rollback target loaded %v items, expected 500", n)
	}
}

// setFileDescRLimit lowers RLIMIT_NOFILE below the descriptors the process already
// holds, so the next open fails with EMFILE while existing ones keep working. The
// returned func restores the limit: keep the window short, every goroutine in the
// test binary shares it.
//
// plasma implements this only on linux; elsewhere it reports unsupported and the
// test skips.
func setFileDescRLimit(t *testing.T, soft uint64) func() {
	t.Helper()

	cur, max, err := plasma.GetFileDescRLimit()
	if err != nil {
		t.Skipf("file descriptor rlimit unsupported on this platform: %v", err)
	}

	if err := plasma.SetFileDescRLimit(soft, max); err != nil {
		t.Skipf("set file descriptor rlimit: %v", err)
	}

	return func() { plasma.SetFileDescRLimit(cur, max) }
}

// TestMemDBSliceDropKeysUnreadableManifests verifies that DropKeys reports failure
// when the snapshot list cannot be read with a retryable error, instead of treating
// "no snapshots" as "every key dropped". filepath.Glob ignores filesystem errors
// by contract, so the old code saw an empty list, rotated nothing and reported
// success - after which the caller is free to purge a key the snapshots on disk
// still carry.
//
// The injected failure is descriptor exhaustion, which is what makes this
// reachable in production: a transient resource limit, not a damaged store.
func TestMemDBSliceDropKeysUnreadableManifests(t *testing.T) {
	path := filepath.Join(os.TempDir(), "mdbslice-unreadable-manifests")
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	var (
		keyMu       sync.Mutex
		activeKeyId = "keyA"
	)

	key := make([]byte, 32) // key material is irrelevant here, the key id is not
	cbs := SliceEncryptionCallbacks{
		getActiveKeyIdCipher: func(_, _ string) ([]byte, string, string) {
			keyMu.Lock()
			defer keyMu.Unlock()
			return key, activeKeyId, CipherNameAES256GCM
		},
		getKeyCipherById: func(_ string) ([]byte, string) { return key, CipherNameAES256GCM },
		setInUseKeys:     func(_ KeyDataType, _ string) {},
	}

	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", 1)

	slice, err := NewMemDBSlice(path, SliceId(0), common.IndexDefn{}, common.IndexInstId(0),
		common.PartitionId(0), false, true, 1, cfg, stats, 1024, cbs)
	if err != nil {
		t.Fatalf("NewMemDBSlice: %v", err)
	}
	defer slice.Close()

	for i := 0; i < 200; i++ {
		meta := NewMutationMeta()
		meta.vbucket = Vbucket(0)
		if err := slice.Insert([]byte(fmt.Sprintf("[\"key-%d\"]", i)),
			[]byte(fmt.Sprintf("docid-%d", i)), nil, nil, nil, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		meta.Free()
	}

	info, err := slice.NewSnapshot(nil, true)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	snap, err := slice.OpenSnapshot(info, nil)
	if err != nil {
		t.Fatalf("OpenSnapshot: %v", err)
	}
	for i := 0; len(snapshotDirs(t, slice)) == 0 ||
		atomic.LoadInt32(&slice.isPersistorActive) != 0; i++ {
		if i == 6000 {
			t.Fatal("timed out waiting for a disk snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	snap.Close()

	// the snapshot list cannot be read while descriptors are exhausted.
	// 3 leaves only stdin/stdout/stderr below the limit, so the next open fails
	restore := setFileDescRLimit(t, 3)
	defer func() {
		restore()
	}()

	keyMu.Lock()
	activeKeyId = "keyB"
	keyMu.Unlock()

	if err := slice.SetCurrentEncryptionKey(key, []byte("keyB"), CipherNameAES256GCM); err != nil {
		t.Fatalf("SetCurrentEncryptionKey: %v", err)
	}

	_, dirsErr := slice.getSnapshotDirs()

	dropCh := make(chan error, 1)
	slice.DropKeys([][]byte{[]byte("keyA")}, dropCh)
	var dropErr error
	select {
	case dropErr = <-dropCh:
	case <-time.After(300 * time.Second):
		t.Fatal("DropKeys did not report completion")
	}

	if dirsErr == nil {
		t.Fatal("getSnapshotDirs reported no error while descriptors were exhausted")
	}
	if dropErr == nil {
		t.Fatal("DropKeys reported success while the snapshot list was unreadable")
	} else if !errors.Is(dropErr, memdb.ErrRetryDropKey) {
		t.Fatalf("expected retryable drop key error :%v", dropErr)
	} else {
		t.Logf("(expected) %v", dropErr)
	}
}

// TestMemDBSliceRollbackTargetWithConcurrentRemoveSnapshot verifies that Rollback refuses a target that
// is no longer on disk *before* it removes anything.
//
// findRollbackSnapshot lists the snapshots and hands one to Rollback, which lists
// them again; cleanupOldSnapshotFiles runs from the persistor goroutine and can
// prune the target in between (waitPersist drains the mutation queue, not the
// persistor). The removal loop stops at the target, so a target that is missing
// from the second listing means nothing stops it: without the pre-check every
// remaining snapshot is deleted and Rollback still reports success, after which
// the caller restarts the stream from the target's timestamp against an empty
// index.
//
// The assertion that matters is not that Rollback errors, it is that the other
// snapshots are untouched -- that is what pins the check ahead of the loop.
func TestMemDBSliceRollbackTargetWithConcurrentRemoveSnapshot(t *testing.T) {
	path := filepath.Join(os.TempDir(), "mdbslice-rollback-target-removed")
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	key := make([]byte, 32)
	cbs := SliceEncryptionCallbacks{
		getActiveKeyIdCipher: func(_, _ string) ([]byte, string, string) {
			return key, "keyA", CipherNameAES256GCM
		},
		getKeyCipherById: func(_ string) ([]byte, string) { return key, CipherNameAES256GCM },
		setInUseKeys:     func(_ KeyDataType, _ string) {},
	}

	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", 1)

	slice, err := NewMemDBSlice(path, SliceId(0), common.IndexDefn{}, common.IndexInstId(0),
		common.PartitionId(0), false, true, 1, cfg, stats, 1024, cbs)
	if err != nil {
		t.Fatalf("NewMemDBSlice: %v", err)
	}
	defer slice.Close()

	insert := func(from, to int) {
		for i := from; i < to; i++ {
			meta := NewMutationMeta()
			meta.vbucket = Vbucket(0)
			if err := slice.Insert([]byte(fmt.Sprintf("[\"key-%d\"]", i)),
				[]byte(fmt.Sprintf("docid-%d", i)), nil, nil, nil, meta); err != nil {
				t.Fatalf("Insert: %v", err)
			}
			meta.Free()
		}
	}

	// a real TsVbuuid: with three snapshots on disk cleanupOldSnapshotFiles runs its
	// pruning loop, which dereferences snapInfo.Timestamp()
	seqno := uint64(0)
	persist := func(want int) {
		seqno += 100
		ts := common.NewTsVbuuid("default", 1)
		ts.Seqnos[0] = seqno
		ts.Vbuuids[0] = 1

		info, err := slice.NewSnapshot(ts, true)
		if err != nil {
			t.Fatalf("NewSnapshot: %v", err)
		}
		snap, err := slice.OpenSnapshot(info, nil)
		if err != nil {
			t.Fatalf("OpenSnapshot: %v", err)
		}

		// the next persist is skipped unless this one has fully finished:
		// doPersistSnapshot CAS-guards on isPersistorActive
		for i := 0; len(snapshotDirs(t, slice)) < want ||
			atomic.LoadInt32(&slice.isPersistorActive) != 0; i++ {
			if i == 300 {
				t.Fatalf("timed out waiting for disk snapshot %d", want)
			}
			time.Sleep(1 * time.Second)
		}
		snap.Close()
	}

	// three snapshots, so the target below has two newer ones that a missing-target
	// rollback would wrongly delete
	insert(0, 500)
	persist(1)
	insert(500, 1000)
	persist(2)
	insert(1000, 1500)
	persist(3)

	// GetSnapshots lists newest first, so the last entry is the oldest: as the
	// rollback target it is the one whose removal costs the most.
	infos, err := slice.GetSnapshots()
	if err != nil || len(infos) != 3 {
		t.Fatalf("GetSnapshots: %v (n=%d)", err, len(infos))
	}
	target := infos[2]
	targetPath := infos[2].(*memdbSnapshotInfo).dataPath

	// stand in for cleanupOldSnapshotFiles pruning the target between the two
	// listings: the whole directory goes, so the glob no longer reports it
	if err := os.RemoveAll(targetPath); err != nil {
		t.Fatalf("RemoveAll(%v): %v", targetPath, err)
	}

	store := slice.mainstore

	err = slice.Rollback(target)
	if err == nil {
		t.Fatal("Rollback to a target that is no longer on disk reported success")
	}
	t.Logf("(expected) %v", err)

	// the survivors must still be on disk: if the check ran after the removal
	// loop these would already be gone
	dirs := snapshotDirs(t, slice)
	if len(dirs) != 2 {
		t.Fatalf("expected the 2 remaining snapshots to survive a rejected rollback, got %v", dirs)
	}
	for _, d := range dirs {
		if _, err := os.Stat(d); err != nil {
			t.Fatalf("snapshot %v was removed: %v", d, err)
		}
	}

	// bailing out before resetStores leaves the live instance in place
	if slice.mainstore != store {
		t.Fatal("a rejected rollback swapped the mainstore")
	}
}
