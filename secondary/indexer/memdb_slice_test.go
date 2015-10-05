package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	rand2 "math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const keySize = 10

func randString(r *rand2.Rand, n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = alphanum[r.Intn(len(alphanum))]
	}
	return string(bytes)
}

func mutationProducer(wg *sync.WaitGroup, s Slice, offset, n, id int, isRand bool) {
	defer wg.Done()

	rnd := rand2.New(rand2.NewSource(int64(rand2.Int())))
	for i := 0; i < n; i++ {
		docN := i + offset
		if isRand {
			docN = rnd.Int()%n + offset
		}

		docid := []byte(fmt.Sprintf("docid-%d", docN))
		key := []byte("[\"" + randString(rnd, keySize) + "\"]")
		//key := []byte(fmt.Sprintf("[\"%025d\"]", rnd.Int()))
		k, err := GetIndexEntryBytesFromKey(key, docid, false)
		common.CrashOnError(err)

		meta := NewMutationMeta()
		meta.vbucket = Vbucket(id)

		s.Insert(k, docid, meta)
		meta.Free()
	}
}

func TestMemDBInsertionPerf(t *testing.T) {
	var wg sync.WaitGroup

	nPerWriter := 600000
	nw := runtime.GOMAXPROCS(0)
	stats := &IndexStats{}
	stats.Init()
	cfg := common.SystemConfig.SectionConfig("indexer.", true)
	cfg.SetValue("numSliceWriters", nw)

	slice, err := NewMemDBSlice("/tmp/mdbslice",
		SliceId(0), common.IndexDefnId(0), common.IndexInstId(0), false,
		cfg, stats)
	common.CrashOnError(err)

	n := nw * nPerWriter

	t1 := time.Now()
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go mutationProducer(&wg, slice, i*nPerWriter, nPerWriter, i, false)
	}

	wg.Wait()

	dur1 := time.Since(t1)

	t2 := time.Now()
	for i := 0; i < nw; i++ {
		wg.Add(1)
		go mutationProducer(&wg, slice, i*nPerWriter, nPerWriter, i, true)
	}

	wg.Wait()

	dur2 := time.Since(t2)
	fmt.Printf("Initial build: %d items took %v -> %v items/s\n", n, dur1, float64(n)/dur1.Seconds())
	fmt.Printf("Incr build: %d items took %v -> %v items/s\n", n, dur2, float64(n)/dur2.Seconds())
}
