package skiplist

import "testing"
import "fmt"
import "math/rand"
import "runtime"
import "sync"
import "time"
import "unsafe"
import "github.com/couchbase/indexing/secondary/stubs/nitro/mm"

func TestInsert(t *testing.T) {
	s := New()
	cmp := CompareBytes
	buf := s.MakeBuf()
	defer s.FreeBuf(buf)

	for i := 0; i < 2000; i++ {
		s.Insert(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf, &s.Stats)
	}

	for i := 1750; i < 2000; i++ {
		s.Delete(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf, &s.Stats)
	}

	itr := s.NewIterator(cmp, buf)
	count := 0
	itr.SeekFirst()
	itr.Seek(NewByteKeyItem([]byte(fmt.Sprintf("%010d", 1500))))
	for ; itr.Valid(); itr.Next() {
		expected := fmt.Sprintf("%010d", count+1500)
		got := string(*(*byteKeyItem)(itr.Get()))
		count++
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
	}

	if count != 250 {
		t.Errorf("Expected count = 250, got %v", count)
	}
}

func doInsert(sl *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	buf := sl.MakeBuf()
	defer sl.FreeBuf(buf)

	cmp := CompareInt
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	for i := 0; i < n; i++ {
		var val int
		if isRand {
			val = rnd.Int()
		} else {
			val = i
		}

		itm := intKeyItem(val)
		sl.Insert2(unsafe.Pointer(&itm), cmp, nil, buf, rnd.Float32, &sl.Stats)
	}
}

func doGet(sl *Skiplist, wg *sync.WaitGroup, n int) {
	defer wg.Done()
	rnd := rand.New(rand.NewSource(int64(rand.Int())))
	cmp := CompareInt
	buf := sl.MakeBuf()
	defer sl.FreeBuf(buf)

	itr := sl.NewIterator(cmp, buf)
	for i := 0; i < n; i++ {
		val := rnd.Int() % n
		itm := intKeyItem(val)
		itr.Seek(unsafe.Pointer(&itm))
	}

}

func TestInsertPerf(t *testing.T) {
	var wg sync.WaitGroup
	sl := New()
	n := 1000000
	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doInsert(sl, &wg, n, true)
	}
	wg.Wait()

	dur := time.Since(t0)

	fmt.Printf("%d items took %v -> %v items/s conflicts %v\n", total, dur, float64(total)/float64(dur.Seconds()), sl.GetStats().InsertConflicts)
}

func TestGetPerf(t *testing.T) {
	var wg sync.WaitGroup
	sl := New()
	n := 1000000
	wg.Add(1)
	go doInsert(sl, &wg, n, false)
	wg.Wait()

	t0 := time.Now()
	total := n * runtime.GOMAXPROCS(0)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go doGet(sl, &wg, n)
	}
	wg.Wait()
	dur := time.Since(t0)
	fmt.Printf("%d items took %v -> %v items/s\n", total, dur, float64(total)/float64(dur.Seconds()))

}

func TestGetRangeSplitItems(t *testing.T) {
	var wg sync.WaitGroup
	sl := New()
	n := 1000000
	wg.Add(1)
	go doInsert(sl, &wg, n, false)
	wg.Wait()

	fmt.Println(sl.GetStats())

	var keys []int
	var diff []int
	var curr int
	for i, itm := range sl.GetRangeSplitItems(8) {
		k := int(*(*intKeyItem)(itm))
		keys = append(keys, k)
		diff = append(diff, keys[i]-curr)
		curr = keys[i]
	}

	diff = append(diff, n-keys[len(keys)-1])

	fmt.Println("Split range keys", keys)
	fmt.Println("No of items in each range", diff)
}

func TestBuilder(t *testing.T) {
	var wg sync.WaitGroup

	n := 50000000
	nsplit := 8
	segs := make([]*Segment, nsplit)
	t0 := time.Now()
	b := NewBuilder()
	for i := 0; i < nsplit; i++ {
		segs[i] = b.NewSegment()
	}

	perSplit := n / nsplit
	for i := 0; i < nsplit; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, shard int) {
			defer wg.Done()
			for x := 0; x < perSplit; x++ {
				itm := intKeyItem(perSplit*shard + x)
				segs[shard].Add(unsafe.Pointer(&itm))
			}
		}(&wg, i)
	}

	wg.Wait()

	sl := b.Assemble(segs...)
	fmt.Println(sl.GetStats())
	dur := time.Since(t0)
	fmt.Printf("Took %v to build %d items, %v items/sec\n", dur, n, float32(n)/float32(dur.Seconds()))
	buf := sl.MakeBuf()
	defer sl.FreeBuf(buf)
	count := 0

	t0 = time.Now()
	itr := sl.NewIterator(CompareInt, buf)
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		if int(*(*intKeyItem)(itr.Get())) != count {
			t.Errorf("Expected %d, got %d", count, itr.Get())
		}
		count++
	}
	fmt.Printf("Took %v to iterate %d items\n", time.Since(t0), n)

	if count != n {
		t.Errorf("Expected %d, got %d", n, count)
	}

}

func TestNodeDCAS(t *testing.T) {
	level := 0

	// golang memory

	pval1 := new(int)
	p1 := allocNode(unsafe.Pointer(pval1), level, nil)

	pval2 := new(int)
	p2 := allocNode(unsafe.Pointer(pval2), level, nil)

	pval3 := new(int)
	p3 := allocNode(unsafe.Pointer(pval3), level, nil)

	// initialize
	if !p1.dcasNext(level, nil, p2, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	if !p1.dcasNext(level, p2, p2, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	// not valid case but mark as deleted
	if !p2.dcasNext(level, nil, p3, false, true) {
		t.Errorf("dcas failed!")
		return
	}

	// already deleted
	if p2.dcasNext(level, p3, p3, false, false) {
		t.Errorf("dcas should fail!")
		return
	}

	if !p1.dcasNext(level, p2, p3, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	// soft delete
	if !p1.dcasNext(level, p3, p3, false, true) {
		t.Errorf("dcas failed!")
		return
	}

	// user memory

	qval1 := mm.Malloc(8)
	q1 := allocNode(unsafe.Pointer(qval1), level, mm.Malloc)

	qval2 := mm.Malloc(8)
	q2 := allocNode(unsafe.Pointer(qval2), level, mm.Malloc)

	qval3 := mm.Malloc(8)
	q3 := allocNode(unsafe.Pointer(qval3), level, mm.Malloc)

	// initialize
	if !q1.dcasNext(level, nil, q2, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	if !q1.dcasNext(level, q2, q2, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	// not valid case but mark as deleted
	if !q2.dcasNext(level, nil, q3, false, true) {
		t.Errorf("dcas failed!")
		return
	}

	// already deleted
	if q2.dcasNext(level, q3, q3, false, false) {
		t.Errorf("dcas should fail!")
		return
	}

	if !q1.dcasNext(level, q2, q3, false, false) {
		t.Errorf("dcas failed!")
		return
	}

	// soft delete
	if !q1.dcasNext(level, q3, q3, false, true) {
		t.Errorf("dcas failed!")
		return
	}

	mm.Free(qval1)
	mm.Free(qval2)
	mm.Free(qval3)
}
