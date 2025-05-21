package indexer

import (
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
)

var memUsed int64
var maxMemory int64

func TestBasicsA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	if q == nil {
		t.Errorf("expected new queue allocation to work")
	}

	m := &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 1}}

	q.Enqueue(m, 0, nil)
	checkSizeA(t, q, 0, 1)

	m1, _ := q.DequeueSingleElement(0)
	checkItemA(t, m, m1)
	checkSizeA(t, q, 0, 0)

	m2 := &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 2}}

	q.Enqueue(m, 0, nil)
	q.Enqueue(m2, 0, nil)
	checkSizeA(t, q, 0, 2)

	m1, _ = q.DequeueSingleElement(0)
	checkSizeA(t, q, 0, 1)
	checkItemA(t, m, m1)

	m1, _ = q.DequeueSingleElement(0)
	checkSizeA(t, q, 0, 0)
	checkItemA(t, m2, m1)

}

func checkSizeA(t *testing.T, q MutationQueue, v uint16, s int64) {

	r := q.GetSize(Vbucket(v))
	if r != s {
		t.Errorf("expected queue size %v doesn't match returned size %v", s, r)
	}
}

func checkItemA(t *testing.T, m1 *MutationKeys, m2 *MutationKeys) {
	if !reflect.DeepEqual(m1, m2) {
		t.Errorf("Item returned after dequeue doesn't match enqueued item")
	}
}

func TestSizeA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 10000)
	for i := 0; i < 10000; i++ {
		m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
		q.Enqueue(m[i], 0, nil)
	}
	checkSizeA(t, q, 0, 10000)

	for i := 0; i < 10000; i++ {
		p, _ := q.DequeueSingleElement(0)
		checkItemA(t, p, m[i])
	}
	checkSizeA(t, q, 0, 0)

}

func TestSizeWithFreelistA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 10000)
	for i := 0; i < 10000; i++ {
		m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
		q.Enqueue(m[i], 0, nil)
		if (i+1)%100 == 0 {
			checkSizeA(t, q, 0, 100)
			for j := 0; j < 100; j++ {
				p, _ := q.DequeueSingleElement(0)
				checkItemA(t, p, m[(i-99)+j])
			}
			checkSizeA(t, q, 0, 0)
		}
	}
}

func TestDequeueUptoSeqnoA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 10)
	//multiple items with dup seqno
	m[0] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 1}}
	m[1] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 1}}
	m[2] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 2}}

	q.Enqueue(m[0], 0, nil)
	q.Enqueue(m[1], 0, nil)
	q.Enqueue(m[2], 0, nil)
	checkSizeA(t, q, 0, 3)

	ch, _, err := q.DequeueUptoSeqno(0, 1)

	if err != nil {
		t.Errorf("DequeueUptoSeqno returned error")
	}

	i := 0
	for p := range ch {
		checkItemA(t, m[i], p)
		i++
	}
	checkSizeA(t, q, 0, 2)

	ch, _, err = q.DequeueUptoSeqno(0, 1)
	for p := range ch {
		checkItemA(t, m[1], p)
	}
	checkSizeA(t, q, 0, 1)

	//one more
	m[3] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
		seqno: 3}}
	q.Enqueue(m[3], 0, nil)
	ch, _, err = q.DequeueUptoSeqno(0, 2)
	for p := range ch {
		checkItemA(t, m[2], p)
	}
	checkSizeA(t, q, 0, 1)

	//check if blocking is working
	ch, _, err = q.DequeueUptoSeqno(0, 4)

	go func() {
		time.Sleep(100 * time.Millisecond)
		m[4] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: 3}}
		q.Enqueue(m[4], 0, nil)
		m[5] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: 3}}
		q.Enqueue(m[5], 0, nil)
		m[6] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: 4}}
		q.Enqueue(m[6], 0, nil)
	}()

	i = 3
	for p := range ch {
		checkItemA(t, m[i], p)
		i++
	}

	checkSizeA(t, q, 0, 0)
}

func TestDequeueA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	mut := make([]*MutationKeys, 10)
	for i := 0; i < 10; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i / 2)}}
	}
	checkSizeA(t, q, 0, 0)

	//start blocking dequeue call
	ch, stop, _ := q.Dequeue(0)
	go func() {
		for _, m := range mut {
			q.Enqueue(m, 0, nil)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
		stop <- true
	}()

	i := 0
	for p := range ch {
		checkItemA(t, mut[i], p)
		i++
	}
	checkSizeA(t, q, 0, 0)

}

func TestMultipleVbucketsA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 3, &maxMemory, &memUsed, conf)

	mut := make([]*MutationKeys, 15)
	for i := 0; i < 15; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
	}
	checkSizeA(t, q, 0, 0)
	checkSizeA(t, q, 1, 0)
	checkSizeA(t, q, 2, 0)

	for i := 0; i < 5; i++ {
		q.Enqueue(mut[i], 0, nil)
		q.Enqueue(mut[i+5], 1, nil)
		q.Enqueue(mut[i+10], 2, nil)
	}
	checkSizeA(t, q, 0, 5)
	checkSizeA(t, q, 1, 5)
	checkSizeA(t, q, 2, 5)

	var p *MutationKeys
	for i := 0; i < 5; i++ {
		p, _ = q.DequeueSingleElement(0)
		checkItemA(t, p, mut[i])
		p, _ = q.DequeueSingleElement(1)
		checkItemA(t, p, mut[i+5])
		p, _ = q.DequeueSingleElement(2)
		checkItemA(t, p, mut[i+10])
	}

}

func TestDequeueUptoFreelistA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	for i := 0; i < 100; i++ {
		m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
		q.Enqueue(m[i], 0, nil)
		if (i+1)%10 == 0 {
			checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueUptoSeqno(0, uint64(i))
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
			}
			checkSizeA(t, q, 0, 0)
		}
	}
}

func TestDequeueUptoFreelistMultVbA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 2, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	for i := 0; i < 100; i++ {
		m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
		q.Enqueue(m[i], 0, nil)
		q.Enqueue(m[i], 1, nil)
		if (i+1)%10 == 0 {
			checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueUptoSeqno(0, uint64(i))
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
			}
			checkSizeA(t, q, 0, 0)

			checkSizeA(t, q, 1, 10)
			retch, _, _ = q.DequeueUptoSeqno(1, uint64(i))
			j = 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
			}
			checkSizeA(t, q, 1, 0)
		}
	}
}

func TestConcurrentEnqueueDequeueA(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	go func() {
		for i := 0; i < 100; i++ {
			m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
				seqno: uint64(i)}}
			q.Enqueue(m[i], 0, nil)
		}
	}()

	dequeueCount := 0
	for i := 0; i < 100; i++ {
		if (i+1)%10 == 0 {
			//time.Sleep(time.Second * 1)
			//checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueUptoSeqno(0, uint64(i))
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
				dequeueCount++
			}
		}
	}

	if dequeueCount != 100 {
		t.Errorf("Unexpected Dequeue Count %v, expected %v", dequeueCount, 100)
	}
}

func TestConcurrentEnqueueDequeueA1(t *testing.T) {

	maxMemory = 1
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)
	conf.SetValue("settings.minVbQueueLength", 10)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	go func() {
		for i := 0; i < 100; i++ {
			m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
				seqno: uint64(i)}}
			q.Enqueue(m[i], 0, nil)
		}
	}()

	dequeueCount := 0
	for i := 0; i < 100; i++ {
		if (i+1)%10 == 0 {
			time.Sleep(time.Second * 1)
			checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueUptoSeqno(0, uint64(i))
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
				dequeueCount++
			}
		}
	}

	if dequeueCount != 100 {
		t.Errorf("Unexpected Dequeue Count %v, expected %v", dequeueCount, 100)
	}
}

func TestEnqueueAppCh(t *testing.T) {

	maxMemory = 1
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)
	conf.SetValue("settings.minVbQueueLength", 10)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	appch := make(StopChannel)

	m := make([]*MutationKeys, 20)
	go func() {
		for i := 0; i < 20; i++ {
			m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
				seqno: uint64(i)}}
			q.Enqueue(m[i], 0, appch)
		}
	}()

	//wait for first batch to enqueue
	time.Sleep(time.Second * 1)
	checkSizeA(t, q, 0, 10)

	//close the app ch to allow next enqueue
	//to proceed
	close(appch)
	time.Sleep(time.Second * 1)
	checkSizeA(t, q, 0, 20)

}

func TestDequeueN(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	mut := make([]*MutationKeys, 20)
	for i := 0; i < 20; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
	}
	checkSizeA(t, q, 0, 0)

	for i := 0; i < 10; i++ {
		q.Enqueue(mut[i], 0, nil)
	}

	checkSizeA(t, q, 0, 10)

	//dequeue 5
	retch, _, _ := q.DequeueN(0, 5)
	i := 0
	for p := range retch {
		checkItemA(t, mut[i], p)
		i++
	}
	checkSizeA(t, q, 0, 5)

	//dequeue 5
	retch, _, _ = q.DequeueN(0, 5)
	for p := range retch {
		checkItemA(t, mut[i], p)
		i++
	}
	checkSizeA(t, q, 0, 0)

	for j := 10; j < 20; j++ {
		q.Enqueue(mut[j], 0, nil)
	}

	//dequeue 10
	retch, _, _ = q.DequeueN(0, 10)
	for p := range retch {
		checkItemA(t, mut[i], p)
		i++
	}
	checkSizeA(t, q, 0, 0)
}

func TestConcurrentEnqueueDequeueN(t *testing.T) {

	maxMemory = 100 * 1024 * 1024
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	go func() {
		for i := 0; i < 100; i++ {
			m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
				seqno: uint64(i)}}
			q.Enqueue(m[i], 0, nil)
		}
	}()

	dequeueCount := 0
	for i := 0; i < 100; i++ {
		if (i+1)%10 == 0 {
			//time.Sleep(time.Second * 1)
			//checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueN(0, 10)
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
				dequeueCount++
			}
		}
	}

	if dequeueCount != 100 {
		t.Errorf("Unexpected Dequeue Count %v, expected %v", dequeueCount, 100)
	}
}

func TestConcurrentEnqueueDequeueN1(t *testing.T) {

	maxMemory = 1
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)
	conf.SetValue("settings.minVbQueueLength", 10)

	q := NewAtomicMutationQueue("default", 1, &maxMemory, &memUsed, conf)

	m := make([]*MutationKeys, 100)
	go func() {
		for i := 0; i < 100; i++ {
			m[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
				seqno: uint64(i)}}
			q.Enqueue(m[i], 0, nil)
		}
	}()

	dequeueCount := 0
	for i := 0; i < 100; i++ {
		if (i+1)%10 == 0 {
			time.Sleep(time.Second * 1)
			checkSizeA(t, q, 0, 10)
			retch, _, _ := q.DequeueN(0, 10)
			j := 0
			for d := range retch {
				checkItemA(t, d, m[(i-9)+j])
				j += 1
				dequeueCount++
			}
		}
	}

	if dequeueCount != 100 {
		t.Errorf("Unexpected Dequeue Count %v, expected %v", dequeueCount, 100)
	}
}

/*
func BenchmarkEnqueueA(b *testing.B) {

	q := NewAtomicMutationQueue(1, int64(b.N))

	mut := make([]*MutationKeys, b.N)
	for i := 0; i < b.N; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(mut[i], 0, nil)
	}

}
func BenchmarkDequeueA(b *testing.B) {

	q := NewAtomicMutationQueue(1, int64(b.N))

	mut := make([]*MutationKeys, b.N)
	for i := 0; i < b.N; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
	}
	for _, m := range mut {
		q.Enqueue(m, 0, nil)
	}

	//start blocking dequeue call
	ch, stop, _ := q.Dequeue(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-ch
	}
	stop <- true
}

func BenchmarkSingleVbucketA(b *testing.B) {

	q := NewAtomicMutationQueue(1, int64(b.N))

	mut := make([]*MutationKeys, b.N)
	for i := 0; i < b.N; i++ {
		mut[i] = &MutationKeys{meta: &MutationMeta{vbucket: 0,
			seqno: uint64(i)}}
	}

	ch, stop, _ := q.Dequeue(0)

	b.ResetTimer()
	//start blocking dequeue call
	go func() {
		for i := 0; i < b.N; i++ {
			q.Enqueue(mut[i], 0, nil)
		}
	}()

	for i := 0; i < b.N; i++ {
		<-ch
	}
	stop <- true
}
*/
