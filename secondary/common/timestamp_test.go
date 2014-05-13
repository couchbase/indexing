//TODO: Benchmark for AsRecent() and Sort()

package common

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestSortTimestamp(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}

	if ts.CompareVbuckets(tsRef) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}

	if err := verifyTimestamp(ts, tsRef); err != nil {
		t.Fatal(err)
	}
}

func TestSelectByVbuckets(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 60, 70}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts = ts.SelectByVbuckets([]uint16{29, 41})
	if reflect.DeepEqual(ts.Vbnos, []uint16{29, 42}) {
		t.Fatal(fmt.Errorf("vbnos mismatch in selecting timestamps"))
	}
	if reflect.DeepEqual(ts.Seqnos, []uint16{4, 5}) {
		t.Fatal(fmt.Errorf("seqnos mismatch in selecting timestamps"))
	}
	if reflect.DeepEqual(ts.Vbuuids, []uint16{40, 50}) {
		t.Fatal(fmt.Errorf("vbuuids mismatch in selecting timestamps"))
	}
}

func TestFilterByVbuckets(t *testing.T) {
	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{11, 18, 21, 49, 62}
	ts.Seqnos = []uint64{1, 2, 3, 6, 7}
	ts.Vbuuids = []uint64{10, 20, 30, 60, 70}

	ts = ts.FilterByVbuckets([]uint16{21, 49})
	if reflect.DeepEqual(ts.Vbnos, []uint16{11, 18, 62}) == false {
		t.Fatal(fmt.Errorf("vbnos mismatch in filtering timestamps"))
	}
	if reflect.DeepEqual(ts.Seqnos, []uint64{1, 2, 7}) == false {
		t.Fatal(fmt.Errorf("seqnos mismatch in filtering timestamps"))
	}
	if reflect.DeepEqual(ts.Vbuuids, []uint64{10, 20, 70}) == false {
		t.Fatal(fmt.Errorf("vbuuids mismatch in filtering timestamps"))
	}
}

func TestCompareVbuckets(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}

	if ts.CompareVbuckets(tsRef) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}
	ts.Vbnos[len(ts.Vbnos)-1]++
	if ts.CompareVbuckets(tsRef) == true {
		t.Fatal(fmt.Errorf("expected false"))
	}
}

func TestCompareVbuuids(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}

	if ts.CompareVbuuids(tsRef) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}
	ts.Vbuuids[len(ts.Vbuuids)-1]++
	if ts.CompareVbuuids(tsRef) == true {
		t.Fatal(fmt.Errorf("expected false"))
	}
}

func TestAsRecent(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}

	if ts.AsRecent(tsRef) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}
	ts.Seqnos[len(ts.Seqnos)-1]--
	if ts.AsRecent(tsRef) == true {
		t.Fatal(fmt.Errorf("expected false"))
	}
	ts.Seqnos[len(ts.Seqnos)-1]++
	ts.Seqnos[0]++
	if ts.AsRecent(tsRef) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}
}

func TestUnionTimestamp(t *testing.T) {
	ts1 := NewTimestamp("default", 1024)
	ts1.Vbnos = []uint16{11, 18, 22}
	ts1.Seqnos = []uint64{1, 2, 4}
	ts1.Vbuuids = []uint64{10, 20, 31}

	ts2 := NewTimestamp("default", 1024)
	ts2.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts2.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts2.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}

	tsref := NewTimestamp("default", 1024)
	tsref.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62, 22}
	tsref.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7, 4}
	tsref.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70, 31}
	sort.Sort(tsref)

	uts := ts1.Union(ts2)
	if err := verifyTimestamp(tsref, uts); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSortTimestamp(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := 1; i < 512; i += 2 {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, uint64(i))
		ts.Vbuuids = append(ts.Vbuuids, uint64(i))
		ts.Vbnos = append(ts.Vbnos, uint16(i*2))
		ts.Seqnos = append(ts.Seqnos, uint64(i*2))
		ts.Vbuuids = append(ts.Vbuuids, uint64(i*2))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Sort(ts)
	}
}

func BenchmarkSelectByVbuckets(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, uint64(1000000+i))
		ts.Vbuuids = append(ts.Vbuuids, uint64(2000000+i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.SelectByVbuckets([]uint16{29, 41})
	}
}

func BenchmarkFilterByVbuckets(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, uint64(1000000+i))
		ts.Vbuuids = append(ts.Vbuuids, uint64(2000000+i))
	}
	vbuckets := []uint16{1, 22, 344, 455, 1000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.FilterByVbuckets(vbuckets)
	}
}

func BenchmarkCompareVbuckets(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, uint64(1000000+i))
		ts1.Vbuuids = append(ts1.Vbuuids, uint64(2000000+i))
	}
	ts2 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, uint64(1000000+i))
		ts2.Vbuuids = append(ts2.Vbuuids, uint64(2000000+i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1.CompareVbuckets(ts2)
	}
}

func BenchmarkCompareVbuuuids(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, uint64(1000000+i))
		ts1.Vbuuids = append(ts1.Vbuuids, uint64(2000000+i))
	}
	ts2 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, uint64(1000000+i))
		ts2.Vbuuids = append(ts2.Vbuuids, uint64(2000000+i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1.CompareVbuuids(ts2)
	}
}

func BenchmarkUnionTimestamp(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, uint64(1000000+i))
		ts1.Vbuuids = append(ts1.Vbuuids, uint64(2000000+i))
	}
	ts2 := NewTimestamp("default", 1024)
	for i := 1; i < 1024; i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, uint64(1000000+i))
		ts2.Vbuuids = append(ts2.Vbuuids, uint64(2000000+i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1.Union(ts2)
	}
}

func verifyTimestamp(ts, ref *Timestamp) (err error) {
	sort.Sort(ts)
	sort.Sort(ref)
	for i := range ref.Vbnos {
		if ref.Vbnos[i] != ts.Vbnos[i] {
			return fmt.Errorf("failed with Vbnos")
		}
		if ref.Seqnos[i] != ts.Seqnos[i] {
			return fmt.Errorf("failed with Seqnos")
		}
		if ref.Vbuuids[i] != ts.Vbuuids[i] {
			return fmt.Errorf("failed with Vbuuids")
		}
	}
	return
}
