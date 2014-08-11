//TODO: Benchmark for AsRecent() and Sort()

package common

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestSortTimestamp(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}
	tsRef.Snapshots = [][2]uint64{
		{100, 200}, {101, 201}, {102, 202}, {103, 203},
		{104, 204}, {105, 205}, {106, 206},
	}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts.Snapshots = [][2]uint64{
		{102, 202}, {101, 201}, {103, 203}, {100, 200},
		{105, 205}, {104, 204}, {106, 206},
	}

	if ts.CompareVbuuids(tsRef) == false {
		t.Fatal("expected true")
	}

	if err := verifyTimestamp(ts, tsRef); err != nil {
		t.Fatal(err)
	}
}

func TestSelectByVbuckets(t *testing.T) {
	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts.Snapshots = [][2]uint64{
		{102, 202}, {100, 200}, {103, 203}, {101, 201},
		{105, 205}, {104, 204}, {106, 206},
	}

	ts = ts.SelectByVbuckets([]uint16{29, 41})
	if reflect.DeepEqual(ts.Vbnos, []uint16{29, 42}) {
		t.Fatal("vbnos mismatch in selecting timestamps")
	}
	if reflect.DeepEqual(ts.Seqnos, []uint16{4, 5}) {
		t.Fatal("seqnos mismatch in selecting timestamps")
	}
	if reflect.DeepEqual(ts.Vbuuids, []uint16{40, 50}) {
		t.Fatal("vbuuids mismatch in selecting timestamps")
	}
	if reflect.DeepEqual(ts.Snapshots, [][2]uint16{{103, 203}, {104, 204}}) {
		t.Fatal("snapshot mismatch in selecting timestamps")
	}
}

func TestFilterByVbuckets(t *testing.T) {
	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{11, 18, 21, 49, 62}
	ts.Seqnos = []uint64{1, 2, 3, 6, 7}
	ts.Vbuuids = []uint64{10, 20, 30, 60, 70}
	ts.Snapshots = [][2]uint64{
		{100, 200}, {101, 201}, {102, 202}, {105, 205}, {106, 206},
	}

	ts = ts.FilterByVbuckets([]uint16{21, 49})
	if reflect.DeepEqual(ts.Vbnos, []uint16{11, 18, 62}) == false {
		t.Fatal("vbnos mismatch in filtering timestamps")
	}
	if reflect.DeepEqual(ts.Seqnos, []uint64{1, 2, 7}) == false {
		t.Fatal("seqnos mismatch in filtering timestamps")
	}
	if reflect.DeepEqual(ts.Vbuuids, []uint64{10, 20, 70}) == false {
		t.Fatal("vbuuids mismatch in filtering timestamps")
	}
	ref := [][2]uint64{{100, 200}, {101, 201}, {106, 206}}
	if reflect.DeepEqual(ts.Snapshots, ref) == false {
		t.Fatal("snapshot mismatch in filtering timestamps")
	}
}

func TestCompareVbuckets(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}
	tsRef.Snapshots = [][2]uint64{
		{100, 200}, {101, 201}, {102, 202}, {103, 203},
		{104, 204}, {105, 205}, {106, 206},
	}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts.Snapshots = [][2]uint64{
		{102, 202}, {100, 200}, {103, 203}, {101, 201},
		{105, 205}, {104, 204}, {106, 206},
	}

	if ts.CompareVbuckets(tsRef) == false {
		t.Fatal("expected true")
	}
	ts.Vbnos[len(ts.Vbnos)-1]++
	if ts.CompareVbuckets(tsRef) == true {
		t.Fatal("expected false")
	}
}

func TestCompareVbuuids(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}
	tsRef.Snapshots = [][2]uint64{
		{100, 200}, {101, 201}, {102, 202}, {103, 203},
		{104, 204}, {105, 205}, {106, 206},
	}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts.Snapshots = [][2]uint64{
		{102, 202}, {101, 201}, {103, 203}, {100, 200},
		{105, 205}, {104, 204}, {106, 206},
	}

	if ts.CompareVbuuids(tsRef) == false {
		t.Fatal("expected true")
	}
	ts.Vbuuids[len(ts.Vbuuids)-1]++
	if ts.CompareVbuuids(tsRef) == true {
		t.Fatal("expected false")
	}
	ts.Vbuuids[len(ts.Vbuuids)-1]--
	ts.Snapshots[len(ts.Snapshots)-1][0]++
	if ts.CompareVbuuids(tsRef) == true {
		t.Fatal("expected false")
	}
}

func TestAsRecent(t *testing.T) {
	tsRef := NewTimestamp("default", 1024)
	tsRef.Vbnos = []uint16{11, 18, 21, 29, 41, 49, 62}
	tsRef.Seqnos = []uint64{1, 2, 3, 4, 5, 6, 7}
	tsRef.Vbuuids = []uint64{10, 20, 30, 40, 50, 60, 70}
	tsRef.Snapshots = [][2]uint64{
		{100, 200}, {101, 201}, {102, 202}, {103, 203},
		{104, 204}, {105, 205}, {106, 206},
	}

	ts := NewTimestamp("default", 1024)
	ts.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts.Snapshots = [][2]uint64{
		{102, 202}, {100, 200}, {103, 203}, {101, 201},
		{105, 205}, {104, 204}, {106, 206},
	}

	if ts.AsRecent(tsRef) == false {
		t.Fatal("expected true")
	}
	ts.Seqnos[len(ts.Seqnos)-1]--
	if ts.AsRecent(tsRef) == true {
		t.Fatal("expected false")
	}
	ts.Seqnos[len(ts.Seqnos)-1]++
	ts.Seqnos[0]++
	if ts.AsRecent(tsRef) == false {
		t.Fatal("expected true")
	}
}

func TestUnionTimestamp(t *testing.T) {
	ts1 := NewTimestamp("default", 1024)
	ts1.Vbnos = []uint16{11, 18, 22}
	ts1.Seqnos = []uint64{1, 2, 4}
	ts1.Vbuuids = []uint64{10, 20, 31}
	ts1.Snapshots = [][2]uint64{{1000, 2000}, {1001, 2001}, {1002, 2002}}

	ts2 := NewTimestamp("default", 1024)
	ts2.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62}
	ts2.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7}
	ts2.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70}
	ts2.Snapshots = [][2]uint64{
		{102, 202}, {100, 200}, {103, 203}, {101, 201},
		{105, 205}, {104, 204}, {106, 206},
	}

	tsref := NewTimestamp("default", 1024)
	tsref.Vbnos = []uint16{21, 18, 29, 11, 49, 41, 62, 22}
	tsref.Seqnos = []uint64{3, 2, 4, 1, 6, 5, 7, 4}
	tsref.Vbuuids = []uint64{30, 20, 40, 10, 60, 50, 70, 31}
	tsref.Snapshots = [][2]uint64{
		{102, 202}, {100, 200}, {103, 203}, {101, 201},
		{105, 205}, {104, 204}, {106, 206}, {1002, 2002},
	}
	sort.Sort(tsref)

	uts := ts1.Union(ts2)
	if err := verifyTimestamp(tsref, uts); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSortTimestamp(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(512); i += 2 {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, i)
		ts.Vbuuids = append(ts.Vbuuids, i)
		ts.Snapshots = append(ts.Snapshots, [2]uint64{i, i + 10})

		ts.Vbnos = append(ts.Vbnos, uint16(i*2))
		ts.Seqnos = append(ts.Seqnos, i*2)
		ts.Vbuuids = append(ts.Vbuuids, i*2)
		ts.Snapshots = append(ts.Snapshots, [2]uint64{i * 2, i*2 + 10})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Sort(ts)
	}
}

func BenchmarkSelectByVbuckets(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, 1000000+i)
		ts.Vbuuids = append(ts.Vbuuids, 2000000+i)
		ts.Snapshots = append(ts.Snapshots, [2]uint64{i, i + 10})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.SelectByVbuckets([]uint16{29, 41})
	}
}

func BenchmarkFilterByVbuckets(b *testing.B) {
	ts := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts.Vbnos = append(ts.Vbnos, uint16(i))
		ts.Seqnos = append(ts.Seqnos, 1000000+i)
		ts.Vbuuids = append(ts.Vbuuids, 2000000+i)
		ts.Snapshots = append(ts.Snapshots, [2]uint64{i, i + 10})
	}
	vbuckets := []uint16{1, 22, 344, 455, 1000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.FilterByVbuckets(vbuckets)
	}
}

func BenchmarkCompareVbuckets(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, 1000000+i)
		ts1.Vbuuids = append(ts1.Vbuuids, 2000000+i)
		ts1.Snapshots = append(ts1.Snapshots, [2]uint64{i, i + 10})
	}

	ts2 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, 1000000+i)
		ts2.Vbuuids = append(ts2.Vbuuids, 2000000+i)
		ts2.Snapshots = append(ts2.Snapshots, [2]uint64{i, i + 10})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1.CompareVbuckets(ts2)
	}
}

func BenchmarkCompareVbuuuids(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, uint64(1000000+i))
		ts1.Vbuuids = append(ts1.Vbuuids, uint64(2000000+i))
		ts1.Snapshots = append(ts1.Snapshots, [2]uint64{i, i + 10})
	}

	ts2 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, uint64(1000000+i))
		ts2.Vbuuids = append(ts2.Vbuuids, uint64(2000000+i))
		ts2.Snapshots = append(ts2.Snapshots, [2]uint64{i, i + 10})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts1.CompareVbuuids(ts2)
	}
}

func BenchmarkUnionTimestamp(b *testing.B) {
	ts1 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts1.Vbnos = append(ts1.Vbnos, uint16(i))
		ts1.Seqnos = append(ts1.Seqnos, uint64(1000000+i))
		ts1.Vbuuids = append(ts1.Vbuuids, uint64(2000000+i))
		ts1.Snapshots = append(ts1.Snapshots, [2]uint64{i, i + 10})
	}
	ts2 := NewTimestamp("default", 1024)
	for i := uint64(1); i < uint64(1024); i++ {
		ts2.Vbnos = append(ts2.Vbnos, uint16(i))
		ts2.Seqnos = append(ts2.Seqnos, uint64(1000000+i))
		ts2.Vbuuids = append(ts2.Vbuuids, uint64(2000000+i))
		ts2.Snapshots = append(ts2.Snapshots, [2]uint64{i, i + 10})
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
			return errors.New("failed with Vbnos")
		}
		if ref.Seqnos[i] != ts.Seqnos[i] {
			return errors.New("failed with Seqnos")
		}
		if ref.Vbuuids[i] != ts.Vbuuids[i] {
			return errors.New("failed with Vbuuids")
		}
	}
	return
}
