package common

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestVbucketsSort(t *testing.T) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 1; i < 512; i += 2 {
		vbuckets = append(vbuckets, uint16(i*2))
		vbuckets = append(vbuckets, uint16(i))
	}
	sort.Sort(Vbuckets(vbuckets))
	x := vbuckets[0]
	for _, y := range vbuckets[1:] {
		if x >= y {
			t.Fatal(fmt.Errorf("not sorted"))
		}
		x = y
	}
}

func TestVbucketsIntersection(t *testing.T) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets = append(vbuckets, uint16(i))
	}
	v1 := vbuckets[:512]
	v2 := vbuckets[512:]
	vbuckets = Intersection(v1, v2)
	if len(vbuckets) != 0 {
		t.Fatal(fmt.Errorf("exprected empty"))
	}
}

func TestVbucketsUnion(t *testing.T) {
	ref := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		ref = append(ref, uint16(i))
	}
	v1 := ref[:512]
	v2 := ref[512:]
	vbuckets := Union(v1, v2)
	if reflect.DeepEqual(ref, vbuckets) == false {
		t.Fatal(fmt.Errorf("exprected true"))
	}
}

func TestVbucketConversion(t *testing.T) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets = append(vbuckets, uint16(i))
	}
	if reflect.DeepEqual(vbuckets, Vbno32to16(Vbno16to32(vbuckets))) == false {
		t.Fatal(fmt.Errorf("expected true"))
	}
}

func BenchmarkVbucketsSort(b *testing.B) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 1; i < 512; i += 2 {
		vbuckets = append(vbuckets, uint16(i*2))
		vbuckets = append(vbuckets, uint16(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Sort(Vbuckets(vbuckets))
	}
}

func BenchmarkVbucketsIntersection(b *testing.B) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets = append(vbuckets, uint16(i))
	}
	v1 := vbuckets[:512]
	v2 := vbuckets[512:]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Intersection(v1, v2)
	}
}

func BenchmarkVbucketsUnion(b *testing.B) {
	ref := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		ref = append(ref, uint16(i))
	}
	v1 := ref[:512]
	v2 := ref[512:]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Union(v1, v2)
	}
}

func BenchmarkVbno32to16(b *testing.B) {
	vbuckets := make([]uint32, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets = append(vbuckets, uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Vbno32to16(vbuckets)
	}
}

func BenchmarkVbno16to32(b *testing.B) {
	vbuckets := make([]uint16, 0, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets = append(vbuckets, uint16(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Vbno16to32(vbuckets)
	}
}
