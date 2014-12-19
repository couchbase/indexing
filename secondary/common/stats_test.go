package common

import (
	"io/ioutil"
	"log"
	"reflect"
	"testing"
)

var testStats Statistics

func init() {
	text, err := ioutil.ReadFile("../tests/testdata/statistics.json")
	if err != nil {
		log.Fatal(err)
	}
	testStats, _ = NewStatistics(text)
}

func TestNewStatistics(t *testing.T) {
	// init with JSON
	if stat, err := NewStatistics(`{"x" : 10}`); err != nil {
		t.Fatal(err)
	} else if len(stat) != 1 || stat["x"].(float64) == 0.0 {
		t.Fatalf("Failed init-ing NewStatistics with JSON string")
	}
	// init with []byte
	if stat, err := NewStatistics([]byte(`{"x" : 10}`)); err != nil {
		t.Fatal(err)
	} else if len(stat) != 1 || stat["x"].(float64) == 0.0 {
		t.Fatalf("Failed init-ing NewStatistics with JSON string")
	}
	// init with map[string]interface{}
	m := map[string]interface{}{"x": 10.0}
	if stat, err := NewStatistics(m); err != nil {
		t.Fatal(err)
	} else if len(stat) != 1 || stat["x"].(float64) == 0.0 {
		t.Fatalf("Failed init-ing NewStatistics with JSON string")
	}
}

func TestStatMarshaller(t *testing.T) {
	ref := Statistics{"componentName": "indexer", "count": 10.0}
	out := Statistics{"componentName": "indexer"}
	if data, err := ref.Encode(); err != nil {
		t.Error(err)
	} else {
		if err := out.Decode(data); err != nil {
			t.Error(err)
		} else if reflect.DeepEqual(ref, out) == false {
			t.Errorf("mistmatch in component stats")
		}
	}
}

func TestStatIncr(t *testing.T) {
	stat, err := NewStatistics(nil)
	if err != nil {
		t.Fatal(err)
	}

	// scalar increment
	stat["count"] = 10.0
	stat.Incr("count", 2)
	if stat["count"] != 12.0 {
		t.Errorf("scalar increment failed for Statistics")
	}
	// vector increment
	stat["count"] = []float64{10.0, -1.0}
	stat.Incr("count", 2, 2)
	vs := stat["count"].([]float64)
	t.Log(vs)
	if vs[0] != 12.0 || vs[1] != 1.0 {
		t.Errorf("vector increment failed for Statistics")
	}
}

func TestStatDecr(t *testing.T) {
	stat, err := NewStatistics(nil)
	if err != nil {
		t.Error(err)
	}

	// scalar decrement
	stat["count"] = 10.0
	stat.Decr("count", 2)
	if stat["count"] != 8.0 {
		t.Errorf("scalar decrement failed for Statistics")
	}
	// vector decrement
	stat["count"] = []float64{10.0, -1.0}
	stat.Decr("count", 2, 2)
	vs := stat["count"].([]float64)
	if vs[0] != 8.0 || vs[1] != -3.0 {
		t.Errorf("vector decrement failed for Statistics")
	}
}

func TestStatSet(t *testing.T) {
	stat, err := NewStatistics(nil)
	if err != nil {
		t.Error(err)
	}
	stat.Set("count", 10.0)
	if stat.Get("count").(float64) != 10.0 {
		t.Errorf("set/get failed for Statistics")
	}
}

func BenchmarkStatEncode(b *testing.B) {
	data, _ := testStats.Encode()
	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testStats.Encode()
	}
}

func BenchmarkStatDecode(b *testing.B) {
	data, _ := testStats.Encode()
	outStats, _ := NewStatistics(nil)
	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outStats.Decode(data)
	}
}

func BenchmarkStatGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testStats.Get("topics")
	}
}

func BenchmarkStatSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testStats.Set("count", 10)
	}
}

func BenchmarkStatIncrScalar(b *testing.B) {
	testStats.Set("count", 1)
	for i := 0; i < b.N; i++ {
		testStats.Incr("count", 1)
	}
}

func BenchmarkStatIncrVector(b *testing.B) {
	testStats.Set("count", []float64{1, 2, 3, 4})
	for i := 0; i < b.N; i++ {
		testStats.Incr("count", 1, 1, 1, 1)
	}
}

func BenchmarkStatDecrScalar(b *testing.B) {
	testStats.Set("count", 1)
	for i := 0; i < b.N; i++ {
		testStats.Decr("count", 1)
	}
}

func BenchmarkStatDecrVector(b *testing.B) {
	testStats.Set("count", []float64{1, 2, 3, 4})
	for i := 0; i < b.N; i++ {
		testStats.Decr("count", 1, 1, 1, 1)
	}
}
