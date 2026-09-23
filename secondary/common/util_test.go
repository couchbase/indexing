package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

var _ = fmt.Sprintf("dummy")

func TestExcludeStrings(t *testing.T) {
	a := []string{"1", "2", "3", "4"}
	b := []string{"2", "4"}
	c := ExcludeStrings(a, b)
	if len(c) != 2 || c[0] != "1" || c[1] != "3" {
		t.Fatal("failed ExcludeStrings")
	}
}

func TestCommonStrings(t *testing.T) {
	a := []string{"1", "2", "3", "4"}
	b := []string{"2", "4", "5"}
	c := CommonStrings(a, b)
	if len(c) != 2 || c[0] != "2" || c[1] != "4" {
		t.Fatal("failed CommonStrings")
	}
}

func TestHasString(t *testing.T) {
	a := []string{"1", "2", "3", "4"}
	if HasString("1", a) == false || HasString("5", a) == true {
		t.Fatal("failed HasString")
	}
}

func TestExcludeUint32(t *testing.T) {
	a := []uint32{1, 2, 3, 4}
	b := []uint32{2, 4}
	c := ExcludeUint32(b, a)
	if len(c) != 2 || c[0] != 1 || c[1] != 3 {
		t.Fatal("failed ExcludeUint32")
	}
}

func TestExcludeUint64(t *testing.T) {
	a := []uint64{1, 2, 3, 4}
	b := []uint64{2, 4}
	c := ExcludeUint64(b, a)
	if len(c) != 2 || c[0] != 1 || c[1] != 3 {
		t.Fatal("failed ExcludeUint64")
	}

	a = []uint64{1, 3, 5}
	c = ExcludeUint64(b, a)
	if len(c) != 3 || c[0] != 1 || c[1] != 3 || c[2] != 5 {
		t.Fatal("failed ExcludeUint64")
	}
}

func TestHasUint32(t *testing.T) {
	a := []uint32{1, 2, 3, 4}
	if HasUint32(uint32(1), a) == false || HasUint32(uint32(5), a) == true {
		t.Fatal("failed HasUint32")
	}
}

func TestHasUint64(t *testing.T) {
	a := []uint64{1, 2, 3, 4}
	if HasUint64(uint64(1), a) == false || HasUint64(uint64(5), a) == true {
		t.Fatal("failed HasUint64")
	}
}

func TestRemoveUint32(t *testing.T) {
	a := []uint32{1, 2, 3, 4}
	b := RemoveUint32(4, a)
	if len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Fatal("failed RemoveUint32")
	}
}

func TestRemoveUint16(t *testing.T) {
	a := []uint16{1, 2, 3, 4}
	b := RemoveUint16(4, a)
	if len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Fatal("failed RemoveUint16")
	}

	c := RemoveUint16(5, b)
	if len(c) != 3 || c[0] != 1 || c[1] != 2 || c[2] != 3 {
		t.Fatal("failed RemoveUint16")
	}
}

func TestRemoveString(t *testing.T) {
	a := []string{"1", "2", "3", "4"}
	b := RemoveString("4", a)
	if len(b) != 3 || b[0] != "1" || b[1] != "2" || b[2] != "3" {
		t.Fatal("failed RemoveString")
	}

	c := RemoveString("5", b)
	if len(c) != 3 || c[0] != "1" || c[1] != "2" || c[2] != "3" {
		t.Fatal("failed RemoveString")
	}
}

func TestIP(t *testing.T) {
	if IsIPLocal("127.0.0.1") != true {
		t.Fatal(`failed IsIPLocal("127.0.0.1")`)
	}
	if IsIPLocal("localhost") == true {
		t.Fatal(`failed IsIPLocal("localhost")`)
	}
	if ip, err := GetLocalIP(); err != nil {
		t.Fatal(err)
	} else if IsIPLocal(ip.String()) != true {
		t.Fatal(`failed IsIPLocal(GetLocalIP())`)
	}
}

func TestEquivalentIP(t *testing.T) {
	var a string = "127.0.0.1:8091"
	b := []string{"127.0.0.1:8091", "192.1.2.1:8091"}

	raddr, raddr1, err := EquivalentIP(a, b)
	if raddr != "127.0.0.1:8091" || raddr1 != "127.0.0.1:8091" || err != nil {
		t.Fatal("failed EquivalentIP")
	}

	a = "192.1.2.1:8091"
	raddr, raddr1, err = EquivalentIP(a, b)
	if raddr != "192.1.2.1:8091" || raddr1 != "192.1.2.1:8091" || err != nil {
		t.Fatal("failed EquivalentIP")
	}

	a = "localhost:8091"
	raddr, raddr1, err = EquivalentIP(a, b)
	if raddr != "127.0.0.1:8091" || raddr1 != "127.0.0.1:8091" || err != nil {
		t.Fatal("failed EquivalentIP")
	}
}

func TestCopyDir(t *testing.T) {
	tmpdir := os.TempDir()

	// On mac/windows this srcdir and destdir are same.
	srcdir1 := strings.ToLower(filepath.Join(tmpdir, "SRC"))
	destdir1 := filepath.Join(tmpdir, "SRC")
	defer func() {
		if err := os.RemoveAll(srcdir1); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(destdir1); err != nil {
			t.Fatal(err)
		}
	}()

	// create source tree
	dir1 := filepath.Join(srcdir1, "d")
	file1 := filepath.Join(dir1, "f")
	if err := os.MkdirAll(dir1, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(file1, []byte("hello world"), 0666); err != nil {
		t.Fatal(err)
	}

	// Copy to an same destination.
	CopyDir(destdir1, srcdir1)
	if es, err := ioutil.ReadDir(destdir1); err != nil {
		t.Fatal(err)
	} else if len(es) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(es))
	} else if es[0].Name() != "d" {
		t.Fatalf("expected %v, got %v", "d", es[0].Name())
	} else if es, err = ioutil.ReadDir(dir1); err != nil {
		t.Fatal(err)
	} else if len(es) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(es))
	} else if es[0].Name() != "f" {
		t.Fatalf("expected %v, got %v", "f", es[0].Name())
	}
	destfile1 := filepath.Join(destdir1, "d", "f")
	if data, err := ioutil.ReadFile(destfile1); err != nil {
		t.Fatal(err)
	} else if ref := "hello world"; string(data) != ref {
		t.Fatalf("expected %q, got %q", ref, string(data))
	}

	// On mac/windows/linux this srcdir and destdir are different.
	srcdir2 := strings.ToLower(filepath.Join(tmpdir, "SOURCE"))
	destdir2 := filepath.Join(tmpdir, "DEST")
	defer func() {
		if err := os.RemoveAll(srcdir2); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(destdir2); err != nil {
			t.Fatal(err)
		}
	}()

	// create source tree
	dir1 = filepath.Join(srcdir2, "d")
	file1 = filepath.Join(dir1, "f")
	if err := os.MkdirAll(dir1, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(file1, []byte("hello world"), 0666); err != nil {
		t.Fatal(err)
	}

	CopyDir(destdir2, srcdir2)
	if es, err := ioutil.ReadDir(destdir2); err != nil {
		t.Fatal(err)
	} else if len(es) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(es))
	} else if es[0].Name() != "d" {
		t.Fatalf("expected %v, got %v", "d", es[0].Name())
	} else if es, err = ioutil.ReadDir(dir1); err != nil {
		t.Fatal(err)
	} else if len(es) != 1 {
		t.Fatalf("expected %v, got %v", 1, len(es))
	} else if es[0].Name() != "f" {
		t.Fatalf("expected %v, got %v", "f", es[0].Name())
	}
	destfile1 = filepath.Join(destdir2, "d", "f")
	if data, err := ioutil.ReadFile(destfile1); err != nil {
		t.Fatal(err)
	} else if ref := "hello world"; string(data) != ref {
		t.Fatalf("expected %v, got %v", ref, string(data))
	}

	// add a directory and file.
	dir2 := filepath.Join(srcdir2, "dr")
	file2 := filepath.Join(dir1, "fl")
	if err := os.MkdirAll(dir2, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(file1, []byte("good bye"), 0666); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(file2, []byte("third world"), 0666); err != nil {
		t.Fatal(err)
	}

	CopyDir(destdir2, srcdir2)
	es, err := ioutil.ReadDir(destdir2)
	if err != nil {
		t.Fatal(err)
	} else if len(es) != 2 {
		t.Fatalf("expected %v, got %v", 2, len(es))
	}
	ref, names := []string{"d", "dr"}, []string{es[0].Name(), es[1].Name()}
	if !reflect.DeepEqual(ref, names) {
		t.Fatalf("expected %v, got %v", ref, names)
	} else if es, err = ioutil.ReadDir(dir1); err != nil {
		t.Fatal(err)
	} else if len(es) != 2 {
		t.Fatalf("expected %v, got %v", 1, len(es))
	}
	ref, names = []string{"f", "fl"}, []string{es[0].Name(), es[1].Name()}
	if !reflect.DeepEqual(ref, names) {
		t.Fatalf("expected %v, got %v", ref, names)
	} else if es, err := ioutil.ReadDir(dir2); err != nil {
		t.Fatal(err)
	} else if len(es) > 0 {
		t.Fatalf("expected %v, got %v", 0, len(es))
	}
	destfile1 = filepath.Join(destdir2, "d", "f")
	destfile2 := filepath.Join(destdir2, "d", "fl")
	if data, err := ioutil.ReadFile(destfile1); err != nil {
		t.Fatal(err)
	} else if ref := "hello world"; string(data) != ref {
		t.Fatalf("expected %v, got %v", ref, string(data))
	} else if data, err := ioutil.ReadFile(destfile2); err != nil {
		t.Fatal(err)
	} else if ref := "third world"; string(data) != ref {
		t.Fatalf("expected %v, got %v", ref, string(data))
	}
}

func TestServerVersion(t *testing.T) {
	verMismatchStr := "expected version %v, but got %v"

	for _, tcase := range []struct {
		v int
		s ServerPriority
	}{
		{v: 7020100, s: ServerPriority("7.2.1")},
		{v: 7200100, s: ServerPriority("7.20.1")},
		{v: 7020101, s: ServerPriority("7.2.1-mp1")},
		{v: 8020000, s: ServerPriority("8.2")},
		{v: 10000000, s: ServerPriority("10")},
		{v: 7000001, s: ServerPriority("7-mp1")},
		{v: 7020000, s: ServerPriority("7.2.100")},
		{v: 7020100, s: ServerPriority("7.2.1-mp100")},
		{v: 0, s: ServerPriority("")},
		{v: 0, s: ServerPriority("7.0.0-mp")},
		{v: 0, s: ServerPriority("7.0.")},
		{v: 7000000, s: ServerPriority("7.0.0-0000")},
		{v: 7000060, s: ServerPriority("7.0.0-6000")},
	} {
		if tcase.v != int(tcase.s.GetVersion()) {
			t.Fatalf(verMismatchStr, tcase.v, tcase.s.GetVersion())
		}
	}
}

func TestRecomputeCPUBasedConfigs(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

	n0 := runtime.GOMAXPROCS(0)
	settings := []byte(fmt.Sprintf(`{"indexer.bhive.numReaders": %v}`, n0*3))
	config := SystemConfig.Clone()
	if err := config.Update(settings); err != nil {
		t.Fatal(err)
	}
	explicit, err := NewConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	check := func(ncpu int) {
		t.Helper()
		if cv := config["indexer.bhive.numBuilder"]; cv.Value != ncpu/8 {
			t.Errorf("GOMAXPROCS %v: numBuilder %v, want %v", ncpu, cv.Value, ncpu/8)
		}
		if cv := config["indexer.bhive.numReaders"]; cv.Value != n0*3 {
			t.Errorf("GOMAXPROCS %v: explicitly set numReaders %v, want %v", ncpu, cv.Value, n0*3)
		}
	}
	check(n0)

	ncpu := SetNumCPUs((n0 + 8) * 100)
	config.RecomputeCPUBasedConfigs(ncpu, explicit)
	check(ncpu)
}

func TestCPUBasedConfigs(t *testing.T) {
	if path := os.Getenv("SYSTEM_CONFIG_DUMP"); path != "" {
		data, err := json.Marshal(SystemConfig.Map())
		if err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		return
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

	// SystemConfig values as built by a process started with GOMAXPROCS ncpu.
	systemConfigAt := func(ncpu int) map[string]json.RawMessage {
		path := filepath.Join(t.TempDir(), "config.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestCPUBasedConfigs$")
		cmd.Env = append(os.Environ(), "SYSTEM_CONFIG_DUMP="+path, fmt.Sprintf("GOMAXPROCS=%v", ncpu))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		values := make(map[string]json.RawMessage)
		if err := json.Unmarshal(data, &values); err != nil {
			t.Fatal(err)
		}
		return values
	}

	// The exclusions listed at cpuBasedConfigs.
	skip := map[string]bool{
		"indexer.plasma.minNumShard": true,
		"indexer.numSliceWriters":    true,
		"projector.maxCpuPercent":    true,
	}
	for key := range SystemConfig {
		if strings.HasPrefix(key, "indexer.settings.") {
			skip[key] = true
		}
	}
	for _, n := range []int{3, 64} {
		config := SystemConfig.Clone()
		ncpu := SetNumCPUs(n * 100)
		config.RecomputeCPUBasedConfigs(ncpu, nil)

		want := systemConfigAt(ncpu)
		for key, cv := range config {
			got, _ := json.Marshal(cv.Value)
			if skip[key] || bytes.Equal(got, want[key]) {
				continue
			}
			if _, ok := cpuBasedConfigs[key]; ok {
				t.Errorf("%v: formula gives %s at GOMAXPROCS %v, SystemConfig has %s", key, got, ncpu, want[key])
			} else {
				t.Errorf("%v changes with GOMAXPROCS but is not in cpuBasedConfigs", key)
			}
		}
	}
	for key, formula := range cpuBasedConfigs {
		if got, want := reflect.TypeOf(formula(1)), reflect.TypeOf(SystemConfig[key].DefaultVal); got != want {
			t.Errorf("%v: formula returns %v, default is %v", key, got, want)
		}
	}
}

func BenchmarkExcludeStrings(b *testing.B) {
	x := []string{"1", "2", "3", "4"}
	y := []string{"2", "4"}
	for i := 0; i < b.N; i++ {
		ExcludeStrings(x, y)
	}
}

func BenchmarkCommonStrings(b *testing.B) {
	x := []string{"1", "2", "3", "4"}
	y := []string{"2", "4", "5"}
	for i := 0; i < b.N; i++ {
		CommonStrings(x, y)
	}
}

func BenchmarkHasString(b *testing.B) {
	a := []string{"1", "2", "3", "4"}
	for i := 0; i < b.N; i++ {
		HasString("1", a)
	}
}

func BenchmarkExcludeUint32(b *testing.B) {
	x := []uint32{1, 2, 3, 4}
	y := []uint32{2, 4}
	for i := 0; i < b.N; i++ {
		ExcludeUint32(x, y)
	}
}

func BenchmarkExcludeUint64(b *testing.B) {
	x := []uint64{1, 2, 3, 4}
	y := []uint64{2, 4}
	for i := 0; i < b.N; i++ {
		ExcludeUint64(x, y)
	}
}

func BenchmarkHasUint32(b *testing.B) {
	a := []uint32{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		HasUint32(1, a)
	}
}

func BenchmarkHasUint64(b *testing.B) {
	a := []uint64{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		HasUint64(1, a)
	}
}

func BenchmarkRemoveUint32(b *testing.B) {
	a := []uint32{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		RemoveUint32(4, a)
	}
}

func BenchmarkRemoveUint64(b *testing.B) {
	a := []uint16{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		RemoveUint16(4, a)
	}
}

func BenchmarkRemoveString(b *testing.B) {
	a := []string{"1", "2", "3", "4"}
	for i := 0; i < b.N; i++ {
		RemoveString("4", a)
	}
}
