package common

import "testing"

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

func TestHasUint32(t *testing.T) {
	a := []uint32{1, 2, 3, 4}
	if HasUint32(uint32(1), a) == false || HasUint32(uint32(5), a) == true {
		t.Fatal("failed HasUint32")
	}
}

func TestRemoveUint32(t *testing.T) {
	a := []uint32{1, 2, 3, 4}
	b := RemoveUint32(4, a)
	if len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Fatal("failed RemoveUint32")
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

func BenchmarkHasUint32(b *testing.B) {
	a := []uint32{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		HasUint32(1, a)
	}
}

func BenchmarkRemoveUint32(b *testing.B) {
	a := []uint32{1, 2, 3, 4}
	for i := 0; i < b.N; i++ {
		RemoveUint32(4, a)
	}
}
