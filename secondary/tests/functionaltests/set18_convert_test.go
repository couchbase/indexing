package functionaltests

import (
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/conversions"
)

func TestStringToByteSlice(t *testing.T) {
	s := "hello"
	b := conversions.StringToByteSlice(s)

	// Should have the same length
	if len(b) != 5 {
		t.Fatalf("Converted bytes have different length (%d) than the string (%v)", len(b), len(s))
	}
	if len(b) != 5 {
		t.Fatalf("Converted bytes have capacity (%d) beyond the length of string (%v)", cap(b), len(s))
	}
	// Should have same content
	if s != string(b) {
		t.Fatalf("Converted bytes has different value %q than the string %q", string(b), s)
	}
	// Should point to the same data in memory
	sData := (*(*reflect.StringHeader)(unsafe.Pointer(&s))).Data
	bData := (*(*reflect.SliceHeader)(unsafe.Pointer(&b))).Data
	if sData != bData {
		t.Fatalf("Converted bytes points to different data %d than the string %d", sData, bData)
	}
}

func TestStringToByteSlice_Stack(t *testing.T) {
	// Allocates string on stack and forces a garbage collection
	// Even after garbage collection, the byte slice should refer
	// to the string
	b := conversions.StringToByteSlice("hello")
	runtime.GC()

	time.Sleep(1 * time.Second)
	// Force garbage collection
	runtime.GC()

	// Should have the same length
	if len(b) != 5 {
		t.Fatalf("Converted bytes have different length (%d) than the string (5)", len(b))
	}
	if len(b) != 5 {
		t.Fatalf("Converted bytes have capacity (%d) beyond the length of string (5)", cap(b))
	}
	// Should have same content
	if "hello" != string(b) {
		t.Fatalf("Converted bytes has different value %q than the string \"hello\"", string(b))
	}
}

func TestByteSliceToString(t *testing.T) {
	b := []byte("bytes!")
	s := conversions.ByteSliceToString(b)

	// Should have the same length
	if len(s) != len(b) {
		t.Fatalf("Converted string has a different length (%d) than the bytes (%d)", len(s), len(b))
	}
	// Should have same content
	if s != string(b) {
		t.Fatalf("Converted string has a different value %q than the bytes %q", s, string(b))
	}
	// Should point to the same data in memory
	sData := (*(*reflect.StringHeader)(unsafe.Pointer(&s))).Data
	bData := (*(*reflect.SliceHeader)(unsafe.Pointer(&b))).Data
	if sData != bData {
		t.Fatalf("Converted string points to different data %d than the bytes %d", sData, bData)
	}
}

// Check we don't access the entire byte slice's capacity
func TestBytesToString_WithUnusedBytes(t *testing.T) {
	// make a long slice of bytes
	bLongDontUse := []byte("bytes! and all these other bytes")
	// just take the first 6 characters
	b := bLongDontUse[:6]
	s := conversions.ByteSliceToString(b)
	// Should have the same length
	if len(s) != len(b) {
		t.Fatalf("Converted string has a different length (%d) than the bytes (%d)", len(s), len(b))
	}
	// Should have same content
	if s != string(b) {
		t.Fatalf("Converted string has a different value %q than the bytes %q", s, string(b))
	}
	// Should point to the same data in memory
	sData := (*(*reflect.StringHeader)(unsafe.Pointer(&s))).Data
	bData := (*(*reflect.SliceHeader)(unsafe.Pointer(&b))).Data
	if sData != bData {
		t.Fatalf("Converted string points to different data %d than the bytes %d", sData, bData)
	}
}

func TestStringHeadersCompatible(t *testing.T) {
	// Check to make sure string header is what reflect thinks it is.
	// They should be the same except for the type of the data field.
	if unsafe.Sizeof(conversions.StringHeader{}) != unsafe.Sizeof(reflect.StringHeader{}) {
		t.Fatalf("stringHeader layout has changed ours %#v theirs %#v", conversions.StringHeader{}, reflect.StringHeader{})
	}
	x := conversions.StringHeader{}
	y := reflect.StringHeader{}
	x.Data = unsafe.Pointer(y.Data)
	y.Data = uintptr(x.Data)
	x.StringLen = y.Len
	y.Len = x.StringLen
	// If we can do all of that then the two structs are compatible
}

func TestSliceHeadersCompatible(t *testing.T) {
	// Check to make sure string header is what reflect thinks it is.
	// They should be the same except for the type of the data field.
	if unsafe.Sizeof(conversions.SliceHeader{}) != unsafe.Sizeof(reflect.SliceHeader{}) {
		t.Fatalf("sliceHeader layout has changed ours %#v theirs %#v", conversions.SliceHeader{}, reflect.SliceHeader{})
	}
	x := conversions.SliceHeader{}
	y := reflect.SliceHeader{}
	x.Data = unsafe.Pointer(y.Data)
	y.Data = uintptr(x.Data)
	x.SliceLen = y.Len
	y.Len = x.SliceLen
	x.SliceCap = y.Cap
	y.Cap = x.SliceCap
	// If we can do all of that then the two structs are compatible
}
