package conversions

import (
	"unsafe"
)

// stringHeader struct is the runtime representation
// of reflect.StringHeader. It uses unsafe.Pointer instead
// of unintptr to refer to data. This is to make sure that
// a valid reference to underlying data exists even after
// conversion to a slice and garbage collector would not
// collect the underlying data.
// See: https://groups.google.com/forum/#!topic/golang-nuts/Zsfk-VMd_fU/discussion
// for complete discussion
type StringHeader struct {
	Data      unsafe.Pointer
	StringLen int
}

// sliceHeader struct is the runtime representation
// of reflect.SliceHeader. It uses unsafe.Pointer instead
// of unintptr to refer to data. This is to make sure that
// a valid reference to underlying data exists even after
// conversion to a string and garbage collector would not
// collect the underlying data
// See: https://groups.google.com/forum/#!topic/golang-nuts/Zsfk-VMd_fU/discussion
// for complete discussion
type SliceHeader struct {
	Data     unsafe.Pointer
	SliceLen int
	SliceCap int
}

// This method converts a string to byte slice without allocating
// any new memory for byte slice. The converted byte slice will
// refer to the same data as string and therefore, the immutable
// property of string is violated. Modifying the byte slice
// will modify the string as well
// See: https://groups.google.com/forum/#!topic/golang-nuts/Zsfk-VMd_fU/discussion
// for complete discussion
func StringToByteSlice(s string) (bs []byte) {
	stringHdr := (*StringHeader)(unsafe.Pointer(&s))
	sliceHdr := (*SliceHeader)(unsafe.Pointer(&bs))
	sliceHdr.Data = stringHdr.Data
	sliceHdr.SliceLen = len(s)
	sliceHdr.SliceCap = len(s)
	return bs
}

// This method converts a byte slice to string without allocating
// any new memory for string. Modifying the byte slice will modify
// the string as well and therefore the immutable property of
// string is violated
// See: https://groups.google.com/forum/#!topic/golang-nuts/Zsfk-VMd_fU/discussion
// for complete discussion
func ByteSliceToString(bs []byte) (s string) {
	sliceHdr := (*SliceHeader)(unsafe.Pointer(&bs))
	stringHdr := (*StringHeader)(unsafe.Pointer(&s))
	stringHdr.Data = sliceHdr.Data
	stringHdr.StringLen = len(bs)
	return s
}
