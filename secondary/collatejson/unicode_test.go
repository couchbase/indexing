//  Copyright (c) 2013 Couchbase, Inc.

// +build ignore

package collatejson

func BenchmarkUtf8(b *testing.B) {
	s := "prográmming"
	codec := NewCodec()
	codec.SortbyUTF8(true)
	for i := 0; i < b.N; i++ {
		codec.EncodeUnicodeString(s)
	}
}

func BenchmarkNFKD(b *testing.B) {
	s := "prográmming"
	codec := NewCodec()
	codec.SortbyNFKD(true)
	for i := 0; i < b.N; i++ {
		codec.EncodeUnicodeString(s)
	}
}

func BenchmarkStringCollate(b *testing.B) {
	s := "prográmming"
	codec := NewCodec()
	for i := 0; i < b.N; i++ {
		codec.EncodeUnicodeString(s)
	}
}
