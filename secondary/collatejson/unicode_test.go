//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
