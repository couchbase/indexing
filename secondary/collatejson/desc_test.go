//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package collatejson

import (
	"bytes"
	"math"
	"testing"

	n1ql "github.com/couchbase/query/value"
)

var testcasesdesc = []struct {
	text     string
	desc     []bool
	encoded  []byte
	reversed []byte
}{

	{`[10]`,
		[]bool{true},
		[]byte{8, 5, 62, 62, 50, 49, 45, 0, 0},
		[]byte{8, 250, 193, 193, 205, 206, 210, 255, 0},
	},

	{`[99.9876514]`,
		[]bool{true},
		[]byte{8, 5, 62, 62, 50, 57, 57, 57, 56, 55, 54, 53, 49, 52, 45, 0, 0},
		[]byte{8, 250, 193, 193, 205, 198, 198, 198, 199, 200, 201, 202, 206, 203, 210, 255, 0},
	},

	{`[true]`,
		[]bool{true},
		[]byte{8, 4, 0, 0},
		[]byte{8, 251, 255, 0},
	},

	{`[false]`,
		[]bool{true},
		[]byte{8, 3, 0, 0},
		[]byte{8, 252, 255, 0},
	},

	{`[null]`,
		[]bool{true},
		[]byte{8, 2, 0, 0},
		[]byte{8, 253, 255, 0},
	},

	{`[""]`,
		[]bool{true},
		[]byte{8, 6, 0, 0, 0},
		[]byte{8, 249, 255, 255, 0},
	},

	{`["abcde"]`,
		[]bool{true},
		[]byte{8, 6, 97, 98, 99, 100, 101, 0, 0, 0},
		[]byte{8, 249, 158, 157, 156, 155, 154, 255, 255, 0},
	},

	{`["ab","bcd","de"]`,
		[]bool{true, false, false},
		[]byte{8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 6, 100, 101, 0, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 6, 98, 99, 100, 0, 0, 6, 100, 101, 0, 0, 0},
	},

	{`["ab","bcd","de"]`,
		[]bool{false, false, true},
		[]byte{8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 6, 100, 101, 0, 0, 0},
		[]byte{8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 249, 155, 154, 255, 255, 0},
	},

	{`["ab","bcd","de"]`,
		[]bool{true, false, true},
		[]byte{8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 6, 100, 101, 0, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 6, 98, 99, 100, 0, 0, 249, 155, 154, 255, 255, 0},
	},

	{`["ab","bcd","de"]`,
		[]bool{true, true, true},
		[]byte{8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 6, 100, 101, 0, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 249, 157, 156, 155, 255, 255, 249, 155, 154, 255, 255, 0},
	},

	{`["ab",null,10]`,
		[]bool{false, true, true},
		[]byte{8, 6, 97, 98, 0, 0, 2, 0, 5, 62, 62, 50, 49, 45, 0, 0},
		[]byte{8, 6, 97, 98, 0, 0, 253, 255, 250, 193, 193, 205, 206, 210, 255, 0},
	},

	{`["ab",null,10]`,
		[]bool{true, false, true},
		[]byte{8, 6, 97, 98, 0, 0, 2, 0, 5, 62, 62, 50, 49, 45, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 2, 0, 250, 193, 193, 205, 206, 210, 255, 0},
	},

	{`[[1,2],[2],[3]]`,
		[]bool{true, false, true},
		[]byte{8, 8, 5, 62, 62, 49, 49, 45, 0, 5, 62, 62, 49, 50, 45, 0, 0, 8, 5, 62, 62, 49, 50, 45,
			0, 0, 8, 5, 62, 62, 49, 51, 45, 0, 0, 0},
		[]byte{8, 247, 250, 193, 193, 206, 206, 210, 255, 250, 193, 193, 206, 205, 210, 255, 255,
			8, 5, 62, 62, 49, 50, 45, 0, 0, 247, 250, 193, 193, 206, 204, 210, 255, 255, 0},
	},

	{`[[1,2,3],["ab",null,10],["ab","bcd","de"]]`,
		[]bool{true, false, true},
		[]byte{8, 8, 5, 62, 62, 49, 49, 45, 0, 5, 62, 62, 49, 50, 45, 0, 5, 62, 62, 49, 51, 45, 0, 0,
			8, 6, 97, 98, 0, 0, 2, 0, 5, 62, 62, 50, 49, 45, 0, 0, 8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0,
			0, 6, 100, 101, 0, 0, 0, 0},
		[]byte{8, 247, 250, 193, 193, 206, 206, 210, 255, 250, 193, 193, 206, 205, 210, 255, 250, 193,
			193, 206, 204, 210, 255, 255, 8, 6, 97, 98, 0, 0, 2, 0, 5, 62, 62, 50, 49, 45, 0, 0, 247, 249,
			158, 157, 255, 255, 249, 157, 156, 155, 255, 255, 249, 155, 154, 255, 255, 255, 0},
	},

	{`[{ "intf" : 10 }]`,
		[]bool{true},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0, 0},
		[]byte{8, 246, 248, 193, 206, 255, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193,
			205, 206, 210, 255, 255, 0},
	},

	{`[{ "intf" : 10, "boolf" : true, "strf" : "abc"}]`,
		[]bool{true},
		[]byte{8, 9, 7, 62, 51, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 6, 105, 110, 116,
			102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0,
			0, 0},
		[]byte{8, 246, 248, 193, 204, 255, 249, 157, 144, 144, 147, 153, 255, 255, 251, 255,
			249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205, 206, 210, 255, 249, 140, 139,
			141, 153, 255, 255, 249, 158, 157, 156, 255, 255, 255, 0},
	},

	{`[{ "arrf" : ["ab","bcd","de"], "intf" : 10, "boolf" : true, "strf" : "abc"}]`,
		[]bool{true},
		[]byte{8, 9, 7, 62, 52, 0, 6, 97, 114, 114, 102, 0, 0, 8, 6, 97, 98, 0, 0, 6, 98,
			99, 100, 0, 0, 6, 100, 101, 0, 0, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 6, 105,
			110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97,
			98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 248, 193, 203, 255, 249, 158, 141, 141, 153, 255, 255, 247, 249, 158,
			157, 255, 255, 249, 157, 156, 155, 255, 255, 249, 155, 154, 255, 255, 255, 249, 157,
			144, 144, 147, 153, 255, 255, 251, 255, 249, 150, 145, 139, 153, 255, 255, 250, 193,
			193, 205, 206, 210, 255, 249, 140, 139, 141, 153, 255, 255, 249, 158, 157, 156, 255,
			255, 255, 0},
	},

	{`[{ "intf" : 10},  {"boolf" : true}, {"strf" : "abc"}]`,
		[]bool{true, true, false},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 248, 193, 206, 255, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205,
			206, 210, 255, 255, 246, 248, 193, 206, 255, 249, 157, 144, 144, 147, 153, 255, 255, 251, 255,
			255, 9, 7, 62, 49, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
	},

	{`[{ "intf" : 10},  {"boolf" : true}, {"strf" : "abc"}]`,
		[]bool{true, true, true},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 248, 193, 206, 255, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205,
			206, 210, 255, 255, 246, 248, 193, 206, 255, 249, 157, 144, 144, 147, 153, 255, 255, 251,
			255, 255, 246, 248, 193, 206, 255, 249, 140, 139, 141, 153, 255, 255, 249, 158, 157, 156,
			255, 255, 255, 0},
	},

	{`[{ "intf" : 10},  {"boolf" : true}, {"strf" : "abc"}]`,
		[]bool{true, false, true},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 248, 193, 206, 255, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205,
			206, 210, 255, 255, 9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 246, 248, 193,
			206, 255, 249, 140, 139, 141, 153, 255, 255, 249, 158, 157, 156, 255, 255, 255, 0},
	},

	{`[{ "intf" : 10},  {"boolf" : true}, {"strf" : "abc"}]`,
		[]bool{false, true, false},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			246, 248, 193, 206, 255, 249, 157, 144, 144, 147, 153, 255, 255, 251, 255, 255,
			9, 7, 62, 49, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
	},

	{`[{ "intf" : 10},  {"boolf" : true}, {"strf" : "abc"}]`,
		[]bool{false, false, false},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 9, 7, 62, 49, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0,
			9, 7, 62, 49, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 0, 9, 7, 62, 49, 0, 6,
			115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
	},

	{`[[1,2],[5,""],[null]]`,
		[]bool{false, true, true},
		[]byte{8, 8, 5, 62, 62, 49, 49, 45, 0, 5, 62, 62, 49, 50, 45, 0, 0,
			8, 5, 62, 62, 49, 53, 45, 0, 6, 0, 0, 0, 8, 2, 0, 0, 0},
		[]byte{8, 8, 5, 62, 62, 49, 49, 45, 0, 5, 62, 62, 49, 50, 45, 0, 0,
			247, 250, 193, 193, 206, 202, 210, 255, 249, 255, 255, 255, 247, 253, 255, 255, 0},
	},

	{`["[1,2]",[5,""],[null]]`,
		[]bool{true, true, false},
		[]byte{8, 6, 91, 49, 44, 50, 93, 0, 0, 8, 5, 62, 62, 49, 53, 45, 0, 6, 0, 0, 0,
			8, 2, 0, 0, 0},
		[]byte{8, 249, 164, 206, 211, 205, 162, 255, 255, 247, 250, 193, 193, 206, 202, 210,
			255, 249, 255, 255, 255, 8, 2, 0, 0, 0},
	},

	{`["ab","~[]{}falsenilNA~",10]`,
		[]bool{true, true, false},
		[]byte{8, 6, 97, 98, 0, 0, 1, 0, 5, 62, 62, 50, 49, 45, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 254, 255, 5, 62, 62, 50, 49, 45, 0, 0},
	},

	{`["ab","~[]{}falsenilNA~",null]`,
		[]bool{true, true, true},
		[]byte{8, 6, 97, 98, 0, 0, 1, 0, 2, 0, 0},
		[]byte{8, 249, 158, 157, 255, 255, 254, 255, 253, 255, 0},
	},
}

func TestCodecDesc(t *testing.T) {
	codec := NewCodec(16)
	for _, tcase := range testcasesdesc {

		code, err := codec.Encode([]byte(tcase.text), code[:0])
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(code, tcase.encoded) {
			t.Errorf("Enocded bytes mismatch %v %v", code, tcase.encoded)
		}

		codec.ReverseCollate(code, tcase.desc)

		if !bytes.Equal(code, tcase.reversed) {
			t.Errorf("Reversed bytes mismatch %v %v", code, tcase.reversed)
		}

		codec.ReverseCollate(code, tcase.desc)

		if !bytes.Equal(code, tcase.encoded) {
			t.Errorf("Re-reversed bytes mismatch %v %v", code, tcase.encoded)
		}

	}
}

var testcasesproplen = []struct {
	text     string
	desc     []bool
	encoded  []byte
	reversed []byte
}{

	{`[{ "intf" : 10 }]`,
		[]bool{true},
		[]byte{8, 9, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62, 50, 49, 45, 0, 0, 0},
		[]byte{8, 246, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205, 206, 210, 255, 255, 0},
	},

	{`[{ "intf" : 10, "boolf" : true, "strf" : "abc"}]`,
		[]bool{true},
		[]byte{8, 9, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62,
			50, 49, 45, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 249, 157, 144, 144, 147, 153, 255, 255, 251, 255, 249, 150, 145, 139, 153,
			255, 255, 250, 193, 193, 205, 206, 210, 255, 249, 140, 139, 141, 153, 255, 255, 249, 158,
			157, 156, 255, 255, 255, 0},
	},

	{`[{ "arrf" : ["ab","bcd","de"], "intf" : 10, "boolf" : true, "strf" : "abc"}]`,
		[]bool{true},
		[]byte{8, 9, 6, 97, 114, 114, 102, 0, 0, 8, 6, 97, 98, 0, 0, 6, 98, 99, 100, 0, 0, 6, 100,
			101, 0, 0, 0, 6, 98, 111, 111, 108, 102, 0, 0, 4, 0, 6, 105, 110, 116, 102, 0, 0, 5, 62, 62,
			50, 49, 45, 0, 6, 115, 116, 114, 102, 0, 0, 6, 97, 98, 99, 0, 0, 0, 0},
		[]byte{8, 246, 249, 158, 141, 141, 153, 255, 255, 247, 249, 158, 157, 255, 255, 249, 157,
			156, 155, 255, 255, 249, 155, 154, 255, 255, 255, 249, 157, 144, 144, 147, 153, 255, 255, 251,
			255, 249, 150, 145, 139, 153, 255, 255, 250, 193, 193, 205, 206, 210, 255, 249, 140, 139, 141,
			153, 255, 255, 249, 158, 157, 156, 255, 255, 255, 0},
	},
}

func TestCodecDescPropLen(t *testing.T) {
	codec := NewCodec(16)
	codec.SortbyPropertyLen(false)
	for _, tcase := range testcasesproplen {

		code, err := codec.Encode([]byte(tcase.text), code[:0])
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(code, tcase.encoded) {
			t.Errorf("Enocded bytes mismatch %v %v", code, tcase.encoded)
		}

		codec.ReverseCollate(code, tcase.desc)
		if !bytes.Equal(code, tcase.reversed) {
			t.Errorf("Reversed bytes mismatch %v %v", code, tcase.reversed)
		}

		codec.ReverseCollate(code, tcase.desc)
		if !bytes.Equal(code, tcase.encoded) {
			t.Errorf("Re-reversed bytes mismatch %v %v", code, tcase.encoded)
		}

	}
}

var testcasesdescspl = []struct {
	key  []interface{}
	desc []bool
}{

	{[]interface{}{"a\x00c"},
		[]bool{true},
	},
	{[]interface{}{"ab\x00"},
		[]bool{true},
	},
	{[]interface{}{"ab\xff"},
		[]bool{true},
	},
	{[]interface{}{"a\xffc"},
		[]bool{true},
	},

	{[]interface{}{"a\x00c"},
		[]bool{false},
	},
	{[]interface{}{"ab\x00"},
		[]bool{false},
	},
	{[]interface{}{"ab\xff"},
		[]bool{false},
	},
	{[]interface{}{"a\xffc"},
		[]bool{false},
	},

	{[]interface{}{"hello", "ab\x00"},
		[]bool{true, false},
	},
	{[]interface{}{"hello", "ab\x00"},
		[]bool{false, true},
	},
	{[]interface{}{"\x00ab", "hello"},
		[]bool{false, true},
	},
	{[]interface{}{"\x00ab", "hello"},
		[]bool{true, false},
	},

	{[]interface{}{"hello", "ab\xff"},
		[]bool{true, false},
	},
	{[]interface{}{"hello", "ab\xff"},
		[]bool{false, true},
	},
	{[]interface{}{"\xffab", "hello"},
		[]bool{false, true},
	},
	{[]interface{}{"\xffab", "hello"},
		[]bool{true, false},
	},

	{[]interface{}{"he\t\rllo", "ab\x00", 123},
		[]bool{true, true, true},
	},
	{[]interface{}{"hel\tlo", nil, "ab\x00"},
		[]bool{true, true, true},
	},
	{[]interface{}{"hel\t\rlo", "ab\x00", true},
		[]bool{true, true, true},
	},

	{[]interface{}{"hel\t\rlo", "ab\xff", 123},
		[]bool{true, true, true},
	},
	{[]interface{}{"hel\t\rlo", nil, "ab\xff"},
		[]bool{true, true, true},
	},
	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c"},
		[]bool{true, true, true},
	},
	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c"},
		[]bool{true, true, false},
	},
	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c"},
		[]bool{true, false, true},
	},

	{[]interface{}{"a\x00b", "a\xffb"},
		[]bool{true, true},
	},
	{[]interface{}{"a\x00b", "a\xffb"},
		[]bool{false, false},
	},
	{[]interface{}{"a\x00b", "a\xffb"},
		[]bool{true, false},
	},
	{[]interface{}{"a\x00b", "a\xffb"},
		[]bool{false, true},
	},

	{[]interface{}{"a\xffb", "a\x00b"},
		[]bool{true, true},
	},
	{[]interface{}{"a\xffb", "a\x00b"},
		[]bool{false, false},
	},
	{[]interface{}{"a\xffb", "a\x00b"},
		[]bool{true, false},
	},
	{[]interface{}{"a\xffb", "a\x00b"},
		[]bool{false, true},
	},

	{[]interface{}{"\xff", "a\x00b"},
		[]bool{true, true},
	},
	{[]interface{}{"\x00", "a\xffb"},
		[]bool{true, true},
	},
	{[]interface{}{"\xff", "a\x00b"},
		[]bool{true, false},
	},
	{[]interface{}{"\x00", "a\xffb"},
		[]bool{true, false},
	},
	{[]interface{}{"\xff", "a\x00b"},
		[]bool{false, true},
	},
	{[]interface{}{"\x00", "a\xffb"},
		[]bool{false, true},
	},

	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c", "abc", "ac\x00", "\xffab"},
		[]bool{true, true, true, true, true, true},
	},
	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c", "abc", "ac\x00", "\xffab"},
		[]bool{false, false, false, false, false, true},
	},
	{[]interface{}{"hel\t\rlo", "ab\xff", "b\x00c", "abc", "ac\x00", "\xffab"},
		[]bool{false, true, false, true, false, true},
	},

	//255,0
	{[]interface{}{"a\xff\x00b", "a\x00b"},
		[]bool{false, true},
	},
	//255,254
	{[]interface{}{"a\xff\xfeb", "a\x00b"},
		[]bool{false, true},
	},
}

func TestCodecDescSplChar(t *testing.T) {

	codec := NewCodec(16)
	for _, tcase := range testcasesdescspl {
		n1qlVal := n1ql.NewValue(tcase.key)
		encVal, err := codec.EncodeN1QLValue(n1qlVal, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}

		//reverse
		codec.ReverseCollate(encVal, tcase.desc)

		//re-reverse
		codec.ReverseCollate(encVal, tcase.desc)

		decVal, err1 := codec.DecodeN1QLValue(encVal, make([]byte, 0, 10000))

		if err1 != nil {
			t.Fatalf("Unexpected error %v", err1)
		}

		if !decVal.EquivalentTo(n1qlVal) {
			t.Errorf("Expected original and decoded n1ql values to be the same. Orig %v Decoded %v", n1qlVal, decVal)
		}
	}

}

func TestSubstituteEncodedField(t *testing.T) {

	codec := NewCodec(16)

	data := make([]interface{}, 0)
	data = append(data, "name")
	data = append(data, int64(2))
	data = append(data, 30)
	data = append(data, "random")

	// Encode current Array
	origVal := n1ql.NewValue(data)
	encVal, err := codec.EncodeN1QLValue(origVal, make([]byte, 0, 10000))
	if err != nil {
		t.Fatal(err)
	}

	// decVal, err1 := codec.DecodeN1QLValue(encVal, make([]byte, 0, 10000))
	// if err1 != nil {
	// 	t.Fatal(err1)
	// }
	// t.Logf("Expected original and decoded n1ql values to be the same. Orig: %v DecVal: %v", origVal, decVal)

	// Encode new Data
	newData := math.Pi
	newDataVal := n1ql.NewValue(newData)
	newDataCode, err := codec.EncodeN1QLValue(newDataVal, make([]byte, 0, 1000))
	if err != nil {
		t.Fatal(err)
	}

	// Replace data at position 2
	newBuf := make([]byte, 0, len(encVal)+len(newDataCode))
	newEncVal, err := codec.ReplaceEncodedFieldInArray(encVal, 1, newDataCode, newBuf)
	if err != nil {
		t.Fatal(err)
	}

	// Decode the code in which we replaced the data
	decVal1, err1 := codec.DecodeN1QLValue(newEncVal, make([]byte, 0, 10000))
	if err1 != nil {
		t.Fatal(err1)
	}

	// Replace the original data and compare with previously decoded data
	data[1] = newData
	n1qlVal := n1ql.NewValue(data)
	if !decVal1.EquivalentTo(n1qlVal) {
		t.Errorf("Error: Expected original and decoded n1ql values to be the same. Orig %v Decoded %v", n1qlVal, decVal1)
	} else {
		t.Logf("Expected original and decoded n1ql values to be the same. Orig %v Decoded %v", n1qlVal, decVal1)
	}

	// Again Replace data at position 4
	newBuf = make([]byte, 0, len(newEncVal)+len(newDataCode))
	newEncVal, err = codec.ReplaceEncodedFieldInArray(newEncVal, 3, newDataCode, newBuf)
	if err != nil {
		t.Fatal(err)
	}

	// Decode the code in which we replaced the data
	decVal2, err2 := codec.DecodeN1QLValue(newEncVal, make([]byte, 0, 10000))
	if err2 != nil {
		t.Fatal(err2)
	}

	// Replace the original data and compare with previously decoded data
	data[3] = newData
	n1qlVal2 := n1ql.NewValue(data)
	if !decVal2.EquivalentTo(n1qlVal2) {
		t.Errorf("Error: Expected original and decoded n1ql values to be the same. Orig %v Decoded %v", n1qlVal2, decVal2)
	} else {
		t.Logf("Expected original and decoded n1ql values to be the same. Orig %v Decoded %v", n1qlVal2, decVal2)
	}

	// Again Replace data at position 5
	newBuf = make([]byte, 0, len(newEncVal)+len(newDataCode))
	_, err = codec.ReplaceEncodedFieldInArray(newEncVal, 5, newDataCode, newBuf)
	if err != ErrInvalidInput {
		t.Fatal(err)
	}
}
