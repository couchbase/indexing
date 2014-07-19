//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

package collatejson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var json1 = []byte(
	`{ "inelegant":27.53096820876087,
"horridness":true,
"iridodesis":[79.1253026404128,null],
"arrogantness":null,
"unagrarian":false
}`)

func TestEncode(t *testing.T) {
	codec := NewCodec(cap(code), 128)
	code, err := codec.Encode(json1, code[:0])
	if err != nil {
		t.Error(err)
	}

	ref := `\b` +
		`\x05>5\x00` +
		`\x06arrogantness\x00\x00\x02\x00` +
		`\x06horridness\x00\x00\x04\x00` +
		`\x06inelegant\x00\x00\x05>>22753096820876087-\x00` +
		`\x06iridodesis\x00\x00\a\x05>2\x00\x05>>2791253026404128-\x00\x02\x00\x00` +
		`\x06unagrarian\x00\x00\x03\x00\x00`

	out := fmt.Sprintf("%q", code)
	out = out[1 : len(out)-1]
	if out != ref {
		t.Error("Encode fails, did you change the encoding format ?")
		fmt.Printf("%q\n\n%q\n", ref, out)
	}
}

func TestDecode(t *testing.T) {
	var one, two map[string]interface{}

	codec := NewCodec(cap(code), 128)
	code, err := codec.Encode(json1, code[:0])
	if err != nil {
		t.Error(err)
	}
	text := make([]byte, 0, 1024)
	text, err = codec.Decode(code, text)
	if err != nil {
		t.Error(err)
	}

	if err := json.Unmarshal(json1, &one); err != nil {
		t.Error("Unmarshaling reference")
	}
	if err := json.Unmarshal(text, &two); err != nil {
		t.Error("Unmarshaling decoded text")
	}
	if !reflect.DeepEqual(one, two) {
		t.Error("Decode fails, did you change the encoding format ?")
		t.Log(one)
		t.Log(two)
	}
}

func BenchmarkEncode(b *testing.B) {
	codec := NewCodec(cap(code), 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Encode(json1, code[:0])
	}
}

func BenchmarkCompare(b *testing.B) {
	codec := NewCodec(cap(code), 128)
	code, _ := codec.Encode(json1, code[:0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes.Compare(code, code)
	}
}

func BenchmarkDecode(b *testing.B) {
	codec := NewCodec(cap(code), 128)
	code, _ := codec.Encode(json1, code[:0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Decode(code, text[:0])
	}
}
