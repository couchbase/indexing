//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

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
	codec := NewCodec()
	code := codec.Encode(json1)
	ref := `\b` +
		`\x05>5\x00` +
		`\x06arrogantness\x00\x02\x00` +
		`\x06horridness\x00\x04\x00` +
		`\x06inelegant\x00\x05>>22753096820876087-\x00` +
		`\x06iridodesis\x00\a\x05>2\x00\x05>>2791253026404128-\x00\x02\x00\x00\x06` +
		`unagrarian\x00\x03\x00\x00`

	out := fmt.Sprintf("%q", code)
	out = out[1 : len(out)-1]
	if out != ref {
		t.Error("Encode fails, did you change the encoding format ?")
	}
}

func TestDecode(t *testing.T) {
	var one, two map[string]interface{}

	codec := NewCodec()
	out := codec.Decode(codec.Encode(json1))

	json.Unmarshal(json1, &one)
	json.Unmarshal(out, &two)
	if !reflect.DeepEqual(one, two) {
		t.Error("Decode fails, did you change the encoding format ?")
	}
}

func BenchmarkEncode(b *testing.B) {
	codec := NewCodec()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Encode(json1)
	}
}

func BenchmarkCompare(b *testing.B) {
	codec := NewCodec()
	code := codec.Encode(json1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes.Compare(code, code)
	}
}
