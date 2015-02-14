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
	"encoding/json"
	qv "github.com/couchbase/query/value"
	"reflect"
	"testing"
)

func BenchmarkN1QLValue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		qv.NewValue(testcases[0].text)
	}
}

func BenchmarkN1QLCollateInt(b *testing.B) {
	jsonb := []byte(`1234567890`)
	v := qv.NewValue(jsonb)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Collate(qv.NewValue(jsonb))
	}
}

func BenchmarkN1QLCollateFloat(b *testing.B) {
	jsonb := []byte(`1234567890.001234556`)
	v := qv.NewValue(jsonb)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Collate(qv.NewValue(jsonb))
	}
}

func BenchmarkN1QLCollateArray(b *testing.B) {
	jsonb := []byte(
		`[123456789, 123456789.1234567879, "hello world", true, false, null]`)
	v := qv.NewValue(jsonb)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Collate(qv.NewValue(jsonb))
	}
}

func BenchmarkN1QLCollateMap(b *testing.B) {
	v := qv.NewValue(testcases[0].text)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Collate(qv.NewValue(testcases[0].text))
	}
}

func BenchmarkJSONInt(b *testing.B) {
	var value1, value2 interface{}
	jsonb := []byte(`1234567890`)
	json.Unmarshal([]byte(jsonb), &value1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal([]byte(jsonb), &value2)
		reflect.DeepEqual(value1, value2)
	}
}

func BenchmarkJSONFloat(b *testing.B) {
	var value1, value2 interface{}
	jsonb := []byte(`1234567890.001234556`)
	json.Unmarshal([]byte(jsonb), &value1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal([]byte(jsonb), &value2)
		reflect.DeepEqual(value1, value2)
	}
}

func BenchmarkJSONArray(b *testing.B) {
	var value1, value2 interface{}
	jsonb := []byte(
		`[123456789, 123456789.1234567879, "hello world", true, false, null]`)
	json.Unmarshal([]byte(jsonb), &value1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal([]byte(jsonb), &value2)
		reflect.DeepEqual(value1, value2)
	}
}

func BenchmarkJSONMap(b *testing.B) {
	var value1, value2 interface{}
	json.Unmarshal([]byte(testcases[0].text), &value1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		json.Unmarshal([]byte(testcases[0].text), &value2)
		reflect.DeepEqual(value1, value2)
	}
}
