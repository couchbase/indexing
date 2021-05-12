//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package collatejson

import "encoding/json"
import "reflect"
import "testing"

import qv "github.com/couchbase/query/value"

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
