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
    "encoding/json"
    "github.com/couchbaselabs/dparval"
    tuqcollate "github.com/couchbaselabs/tuqtng/ast"
    "reflect"
    "testing"
)

func BenchmarkDParVal(b *testing.B) {
    for i := 0; i < b.N; i++ {
        doc := dparval.NewValueFromBytes(json1)
        doc.Value()
    }
}

func BenchmarkTuqCollateInt(b *testing.B) {
    jsonb := []byte(`1234567890`)
    key1 := dparval.NewValueFromBytes(jsonb)
    value1 := key1.Value()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key2 := dparval.NewValueFromBytes(jsonb)
        value2 := key2.Value()
        tuqcollate.CollateJSON(value1, value2)
    }
}

func BenchmarkTuqCollateFloat(b *testing.B) {
    jsonb := []byte(`1234567890.001234556`)
    key1 := dparval.NewValueFromBytes(jsonb)
    value1 := key1.Value()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key2 := dparval.NewValueFromBytes(jsonb)
        value2 := key2.Value()
        tuqcollate.CollateJSON(value1, value2)
    }
}

func BenchmarkTuqCollateArray(b *testing.B) {
    jsonb := []byte(
        `[123456789, 123456789.1234567879, "hello world", true, false, null]`)
    key1 := dparval.NewValueFromBytes(jsonb)
    value1 := key1.Value()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key2 := dparval.NewValueFromBytes(jsonb)
        value2 := key2.Value()
        tuqcollate.CollateJSON(value1, value2)
    }
}

func BenchmarkTuqCollateMap(b *testing.B) {
    key1 := dparval.NewValueFromBytes(json1)
    value1 := key1.Value()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key2 := dparval.NewValueFromBytes(json1)
        value2 := key2.Value()
        tuqcollate.CollateJSON(value1, value2)
    }
}

func BenchmarkJsonInt(b *testing.B) {
    var value1, value2 interface{}
    jsonb := []byte(`1234567890`)
    json.Unmarshal([]byte(jsonb), &value1)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        json.Unmarshal([]byte(jsonb), &value2)
        reflect.DeepEqual(value1, value2)
    }
}

func BenchmarkJsonFloat(b *testing.B) {
    var value1, value2 interface{}
    jsonb := []byte(`1234567890.001234556`)
    json.Unmarshal([]byte(jsonb), &value1)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        json.Unmarshal([]byte(jsonb), &value2)
        reflect.DeepEqual(value1, value2)
    }
}

func BenchmarkJsonArray(b *testing.B) {
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

func BenchmarkJsonMap(b *testing.B) {
    var value1, value2 interface{}
    json.Unmarshal([]byte(json1), &value1)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        json.Unmarshal([]byte(json1), &value2)
        reflect.DeepEqual(value1, value2)
    }
}
