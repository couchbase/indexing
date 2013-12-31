package collatejson

import (
    "github.com/couchbaselabs/dparval"
    tuqcollate "github.com/couchbaselabs/tuqtng/ast"
    "testing"
)

func BenchmarkDParVal(b *testing.B) {
    for i := 0; i < b.N; i++ {
        doc := dparval.NewValueFromBytes(json1)
        doc.Value()
    }
}

func BenchmarkTuqCollate(b *testing.B) {
    key1 := dparval.NewValueFromBytes(json1)
    value1 := key1.Value()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key2 := dparval.NewValueFromBytes(json1)
        value2 := key2.Value()
        tuqcollate.CollateJSON(value1, value2)
    }
}
