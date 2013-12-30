package collatejson

import (
    "bytes"
    "fmt"
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
        `>5\x00\x06arrogantness\x02\x00` +
        `\x06horridness\x04\x00` +
        `\x06inelegant\x05>>22753096820876087-\x00` +
        `\x06iridodesis\a>2\x00\x05>>2791253026404128-\x00\x02\x00\x00\x06` +
        `unagrarian\x03\x00\x00`

    out := fmt.Sprintf("%q", code)
    out = out[1 : len(out)-1]
    if out != ref {
        t.Error("Encode fails, did you change the encoding format ?")
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
