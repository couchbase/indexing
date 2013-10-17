package collatejson

import (
    "bytes"
    "fmt"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

var json1 = []byte(
`{ "inelegant":27.53096820876087,
"horridness":true,
"iridodesis":[79.1253026404128,null],
"arrogantness":null,
"unagrarian":false
}`)

func TestEncode(t *testing.T) {
    code := Encode(json1)
    ref := `\a` +
           `>5\x05arrogantness\x01` +
           `\x05horridness\x03` +
           `\x05inelegant\x04>>22753096820876087-` +
           `\x05iridodesis\x06` +
             `>2\x04>>2791253026404128-` +
             `\x01` +
           `\x05unagrarian\x02`

    out := fmt.Sprintf("%q", code)
    out = out[1:len(out)-1]
    if out != ref {
        t.Fail()
    }
}

func BenchmarkEncode(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Encode(json1)
    }
}

func BenchmarkCompare(b *testing.B) {
    code := Encode(json1)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        bytes.Compare(code, code)
    }
}
