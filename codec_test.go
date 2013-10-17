package collatejson

import (
    "fmt"
    "testing"
    "math/rand"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// TODO:
//  - add test cases for floating point encoding and decoding.

func Test_integer(t *testing.T) {
    var samples = []string{
        "+7",
        "+123",
        "+1234567890",
        "-1234567891",
        "-1234567890",
        "-1234567889",
        "0",
        "0",
    }
    for _, sample := range samples {
        sampleE := string(decodeInt(encodeInt([]byte(sample))))
        if sampleE != sample {
            fmt.Println("codec Int failed for", sample, sampleE)
            t.Fail()
        }
    }
    if string(decodeInt(encodeInt([]byte("-0")))) != "0" {
        t.Fail()
    }
    if string(decodeInt(encodeInt([]byte("+0")))) != "0" {
        t.Fail()
    }
}

func Test_smallDecimal(t *testing.T) {
    var samples = []string{
        "-0.9995",
        "-0.999",
        "-0.0123",
        "-0.00123",
        "-0.0001233",
        "-0.000123",
        "+0.000123",
        "+0.0001233",
        "+0.00123",
        "+0.0123",
        "+0.999",
        "+0.9995",
    }
    for _, sample := range samples {
        sampleE := string(decodeSD(encodeSD([]byte(sample))))
        if sampleE != sample {
            fmt.Println("code small decimal failed for", sample, sampleE)
            t.Fail()
        }
    }
    if string(decodeSD(encodeSD([]byte("+.9995")))) != "+0.9995" {
        t.Fail()
    }
    if string(decodeSD(encodeSD([]byte(".9995")))) != "+0.9995" {
        t.Fail()
    }
    if string(decodeSD(encodeSD([]byte("-.9995")))) != "-0.9995" {
        t.Fail()
    }
}

func Test_largeDecimal(t *testing.T) {
    var samples = []string{
        "-100.5",
        "-10.5",
        "-3.145",
        "-3.14",
        "-1.01",
        "+1.01",
        "+3.14",
        "+3.145",
        "+10.5",
        "+100.5",
    }
    for _, sample := range samples {
        sampleE := string(decodeLD(encodeLD([]byte(sample))))
        if sampleE != sample {
            fmt.Println("code large decimal failed for", sample, sampleE)
            t.Fail()
        }
    }
}

func Benchmark_encodeInt(b *testing.B) {
    var samples = [][]byte{
        []byte("+7"),
        []byte("+123"),
        []byte("+1234567890"),
        []byte("-1234567891"),
        []byte("-1234567890"),
        []byte("-1234567889"),
        []byte("0"),
        []byte("0"),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        encodeInt([]byte(samples[k]))
    }
}

func Benchmark_decodeInt(b *testing.B) {
    var samples = [][]byte{
        encodeInt([]byte("+7")),
        encodeInt([]byte("+123")),
        encodeInt([]byte("+1234567890")),
        encodeInt([]byte("-1234567891")),
        encodeInt([]byte("-1234567890")),
        encodeInt([]byte("-1234567889")),
        encodeInt([]byte("0")),
        encodeInt([]byte("0")),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        decodeInt(samples[k])
    }
}

func Benchmark_encodeSD(b *testing.B) {
    var samples = [][]byte{
        []byte("-0.9995"),
        []byte("-0.999"),
        []byte("-0.0123"),
        []byte("-0.00123"),
        []byte("-0.0001233"),
        []byte("-0.000123"),
        []byte("+0.000123"),
        []byte("+0.0001233"),
        []byte("+0.00123"),
        []byte("+0.0123"),
        []byte("+0.999"),
        []byte("+0.9995"),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        encodeSD(samples[k])
    }
}

func Benchmark_decodeSD(b *testing.B) {
    var samples = [][]byte{
        encodeSD([]byte("-0.9995")),
        encodeSD([]byte("-0.999")),
        encodeSD([]byte("-0.0123")),
        encodeSD([]byte("-0.00123")),
        encodeSD([]byte("-0.0001233")),
        encodeSD([]byte("-0.000123")),
        encodeSD([]byte("+0.000123")),
        encodeSD([]byte("+0.0001233")),
        encodeSD([]byte("+0.00123")),
        encodeSD([]byte("+0.0123")),
        encodeSD([]byte("+0.999")),
        encodeSD([]byte("+0.9995")),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        decodeSD(samples[k])
    }
}

func Benchmark_encodeLD(b *testing.B) {
    var samples = [][]byte{
        []byte("-100.5"),
        []byte("-10.5"),
        []byte("-3.145"),
        []byte("-3.14"),
        []byte("-1.01"),
        []byte("+1.01"),
        []byte("+3.14"),
        []byte("+3.145"),
        []byte("+10.5"),
        []byte("+100.5"),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        encodeLD(samples[k])
    }
}

func Benchmark_decodeLD(b *testing.B) {
    var samples = [][]byte {
        encodeLD([]byte("-100.5")),
        encodeLD([]byte("-10.5")),
        encodeLD([]byte("-3.145")),
        encodeLD([]byte("-3.14")),
        encodeLD([]byte("-1.01")),
        encodeLD([]byte("+1.01")),
        encodeLD([]byte("+3.14")),
        encodeLD([]byte("+3.145")),
        encodeLD([]byte("+10.5")),
        encodeLD([]byte("+100.5")),
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        k := rand.Intn(len(samples))
        decodeLD(samples[k])
    }
}
