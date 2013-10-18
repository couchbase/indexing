package collatejson

import (
    "testing"
)

// TODO:
//  - add test cases for floating point encoding and decoding.

func TestInteger(t *testing.T) {
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
            t.Error("codec Int failed for", sample, sampleE)
        }
    }
    if string(decodeInt(encodeInt([]byte("-0")))) != "0" {
        t.Error("codec fails on `-0`")
    }
    if string(decodeInt(encodeInt([]byte("+0")))) != "0" {
        t.Error("codec fails on `+0`")
    }
}

func TestSmallDecimal(t *testing.T) {
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
            t.Error("code small decimal failed for", sample, sampleE)
        }
    }
    if string(decodeSD(encodeSD([]byte("+.9995")))) != "+0.9995" {
        t.Error("codec fails on `+.9995`")
    }
    if string(decodeSD(encodeSD([]byte(".9995")))) != "+0.9995" {
        t.Error("codec fails on `.9995`")
    }
    if string(decodeSD(encodeSD([]byte("-.9995")))) != "-0.9995" {
        t.Error("codec fails on `-.9995`")
    }
}

func TestLargeDecimal(t *testing.T) {
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
            t.Error("code large decimal failed for", sample, sampleE)
        }
    }
}

func BenchmarkEncodeInt(b *testing.B) {
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        encodeInt([]byte(samples[i%ln]))
    }
}

func BenchmarkDecodeInt(b *testing.B) {
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        decodeInt(samples[i%ln])
    }
}

func BenchmarkDncodeSD(b *testing.B) {
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        encodeSD(samples[i%ln])
    }
}

func BenchmarkDecodeSD(b *testing.B) {
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        decodeSD(samples[i%ln])
    }
}

func BenchmarkEncodeLD(b *testing.B) {
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        encodeLD(samples[i%ln])
    }
}

func BenchmarkDecodeLD(b *testing.B) {
    var samples = [][]byte{
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
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        decodeLD(samples[i%ln])
    }
}
