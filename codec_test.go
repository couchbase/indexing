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
    "strconv"
    "testing"
)

// TODO:
//  - add test cases for floating point encoding and decoding.

func TestInteger(t *testing.T) {
    var samples = [][2]string{
        {"+7", ">7"},
        {"+123", ">>3123"},
        {"+1234567890", ">>>2101234567890"},
        {"-10", "--789"},
        {"-11", "--788"},
        {"-1234567891", "---7898765432108"},
        {"-1234567890", "---7898765432109"},
        {"-1234567889", "---7898765432110"},
        {"0", "0"},
        {"+0", "0"},
        {"-0", "0"},
    }
    for _, tcase := range samples {
        sample, ref := tcase[0], tcase[1]
        out := string(EncodeInt([]byte(sample)))
        if out != ref {
            t.Error("integer encode failed for:", sample, out, ref)
        }
        out = string(DecodeInt(EncodeInt([]byte(sample))))
        if atoi(out, t) != atoi(sample, t) {
            t.Error("integer decode failed for:", sample, out)
        }
    }
}

func atoi(text string, t *testing.T) int {
    if val, err := strconv.Atoi(text); err == nil {
        return val
    }
    t.Error("atoi: Unable to convert", text)
    return 0
}

func atof(text string, t *testing.T) float64 {
    if val, err := strconv.ParseFloat(text, 64); err == nil {
        return val
    }
    t.Error("atof: Unable to convert", text)
    return 0.0
}

func TestSmallDecimal(t *testing.T) {
    var samples = [][2]string{
        {"-0.9995", "-0004>"},
        {"-0.999", "-000>"},
        {"-0.0123", "-9876>"},
        {"-0.00123", "-99876>"},
        {"-0.0001233", "-9998766>"},
        {"-0.000123", "-999876>"},
        {"+0.000123", ">000123-"},
        {"+0.0001233", ">0001233-"},
        {"+0.00123", ">00123-"},
        {"+0.0123", ">0123-"},
        {"+0.999", ">999-"},
        {"+0.9995", ">9995-"},
    }
    for _, tcase := range samples {
        sample, ref := tcase[0], tcase[1]
        out := string(EncodeSD([]byte(sample)))
        if out != ref {
            t.Error("small decimal encode failed:", sample, out, ref)
        }
        out = string(DecodeSD(EncodeSD([]byte(sample))))
        if atof(out, t) != atof(sample, t) {
            t.Error("small decimal decode failed:", sample, out)
        }
    }
}

func TestLargeDecimal(t *testing.T) {
    var samples = [][2]string{
        {"-100.5", "--68994>"},
        {"-10.5", "--7894>"},
        {"-3.145", "-6854>"},
        {"-3.14", "-685>"},
        {"-1.01", "-898>"},
        {"-1", "-89>"},
        {"+1", ">1-"},
        {"+1.01", ">101-"},
        {"+3.14", ">314-"},
        {"+3.145", ">3145-"},
        {"+10.5", ">>2105-"},
        {"+100.5", ">>31005-"},
    }
    for _, tcase := range samples {
        sample, ref := tcase[0], tcase[1]
        out := string(EncodeLD([]byte(sample)))
        if out != ref {
            t.Error("small decimal encode failed:", sample, out, ref)
        }
        out = string(DecodeLD(EncodeLD([]byte(sample))))
        if atof(out, t) != atof(sample, t) {
            t.Error("small decimal decode failed:", sample, out)
        }
    }
}

func TestFloat(t *testing.T) {
    var samples = [][2]string{
        {"-10000000000.0", "---7888>"},
        {"-1000000000.0", "---7898>"},
        {"-1.4", "--885>"},
        {"-1.3", "--886>"},
        {"-1", "--88>"},
        {"-0.123", "-0876>"},
        {"-0.0123", "->1876>"},
        {"-0.001233", "->28766>"},
        {"-0.00123", "->2876>"},
        {"0", "0"},
        {"+0.00123", ">-7123-"},
        {"+0.001233", ">-71233-"},
        {"+0.0123", ">-8123-"},
        {"+0.123", ">0123-"},
        {"+1", ">>11-"},
        {"+1.3", ">>113-"},
        {"+1.4", ">>114-"},
        {"+1000000000.0", ">>>2101-"},
        {"+10000000000.0", ">>>2111-"},
    }
    for _, tcase := range samples {
        sample, ref := tcase[0], tcase[1]
        f, _ := strconv.ParseFloat(sample, 64)
        sampleF := strconv.FormatFloat(f, 'e', -1, 64)
        out := string(EncodeFloat([]byte(sampleF)))
        if out != ref {
            t.Error("float encode failed:", sample, out, ref)
        }
        out = string(DecodeFloat(EncodeFloat([]byte(sampleF))))
        if atof(out, t) != atof(sample, t) {
            t.Error("float decode failed:", sample, out)
        }
    }
}

func BenchmarkEncodeInt0(b *testing.B) {
    bEncodeInt(b, "0")
}

func BenchmarkEncodeInt7(b *testing.B) {
    bEncodeInt(b, "+7")
}

func BenchmarkEncodeInt1234567890(b *testing.B) {
    bEncodeInt(b, "+1234567890")
}

func BenchmarkEncodeInt1234567890Neg(b *testing.B) {
    bEncodeInt(b, "-1234567890")
}

func BenchmarkDecodeInt0(b *testing.B) {
    bDecodeInt(b, "0")
}

func BenchmarkDecodeInt7(b *testing.B) {
    bDecodeInt(b, ">7")
}

func BenchmarkDecodeInt1234567890(b *testing.B) {
    bDecodeInt(b, ">>>2101234567890")
}

func BenchmarkDecodeInt1234567891Neg(b *testing.B) {
    bDecodeInt(b, "---7898765432108")
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
        EncodeInt([]byte(samples[i%ln]))
    }
}

func BenchmarkDecodeInt(b *testing.B) {
    var samples = [][]byte{
        EncodeInt([]byte("+7")),
        EncodeInt([]byte("+123")),
        EncodeInt([]byte("+1234567890")),
        EncodeInt([]byte("-1234567891")),
        EncodeInt([]byte("-1234567890")),
        EncodeInt([]byte("-1234567889")),
        EncodeInt([]byte("0")),
        EncodeInt([]byte("0")),
    }
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        DecodeInt(samples[i%ln])
    }
}

func BenchmarkEncodeFloat10000000000(b *testing.B) {
    bEncodeFloat(b, "+10000000000")
}

func BenchmarkEncodeFloat001233(b *testing.B) {
    bEncodeFloat(b, "+0.001233")
}

func BenchmarkEncodeFloat001233Neg(b *testing.B) {
    bEncodeFloat(b, "-0.001233")
}

func BenchmarkEncodeFloat1Neg(b *testing.B) {
    bEncodeFloat(b, "-1")
}

func BenchmarkEncodeFloat10000000000Neg(b *testing.B) {
    bEncodeFloat(b, "-10000000000")
}

func BenchmarkDecodeFloat10000000000(b *testing.B) {
    bDecodeFloat(b, ">>>2111-")
}

func BenchmarkDecodeFloat001233(b *testing.B) {
    bDecodeFloat(b, "->28766>")
}

func BenchmarkDecodeFloat001233Neg(b *testing.B) {
    bDecodeFloat(b, "->28766>")
}

func BenchmarkDecodeFloat1Neg(b *testing.B) {
    bDecodeFloat(b, "--88>")
}

func BenchmarkDecodeFloat10000000000Neg(b *testing.B) {
    bDecodeFloat(b, "---7888>")
}

func BenchmarkEncodeFloat(b *testing.B) {
    var samples = [][]byte{
        []byte("-10000000000.0"),
        []byte("-1000000000.0"),
        []byte("-1.4"),
        []byte("-1.3"),
        []byte("-1"),
        []byte("-0.123"),
        []byte("-0.0123"),
        []byte("-0.001233"),
        []byte("-0.00123"),
        []byte("0"),
        []byte("+0.00123"),
        []byte("+0.001233"),
        []byte("+0.0123"),
        []byte("+0.123"),
        []byte("+1"),
        []byte("+1.3"),
        []byte("+1.4"),
        []byte("+1000000000.0"),
        []byte("+10000000000.0"),
    }
    ln := len(samples)
    for i := 0; i < ln; i++ {
        f, _ := strconv.ParseFloat(string(samples[i]), 64)
        samples[i] = []byte(strconv.FormatFloat(f, 'e', -1, 64))
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        EncodeFloat(samples[i%ln])
    }
}

func BenchmarkDecodeFloat(b *testing.B) {
    var samples = [][]byte{
        []byte("-10000000000.0"),
        []byte("-1000000000.0"),
        []byte("-1.4"),
        []byte("-1.3"),
        []byte("-1"),
        []byte("-0.123"),
        []byte("-0.0123"),
        []byte("-0.001233"),
        []byte("-0.00123"),
        []byte("0"),
        []byte("+0.00123"),
        []byte("+0.001233"),
        []byte("+0.0123"),
        []byte("+0.123"),
        []byte("+1"),
        []byte("+1.3"),
        []byte("+1.4"),
        []byte("+1000000000.0"),
        []byte("+10000000000.0"),
    }
    ln := len(samples)
    for i := 0; i < ln; i++ {
        f, _ := strconv.ParseFloat(string(samples[i]), 64)
        samples[i] = EncodeFloat([]byte(strconv.FormatFloat(f, 'e', -1, 64)))
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        DecodeFloat(samples[i%ln])
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
        EncodeSD(samples[i%ln])
    }
}

func BenchmarkDecodeSD(b *testing.B) {
    var samples = [][]byte{
        EncodeSD([]byte("-0.9995")),
        EncodeSD([]byte("-0.999")),
        EncodeSD([]byte("-0.0123")),
        EncodeSD([]byte("-0.00123")),
        EncodeSD([]byte("-0.0001233")),
        EncodeSD([]byte("-0.000123")),
        EncodeSD([]byte("+0.000123")),
        EncodeSD([]byte("+0.0001233")),
        EncodeSD([]byte("+0.00123")),
        EncodeSD([]byte("+0.0123")),
        EncodeSD([]byte("+0.999")),
        EncodeSD([]byte("+0.9995")),
    }
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        DecodeSD(samples[i%ln])
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
        EncodeLD(samples[i%ln])
    }
}

func BenchmarkDecodeLD(b *testing.B) {
    var samples = [][]byte{
        EncodeLD([]byte("-100.5")),
        EncodeLD([]byte("-10.5")),
        EncodeLD([]byte("-3.145")),
        EncodeLD([]byte("-3.14")),
        EncodeLD([]byte("-1.01")),
        EncodeLD([]byte("+1.01")),
        EncodeLD([]byte("+3.14")),
        EncodeLD([]byte("+3.145")),
        EncodeLD([]byte("+10.5")),
        EncodeLD([]byte("+100.5")),
    }
    ln := len(samples)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        DecodeLD(samples[i%ln])
    }
}

func bEncodeInt(b *testing.B, in string) {
    inb := []byte(in)
    for i := 0; i < b.N; i++ {
        EncodeInt(inb)
    }
}

func bDecodeInt(b *testing.B, in string) {
    inb := []byte(in)
    for i := 0; i < b.N; i++ {
        DecodeInt(inb)
    }
}

func bEncodeFloat(b *testing.B, in string) {
    f, _ := strconv.ParseFloat(in, 64)
    inb := []byte(strconv.FormatFloat(f, 'e', -1, 64))
    for i := 0; i < b.N; i++ {
        EncodeFloat(inb)
    }
}

func bDecodeFloat(b *testing.B, in string) {
    inb := []byte(in)
    for i := 0; i < b.N; i++ {
        DecodeFloat(inb)
    }
}
