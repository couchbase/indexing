//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package collatejson

import "bytes"
import "strconv"
import "testing"

var code = make([]byte, 0, 1024)
var text = make([]byte, 0, 1024)

func TestInteger(t *testing.T) {
	var samples = [][2]string{
		{"7", ">7"},
		{"+7", ">7"},
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
		out := EncodeInt([]byte(sample), code[:0])
		if string(out) != ref {
			t.Error("error encode failed for:", sample, string(out), ref)
		}
		_, out = DecodeInt([]byte(out), text[:0])
		if atoi(string(out), t) != atoi(sample, t) {
			t.Error("error decode failed for:", sample, string(out))
		}
	}
}

func atoi(text string, t *testing.T) int {
	if val, err := strconv.Atoi(text); err == nil {
		return val
	}
	t.Errorf("atoi: Unable to convert %v", text)
	return 0
}

func atof(text string, t *testing.T) float64 {
	val, err := strconv.ParseFloat(text, 64)
	if err == nil {
		return val
	}
	t.Error("atof: Unable to convert", text, err)
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
		out := string(EncodeSD([]byte(sample), code[:0]))
		if out != ref {
			t.Error("error small decimal encode failed:", sample, out, ref)
		}
		out = string(DecodeSD([]byte(out), text[:0]))
		if atof(out, t) != atof(sample, t) {
			t.Error("error small decimal decode failed:", sample, out)
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
		out := string(EncodeLD([]byte(sample), code[:0]))
		if out != ref {
			t.Error("large decimal encode failed:", sample, out, ref)
		}
		out = string(DecodeLD([]byte(out), text[:0]))
		if atof(out, t) != atof(sample, t) {
			t.Error("large decimal decode failed:", sample, out)
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
		out := string(EncodeFloat([]byte(sampleF), code[:0]))
		if out != ref {
			t.Error("float encode failed:", sample, out, ref)
		}
		out = string(DecodeFloat([]byte(out), text[:0]))
		if atof(out, t) != atof(sample, t) {
			t.Error("float decode failed:", sample, out)
		}
	}
}

func TestSuffixCoding(t *testing.T) {
	bs := []byte("hello\x00wo\xffrld\x00")
	code := suffixEncodeString(bs, code[:0])
	code = append(code, Terminator)
	text, remn, err := suffixDecodeString(code, text[:0])
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(bs, text) != 0 {
		t.Error("Suffix coding for strings failed")
	}
	if len(remn) != 0 {
		t.Errorf("Suffix coding for strings failed, residue found %q", remn)
	}
}

func BenchmarkEncodeInt0(b *testing.B) {
	bEncodeInt(b, []byte("0"))
}

func BenchmarkEncodeInt7(b *testing.B) {
	bEncodeInt(b, []byte("+7"))
}

func BenchmarkEncodeInt1234567890(b *testing.B) {
	bEncodeInt(b, []byte("+1234567890"))
}

func BenchmarkEncodeInt1234567890Neg(b *testing.B) {
	bEncodeInt(b, []byte("-1234567890"))
}

func BenchmarkDecodeInt0(b *testing.B) {
	bDecodeInt(b, []byte("0"))
}

func BenchmarkDecodeInt7(b *testing.B) {
	bDecodeInt(b, []byte(">7"))
}

func BenchmarkDecodeInt1234567890(b *testing.B) {
	bDecodeInt(b, []byte(">>>2101234567890"))
}

func BenchmarkDecodeInt1234567891Neg(b *testing.B) {
	bDecodeInt(b, []byte("---7898765432108"))
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
		EncodeInt([]byte(samples[i%ln]), code[:0])
	}
}

func BenchmarkDecodeInt(b *testing.B) {
	var samples = [][]byte{
		[]byte(">7"),
		[]byte(">>3123"),
		[]byte(">>>2101234567890"),
		[]byte("---7898765432108"),
		[]byte("---7898765432109"),
		[]byte("---7898765432110"),
		[]byte("0"),
	}
	ln := len(samples)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeInt(samples[i%ln], text[:0])
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
		EncodeFloat(samples[i%ln], code[:0])
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
		samples[i] = make([]byte, 0)
		samples[i] = append(samples[i],
			EncodeFloat(
				[]byte(strconv.FormatFloat(f, 'e', -1, 64)), code[:0])...)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeFloat(samples[i%ln], code[:0])
	}
}

func BenchmarkEncodeSD(b *testing.B) {
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
		EncodeSD(samples[i%ln], code[:0])
	}
}

func BenchmarkDecodeSD(b *testing.B) {
	var samples = [][]byte{
		[]byte("-0004>"),
		[]byte("-000>"),
		[]byte("-9876>"),
		[]byte("-99876>"),
		[]byte("-9998766>"),
		[]byte("-999876>"),
		[]byte(">000123-"),
		[]byte(">0001233-"),
		[]byte(">00123-"),
		[]byte(">0123-"),
		[]byte(">999-"),
		[]byte(">9995-"),
	}
	ln := len(samples)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeSD(samples[i%ln], text[:0])
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
		EncodeLD(samples[i%ln], code[:0])
	}
}

func BenchmarkDecodeLD(b *testing.B) {
	var samples = [][]byte{
		[]byte("--68994>"),
		[]byte("--7894>"),
		[]byte("-6854>"),
		[]byte("-685>"),
		[]byte("-898>"),
		[]byte("-89>"),
		[]byte(">1-"),
		[]byte(">101-"),
		[]byte(">314-"),
		[]byte(">3145-"),
		[]byte(">>2105-"),
		[]byte(">>31005-"),
	}
	ln := len(samples)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeLD(samples[i%ln], text[:0])
	}
}

func BenchmarkSuffixEncode(b *testing.B) {
	bs := []byte("hello\x00wo\xffrld\x00")
	for i := 0; i < b.N; i++ {
		suffixEncodeString(bs, code[:0])
	}
}

func BenchmarkSuffixDecode(b *testing.B) {
	bs := []byte("hello\x00wo\xffrld\x00")
	code := suffixEncodeString(bs, code[:0])
	code = joinBytes(code, []byte{Terminator})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		suffixDecodeString(code, text[:])
	}
}

func bEncodeInt(b *testing.B, in []byte) {
	for i := 0; i < b.N; i++ {
		EncodeInt(in, code[:0])
	}
}

func bDecodeInt(b *testing.B, in []byte) {
	for i := 0; i < b.N; i++ {
		DecodeInt(in, text[:0])
	}
}

func bEncodeFloat(b *testing.B, in string) {
	f, _ := strconv.ParseFloat(in, 64)
	inb := []byte(strconv.FormatFloat(f, 'e', -1, 64))
	for i := 0; i < b.N; i++ {
		EncodeFloat(inb, code[:0])
	}
}

func bDecodeFloat(b *testing.B, in string) {
	inb := []byte(in)
	for i := 0; i < b.N; i++ {
		DecodeFloat(inb, text[:0])
	}
}
