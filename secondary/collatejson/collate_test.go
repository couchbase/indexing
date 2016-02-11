//  Copyright (c) 2013 Couchbase, Inc.

package collatejson

import "bytes"
import "encoding/json"
import "fmt"
import "io/ioutil"
import "log"
import "path/filepath"
import "reflect"
import "sort"
import "strings"
import "testing"
import n1ql "github.com/couchbase/query/value"
import "github.com/couchbase/indexing/secondary/common"

var testcases = []struct {
	text string
	ref  []byte
}{
	{`{ "inelegant":27.53096820876087, "horridness":true,
        "iridodesis":[79.1253026404128,null], "arrogantness":null,
        "unagrarian":false }`,
		[]byte(`\t\a>5\x00` +
			`\x06arrogantness\x00\x00` +
			`\x02\x00` +
			`\x06horridness\x00\x00` +
			`\x04\x00` +
			`\x06inelegant\x00\x00` +
			`\x05>>22753096820876087-\x00` +
			`\x06iridodesis\x00\x00` +
			`\b\a>2\x00\x05>>2791253026404128-\x00\x02\x00\x00` +
			`\x06unagrarian\x00\x00` +
			`\x03\x00` +
			`\x00`),
	},
	{`{"mettled":{"ravening":null,"unguiltily":40.959598968246475},"wiriness":false}`,
		[]byte(`\t\a>2\x00` +
			`\x06mettled\x00\x00` +
			`\t\a>2\x00` +
			`\x06ravening\x00\x00` +
			`\x02\x00` +
			`\x06unguiltily\x00\x00` +
			`\x05>>240959598968246475-\x00` +
			`\x00` +
			`\x06wiriness\x00\x00` +
			`\x03\x00` +
			`\x00`),
	},
}

var testData = "./testdata"
var testFiles, refFiles []string

func init() {
	testFiles = make([]string, 0, 16)
	refFiles = make([]string, 0, 16)

	fs, err := ioutil.ReadDir(testData)
	if err != nil {
		log.Fatal(err)
	}

	var filenames []string

	for _, f := range fs {
		filenames = append(filenames, f.Name())
	}

	sort.Strings(filenames)
	filenames = []string{"strings", "strings.ref"}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, "ref") {
			refFiles = append(refFiles, filepath.Join(testData, filename))
		} else {
			testFiles = append(testFiles, filepath.Join(testData, filename))
		}
	}
}

func TestCodecLength(t *testing.T) {
	var samples = [][2]string{
		{"[]", `\b\a0\x00\x00`},
		{`[null,true,10,10.2,[],{"key":{}}]`,
			`\b\a>6\x00\x02\x00\x04\x00\x05>>21-\x00\x05>>2102-` +
				`\x00\b\a0\x00\x00\t\a>1\x00\x06key\x00\x00\t\a0\x00\x00\x00\x00`},
	}
	codec := NewCodec(128)
	codec.NumberType("decimal")
	codec.SortbyArrayLen(true)
	for _, tcase := range samples {
		sample, ref := tcase[0], tcase[1]

		code, err := codec.Encode([]byte(sample), code[:0])
		if err != nil {
			t.Error("encode failed", err)
		}
		out := fmt.Sprintf("%q", code)
		out = out[1 : len(out)-1]
		if out != ref {
			t.Errorf("encode failed for: %q %q %q", sample, out, ref)
		}

		text, err = codec.Decode(code, text[:0])
		if err != nil {
			t.Error("encode failed", err)
		}
		if string(text) != sample {
			t.Errorf("decode failed for: %q %q", sample, text)
		}
	}
}

func TestSpecialString(t *testing.T) {
	var err error

	jsoncodec := NewCodec(16)
	data := []byte(`"\n"`)
	code := make([]byte, 0, 1024)
	code, err = jsoncodec.Encode(data, code)
	if err != nil {
		t.Fatal(err)
	}
	text := make([]byte, 0, 1024)
	if text, err = jsoncodec.Decode(code, text); err != nil {
		t.Fatal(err)
	}
}

func TestCodecNoLength(t *testing.T) {
	var samples = [][2]string{
		{"[]", `\b\x00`},
		{`[null,true,10,10.2,[],{"key":{}}]`,
			`\b\x02\x00\x04\x00\x05>>21-\x00\x05>>2102-\x00` +
				`\b\x00\t\x06key\x00\x00\t\x00\x00\x00`},
	}
	codec := NewCodec(128)
	codec.NumberType("decimal")
	codec.SortbyArrayLen(false)
	codec.SortbyPropertyLen(false)
	for _, tcase := range samples {
		sample, ref := tcase[0], tcase[1]

		code, err := codec.Encode([]byte(sample), code[:0])
		if err != nil {
			t.Error("encode failed", err)
		}
		out := fmt.Sprintf("%q", code)
		out = out[1 : len(out)-1]
		if out != ref {
			t.Errorf("encode failed for: %q %q %q", sample, out, ref)
		}

		text, err = codec.Decode(code, text[:0])
		if err != nil {
			t.Error("encode failed", err)
		}
		if string(text) != sample {
			t.Errorf("decode failed for: %q %q", sample, text)
		}
	}
}

func TestCodecJSON(t *testing.T) {
	codec := NewCodec(128)
	codec.SortbyArrayLen(true)
	for _, tcase := range testcases {
		var one, two map[string]interface{}

		code, err := codec.Encode([]byte(tcase.text), code[:0])
		if err != nil {
			t.Error(err)
		}

		out := fmt.Sprintf("%q", code)
		out = out[1 : len(out)-1]
		if out != string(tcase.ref) {
			t.Error("Encode fails, did you change the encoding format ?")
			t.Logf("ref: %q", string(tcase.ref))
			t.Logf("out: %q", out)
		}

		text, err = codec.Decode(code, text[:0])
		if err != nil {
			t.Error(err)
		}

		if err := json.Unmarshal([]byte(tcase.text), &one); err != nil {
			t.Error("Unmarshaling reference")
		}
		if err := json.Unmarshal(text, &two); err != nil {
			t.Error("Unmarshaling decoded text")
		}
		if !reflect.DeepEqual(one, two) {
			t.Error("Decode fails, did you change the encoding format ?")
			t.Log(one)
			t.Log(two)
		}
	}
}

func TestReference(t *testing.T) {
	codec := NewCodec(32)
	codec.NumberType("decimal")
	for i, testFile := range testFiles {
		lines := readLines(testFile, t)
		blines := make([][]byte, 0, len(lines))
		for _, line := range lines {
			code, err := codec.Encode(line, make([]byte, 0, 1024))
			if err != nil {
				t.Error(err)
			}
			blines = append(blines, code)
		}

		sort.Sort(common.ByteSlices(blines))

		lines = lines[:0]
		for _, line := range blines {
			//fmt.Println(string(line))
			text, err := codec.Decode(line, make([]byte, 0, 1024))
			if err != nil {
				t.Error(err)
			}
			lines = append(lines, text)
		}

		refLines := readLines(refFiles[i], t)
		for j, line := range lines {
			x, y := string(line), string(refLines[j])
			if x != y {
				t.Errorf("Mismatch in %v for %q != %q", testFile, x, y)
			}
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	codec := NewCodec(128)
	codec.NumberType("decimal")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Encode([]byte(testcases[0].text), code[:0])
	}
}

func BenchmarkCompare(b *testing.B) {
	codec := NewCodec(128)
	code, _ := codec.Encode([]byte(testcases[0].text), code[:0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes.Compare(code, code)
	}
}

func BenchmarkDecode(b *testing.B) {
	codec := NewCodec(128)
	codec.NumberType("decimal")
	code, _ := codec.Encode([]byte(testcases[0].text), code[:0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Decode(code, text[:0])
	}
}

func readLines(filename string, t *testing.T) [][]byte {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Error(err)
	}

	var lines [][]byte

	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func TestN1QLEncode(t *testing.T) {
	codec := NewCodec(16)
	var object interface{}
	bs := []byte(`["hello", "test", true, [1,2,3], 1, 23.3, null, {"key" : 100}]`)
	json.Unmarshal(bs, &object)
	val := n1ql.NewValue(object)

	jsonBytes, err1 := codec.Encode(bs, make([]byte, 0, 10000))
	n1qlBytes, err2 := codec.EncodeN1QLValue(val, make([]byte, 0, 10000))

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors %v, %v", err1, err2)
	}

	if !bytes.Equal(jsonBytes, n1qlBytes) {
		t.Errorf("Expected json and n1ql encoded values to be the same")
	}
}

func TestArrayExplodeJoin(t *testing.T) {
	codec := NewCodec(16)
	e1, e2 := n1ql.NewValue("string"), n1ql.NewValue([]interface{}{1, 2, 3})
	arrayBS1, _ := codec.EncodeN1QLValue(n1ql.NewValue([]interface{}{e1, e2}), make([]byte, 0, 1000))

	elemBS1, _ := codec.EncodeN1QLValue(e1, make([]byte, 0, 10000))
	elemBS2, _ := codec.EncodeN1QLValue(e2, make([]byte, 0, 10000))

	array, err1 := codec.ExplodeArray(arrayBS1, make([]byte, 0, 10000))
	arrayBS2, err2 := codec.JoinArray(array, make([]byte, 0, 10000))

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected error %v %v", err1, err2)
	}

	if !bytes.Equal(arrayBS1, arrayBS2) {
		t.Errorf("Unexpected mismatch")
	}

	if !bytes.Equal(array[0], elemBS1) {
		t.Errorf("Unexpected mismatch")
	}

	if !bytes.Equal(array[1], elemBS2) {
		t.Errorf("Unexpected mismatch")
	}
}
