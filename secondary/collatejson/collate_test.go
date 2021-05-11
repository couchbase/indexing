//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
import "github.com/couchbase/indexing/secondary/collatejson/util"
import "bufio"
import "os"

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
	//filenames = []string{"strings", "strings.ref"}
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

		sort.Sort(util.ByteSlices(blines))

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

func TestN1QLDecode(t *testing.T) {
	codec := NewCodec(16)
	var object interface{}
	bsArr := [][]byte{[]byte(`["hello", "test", true, [1,2,3], 1, 23.3, null, {"key" : 100}]`),
		[]byte(`["hello", true, [1,2,3], 23.3, null, {"key" : ["a","b","c"], "key2" : [1,null, true, 3], "key3" : { "subdoc" : true, "subdoc1" : [true, false, "y"]}}]`),
		[]byte(`[{"key" : ["a","b","c"], "key2" : [1,null, true, 3], "key3" : { "subdoc" : true, "subdoc1" : [true, false, "y"]}}]`),
		[]byte(`[4111686018427387900, 8223372036854775808, 822337203685477618]`),
	}

	for _, bs := range bsArr {
		err := json.Unmarshal(bs, &object)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
		val := n1ql.NewValue(object)

		n1qlBytes, err1 := codec.EncodeN1QLValue(val, make([]byte, 0, 1024))
		n1qlVal, err2 := codec.DecodeN1QLValue(n1qlBytes, make([]byte, 0, 1024))

		if err1 != nil || err2 != nil {
			t.Fatalf("Unexpected errors %v, %v", err1, err2)
		}

		if !val.EquivalentTo(n1qlVal) {
			t.Errorf("Expected original and decoded n1ql values to be the same")
		}
	}
}

func TestN1QLDecode2(t *testing.T) {
	codec := NewCodec(32)
	codec.NumberType("decimal")
	var object interface{}
	for i, testFile := range testFiles {
		lines := readLines(testFile, t)
		blines := make([][]byte, 0, len(lines))
		for _, line := range lines {
			json.Unmarshal(line, &object)
			code, err := codec.EncodeN1QLValue(n1ql.NewValue(object), make([]byte, 0, 1024))
			if err != nil {
				t.Error(err)
			}
			blines = append(blines, code)
		}

		sort.Sort(util.ByteSlices(blines))

		lines = lines[:0]
		for _, line := range blines {
			//fmt.Println(string(line))
			n1qlval, err := codec.DecodeN1QLValue(line, make([]byte, 0, 1024))
			if err != nil {
				t.Error(err)
			}
			text, err := n1qlval.MarshalJSON()
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

func TestArrayExplodeJoin2(t *testing.T) {
	codec := NewCodec(16)
	e1, e2 := n1ql.NewValue("string"), n1ql.NewValue([]interface{}{1, 2, 3})
	arrayBS1, _ := codec.EncodeN1QLValue(n1ql.NewValue([]interface{}{e1, e2}), make([]byte, 0, 1000))

	elemBS1, _ := codec.EncodeN1QLValue(e1, make([]byte, 0, 1000))
	elemBS2, _ := codec.EncodeN1QLValue(e2, make([]byte, 0, 1000))

	array, _, err1 := codec.ExplodeArray3(arrayBS1, make([]byte, 0, 1000), make([][]byte, 2), nil, []bool{true, true}, nil, 1)
	arrayBS2, err2 := codec.JoinArray(array, make([]byte, 0, 1000))

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

func TestMB28956(t *testing.T) {
	codec := NewCodec(16)

	var valArr []int64 = []int64{360, 1234, 8223372036854775808, 1, 100000000, 360000000,
		-360, -1234, -8223372036854775808, -1, -100000000, -360000000}

	var number Integer
	var cs []byte

	for _, val := range valArr {
		code = make([]byte, 0, 1000)
		code = append(code, TypeNumber)

		//incorrect format
		intStr, err := number.ConvertToScientificNotation_TestOnly(val)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
		cs = EncodeFloat([]byte(intStr), code[1:])
		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)

		//fixed format
		fixed, err := codec.FixEncodedInt([]byte(code), make([]byte, 0, 1000))
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}

		//correct format
		code = code[:1]
		intStr1, err := number.ConvertToScientificNotation(val)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}

		cs = EncodeFloat([]byte(intStr1), code[1:])

		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)

		if !bytes.Equal(code, fixed) {
			t.Errorf("Expected fixed and encoded values to be the same")
		}

	}

}

func TestFixEncodedInt(t *testing.T) {
	codec := NewCodec(16)
	var object interface{}
	bsArr := [][]byte{[]byte(`1.1`),
		[]byte(`["hello", "test", true, [1,2,3], 1, 23.3, null, {"key" : 100}]`),
		[]byte(`["hello", true, [1,2,3], -23.3, null, {"key" : ["apple","blue","cat"], "key2" : [1,null, "test", true, -0.099836], "key3" : { "subdoc" : true, "subdoc1" : [true, false, "yellow"]}}]`),
		[]byte(`[{"key" : ["abcde","bdab","cat"], "key2" : [-1.2500,null, true, 310, "test", 0.00034], "key3" : { "subdoc" : true, "subdoc1" : [true, false, "yellow", 342.60, 36000000]}}]`),
		[]byte(`[4111686018427387900, -8223372036854775808, 822337203685477618, 123456789123456700000, 12345678912345678912345]`),
	}

	for _, bs := range bsArr {
		err := json.Unmarshal(bs, &object)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
		val := n1ql.NewValue(object)

		n1qlBytes, err1 := codec.EncodeN1QLValue(val, make([]byte, 0, 10000))
		fixedBytes, err2 := codec.FixEncodedInt(n1qlBytes, make([]byte, 0, 10000))

		if err1 != nil || err2 != nil {
			t.Fatalf("Unexpected errors %v, %v", err1, err2)
		}

		if !bytes.Equal(fixedBytes, n1qlBytes) {
			t.Errorf("Expected json and n1ql encoded values to be the same")
			t.Logf("n1qlBytes %v \t %v \n", n1qlBytes, string(n1qlBytes))
			t.Logf("fixedByte %v \t %v \n", fixedBytes, string(fixedBytes))
		}
	}
}

func TestN1QLDecodeLargeInt64(t *testing.T) {
	codec := NewCodec(16)
	var object interface{}
	bsArr := [][]byte{[]byte(`10068046444225730000`)}

	for _, bs := range bsArr {
		err := json.Unmarshal(bs, &object)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
		val := n1ql.NewValue(object)

		n1qlBytes, err1 := codec.EncodeN1QLValue(val, make([]byte, 0, 1024))
		n1qlVal, err2 := codec.DecodeN1QLValue(n1qlBytes, make([]byte, 0, 1024))

		if err1 != nil || err2 != nil {
			t.Fatalf("Unexpected errors %v, %v", err1, err2)
		}

		if !val.EquivalentTo(n1qlVal) {
			t.Errorf("Expected original and decoded n1ql values to be the same")
		}
	}
}

func TestMixedModeFixEncodedInt(t *testing.T) {
	codec := NewCodec(16)
	var object interface{}

	bsArr := [][]byte{[]byte(`[4111686018427387900, -8223372036854775808, 822337203685477618]`)}

	file, err := os.Open("../tests/testdata/numbertests")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		num := scanner.Text()
		if len(num) == 0 || num[0] == '#' {
			continue
		}
		bsArr = append(bsArr, []byte(fmt.Sprintf("[%v]", num)))
	}

	for _, bs := range bsArr {
		fmt.Printf("TESTING %s \n", bs)

		err := json.Unmarshal(bs, &object)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
		val := n1ql.NewValue(object)

		oldn1qlBytes, err := codec.encodeN1QL_50(val, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error from encodeN1QL_50 %v", err)
		}

		fixedBytes, err := codec.FixEncodedInt(oldn1qlBytes, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error from FixEncodedInt %v", err)
		}

		val1, err := codec.DecodeN1QLValue(fixedBytes, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error from first DecodeN1QLValue %v", err)
		}

		n1qlBytes, err := codec.EncodeN1QLValue(val, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error from EncodeN1QLValue %v", err)
		}

		val2, err := codec.DecodeN1QLValue(fixedBytes, make([]byte, 0, 10000))
		if err != nil {
			t.Fatalf("Unexpected error from second DecodeN1QLValue %v", err)
		}

		if !bytes.Equal(fixedBytes, n1qlBytes) {
			t.Errorf("Expected fixedEncodedBytes and N1QLEncoded values to be the same")
			t.Logf("fixedBytes %v \t %v \n", fixedBytes, string(fixedBytes))
			t.Logf("n1qlBytes %v \t %v \n", n1qlBytes, string(n1qlBytes))
		}

		if !val.EquivalentTo(val1) {
			t.Errorf("Expected original and fixEncodedInt decoded n1ql values to be the same")
		}

		if !val.EquivalentTo(val2) {
			t.Errorf("Expected original and decoded n1ql values to be the same")
		}

		fmt.Printf("PASS \n")
	}

}

// This is for unit test only to test
// for projector < 5.1.1 and indexer >= 5.1.1
func (codec *Codec) encodeN1QL_50(val n1ql.Value, code []byte) ([]byte, error) {
	var cs []byte
	var err error

	switch val.Type() {
	case n1ql.NUMBER:
		act := val.ActualForIndex()
		code = append(code, TypeNumber)
		var cs []byte
		switch act.(type) {
		case float64:
			cs, err = codec.normalizeFloat(act.(float64), code[1:])
		case int64:
			var intStr string
			var number Integer
			intStr, err = number.ConvertToScientificNotation_TestOnly(act.(int64))
			cs = EncodeFloat([]byte(intStr), code[1:])
		}
		if err == nil {
			code = code[:len(code)+len(cs)]
			code = append(code, Terminator)
		}
	case n1ql.ARRAY:
		act := val.ActualForIndex().([]interface{})
		code = append(code, TypeArray)
		if codec.arrayLenPrefix {
			arrlen := Length(len(act))
			if cs, err = codec.json2code(arrlen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}
		if err == nil {
			for _, val := range act {
				l := len(code)
				cs, err = codec.encodeN1QL_50(n1ql.NewValue(val), code[l:])
				if err == nil {
					code = code[:l+len(cs)]
					continue
				}
				break
			}
			code = append(code, Terminator)
		}
	}
	return code, err
}
