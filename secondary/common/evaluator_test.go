package common

import (
	"compress/bzip2"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// TODO:
//  1. add benchmark for different size of documents (small, medium, large)
//     and complex expressions.

var testdata = "../testdata"
var usersBzip2 = filepath.Join(testdata, "users.json.bz2")
var projectsBzip2 = filepath.Join(testdata, "projects.json.bz2")

var testJSON = make(map[string][][]byte)

var doc = []byte(`{"name":"Fireman's Pail Ale","abv":0.5,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"pennichuck_brewi ng_company","updated":"2010-07-22 20:00:20","description":"","style":"American-Style Pale Ale","category":"North American Ale"}`)

func init() {
	fnames := []string{usersBzip2, projectsBzip2}

	for _, fname := range fnames {
		var ds []interface{}

		if f, err := os.Open(fname); err != nil {
			panic(err)
		} else {
			r := bzip2.NewReader(f)
			if data, err := ioutil.ReadAll(r); err != nil {
				panic(err)
			} else if err = json.Unmarshal(data, &ds); err != nil {
				panic(err)
			}
			f.Close()
		}

		testJSON[fname] = make([][]byte, 0, len(ds))
		for _, d := range ds {
			if data, err := json.Marshal(&d); err != nil {
				panic(err)
			} else {
				testJSON[fname] = append(testJSON[fname], data)
			}
		}
	}
}

func TestN1QLTransform(t *testing.T) {
	exprs := []string{`{"type":"property","path":"name"}`, `{"type":"property","path":"abv"}`}
	cExpr, err := CompileN1QLExpression(exprs)
	if err != nil {
		t.Fatal(err)
	}
	secKey, err := N1QLTransform(doc, cExpr)
	if err != nil {
		t.Fatal(err)
	}
	if string(secKey) != `["Fireman's Pail Ale",0.5]` {
		t.Fatal("evaluation failed")
	}
}

func BenchmarkCompileN1QLExpression(b *testing.B) {
	expr := `{"type":"property","path":"age"}`
	for i := 0; i < b.N; i++ {
		CompileN1QLExpression([]string{expr})
	}
}

func BenchmarkN1QLTransform(b *testing.B) {
	expr := `{"type":"property","path":"city"}`
	cExpr, _ := CompileN1QLExpression([]string{expr})
	l := len(testJSON[usersBzip2])
	for i := 0; i < b.N; i++ {
		N1QLTransform(testJSON[usersBzip2][i%l], cExpr)
	}
}
