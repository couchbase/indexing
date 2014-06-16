package common

import (
	"testing"
)

var doc = []byte(`{"name":"Fireman's Pail Ale","abv":0.5,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"pennichuck_brewi ng_company","updated":"2010-07-22 20:00:20","description":"","style":"American-Style Pale Ale","category":"North American Ale"}`)

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
	expr := `{"type":"property","path":"name"}`
	for i := 0; i < b.N; i++ {
		CompileN1QLExpression([]string{expr})
	}
}

func BenchmarkN1QLTransform(b *testing.B) {
	expr := `{"type":"property","path":"name"}`
	cExpr, _ := CompileN1QLExpression([]string{expr})
	for i := 0; i < b.N; i++ {
		N1QLTransform(doc, cExpr)
	}
}
