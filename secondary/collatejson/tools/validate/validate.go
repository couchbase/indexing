//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
//go:build nolint

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"path"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	qv "github.com/couchbase/query/value"
	"github.com/prataprc/collatejson"

	parsec "github.com/prataprc/goparsec"
	"github.com/prataprc/monster"

	mcommon "github.com/prataprc/monster/common"
)

var options struct {
	count   int
	items   int
	integer bool
	float   bool
	sd      bool
	ld      bool
	json    bool
	all     bool
}

func argParse() []string {
	flag.IntVar(&options.count, "count", 1, "number of validations")
	flag.IntVar(&options.items, "items", 1, "number elements to use for validation")
	flag.BoolVar(&options.integer, "i", false, "validate integer codec")
	flag.BoolVar(&options.sd, "sd", false, "validate small-decimal codec")
	flag.BoolVar(&options.ld, "ld", false, "validate large-decimal codec")
	flag.BoolVar(&options.float, "f", false, "validate floating codec")
	flag.BoolVar(&options.json, "json", false, "validate json codec")
	flag.BoolVar(&options.all, "all", false, "validate json codec")
	flag.Parse()
	return flag.Args()
}

func main() {
	argParse()
	if options.integer || options.all {
		runValidations(validateInteger)
	}
	if options.sd || options.all {
		runValidations(validateSD)
	}
	if options.ld || options.all {
		runValidations(validateLD)
	}
	if options.float || options.all {
		runValidations(validateFloats)
	}
	if options.json || options.all {
		runValidations(validateJSON)
	}
}

func runValidations(fn func()) {
	for i := 0; i < options.count; i++ {
		fn()
	}
}

func validateInteger() {
	fmt.Println("Validating Integers")
	fmt.Println("-------------------")
	ints := generateInteger(options.items)
	bints := make([][]byte, 0, options.items)
	jints := make([]string, 0, options.items)
	for _, x := range ints {
		y := collatejson.EncodeInt([]byte(strconv.Itoa(x)), make([]byte, 0, 64))
		bints = append(bints, y)
		js, err := json.Marshal(x)
		if err != nil {
			log.Fatal(err)
		}
		jints = append(jints, string(js))
	}
	raw := &jsonList{vals: jints, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.items, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(common.ByteSlices(bints)) }), options.items)

	sort.IntSlice(ints).Sort()
	for i, x := range ints {
		_, y := collatejson.DecodeInt(bints[i], make([]byte, 0, 64))
		z, err := strconv.Atoi(string(y))
		if err != nil {
			log.Fatal(err)
		} else if x != z {
			log.Fatalf("failed validateInteger() on, %v == %q", x, string(y))
		}
	}
	fmt.Println()
}

func validateSD() {
	fmt.Println("Validating -1 < small-decimal < 1")
	fmt.Println("---------------------------------")
	sds := generateSD(options.items)
	bfloats := make([][]byte, 0, options.items)
	jfloats := make([]string, 0, options.items)
	for _, sd := range sds {
		s := strconv.FormatFloat(sd, 'f', -1, 64)
		y := collatejson.EncodeSD([]byte(s), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
		js, err := json.Marshal(sd)
		if err != nil {
			log.Fatal(err)
		}
		jfloats = append(jfloats, string(js))
	}

	raw := &jsonList{vals: jfloats, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.items, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(common.ByteSlices(bfloats)) }), options.items)

	sort.Float64Slice(sds).Sort()
	for i, sd := range sds {
		y := collatejson.DecodeSD(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatal(err)
		} else if sd != z {
			log.Fatalf("failed validateSD() on, %v == %v", sd, y)
		}
	}
	fmt.Println()
}

func validateLD() {
	fmt.Println("Validating Large decimals")
	fmt.Println("-------------------------")
	floats := generateLD(options.items)
	bfloats := make([][]byte, 0, options.items)
	jfloats := make([]string, 0, options.items)
	for _, x := range floats {
		fvalue := strconv.FormatFloat(x, 'f', -1, 64)
		y := collatejson.EncodeLD([]byte(fvalue), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
		js, err := json.Marshal(x)
		if err != nil {
			log.Fatal(err)
		}
		jfloats = append(jfloats, string(js))
	}

	raw := &jsonList{vals: jfloats, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.items, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(common.ByteSlices(bfloats)) }), options.items)

	sort.Float64Slice(floats).Sort()
	for i, x := range floats {
		y := collatejson.DecodeLD(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatal(err)
		} else if x != z {
			log.Println(string(bfloats[i]), string(y))
			log.Fatalf("failed validateLD() on, %v == %v", x, z)
		}
	}
	fmt.Println()
}

func validateFloats() {
	fmt.Println("Validating Floats")
	fmt.Println("-----------------")
	floats := generateFloats(options.items)
	bfloats := make([][]byte, 0, options.items)
	jfloats := make([]string, 0, options.items)
	for _, x := range floats {
		fvalue := strconv.FormatFloat(x, 'e', -1, 64)
		y := collatejson.EncodeFloat([]byte(fvalue), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
		js, err := json.Marshal(x)
		if err != nil {
			log.Fatal(err)
		}
		jfloats = append(jfloats, string(js))
	}

	raw := &jsonList{vals: jfloats, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.items, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(common.ByteSlices(bfloats)) }), options.items)

	sort.Float64Slice(floats).Sort()
	for i, x := range floats {
		y := collatejson.DecodeFloat(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatal(err)
		} else if x != z {
			log.Fatalf("failed validateFloats() on, %v == %v", x, y)
		}
	}
	fmt.Println()
}

func validateJSON() {
	fmt.Println("Validating JSONs")
	fmt.Println("----------------")

	_, filename, _, _ := runtime.Caller(0)
	prodfile := path.Join(path.Dir(filename), "json.prod")

	codec := collatejson.NewCodec(32)
	jsons := generateJSON(prodfile, options.items)
	bjsons := make([][]byte, 0, len(jsons))
	for _, j := range jsons {
		outbuf := make([]byte, 0, len(j)*3+collatejson.MinBufferSize)
		b, err := codec.Encode([]byte(j), outbuf)
		if err != nil {
			log.Fatal(err)
		}
		bjsons = append(bjsons, b)
	}

	raw := &jsonList{vals: jsons, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.items, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(common.ByteSlices(bjsons)) }), options.items)

	var one, two interface{}

	for i, x := range jsons {
		outbuf := make([]byte, 0, len(bjsons[i])*3+collatejson.MinBufferSize)
		y, err := codec.Decode(bjsons[i], outbuf)
		if err != nil {
			log.Fatal(err)
		}

		if err := json.Unmarshal([]byte(x), &one); err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(y, &two); err != nil {
			log.Fatal(err)
		}
		//if !reflect.DeepEqual(one, two) {
		//    log.Fatalf("json failed %q != %q", x, string(y))
		//}
	}
	fmt.Println()
}

func generateInteger(items int) []int {
	ints := make([]int, 0, items)
	for i := 0; i < items; i++ {
		x := rand.Int() % 1000000000
		if (x % 3) == 0 {
			ints = append(ints, -x)
		} else {
			ints = append(ints, x)
		}
	}
	return ints
}

func generateSD(items int) []float64 {
	ints := generateInteger(items)
	sds := make([]float64, 0, items)
	for _, x := range ints {
		sds = append(sds, 1/float64(x+1))
	}
	return sds
}

func generateLD(items int) []float64 {
	ints := generateInteger(items)
	lds := make([]float64, 0, items)
	for _, x := range ints {
		y := float64(x)/float64(rand.Int()+1) + 1
		if -1.0 < y && y < 1.0 {
			y += 2
		}
		lds = append(lds, y)
	}
	return lds
}

func generateFloats(items int) []float64 {
	ints := generateInteger(items)
	lds := make([]float64, 0, items)
	for _, x := range ints {
		lds = append(lds, float64(x)/float64(rand.Int()+1))
	}
	return lds
}

func generateJSON(prodfile string, items int) []string {
	bagdir := path.Dir(prodfile)
	text, err := ioutil.ReadFile(prodfile)
	if err != nil {
		log.Fatal(err)
	}
	seed := uint64(10)
	root := compile(parsec.NewScanner(text)).(mcommon.Scope)
	scope := monster.BuildContext(root, seed, bagdir, prodfile)
	nterms := scope["_nonterminals"].(mcommon.NTForms)

	// compile monster production file.
	jsons := make([]string, items)
	for i := 0; i < items; i++ {
		scope = scope.RebuildContext()
		jsons[i] = evaluate("root", scope, nterms["s"]).(string)
	}
	return jsons
}

func compile(s parsec.Scanner) parsec.ParsecNode {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v at %v", r, s.GetCursor())
		}
	}()
	root, _ := monster.Y(s)
	return root
}

func evaluate(name string, scope mcommon.Scope, forms []*mcommon.Form) interface{} {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v", r)
		}
	}()
	return monster.EvalForms(name, scope, forms)
}

func timeIt(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

type jsonList struct {
	compares int
	vals     []string
}

func (jsons *jsonList) Len() int {
	return len(jsons.vals)
}

func (jsons *jsonList) Less(i, j int) bool {
	key1, key2 := jsons.vals[i], jsons.vals[j]
	value1 := qv.NewValue([]byte(key1))
	value2 := qv.NewValue([]byte(key2))
	jsons.compares++
	return value1.Collate(value2) < 0
}

func (jsons *jsonList) Swap(i, j int) {
	jsons.vals[i], jsons.vals[j] = jsons.vals[j], jsons.vals[i]
}
