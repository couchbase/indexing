//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"time"
	//"reflect"
	"path"
	"runtime"

	qv "github.com/couchbaselabs/query/value"
	"github.com/prataprc/collatejson"
	"github.com/prataprc/monster"
)

var options struct {
	count   int
	n       int
	integer bool
	float   bool
	sd      bool
	ld      bool
	json    bool
	all     bool
}

func argParse() []string {
	flag.IntVar(&options.n, "m", 1, "number of validations")
	flag.IntVar(&options.count, "n", 1, "number elements to use for validation")
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
	for i := 0; i < options.n; i++ {
		fn()
	}
}

func validateInteger() {
	fmt.Println("Validating Integers")
	fmt.Println("-------------------")
	ints := generateInteger(options.count)
	bints := make([][]byte, 0, options.count)
	for _, x := range ints {
		y := collatejson.EncodeInt([]byte(strconv.Itoa(x)), make([]byte, 0, 64))
		bints = append(bints, y)
	}

	fmt.Printf("standard sort took %v for %v items\n",
		timeIt(func() { sort.IntSlice(ints).Sort() }), options.count)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(collatejson.ByteSlices(bints)) }), options.count)

	for i, x := range ints {
		_, y := collatejson.DecodeInt(bints[i], make([]byte, 0, 64))
		z, err := strconv.Atoi(string(y))
		if err != nil {
			log.Fatalf(err.Error())
		} else if x != z {
			log.Fatalf("failed validateInteger() on, %v == %v", x, y)
		}
	}
	fmt.Println()
}

func validateSD() {
	fmt.Println("Validating -1 < small-decimal < 1")
	fmt.Println("---------------------------------")
	sds := generateSD(options.count)
	bfloats := make([][]byte, 0, options.count)
	for _, sd := range sds {
		s := strconv.FormatFloat(sd, 'f', -1, 64)
		y := collatejson.EncodeSD([]byte(s), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
	}

	fmt.Printf("standard sort took %v for %v items\n",
		timeIt(func() { sort.Float64Slice(sds).Sort() }), options.count)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(collatejson.ByteSlices(bfloats)) }), options.count)

	for i, sd := range sds {
		y := collatejson.DecodeSD(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatalf(err.Error())
		} else if sd != z {
			log.Fatalf("failed validateSD() on, %v == %v", sd, y)
		}
	}
	fmt.Println()
}

func validateLD() {
	fmt.Println("Validating Large decimals")
	fmt.Println("-------------------------")
	floats := generateLD(options.count)
	bfloats := make([][]byte, 0, options.count)
	for _, x := range floats {
		fvalue := strconv.FormatFloat(x, 'f', -1, 64)
		y := collatejson.EncodeLD([]byte(fvalue), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
	}

	fmt.Printf("standard sort took %v for %v items\n",
		timeIt(func() { sort.Float64Slice(floats).Sort() }), options.count)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(collatejson.ByteSlices(bfloats)) }), options.count)

	for i, x := range floats {
		y := collatejson.DecodeLD(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatalf(err.Error())
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
	floats := generateFloats(options.count)
	bfloats := make([][]byte, 0, options.count)
	for _, x := range floats {
		fvalue := strconv.FormatFloat(x, 'e', -1, 64)
		y := collatejson.EncodeFloat([]byte(fvalue), make([]byte, 0, 64))
		bfloats = append(bfloats, y)
	}

	fmt.Printf("standard sort took %v for %v items\n",
		timeIt(func() { sort.Float64Slice(floats).Sort() }), options.count)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(collatejson.ByteSlices(bfloats)) }), options.count)

	for i, x := range floats {
		y := collatejson.DecodeFloat(bfloats[i], make([]byte, 0, 64))
		z, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			log.Fatalf(err.Error())
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
	jsons := generateJSON(prodfile, options.count)
	bjsons := make([][]byte, 0, len(jsons))
	for _, j := range jsons {
		b, err := codec.Encode([]byte(j), make([]byte, 0, len(j)*2))
		if err != nil {
			log.Fatalf(err.Error())
		}
		bjsons = append(bjsons, b)
	}

	raw := &jsonList{vals: jsons, compares: 0}
	ts := timeIt(func() { sort.Sort(raw) })
	fmt.Printf("standard sort took %v for %v items (%v compares)\n",
		ts, options.count, raw.compares)
	fmt.Printf("collatejson sort took %v for %v items\n",
		timeIt(func() { sort.Sort(collatejson.ByteSlices(bjsons)) }), options.count)

	var one, two map[string]interface{}

	for i, x := range jsons {
		y, err := codec.Decode(bjsons[i], make([]byte, 0, len(bjsons[i])*2))
		if err != nil {
			log.Fatalf(err.Error())
		}

		if err := json.Unmarshal([]byte(x), &one); err != nil {
			log.Fatalf(err.Error())
		}
		if err := json.Unmarshal(y, &two); err != nil {
			log.Fatalf(err.Error())
		}
		//if !reflect.DeepEqual(one, two) {
		//    log.Fatalf("json failed %q != %q", x, string(y))
		//}
	}
	fmt.Println()
}

func generateInteger(count int) []int {
	ints := make([]int, 0, count)
	for i := 0; i < count; i++ {
		x := rand.Int()
		if (x % 3) == 0 {
			ints = append(ints, -x)
		} else {
			ints = append(ints, x)
		}
	}
	return ints
}

func generateSD(count int) []float64 {
	ints := generateInteger(count)
	sds := make([]float64, 0, count)
	for _, x := range ints {
		sds = append(sds, 1/float64(x+1))
	}
	return sds
}

func generateLD(count int) []float64 {
	ints := generateInteger(count)
	lds := make([]float64, 0, count)
	for _, x := range ints {
		y := float64(x)/float64(rand.Int()+1) + 1
		if -1.0 < y && y < 1.0 {
			y += 2
		}
		lds = append(lds, y)
	}
	return lds
}

func generateFloats(count int) []float64 {
	ints := generateInteger(count)
	lds := make([]float64, 0, count)
	for _, x := range ints {
		lds = append(lds, float64(x)/float64(rand.Int()+1))
	}
	return lds
}

func generateJSON(prodfile string, count int) []string {
	seed := int(time.Now().UnixNano())
	bagdir := path.Dir(prodfile)
	jsons, err := monster.Generate(seed, count, bagdir, prodfile)
	if err != nil {
		log.Fatalf(err.Error())
	}
	return jsons
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
