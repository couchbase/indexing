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
	collatejson "../.."
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

var options struct {
	count   int
	n       int
	integer bool
	float   bool
	sd      bool
	ld      bool
	str     bool
}

func argParse() []string {
	flag.IntVar(&options.n, "m", 1, "number of validations")
	flag.IntVar(&options.count, "n", 1, "number elements to use for validation")
	flag.BoolVar(&options.integer, "i", false, "validate integer codec")
	flag.BoolVar(&options.sd, "sd", false, "validate small-decimal codec")
	flag.BoolVar(&options.ld, "ld", false, "validate large-decimal codec")
	flag.BoolVar(&options.float, "f", false, "validate floating codec")
	flag.BoolVar(&options.str, "str", false, "validate string codec")
	flag.Parse()
	return flag.Args()
}

func main() {
	argParse()
	if options.integer {
		runValidations(validateInteger)
	}
	if options.sd {
	}
	if options.ld {
	}
	if options.float {
		runValidations(validateFloats)
	}
	if options.str {
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

func generateFloats(count int) []float64 {
	ints := generateInteger(count)
	floats := make([]float64, 0, count)
	for _, x := range ints {
		floats = append(floats, float64(x))
	}
	return floats
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

func timeIt(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}
