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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbaselabs/dparval"
	tuqcollate "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/collatejson"
	"github.com/prataprc/golib"
	"github.com/prataprc/monster"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
)

var options struct {
	prodfile string
	count    int
}

type Codes struct {
	kind  string
	jsons []string
}

var codec *collatejson.Codec

func argParse() {
	flag.StringVar(&options.prodfile, "p", "json.prod",
		"production file to use")
	flag.IntVar(&options.count, "c", 100,
		"Number samples")
	flag.Parse()
}

func main() {
	argParse()

	codec = collatejson.NewCodec()
	fmt.Printf("Generating %v json documents ...\n", options.count)
	jsons := generateJsons(options.prodfile, options.count)
	checkCodec(jsons)
	fmt.Println("Done")
}

func checkCodec(jsons []string) {
	var one, two map[string]interface{}

	fmt.Println("Checking Encoding and Decoding ...")
	for _, j := range jsons {
		out := codec.Decode(codec.Encode([]byte(j)))
		json.Unmarshal([]byte(j), &one)
		json.Unmarshal(out, &two)
		if !reflect.DeepEqual(one, two) {
			panic("monster check fails, did you change the encoding format ?")
		}
	}
	fmt.Println()
}

func compareWithTuq(jsons []string, count int) {
	for i := int(0); i < count; i++ {
		fmt.Printf(".")
		tuqjsons := make([]string, len(jsons))
		copy(tuqjsons, jsons)
		binjsons := make([]string, len(jsons))
		copy(binjsons, jsons)

		tuqcodes := Codes{"tuq", tuqjsons}
		sort.Sort(tuqcodes)
		fd, _ := os.Create("a")
		fd.Write([]byte(strings.Join(tuqcodes.jsons, "\n")))
		fd.Close()

		bincodes := Codes{"binary", binjsons}
		sort.Sort(bincodes)
		fd, _ = os.Create("b")
		fd.Write([]byte(strings.Join(bincodes.jsons, "\n")))
		fd.Close()
	}
	fmt.Println()
}

func generateJsons(prodfile string, count int) (jsons []string) {
	c, conf := make(monster.Context), make(golib.Config)
	start := monster.Parse(prodfile, conf)
	nonterminals, root := monster.Build(start)
	c["_nonterminals"] = nonterminals
	for i := 1; i < count; i++ {
		c["_random"] = rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		monster.Initialize(c)
		text := root.Generate(c)
		jsons = append(jsons, text)
	}
	return
}

func (codes Codes) Len() int {
	return len(codes.jsons)
}

func (codes Codes) Less(i, j int) bool {
	key1, key2 := codes.jsons[i], codes.jsons[j]
	if codes.kind == "tuq" {
		value1 := dparval.NewValueFromBytes([]byte(key1)).Value()
		value2 := dparval.NewValueFromBytes([]byte(key2)).Value()
		return tuqcollate.CollateJSON(value1, value2) < 0
	} else if codes.kind == "binary" {
		value1 := codec.Encode([]byte(key1))
		value2 := codec.Encode([]byte(key2))
		return bytes.Compare(value1, value2) < 0
	} else {
		panic(fmt.Errorf("Unknown kind"))
	}
	return false
}

func (codes Codes) Swap(i, j int) {
	codes.jsons[i], codes.jsons[j] = codes.jsons[j], codes.jsons[i]
}
