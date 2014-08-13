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
	"flag"
	"fmt"
	"log"

	"github.com/prataprc/collatejson"
)

var options struct {
	in string
}

func argParse() {
	flag.StringVar(&options.in, "in", "", "input to encode")
	flag.Parse()
}

func main() {
	argParse()
	codec := collatejson.NewCodec(len(options.in) * 2)
	out := make([]byte, 0, len(options.in)*3)
	out, err := codec.Encode([]byte(options.in), out)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("in : %q\n", options.in)
	fmt.Printf("out: %q\n", string(out))
}
