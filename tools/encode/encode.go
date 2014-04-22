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
	"code.google.com/p/go.text/collate"
	"code.google.com/p/go.text/language"
	"code.google.com/p/go.text/unicode/norm"
	"flag"
	"fmt"
	"github.com/couchbaselabs/indexing/collatejson"
	"strconv"
	"unicode/utf8"
)

var options struct {
	floatText  string
	intText    string
	stringText string
}

func argParse() {
	//flag.BoolVar(&options.ast, "ast", false, "Show the ast of production")
	//flag.IntVar(&options.seed, "s", seed, "Seed value")
	//flag.IntVar(&options.count, "n", 1, "Generate n combinations")
	//flag.StringVar(&options.outfile, "o", "-", "Specify an output file")
	flag.StringVar(&options.floatText, "f", "", "encode floating point number")
	flag.StringVar(&options.intText, "i", "", "encode integer number")
	flag.StringVar(&options.stringText, "s", "", "encode string")
	flag.Parse()
}

func main() {
	argParse()
	if options.floatText != "" {
		encodeFloat(options.floatText)
	} else if options.intText != "" {
		encodeInt(options.intText)
	} else {
		fmt.Printf("composed:(%v) %v\n", len(options.stringText), []byte(options.stringText))
		b := norm.NFKD.Bytes([]byte(options.stringText))
		fmt.Printf("decomposed:(%v) %v\n", len(b), b)
		cl := collate.New(language.De)
		buf := &collate.Buffer{}
		rawkey := cl.Key(buf, b)
		fmt.Printf("rawkey:(%v) %v\n", len(rawkey), rawkey)

		s, i := string(b), 0
		for {
			_, c := utf8.DecodeRune([]byte(s[i:]))
			i += c
			if len(s[i:]) == 0 {
				break
			}
		}
	}
}

func encodeFloat(text string) {
	if f, err := strconv.ParseFloat(text, 64); err != nil {
		panic(err)
	} else {
		ftext := []byte(strconv.FormatFloat(f, 'e', -1, 64))
		fmt.Printf("Encoding %v: %v\n", f, string(collatejson.EncodeFloat(ftext)))
	}
}

func encodeInt(text string) {
	fmt.Println(string(collatejson.EncodeInt([]byte(text))))
}
