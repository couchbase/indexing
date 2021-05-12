//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import "flag"
import "fmt"
import "log"
import "os"

//import "strings"
import "encoding/hex"

import "github.com/prataprc/collatejson"

var options struct {
	encode bool
	decode bool
	inp    string
}

var usageHelp = `Usage : codec [OPTIONS]
to encode specify -encode switch
to decode specify -decode switch and the hexdump as -inp
`

func argParse() {
	flag.StringVar(&options.inp, "inp", "", "input to encode")
	flag.BoolVar(&options.encode, "encode", false, "encode input")
	flag.BoolVar(&options.decode, "decode", false, "decode input")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usageHelp)
		flag.PrintDefaults()
	}
	flag.Parse()
}

func main() {
	var err error

	argParse()
	codec := collatejson.NewCodec(100)
	out := make([]byte, 0, len(options.inp)*3+collatejson.MinBufferSize)
	if options.encode {
		out, err = codec.Encode([]byte(options.inp), out)
		if err != nil {
			log.Fatal(err)
		}
		hexout := make([]byte, len(out)*5)
		n := hex.Encode(hexout, out)
		fmt.Printf("in : %q\n", options.inp)
		fmt.Printf("out: %q\n", string(out))
		fmt.Printf("hex: %q\n", string(hexout[:n]))

	} else if options.decode {
		inpbs := make([]byte, len(options.inp)*5)
		n, err := hex.Decode(inpbs, []byte(options.inp))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(n, inpbs[:n])
		out, err = codec.Decode([]byte(inpbs[:n]), out)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("in : %q\n", options.inp)
		fmt.Printf("out: %q\n", string(out))
	}
}
