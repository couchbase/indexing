//  Copyright (c) 2013 Couchbase, Inc.

package main

import "flag"
import "fmt"
import "log"

import "github.com/prataprc/collatejson"

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
	fmt.Printf("%q \n", options.in)
	out := make([]byte, 0, len(options.in)*3)
	out, err := codec.Encode([]byte(options.in), out)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("in : %q\n", options.in)
	fmt.Printf("out: %q\n", string(out))
}
