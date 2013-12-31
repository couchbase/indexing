package main

import (
	"flag"
	"fmt"
	"github.com/prataprc/collatejson"
	"strconv"
)

var options struct {
	floatText string
	intText   string
}

func argParse() {
	//flag.BoolVar(&options.ast, "ast", false, "Show the ast of production")
	//flag.IntVar(&options.seed, "s", seed, "Seed value")
	//flag.IntVar(&options.count, "n", 1, "Generate n combinations")
	//flag.StringVar(&options.outfile, "o", "-", "Specify an output file")
	flag.StringVar(&options.floatText, "f", "", "encode floating point number")
	flag.StringVar(&options.intText, "i", "", "encode integer number")
	flag.Parse()
}

func main() {
	argParse()
	if options.floatText != "" {
		encodeFloat(options.floatText)
	} else if options.intText != "" {
		encodeInt(options.intText)
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
