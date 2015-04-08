package main

import "log"
import "flag"
import "fmt"
import "encoding/json"

import "github.com/couchbase/query/parser/n1ql"
import "github.com/couchbase/query/expression"

var options struct {
	expr string
	json bool
}

func argParse() {
	flag.StringVar(&options.expr, "expr", "",
		"input expression")
	flag.BoolVar(&options.json, "json", false,
		"marshal parsed expression back to JSON")
	flag.Parse()
}

func main() {
	argParse()
	expr, err := n1ql.ParseExpression(options.expr)
	if err != nil {
		log.Fatal(err)
	}
	if options.json {
		exprb, err := json.Marshal(expr)
		if err != nil {
			log.Fatal(err)
		}
		exprstr := string(exprb)
		fmt.Printf("input: %v\n", options.expr)
		fmt.Printf("expr: %T %v\n", expr, expr)
		fmt.Printf("json: %T %v\n", exprstr, exprstr)
	} else {
		exprstr := expression.NewStringer().Visit(expr)
		fmt.Printf("input: %v\n", options.expr)
		fmt.Printf("expr: %T %v\n", expr, expr)
		fmt.Printf("output: %T %v\n", exprstr, exprstr)
	}
}
