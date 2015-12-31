package main

import "log"
import "flag"
import "io/ioutil"
import "fmt"
import "encoding/json"

import "github.com/couchbase/query/parser/n1ql"
import qexpr "github.com/couchbase/query/expression"
import qvalue "github.com/couchbase/query/value"

var options struct {
	expr string
	data string
	json bool
}

func argParse() {
	flag.StringVar(&options.expr, "expr", "",
		"input expression")
	flag.StringVar(&options.data, "data", "",
		"data similar to curl's --data")
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
		exprstr := qexpr.NewStringer().Visit(expr)
		fmt.Printf("input: %v\n", options.expr)
		fmt.Printf("expr: %T %v\n", expr, expr)
		fmt.Printf("output: %T %v\n", exprstr, exprstr)
	}

	if expr != nil && options.data != "" {
		if options.data[0] == '@' {
			docbytes, err := ioutil.ReadFile(options.data[1:])
			if err != nil {
				log.Fatal(err)
			}
			context := qexpr.NewIndexContext()
			doc := qvalue.NewAnnotatedValue(docbytes)
			v, err := expr.Evaluate(doc, context)
			fmsg := "Evaluate() scalar:%v err:%v"
			fmt.Printf(fmsg, v, err)
		}
	}
}
