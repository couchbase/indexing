// parse.go is purely experimental stuff, wanted to see performance of JSON
// parsing using parsec.

package collatejson

import (
	"fmt"
	"github.com/prataprc/golib/parsec"
	"io/ioutil"
)

type PropertyNode struct {
	propname string
	parsec.ParsecNode
}

var EMPTY = parsec.Terminal{Name: "EMPTY", Value: ""}

func ParseFile(filename string) parsec.ParsecNode {
	if text, err := ioutil.ReadFile(filename); err != nil {
		panic(err.Error())
	} else {
		return Parse(text)
	}
}

func Parse(text []byte) parsec.ParsecNode {
	s := parsec.NewScanner(text)
	nt, _ := y(*s)
	return nt
}

// Construct parser-combinator for parsing JSON string.
func y(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		if ns == nil || len(ns) == 0 {
			return nil
		}
		return ns[0]
	}
	return parsec.Maybe(nodify, value)(s)
}

func array(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		if ns == nil || len(ns) == 0 {
			return nil
		}
		return ns[1]
	}
	return parsec.And(nodify, opensqr, values, closesqr)(s)
}

func object(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		if ns == nil || len(ns) == 0 {
			return nil
		}
		return ns[1]
	}
	return parsec.And(nodify, openparan, properties, closeparan)(s)
}

func properties(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		// Bubble sort properties based on property name.
		for i := 0; i < len(ns)-1; i++ {
			for j := 0; j < len(ns)-i-1; j++ {
				x := ns[j].(*PropertyNode).propname
				y := ns[j+1].(*PropertyNode).propname
				if x <= y {
					continue
				}
				ns[j+1], ns[j] = ns[j], ns[j+1]
			}
		}
		return &parsec.NonTerminal{Name: "PROPERTIES", Children: ns}
	}
	return parsec.Many(nodify, property, comma)(s)
}

func property(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		if ns == nil || len(ns) == 0 {
			return nil
		}
		propname := ns[0].(*parsec.Terminal).Value
		return &PropertyNode{propname, ns[2]}
	}
	return parsec.And(nodify, parsec.String(), colon, value)(s)
}

func values(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		return &parsec.NonTerminal{Name: "VALUES", Children: ns}
	}
	return parsec.Many(nodify, value, comma)(s)
}

func value(s parsec.Scanner) (parsec.ParsecNode, *parsec.Scanner) {
	nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
		if ns == nil || len(ns) == 0 {
			return nil
		}
		return ns[0]
	}
	return parsec.OrdChoice(
		nodify, tRue, fAlse, null,
		parsec.String(), parsec.Float(), parsec.Int(),
		array, object,
	)(s)
}

var tRue = parsec.Token(`^true`, "TRUE")
var fAlse = parsec.Token(`^false`, "FALSE")
var null = parsec.Token(`^null`, "NULL")

var comma = parsec.Token(`^,`, "COMMA")
var colon = parsec.Token(`^:`, "COLON")
var opensqr = parsec.Token(`^\[`, "OPENSQR")
var closesqr = parsec.Token(`^\]`, "CLOSESQR")
var openparan = parsec.Token(`^\{`, "OPENPARAN")
var closeparan = parsec.Token(`^\}`, "CLOSEPARAN")

// INode APIs for Terminal
func Repr(tok parsec.ParsecNode, prefix string) string {
	if term, ok := tok.(*parsec.Terminal); ok {
		return fmt.Sprintf(prefix) +
			fmt.Sprintf("%v : %v ", term.Name, term.Value)
	} else if propterm, ok := tok.(*PropertyNode); ok {
		return fmt.Sprintf(prefix) +
			fmt.Sprintf("property : %v \n", propterm.propname)
	} else {
		nonterm, _ := tok.(*parsec.NonTerminal)
		return fmt.Sprintf(prefix) +
			fmt.Sprintf("%v : %v \n", nonterm.Name, nonterm.Value)
	}
	panic("invalid parsecNode")
}

func Show(tok parsec.ParsecNode, prefix string) {
	if term, ok := tok.(*parsec.Terminal); ok {
		fmt.Println(Repr(term, prefix))
	} else if propterm, ok := tok.(*PropertyNode); ok {
		fmt.Printf("%v", Repr(propterm, prefix))
		Show(propterm.ParsecNode, prefix+"  ")
	} else if nonterm, ok := tok.(*parsec.NonTerminal); ok {
		fmt.Printf("%v", Repr(nonterm, prefix))
		for _, tok := range nonterm.Children {
			Show(tok, prefix+"  ")
		}
	}
}
