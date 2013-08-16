package collatejson
import "fmt"
import "os"
import "bytes"
import "strconv"
import "github.com/prataprc/golib/parsec"

type Terminal struct {
    Name string         // typically contains terminal's token type
    Value string        // value of the terminal
    Tok parsec.Token    // Actual token obtained from the scanner
}
type NonTerminal struct {
    Name string         // typically contains terminal's token type
    Value string        // value of the terminal
    Children []INode
}
type BufferNode struct {
    NonTerminal
    buffer *bytes.Buffer
}
type PropertyNode struct {
    NonTerminal
    propname string
    buffer *bytes.Buffer
}

type INode interface{   // AST functions
    Show(string)
    Repr(prefix string) string
    Bytes() []byte
}

var EMPTY = Terminal{Name: "EMPTY", Value:""}

var _ = fmt.Sprintln("Dummy statement to use fmt")

func ParseFile(filename string) []byte {
    var text []byte
    fd, _ := os.Open(filename)
    fd.Read( text )
    node := Parse(text)
    return node.Bytes()
}

func JSONKey(bin []byte) string {
    buf := bytes.NewBuffer( bin )
    return readValue( buf )
}

func Parse(text []byte) *BufferNode {
    s := parsec.NewGoScan(text)
    return y()(s).(*BufferNode)
}

// Construct parser-combinator for parsing JSON string.
func y() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            if ns == nil {
                return nil
            }
            c := inodes(ns)[0]
            return &BufferNode{NonTerminal{Name:"y"}, c.(*BufferNode).buffer}
        }
        return parsec.Maybe( "y", nodify, value )()(s)
    }
}

func array() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            if ns == nil || len(ns) == 0 {
                return nil
            }
            return ns[1].(*BufferNode)
        }
        return parsec.And(
            "array", nodify, false, opensqr, values, closesqr )()(s)
    }
}

func object() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            if ns == nil || len(ns) == 0 {
                return nil
            }
            cs := ns[1].(*NonTerminal).Children
            buf := bytes.NewBuffer( []byte{} )
            buf = writeObject( cs, buf )
            return &BufferNode{NonTerminal{Name:"OBJECT"}, buf}
        }
        return parsec.And(
            "object", nodify, false, openparan, properties, closeparan,
        )()(s)
    }
}

func properties() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            cs := inodes(ns)
            // Bubble sort properties based on property name.
            for i:=0; i<len(cs)-1; i++ {
                for j:=0; j<len(cs)-i-1; j++ {
                    x := cs[j].(*PropertyNode).propname
                    y := cs[j+1].(*PropertyNode).propname
                    if x <= y { continue }
                    cs[j+1], cs[j] = cs[j], cs[j+1]
                }
            }
            return &NonTerminal{Name: "PROPERTIES", Children:cs}
        }
        return parsec.Many( "properties", nodify, true, property, comma )()(s)
    }
}

func property() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            if ns == nil || len(ns) == 0 {
                return nil
            }
            buf := bytes.NewBuffer( []byte{} )
            propname := ns[0].(*parsec.Terminal).Value
            buf = writeProp(propname, ns[2].(*BufferNode), buf)
            return &PropertyNode{NonTerminal{Name:"PROPERTY"}, propname, buf}
        }
        return parsec.And(
            "property", nodify, false, parsec.Literal, colon, value,
        )()(s)
    }
}

func values() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            buf := bytes.NewBuffer( []byte{} )
            buf = writeArray(inodes(ns), buf)
            return &BufferNode{NonTerminal{Name: "VALUES"}, buf}
        }
        return parsec.Many( "values", nodify, false, value, comma )()(s)
    }
}

func value() parsec.Parser {
    return func(s parsec.Scanner) parsec.ParsecNode {
        nodify := func(ns []parsec.ParsecNode) parsec.ParsecNode {
            if ns == nil {
                return nil
            }
            buf := bytes.NewBuffer( []byte{} )
            t, ok := ns[0].(*parsec.Terminal)
            if ok {
                switch t.Name {
                case "NULL" :
                    buf = writeNull(buf)
                case "TRUE" :
                    buf = writeTrue(buf)
                case "FALSE" :
                    buf = writeFalse(buf)
                case "Int" :
                    val, _ := strconv.Atoi(t.Value)
                    buf = writeInt64(int64(val), buf)
                case "Float" :
                    val, _ := strconv.ParseFloat(t.Value, 64)
                    buf = writeFloat64(val, buf)
                case "String" :
                    buf = writeStr(t.Value, buf)
                }
                return &BufferNode{NonTerminal{Name:t.Name}, buf}
            } else {
                c := ns[0].(*BufferNode)
                buf.Write( c.Bytes() )
                return &BufferNode{NonTerminal{Name:c.Name}, buf}
            }
        }
        return parsec.OrdChoice(
            "value", nodify, false,
            tRue, fAlse, null, parsec.Literal, array, object,
        )()(s)
    }
}

var tRue = parsec.Terminalize( "true", "TRUE", "true" )
var fAlse = parsec.Terminalize( "false", "FALSE", "false" )
var null = parsec.Terminalize( "null", "NULL", "null" )

var comma = parsec.Terminalize( ",", "COMMA", "," )
var colon = parsec.Terminalize( ":", "COLON", ":" )
var opensqr = parsec.Terminalize( "[", "OPENSQR", "[" )
var closesqr = parsec.Terminalize( "]", "CLOSESQR", "]" )
var openparan = parsec.Terminalize( "{", "OPENPARAN", "{" )
var closeparan = parsec.Terminalize( "}", "CLOSEPARAN", "}" )

func inodes( pns []parsec.ParsecNode ) []INode {
    ins := make([]INode, 0)
    for _, n := range pns {
        t, ok := n.(*parsec.Terminal)
        if ok {
            ins = append( ins, kpTerminalof(t) )
        } else {
            ins = append( ins, n.(INode) )
        }
    }
    return ins
}

func kpTerminalof( t *parsec.Terminal ) *Terminal {
    return &Terminal{Name:t.Name, Value:t.Value, Tok:t.Tok}
}

// INode APIs for Terminal
func (t *Terminal) Show( prefix string ) {
    fmt.Println( t.Repr(prefix) )
}
func (t *Terminal) Repr( prefix string ) string {
    return fmt.Sprintf(prefix) + fmt.Sprintf("%v : %v ", t.Name, t.Value)
}
func (n *Terminal) Bytes() []byte {
    panic("Bytes() interface not implemented on Terminal\n")
}

// INode APIs for Terminal
func (n *NonTerminal) Show( prefix string ) {
    fmt.Printf( "%v", n.Repr(prefix) )
    for _, n := range n.Children {
        n.Show(prefix + "  ")
    }
}
func (n *NonTerminal) Repr( prefix string ) string {
    return fmt.Sprintf(prefix) + fmt.Sprintf("%v : %v \n", n.Name, n.Value)
}
func (n *NonTerminal) Bytes() []byte {
    panic("Bytes() interface not implemented on NonTerminal\n")
}

// INode APIs for BufferNode
func (n *BufferNode) Bytes() []byte {
    return n.buffer.Bytes()
}

// INode APIs for PropertyNode
func (n *PropertyNode) Bytes() []byte {
    return n.buffer.Bytes()
}
