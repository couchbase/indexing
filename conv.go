package collatejson
import "fmt"
import "bytes"
import "encoding/binary"
import "strings"

const (
    NULL byte = iota + 1
    TRUE byte = iota + 1
    FALSE byte = iota + 1
    INT64 byte = iota + 1
    FLOAT64 byte = iota + 1
    STRING byte = iota + 1
    ARRAY byte = iota + 1
    OBJECT byte = iota + 1
)

const MAXELEMENTS = 16
const MAXSTR = 
    "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

// Order preserving conversion of JSON types and objects to byte array

func writeNull(buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( NULL )
    return buf
}

func writeTrue(buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( TRUE )
    return buf
}

func writeFalse(buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( FALSE )
    return buf
}

func writeInt64(val int64, buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( INT64 )
    binary.Write( buf, binary.LittleEndian, &val )
    return buf
}

func writeFloat64(val float64, buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( FLOAT64 )
    binary.Write( buf, binary.LittleEndian, &val )
    return buf
}

func writeStr(val string, buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( STRING )
    if len(val) < len(MAXSTR) {
        val = val + MAXSTR[ :len(MAXSTR)-len(val) ]
    }
    buf.WriteString( val[:len(MAXSTR)] )
    return buf
}

func writeArray(cs []INode, buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( ARRAY )
    size := byte( len(cs) )
    binary.Write( buf, binary.LittleEndian, &size )
    for _, c := range cs {
        buf.Write( c.Bytes() )
    }
    return buf
}

func writeProp(propname string, n INode, buf *bytes.Buffer) *bytes.Buffer {
    buf = writeStr( propname, buf ) // literal()
    buf.Write( n.Bytes() )          // value()
    return buf
}

func writeObject(cs []INode, buf *bytes.Buffer) *bytes.Buffer {
    buf.WriteByte( OBJECT )
    size := byte( len(cs) )
    binary.Write( buf, binary.LittleEndian, &size )
    for _, c := range cs {
        buf.Write( c.Bytes() )
    }
    return buf
}


// re-construct JSON representation from byte array.

func readValue(buf *bytes.Buffer) string {
    var s string
    typ, _ := buf.ReadByte()
    switch typ {
    case NULL : s = readNull(buf)
    case TRUE : s = readTrue(buf)
    case FALSE : s = readFalse(buf)
    case INT64 : s = readInt64(buf)
    case FLOAT64 : s = readFloat64(buf)
    case STRING : s = readStr(buf)
    case ARRAY : s = readArray(buf)
    case OBJECT : s = readObject(buf)
    }
    return s
}

func readNull(buf *bytes.Buffer) string {
    return "null"
}

func readTrue(buf *bytes.Buffer) string {
    return "true"
}

func readFalse(buf *bytes.Buffer) string {
    return "false"
}

func readInt64(buf *bytes.Buffer) string {
    var val int64
    binary.Read( buf, binary.LittleEndian, &val )
    return fmt.Sprintf("%v", val)
}

func readFloat64(buf *bytes.Buffer) string {
    var val float64
    binary.Read( buf, binary.LittleEndian, &val )
    return fmt.Sprintf("%v", val)
}

func readStr(buf *bytes.Buffer) string {
    var p = make([]byte, len(MAXSTR))
    buf.Read(p)
    return strings.Trim(string(p), "\000")
}

func readArray(buf *bytes.Buffer) string {
    var size byte
    var ss = make([]string, 0)
    binary.Read( buf, binary.LittleEndian, &size )
    for i:=byte(0); i<size; i++ {
        ss = append( ss, readValue(buf) )
    }
    return "[ " + strings.Join(ss, ", ") + " ]"
}

func readObject(buf *bytes.Buffer) string {
    var size byte
    var ss = make([]string, 0)
    binary.Read( buf, binary.LittleEndian, &size )
    for i:=byte(0); i<size; i++ {
        ss = append( ss, readValue(buf) + " : " + readValue(buf) )
    }
    return "{ " + strings.Join(ss, ", ") + " }"
}
