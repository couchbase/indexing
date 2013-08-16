package collatejson
import "testing"
import "fmt"
import "strings"
import "os"
import "io/ioutil"
import "bufio"
import "bytes"

var _ = fmt.Sprintln("Dummy statement to use fmt")

func TestSamplekey1(t *testing.T) {
    bin, _ := ioutil.ReadFile("./examples/samplekey1")
    reftext := strings.Trim( string(bin), "\r\n" )

    out := Parse(bin).Bytes()
    res := JSONKey( out )
    if res != reftext {
        t.Fail()
    }
}

func TestSamplesort1(t *testing.T) {
    var outs = make([][]byte, 0)
    bin, _ := ioutil.ReadFile("./examples/samplesort1.ref")
    reftext := strings.Trim( string(bin), "\r\n" )

    // Convert keys to binary
    for _, line := range readlines("./examples/samplesort1") {
        out := Parse(line).Bytes()
        outs = append( outs, out )
    }
    outs = sort(outs)
    // Verify
    if verify(outs, reftext) == false {
        t.Fail()
    }
}

func TestSamplesort2(t *testing.T) {
    var outs = make([][]byte, 0)
    bin, _ := ioutil.ReadFile("./examples/samplesort2.ref")
    reftext := strings.Trim( string(bin), "\r\n" )

    // Convert keys to binary
    for _, line := range readlines("./examples/samplesort2") {
        out := Parse(line).Bytes()
        outs = append( outs, out )
    }
    outs = sort(outs)
    // Verify
    if verify( outs, reftext ) == false {
        t.Fail()
    }
}

func TestSamplesort3(t *testing.T) {
    var outs = make([][]byte, 0)
    bin, _ := ioutil.ReadFile("./examples/samplesort3.ref")
    reftext := strings.Trim( string(bin), "\r\n" )

    // Convert keys to binary
    for _, line := range readlines("./examples/samplesort3") {
        out := Parse(line).Bytes()
        outs = append( outs, out )
    }
    outs = sort(outs)
    // Verify
    if verify(outs, reftext) == false {
        t.Fail()
    }
}

func sort( outs [][]byte ) [][]byte {
    for i:=0; i<len(outs)-1; i++ {
        for j:=0; j<len(outs)-i-1; j++ {
            if bytes.Compare( outs[j], outs[j+1] ) == 1 {
                outs[j+1], outs[j] = outs[j], outs[j+1]
            }
        }
    }
    return outs
}

func readlines( filename string ) [][]byte {
    var lines = make( [][]byte, 0 )
    fd, _ := os.Open(filename)
    defer func(){ fd.Close() }()
    scanner := bufio.NewScanner(fd)
    for scanner.Scan() {
        lines = append( lines, scanner.Bytes() )
    }
    return lines
}

func verify( outs [][]byte, reftext string ) bool {
    var ress = make([]string, 0)
    for _, out := range outs {
        ress = append( ress, JSONKey(out) )
    }
    res := strings.Join(ress, "\n")
    return res == reftext
}
