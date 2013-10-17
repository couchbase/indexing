package collatejson
import (
    "testing"
    "fmt"
    "os"
    "bufio"
)

var _ = fmt.Sprintln("Dummy statement to use fmt")

var sortable_files = []string {
    "./examples/samplesort1",
    "./examples/samplesort2",
    "./examples/samplesort3",
    "./examples/empty",
}

func TestParse(t *testing.T) {
    Parse(json1)
}

func BenchmarkParse(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Parse(json1)
    }
}

//---- Local functions
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
