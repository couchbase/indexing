package collatejson

import (
    "bufio"
    "os"
    "testing"
)

func TestParse(t *testing.T) {
    Parse(json1)
}

func BenchmarkParse(b *testing.B) {
    for i := 0; i < b.N; i++ {
        Parse(json1)
    }
}

//---- Local functions
func readlines(filename string) [][]byte {
    var lines = make([][]byte, 0)
    var fd *os.File
    var err error
    if fd, err = os.Open(filename); err != nil {
        panic(err)
    }
    defer func() { fd.Close() }()
    scanner := bufio.NewScanner(fd)
    for scanner.Scan() {
        lines = append(lines, scanner.Bytes())
    }
    return lines
}
