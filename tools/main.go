package main

import (
    "bytes"
    "flag"
    "fmt"
    "github.com/prataprc/collatejson"
    "io/ioutil"
    "os"
    "runtime/pprof"
    "sort"
    "time"
)

var json = []byte(
    `{ "inelegant":27.53096820876087,
"horridness":true,
"iridodesis":[79.1253026404128,null],
"arrogantness":null,
"unagrarian":false
}`)

type Code struct {
    off  int
    code []byte
}
type Codes []Code

func (codes Codes) Len() int {
    return len(codes)
}

func (codes Codes) Less(i, j int) bool {
    return bytes.Compare(codes[i].code, codes[j].code) < 0
}

func (codes Codes) Swap(i, j int) {
    codes[i], codes[j] = codes[j], codes[i]
}

func main() {
    flag.Parse()
    what := flag.Args()[0]
    sortFile(what)
}

func sortFile(filename string) {
    s, err := ioutil.ReadFile(filename)
    if err != nil {
        panic(err.Error())
    }
    texts, codes := lines(s), make(Codes, 0)
    for i, x := range texts {
        code := collatejson.Encode(x)
        codes = append(codes, Code{i, code})
    }
    sort.Sort(codes)
    for _, code := range codes {
        fmt.Println(string(texts[code.off]))
    }
}

func lines(content []byte) [][]byte {
    content = bytes.Trim(content, "\r\n")
    return bytes.Split(content, []byte("\n"))
}
