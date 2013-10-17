package main

import (
    "time"
    "flag"
    "sort"
    "bytes"
    "fmt"
    "os"
    "io/ioutil"
    "runtime/pprof"
    "github.com/prataprc/collatejson"
)

var json = []byte(

`{ "inelegant":27.53096820876087,
"horridness":true,
"iridodesis":[79.1253026404128,null],
"arrogantness":null,
"unagrarian":false
}`)

type Code struct {
    off int
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
    if what == "profile" {
        profile()
    } else {
        sortFile(what)
    }
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
        //fmt.Printf("%q\n", string(code.code))
    }
}

func lines(content []byte) [][]byte {
    ls := make([][]byte, 0, 10)
    start := 0
    for i := 0; i < len(content); {
        if content[i] == 10 {
            ls = append(ls, content[start:i])
            i++
            start = i
        } else {
            i++
        }
    }
    return ls
}

func profile() {
    cpuproffile := "cpuprof"
    os.Remove(cpuproffile)
    cpuproffd, _ := os.Create(cpuproffile)
    pprof.StartCPUProfile(cpuproffd)
    defer pprof.StopCPUProfile()

    count := int64(1000000)
    t1 := time.Now().UnixNano()
    for i := int64(0); i < count; i++ {
        collatejson.Parse(json)
    }
    t2 := time.Now().UnixNano()
    fmt.Printf("Time taken : %vns \n", (t2 - t1)/count)
}
