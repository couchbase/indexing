package main

import (
    "bytes"
    "flag"
    "fmt"
    "log"
    "os"
    "strings"
    "github.com/prataprc/collatejson"
    "io/ioutil"
    "path"
    "sort"
)

type Code struct {
    off  int
    code []byte
}
type Codes []Code

func main() {
    flag.Parse()
    arg := flag.Args()[0]
    if fi, err := os.Stat(arg); err != nil {
        panic(fmt.Errorf("Error stating %v", arg))
    } else if fi.IsDir() {
        runTests(arg)
    } else {
        fmt.Println(strings.Join(sortFile(arg), "\n"))
    }
}

func runTests(rootdir string) {
    if entries, err := ioutil.ReadDir(rootdir); err == nil {
        for _, entry := range entries {
            file := path.Join(rootdir, entry.Name())
            if !strings.HasSuffix(file, ".ref") {
                log.Println("Checking", file, "...")
                out := strings.Join(sortFile(file), "\n")
                if ref, err := ioutil.ReadFile(file + ".ref"); err != nil {
                    panic(fmt.Errorf("Error reading reference file %v", file))
                } else if strings.Trim(string(ref), "\n") != out {
                    panic(fmt.Errorf("Sort mismatch in %v", file))
                }
            }
        }
    } else {
        panic(err)
    }
}

func (codes Codes) Len() int {
    return len(codes)
}

func (codes Codes) Less(i, j int) bool {
    return bytes.Compare(codes[i].code, codes[j].code) < 0
}

func (codes Codes) Swap(i, j int) {
    codes[i], codes[j] = codes[j], codes[i]
}

func sortFile(filename string) (outs []string) {
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
        outs = append(outs, string(texts[code.off]))
    }
    return
}

func lines(content []byte) [][]byte {
    content = bytes.Trim(content, "\r\n")
    return bytes.Split(content, []byte("\n"))
}
