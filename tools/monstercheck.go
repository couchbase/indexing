package main

import (
    "bytes"
    "flag"
    "sort"
    "strconv"
    "strings"
    "os"
    "fmt"
    "math/rand"
    "time"
    "github.com/prataprc/golib"
    "github.com/prataprc/monster"
    "github.com/prataprc/collatejson"
    "github.com/couchbaselabs/dparval"
    tuqcollate "github.com/couchbaselabs/tuqtng/ast"
)

type Codes struct {
    kind  string
    jsons []string
}

func (codes Codes) Len() int {
    return len(codes.jsons)
}

func (codes Codes) Less(i, j int) bool {
    key1, key2 := codes.jsons[i], codes.jsons[j]
    if codes.kind == "tuq" {
        value1 := dparval.NewValueFromBytes([]byte(key1)).Value()
        value2 := dparval.NewValueFromBytes([]byte(key2)).Value()
        return tuqcollate.CollateJSON(value1, value2) < 0
    } else if codes.kind == "binary" {
        value1 := collatejson.Encode([]byte(key1))
        value2 := collatejson.Encode([]byte(key2))
        return bytes.Compare(value1, value2) < 0
    } else {
        panic(fmt.Errorf("Unknown kind"))
    }
    return false
}

func (codes Codes) Swap(i, j int) {
    codes.jsons[i], codes.jsons[j] = codes.jsons[j], codes.jsons[i]
}

func main() {
    flag.Parse()
    prodfile := flag.Args()[0]
    count, _ := strconv.Atoi(flag.Args()[1])
    for i := int(0); i < count; i++ {
        fmt.Printf(".")
        tuqjsons := generateJsons(prodfile, 10)
        binjsons := make([]string, len(tuqjsons))
        copy(binjsons, tuqjsons)

        tuqcodes := Codes{"tuq", tuqjsons}
        sort.Sort(tuqcodes)
        fd, _ := os.Create("a")
        fd.Write([]byte(strings.Join(tuqcodes.jsons, "\n")))
        fd.Close()

        bincodes := Codes{"binary", binjsons}
        sort.Sort(bincodes)
        fd, _ = os.Create("b")
        fd.Write([]byte(strings.Join(bincodes.jsons, "\n")))
        fd.Close()

        if len(tuqcodes.jsons) != len(bincodes.jsons) {
            panic("Mismatch in count of jsons")
        }
        for i, val1 := range tuqcodes.jsons {
            val2 := bincodes.jsons[i]
            if val1 != val2 {
                panic(fmt.Errorf("Mismatch in json", val1, val2))
            }
        }
    }
    fmt.Println()
}

func generateJsons(prodfile string, count int) (jsons []string) {
    c, conf := make(monster.Context), make(golib.Config)
    start := monster.Parse(prodfile, conf)
    nonterminals, root := monster.Build(start)
    c["_nonterminals"] = nonterminals
    for i := 1; i < count; i++ {
        c["_random"] = rand.New(rand.NewSource(int64(time.Now().UnixNano())))
        monster.Initialize(c)
        text := root.Generate(c)
        jsons = append(jsons, text)
    }
    return
}
