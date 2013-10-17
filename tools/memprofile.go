package main

import (
    "fmt"
    "github.com/prataprc/monster"
    "github.com/prataprc/golib"
    "github.com/prataprc/collatejson"
    "math/rand"
    "time"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func main() {
    prodfile, count := "../prods/json.prod", 1000000
    conf := make(golib.Config)

    c := make(monster.Context)
    start := monster.Parse(prodfile, conf)
    nonterminals, root := monster.Build(start)
    c["_nonterminals"] = nonterminals
    ins, outs := make([]int, 0, count), make([]int, 0, count)
    for i:=1; i < count; i++ {
        c["_random"] = rand.New(rand.NewSource(int64(time.Now().UnixNano())))
        monster.Initialize(c)
        text := root.Generate(c)
        code := collatejson.Encode([]byte(text))
        ins = append(ins, len(text))
        outs = append(outs, len(code))
    }
    inavg, outavg := 0, 0
    for i := range ins {
        inavg, outavg = inavg + ins[i], outavg + outs[i]
    }
    inavg = inavg / len(ins)
    outavg = outavg / len(outs)
    fmt.Printf("inavg: %v     outavg: %v\n", inavg, outavg)
}
