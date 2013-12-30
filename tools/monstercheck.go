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
        return CollateJSON(value1, value2) < 0
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

// this is N1QL collation
// like Couch, but strings are compared like memcmp
func CollateJSON(key1, key2 interface{}) int {
    type1 := collationType(key1)
    type2 := collationType(key2)
    if type1 != type2 {
        return type1 - type2
    }
    switch type1 {
    case 0, 1, 2:
        return 0
    case 3:
        n1 := collationToFloat64(key1)
        n2 := collationToFloat64(key2)
        if n1 < n2 {
            return -1
        } else if n1 > n2 {
            return 1
        }
        return 0
    case 4:
        s1 := key1.(string)
        s2 := key2.(string)
        if s1 < s2 {
            return -1
        } else if s1 > s2 {
            return 1
        }
        return 0
    case 5:
        array1 := key1.([]interface{})
        array2 := key2.([]interface{})
        for i, item1 := range array1 {
            if i >= len(array2) {
                return 1
            }
            if cmp := CollateJSON(item1, array2[i]); cmp != 0 {
                return cmp
            }
        }
        return len(array1) - len(array2)
    case 6:
        obj1 := key1.(map[string]interface{})
        obj2 := key2.(map[string]interface{})

        // first see if one object is larger than the other
        if len(obj1) < len(obj2) {
            return -1
        } else if len(obj1) > len(obj2) {
            return 1
        }

        // if not, proceed to do key by ke comparision

        // collect all the keys
        allkeys := make(sort.StringSlice, 0)
        for k, _ := range obj1 {
            allkeys = append(allkeys, k)
        }
        for k, _ := range obj2 {
            allkeys = append(allkeys, k)
        }

        // sort the keys
        allkeys.Sort()

        // now compare the values associated with each key
        for _, key := range allkeys {
            val1, ok := obj1[key]
            if !ok {
                // obj1 didn't have this key, so it is smaller
                return -1
            }
            val2, ok := obj2[key]
            if !ok {
                // ojb2 didnt have this key, so its smaller
                return 1
            }
            // key was in both objects, need to compare them
            comp := CollateJSON(val1, val2)
            if comp != 0 {
                // if this decided anything, return
                return comp
            }
            //otherwise continue to compare next key
        }
        // if got here, both objects are the same
        return 0
    }
    panic("bogus collationType")
}

func collationType(value interface{}) int {
    if value == nil {
        return 0
    }
    switch value := value.(type) {
    case bool:
        if !value {
            return 1
        }
        return 2
    case float64, uint64:
        return 3
    case string:
        return 4
    case []interface{}:
        return 5
    case map[string]interface{}:
        return 6
    }
    panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", value, value))
}

func collationToFloat64(value interface{}) float64 {
    if i, ok := value.(uint64); ok {
        return float64(i)
    }
    if n, ok := value.(float64); ok {
        return n
    }
    panic(fmt.Sprintf("collationToFloat64 doesn't understand %+v", value))
}

