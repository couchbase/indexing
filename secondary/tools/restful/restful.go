//go:build nolint

// 2020-12-10 this code is not used anywhere according to Deepkaran Salooja,
// so I got his permission to comment it out so we won't have to maintain
// it. Note that it contains TCP connection leaks as none of the calls to
// http.DefaultClient.Do() closes the response body. To avoid the leaks,
// whenever the response is not an error, its body must be read to the end
// then and closed.
package main

//
//import "net/http"
//import "encoding/json"
//import "bytes"
//import "time"
//import "strings"
//import "io/ioutil"
//import "io"
//import "log"
//import "fmt"
//
//var reqcreate = map[string]interface{}{
//	"name":      "myindex",
//	"bucket":    "beer-sample",
//	"using":     "memdb",
//	"exprType":  "N1QL",
//	"partnExpr": "",
//	"whereExpr": "",
//	"secExprs":  []string{"city"},
//	"isPrimary": false,
//	"with":      nil,
//}
//
//var reqlookup = map[string]interface{}{
//	"equal":    `["a"]`,
//	"distinct": false,
//	"limit":    200,
//	"stale":    "ok",
//}
//
//var reqrange = map[string]interface{}{
//	"startkey":  `["Austin"]`,
//	"endkey":    `["San Francisco"]`,
//	"inclusion": "both",
//	"distinct":  false,
//	"limit":     200,
//	"stale":     "ok",
//}
//
//var reqscanall = map[string]interface{}{
//	"limit": 10000,
//	"stale": "ok",
//}
//
//var reqcount = map[string]interface{}{
//	"startkey":  `["Austin"]`,
//	"endkey":    `["San Francisco"]`,
//	"inclusion": "both",
//	"limit":     200,
//	"stale":     "ok",
//}
//
//func main() {
//	// get indexes
//	indexes := getall()
//	ids := make([]string, 0)
//	for id := range indexes {
//		ids = append(ids, id)
//	}
//	fmt.Printf("FOUND indexes: %v\n", ids)
//
//	// drop all indexes in 2i cluster and play with drop
//	drop(ids)
//	drop([]string{"badindexid"})
//	drop([]string{"23544142"})
//
//	// play with creates
//	badcreates()
//	fmt.Println()
//
//	// create indexes
//	ids = create_andbuild()
//
//	indexes = getall()
//	for _, id := range ids {
//		found := false
//		for idxid := range indexes {
//			if idxid == id {
//				found = true
//				break
//			}
//		}
//		if found == false {
//			log.Fatalf("expected %v, not found", id)
//		}
//	}
//	fmt.Printf("CREATED indexes: %v\n", ids)
//	fmt.Println()
//
//	lookup(ids)
//	fmt.Println()
//
//	rangescan(ids)
//	fmt.Println()
//
//	fulltablescan(ids)
//	fmt.Println()
//
//	countscan(ids)
//	fmt.Println()
//}
//
//func getall() map[string]interface{} {
//	url := "http://localhost:9108/internal/indexes"
//
//	fmt.Println("GET all indexes")
//	resp, err := http.Get(url)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("%v\n", resp.Status)
//	indexes := make(map[string]interface{}, 0)
//	respbody, _ := ioutil.ReadAll(resp.Body)
//	if len(respbody) == 0 {
//		return nil
//	}
//	err = json.Unmarshal(respbody, &indexes)
//	if err != nil {
//		log.Fatal(err)
//	}
//	return indexes
//}
//
//func drop(ids []string) {
//	furl := "http://localhost:9108/internal/index/%v"
//
//	for _, id := range ids {
//		fmt.Printf("DROP index: %v\n", id)
//		url := fmt.Sprintf(furl, id)
//		req, err := http.NewRequest("DELETE", url, nil)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		resp, err := http.DefaultClient.Do(req)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Println(resp.Status)
//		if strings.HasPrefix(resp.Status, "202") {
//			continue
//		}
//	}
//}
//
//func badcreates() {
//	url := "http://localhost:9108/internal/indexes?create=true"
//
//	post := func(dst map[string]interface{}) {
//		var str string
//
//		data, _ := json.Marshal(dst)
//		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		respbody, _ := ioutil.ReadAll(resp.Body)
//		err = json.Unmarshal(respbody, &str)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%v %q\n", resp.Status, str)
//	}
//
//	fmt.Println("TEST: malformed body")
//	body := bytes.NewBuffer([]byte("{name:"))
//	resp, err := http.Post(url, "application/json", body)
//	if err != nil {
//		log.Fatal(err)
//	}
//	respbody, _ := ioutil.ReadAll(resp.Body)
//	fmt.Println(resp.Status, string(respbody))
//
//	fmt.Println("TEST: missing field ``name``")
//	dst := clonebody(reqcreate)
//	delete(dst, "name")
//	post(dst)
//
//	fmt.Println("TEST: empty field ``name``")
//	dst = clonebody(reqcreate)
//	dst["name"] = ""
//	post(dst)
//
//	fmt.Println("TEST: missing field ``bucket``")
//	dst = clonebody(reqcreate)
//	delete(dst, "bucket")
//	post(dst)
//
//	fmt.Println("TEST: empty field ``bucket``")
//	dst = clonebody(reqcreate)
//	dst["bucket"] = ""
//	post(dst)
//
//	fmt.Println("TEST: missing field ``secExprs``")
//	dst = clonebody(reqcreate)
//	delete(dst, "secExprs")
//	post(dst)
//
//	fmt.Println("TEST: empty field ``secExprs``")
//	dst = clonebody(reqcreate)
//	dst["secExprs"] = []string{}
//	post(dst)
//}
//
//func create_andbuild() []string {
//	ids := make([]string, 0)
//
//	post := func(dst map[string]interface{}) string {
//		url := "http://localhost:9108/internal/indexes?create=true"
//		data, _ := json.Marshal(dst)
//		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Println(resp.Status)
//		var result map[string]interface{}
//		respbody, _ := ioutil.ReadAll(resp.Body)
//		err = json.Unmarshal(respbody, &result)
//		fmt.Println(string(respbody), err)
//		if err != nil {
//			log.Fatal(err)
//		}
//		return result["id"].(string)
//	}
//
//	fmt.Println("CREATE INDEX: idx1")
//	dst := clonebody(reqcreate)
//	dst["name"] = "idx1"
//	ids = append(ids, post(dst))
//
//	fmt.Println("CREATE INDEX: idx2 (defer)")
//	dst = clonebody(reqcreate)
//	dst["with"] = map[string]interface{}{"defer_build": true}
//	dst["name"] = "idx2"
//	ids = append(ids, post(dst))
//
//	fmt.Println("CREATE INDEX: idx3 (defer)")
//	dst = clonebody(reqcreate)
//	dst["with"] = map[string]interface{}{"defer_build": true}
//	dst["name"] = "idx3"
//	ids = append(ids, post(dst))
//
//	fmt.Println("CREATE INDEX: idx4 (defer)")
//	dst = clonebody(reqcreate)
//	dst["with"] = map[string]interface{}{"defer_build": true}
//	dst["name"] = "idx4"
//	ids = append(ids, post(dst))
//
//	// execute defer build.
//	fmt.Println("BUILD single deferred index")
//	url := fmt.Sprintf("http://localhost:9108/internal/index/%v?build=true", ids[1])
//	req, err := http.NewRequest("PUT", url, nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	resp, err := http.DefaultClient.Do(req)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("%v\n", resp.Status)
//
//	time.Sleep(4 * time.Second)
//
//	fmt.Println("BUILD many deferred index")
//	url = "http://localhost:9108/internal/indexes?build=true"
//	data, _ := json.Marshal([]string{ids[2], ids[3]})
//	req, err = http.NewRequest("PUT", url, bytes.NewBuffer(data))
//	if err != nil {
//		log.Fatal(err)
//	}
//	resp, err = http.DefaultClient.Do(req)
//	if err != nil {
//		log.Fatal(err)
//	}
//	respbody, _ := ioutil.ReadAll(resp.Body)
//	fmt.Printf("%v %v\n", resp.Status, string(respbody))
//	return ids
//}
//
//func lookup(ids []string) {
//	getl := func(id string, reqbody map[string]interface{}) []interface{} {
//		url := fmt.Sprintf("http://localhost:9108/internal/index/%v?lookup=true", id)
//		data, _ := json.Marshal(reqbody)
//		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		resp, err := http.DefaultClient.Do(req)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%v\n", resp.Status)
//
//		dec := json.NewDecoder(resp.Body)
//		entries := make([]interface{}, 0)
//		for {
//			var result interface{}
//			if err := dec.Decode(&result); err != nil && err != io.EOF {
//				log.Fatal(err)
//			}
//			if _, ok := result.(string); ok {
//				fmt.Printf("ERROR chunk: %v\n", result)
//				return nil
//			} else if result != nil {
//				fmt.Printf("GOT CHUNK: %v\n", len(result.([]interface{})))
//				entries = append(entries, result.([]interface{})...)
//			} else {
//				break
//			}
//		}
//		return entries
//	}
//
//	fmt.Println("LOOKUP missing index")
//	getl("123", reqlookup)
//
//	// first lookup
//	fmt.Println("LOOKUP san francisco")
//	reqbody := clonebody(reqlookup)
//	reqbody["equal"] = `["San Francisco"]`
//	reqbody["distinct"] = false
//	reqbody["limit"] = 100
//	reqbody["stale"] = "ok"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// second lookup
//	fmt.Println("LOOKUP with different params")
//	reqbody = clonebody(reqlookup)
//	reqbody["equal"] = `["San Francisco"]`
//	reqbody["distinct"] = true
//	delete(reqbody, "limit")
//	reqbody["stale"] = "false"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// third
//	fmt.Println("LOOKUP with delhi")
//	reqbody = clonebody(reqlookup)
//	reqbody["equal"] = `["delhi"]`
//	reqbody["distinct"] = true
//	delete(reqbody, "limit")
//	reqbody["stale"] = "false"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//}
//
//func rangescan(ids []string) {
//	getl := func(id string, reqbody map[string]interface{}) []interface{} {
//		url := fmt.Sprintf("http://localhost:9108/internal/index/%v?range=true", id)
//		data, _ := json.Marshal(reqbody)
//		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		resp, err := http.DefaultClient.Do(req)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%v\n", resp.Status)
//
//		dec := json.NewDecoder(resp.Body)
//		entries := make([]interface{}, 0)
//		for {
//			var result interface{}
//			if err := dec.Decode(&result); err != nil && err != io.EOF {
//				log.Fatal(err)
//			}
//			if _, ok := result.(string); ok {
//				fmt.Printf("ERROR chunk: %v\n", result)
//				return nil
//			} else if result != nil {
//				fmt.Printf("GOT CHUNK: %v\n", len(result.([]interface{})))
//				entries = append(entries, result.([]interface{})...)
//			} else {
//				break
//			}
//		}
//		return entries
//	}
//
//	fmt.Println("RANGE missing index")
//	getl("123", reqrange)
//
//	// first range
//	fmt.Println("RANGE cities - none")
//	reqbody := clonebody(reqrange)
//	reqbody["inclusion"] = "none"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "ok"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// second range
//	fmt.Println("RANGE cities -low")
//	reqbody = clonebody(reqrange)
//	reqbody["inclusion"] = "low"
//	reqbody["limit"] = 10000
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// third range
//	fmt.Println("RANGE cities -high")
//	reqbody = clonebody(reqrange)
//	reqbody["inclusion"] = "high"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "ok"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// fourth range
//	fmt.Println("RANGE cities - both")
//	reqbody = clonebody(reqrange)
//	reqbody["inclusion"] = "both"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "false"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// fifth
//	fmt.Println("RANGE missing cities")
//	reqbody = clonebody(reqrange)
//	reqbody["startkey"] = `["0"]`
//	reqbody["endkey"] = `["9"]`
//	delete(reqbody, "limit")
//	reqbody["stale"] = "false"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//}
//
//func fulltablescan(ids []string) {
//	getl := func(id string, reqbody map[string]interface{}) []interface{} {
//		url := fmt.Sprintf("http://localhost:9108/internal/index/%v?scanall=true", id)
//		data, _ := json.Marshal(reqbody)
//		fmt.Println(string(data))
//		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		resp, err := http.DefaultClient.Do(req)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%v\n", resp.Status)
//
//		dec := json.NewDecoder(resp.Body)
//		entries := make([]interface{}, 0)
//		for {
//			var result interface{}
//			if err := dec.Decode(&result); err != nil && err != io.EOF {
//				log.Fatal(err)
//			}
//			if _, ok := result.(string); ok {
//				fmt.Printf("ERROR chunk: %v\n", result)
//				return nil
//			} else if result != nil {
//				fmt.Printf("GOT CHUNK: %v\n", len(result.([]interface{})))
//				entries = append(entries, result.([]interface{})...)
//			} else {
//				break
//			}
//		}
//		return entries
//	}
//
//	fmt.Println("SCANALL missing index")
//	getl("123", reqscanall)
//
//	// first scanall
//	fmt.Println("SCANALL stale ok")
//	reqbody := clonebody(reqscanall)
//	reqbody["stale"] = "ok"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//
//	// second scanall
//	fmt.Println("SCANALL stale false")
//	reqbody = clonebody(reqscanall)
//	reqbody["stale"] = "false"
//	fmt.Printf("number of entries %v\n", len(getl(ids[0], reqbody)))
//}
//
//func countscan(ids []string) {
//	getl := func(id string, reqbody map[string]interface{}) int {
//		url := fmt.Sprintf("http://localhost:9108/internal/index/%v?count=true", id)
//		data, _ := json.Marshal(reqbody)
//		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
//		if err != nil {
//			log.Fatal(err)
//		}
//		resp, err := http.DefaultClient.Do(req)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%v\n", resp.Status)
//
//		var result interface{}
//
//		respbody, _ := ioutil.ReadAll(resp.Body)
//		if len(respbody) == 0 {
//			return 0
//		}
//		err = json.Unmarshal(respbody, &result)
//		if err != nil {
//			log.Fatal(err)
//		}
//		if count, ok := result.(float64); ok {
//			return int(count)
//		}
//		return 0
//	}
//
//	fmt.Println("COUNT missing index")
//	getl("123", reqcount)
//
//	// first count
//	fmt.Println("COUNT cities - none")
//	reqbody := clonebody(reqcount)
//	reqbody["inclusion"] = "none"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "ok"
//	fmt.Printf("count %v\n", getl(ids[0], reqbody))
//
//	// second count
//	fmt.Println("COUNT cities -low")
//	reqbody = clonebody(reqcount)
//	reqbody["inclusion"] = "low"
//	reqbody["limit"] = 10000
//	fmt.Printf("count %v\n", getl(ids[0], reqbody))
//
//	// third count
//	fmt.Println("COUNT cities -high")
//	reqbody = clonebody(reqcount)
//	reqbody["inclusion"] = "high"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "ok"
//	fmt.Printf("count %v\n", getl(ids[0], reqbody))
//
//	// fourth count
//	fmt.Println("COUNT cities - both")
//	reqbody = clonebody(reqcount)
//	reqbody["inclusion"] = "both"
//	reqbody["limit"] = 10000
//	reqbody["stale"] = "false"
//	fmt.Printf("count %v\n", getl(ids[0], reqbody))
//
//	// fifth
//	fmt.Println("COUNT missing cities")
//	reqbody = clonebody(reqcount)
//	reqbody["startkey"] = `["0"]`
//	reqbody["endkey"] = `["9"]`
//	delete(reqbody, "limit")
//	reqbody["stale"] = "false"
//	fmt.Printf("count %v\n", getl(ids[0], reqbody))
//}
//
//func clonebody(src map[string]interface{}) map[string]interface{} {
//	dst := make(map[string]interface{})
//	for k, v := range src {
//		dst[k] = v
//	}
//	return dst
//}
