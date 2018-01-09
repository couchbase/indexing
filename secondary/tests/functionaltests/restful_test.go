package functionaltests

import "testing"
import "net/http"
import "encoding/json"
import "bytes"
import "time"
import "strings"
import "strconv"
import "io/ioutil"
import "io"
import "log"
import "fmt"

import sifw "github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
import du "github.com/couchbase/indexing/secondary/tests/framework/datautility"
import tc "github.com/couchbase/indexing/secondary/tests/framework/common"

import tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
import "github.com/couchbase/indexing/secondary/tests/framework/kvutility"

func TestRestfulAPI(t *testing.T) {
	log.Printf("In TestRestfulAPI()")

	e := sifw.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(1000, "users.prod")
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)
	UpdateKVDocs(docsToCreate, docs)

	// get indexes
	indexes, err := restful_getall()
	FailTestIfError(err, "Error in restful_getall()", t)

	ids := make([]string, 0)
	for id := range indexes {
		ids = append(ids, id)
	}
	log.Printf("FOUND indexes: %v\n", ids)

	// drop all indexes in 2i cluster and play with drop
	err = restful_drop(ids)
	FailTestIfError(err, "Error in restful_drop() all indexes", t)
	restful_drop([]string{"badindexid"})
	restful_drop([]string{"23544142"})

	// play with creates
	err = restful_badcreates()
	FailTestIfError(err, "Error in restful_badcreates", t)

	log.Println()

	// create indexes
	ids, err = restful_create_andbuild()
	FailTestIfError(err, "Error in restful_create_andbuild() all indexes", t)

	indexes, err = restful_getall()
	FailTestIfError(err, "Error in restful_getall()", t)

	for _, id := range ids {
		found := false
		for idxid := range indexes {
			if idxid == id {
				found = true
				break
			}
		}
		if found == false {
			msg := fmt.Sprintf("expected %v, not found", id)
			FailTestIfError(nil, msg, t)
		}
	}
	log.Printf("CREATED indexes: %v\n", ids)
	log.Println()

	ids = ids[:len(ids)-1]
	err = restful_lookup(ids)
	FailTestIfError(err, "Error in restful_lookup", t)

	err = restful_rangescan(ids)
	FailTestIfError(err, "Error in restful_rangescan", t)
	log.Println()

	err = restful_fulltablescan(ids)
	FailTestIfError(err, "Error in restful_fulltablescan", t)
	log.Println()

	err = restful_countscan(ids)
	FailTestIfError(err, "Error in restful_countscan", t)
	log.Println()
}

func makeurl(path string) (string, error) {
	indexers, _ := sifw.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		return "", fmt.Errorf("no indexer node")
	}
	return fmt.Sprintf("http://%s:%s@%v%v",
		clusterconfig.Username, clusterconfig.Password, indexers[0], path), nil
}

func restful_getall() (map[string]interface{}, error) {
	url, err := makeurl("/internal/indexes")
	if err != nil {
		return nil, err
	}

	log.Printf("GET all indexes\n")
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	log.Printf("%v\n", resp.Status)
	if restful_checkstatus(resp.Status) == true {
		return nil, fmt.Errorf("restful_getall() status: %v", resp.Status)
	}
	indexes := make(map[string]interface{}, 0)
	respbody, _ := ioutil.ReadAll(resp.Body)
	if len(respbody) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(respbody, &indexes)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

func restful_drop(ids []string) error {
	for _, id := range ids {
		log.Printf("DROP index: %v\n", id)
		url, err := makeurl(fmt.Sprintf("/internal/index/%v", id))
		if err != nil {
			return err
		}
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		log.Printf("status: %v\n", resp.Status)
		if strings.HasPrefix(resp.Status, "202") {
			continue
		}
	}
	return nil
}

func restful_badcreates() error {
	url, err := makeurl("/internal/indexes?create=true")
	if err != nil {
		return err
	}

	post := func(dst map[string]interface{}) error {
		var str string

		data, _ := json.Marshal(dst)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		respbody, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(respbody, &str)
		if err != nil {
			return err
		}
		log.Printf("%v %q\n", resp.Status, str)
		return nil
	}

	log.Println("TEST: malformed body")
	body := bytes.NewBuffer([]byte("{name:"))
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		return err
	}
	respbody, _ := ioutil.ReadAll(resp.Body)
	log.Println(resp.Status, string(respbody))

	log.Println("TEST: missing field ``name``")
	dst := restful_clonebody(reqcreate)
	delete(dst, "name")
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: empty field ``name``")
	dst = restful_clonebody(reqcreate)
	dst["name"] = ""
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: missing field ``bucket``")
	dst = restful_clonebody(reqcreate)
	delete(dst, "bucket")
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: empty field ``bucket``")
	dst = restful_clonebody(reqcreate)
	dst["bucket"] = ""
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: missing field ``secExprs``")
	dst = restful_clonebody(reqcreate)
	delete(dst, "secExprs")
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: empty field ``secExprs``")
	dst = restful_clonebody(reqcreate)
	dst["secExprs"] = []string{}
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: incomplete field ``desc``")
	dst = restful_clonebody(reqcreate)
	dst["secExprs"] = []string{"address.city", "address.state"}
	dst["desc"] = []bool{true}
	if err := post(dst); err != nil {
		return err
	}

	log.Println("TEST: invalid field ``desc``")
	dst["desc"] = []int{1}
	if err := post(dst); err != nil {
		return err
	}

	return nil
}

func restful_create_andbuild() ([]string, error) {
	ids := make([]string, 0)

	post := func(dst map[string]interface{}) (string, error) {
		url, err := makeurl("/internal/indexes?create=true")
		if err != nil {
			return "", err
		}
		data, _ := json.Marshal(dst)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return "", err
		}
		log.Printf("status : %v\n", resp.Status)
		if restful_checkstatus(resp.Status) == true {
			return "", fmt.Errorf("restful_getall() status: %v", resp.Status)
		}

		var result map[string]interface{}
		respbody, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(respbody, &result)
		log.Println(string(respbody), err)
		if err != nil {
			return "", err
		}
		return result["id"].(string), nil
	}

	log.Println("CREATE INDEX: idx1")
	dst := restful_clonebody(reqcreate)
	dst["name"] = "idx1"
	id, err := post(dst)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)

	log.Println("CREATE INDEX: idx2 (defer)")
	dst = restful_clonebody(reqcreate)
	dst["with"] = `{"defer_build": true}`
	dst["name"] = "idx2"
	id, err = post(dst)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)

	log.Println("CREATE INDEX: idx3 (defer)")
	dst = restful_clonebody(reqcreate)
	dst["with"] = `{"defer_build": true}`
	dst["name"] = "idx3"
	id, err = post(dst)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)

	log.Println("CREATE INDEX: idx4 (defer)")
	dst = restful_clonebody(reqcreate)
	dst["with"] = `{"defer_build": true}`
	dst["name"] = "idx4"
	id, err = post(dst)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)

	log.Println("CREATE INDEX: idx5")
	dst = restful_clonebody(reqcreate)
	dst["name"] = "idx5"
	dst["secExprs"] = []string{"miscol"}
	dst["desc"] = []bool{true}
	id, err = post(dst)
	if err != nil {
		return nil, err
	}
	ids = append(ids, id)

	// execute defer build.
	log.Println("BUILD single deferred index")
	url, err := makeurl(fmt.Sprintf("/internal/index/%v?build=true", ids[1]))
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	log.Printf("%v\n", resp.Status)
	if restful_checkstatus(resp.Status) == true {
		return nil, fmt.Errorf("restful_getall() status: %v", resp.Status)
	}
	err = waitforindexes(ids[:2], 300*time.Second)
	if err != nil {
		return nil, err
	}

	log.Println("BUILD many deferred index")
	url, err = makeurl("/internal/indexes?build=true")
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal([]string{ids[2], ids[3]})
	req, err = http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	respbody, _ := ioutil.ReadAll(resp.Body)
	log.Printf("%v %v\n", resp.Status, string(respbody))
	if restful_checkstatus(resp.Status) == true {
		return nil, fmt.Errorf("restful_getall() status: %v", resp.Status)
	}
	err = waitforindexes(ids, 300*time.Second)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func restful_lookup(ids []string) error {
	getl := func(id string, body map[string]interface{}) ([]interface{}, error) {
		url, err := makeurl(fmt.Sprintf("/internal/index/%v?lookup=true", id))
		if err != nil {
			return nil, err
		}
		data, _ := json.Marshal(body)
		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		log.Printf("status : %v\n", resp.Status)

		dec := json.NewDecoder(resp.Body)
		entries := make([]interface{}, 0)
		for {
			var result interface{}
			if err := dec.Decode(&result); err != nil && err != io.EOF {
				return nil, err
			} else if result == nil {
				break
			}
			switch resval := result.(type) {
			case []interface{}:
				// log.Printf("GOT CHUNK: %v\n", len(resval))
				entries = append(entries, resval...)
			default:
				err := fmt.Errorf("ERROR CHUNK: %v\n", result)
				return nil, err
			}
		}
		return entries, nil
	}

	log.Println("LOOKUP missing index")
	getl("123", reqlookup)

	// first lookup
	log.Println("LOOKUP Pyongyang")
	reqbody := restful_clonebody(reqlookup)
	reqbody["equal"] = `["Pyongyang"]`
	reqbody["distinct"] = false
	reqbody["limit"] = 1000000
	reqbody["stale"] = "ok"
	entries, err := getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults := du.ExpectedScanResponse_string(
		docs, "address.city", "Pyongyang", "Pyongyang", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// second lookup
	log.Println("LOOKUP with stale as false")
	reqbody = restful_clonebody(reqlookup)
	reqbody["equal"] = `["Pyongyang"]`
	reqbody["distinct"] = false
	reqbody["stale"] = "false"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "Pyongyang", "Pyongyang", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// third
	log.Println("LOOKUP with Rome")
	reqbody = restful_clonebody(reqlookup)
	reqbody["equal"] = `["Rome"]`
	reqbody["distinct"] = false
	reqbody["stale"] = "false"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "Rome", "Rome", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}
	return nil
}

func restful_rangescan(ids []string) error {
	getl := func(id string, body map[string]interface{}) ([]interface{}, error) {
		url, err := makeurl(fmt.Sprintf("/internal/index/%v?range=true", id))
		if err != nil {
			return nil, err
		}
		data, _ := json.Marshal(body)
		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		log.Printf("Status : %v\n", resp.Status)

		dec := json.NewDecoder(resp.Body)
		entries := make([]interface{}, 0)
		for {
			var result interface{}
			if err := dec.Decode(&result); err != nil && err != io.EOF {
				return nil, err
			}
			if _, ok := result.(string); ok {
				err := fmt.Errorf("ERROR chunk: %v\n", result)
				return nil, err
			} else if result != nil {
				// log.Printf("GOT CHUNK: %v\n", len(result.([]interface{})))
				entries = append(entries, result.([]interface{})...)
			} else {
				break
			}
		}
		return entries, nil
	}

	log.Println("RANGE missing index")
	getl("123", reqrange)

	// first range
	log.Println("RANGE cities - none")
	reqbody := restful_clonebody(reqrange)
	reqbody["inclusion"] = "both"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "ok"
	entries, err := getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults := du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// second range
	log.Println("RANGE cities -low")
	reqbody = restful_clonebody(reqrange)
	reqbody["inclusion"] = "low"
	reqbody["limit"] = 1000000
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 1)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// third range
	log.Println("RANGE cities -high")
	reqbody = restful_clonebody(reqrange)
	reqbody["inclusion"] = "high"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "ok"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 2)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// fourth range
	log.Println("RANGE cities - both")
	reqbody = restful_clonebody(reqrange)
	reqbody["inclusion"] = "both"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "false"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// fifth
	log.Println("RANGE missing cities")
	reqbody = restful_clonebody(reqrange)
	reqbody["startkey"] = `["0"]`
	reqbody["endkey"] = `["9"]`
	reqbody["stale"] = "false"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "0", "9", 3)
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}
	return nil
}

func restful_fulltablescan(ids []string) error {
	getl := func(id string, body map[string]interface{}) ([]interface{}, error) {
		url, err := makeurl(fmt.Sprintf("/internal/index/%v?scanall=true", id))
		if err != nil {
			return nil, err
		}
		data, _ := json.Marshal(body)
		log.Println(string(data))
		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		log.Printf("Status : %v\n", resp.Status)

		dec := json.NewDecoder(resp.Body)
		entries := make([]interface{}, 0)
		for {
			var result interface{}
			if err := dec.Decode(&result); err != nil && err != io.EOF {
				return nil, err
			}
			if _, ok := result.(string); ok {
				err := fmt.Errorf("ERROR chunk: %v\n", result)
				return nil, err
			} else if result != nil {
				// log.Printf("GOT CHUNK: %v\n", len(result.([]interface{})))
				entries = append(entries, result.([]interface{})...)
			} else {
				break
			}
		}
		return entries, nil
	}

	log.Println("SCANALL missing index")
	getl("123", reqscanall)

	// first scanall
	log.Println("SCANALL stale ok")
	reqbody := restful_clonebody(reqscanall)
	reqbody["stale"] = "ok"
	entries, err := getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults := du.ExpectedScanAllResponse(docs, "address.city")
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}

	// second scanall
	log.Println("SCANALL stale false")
	reqbody = restful_clonebody(reqscanall)
	reqbody["stale"] = "false"
	entries, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", len(entries))
	docScanResults = du.ExpectedScanAllResponse(docs, "address.city")
	if err := validateEntries(docScanResults, entries); err != nil {
		return err
	}
	return nil
}

func restful_countscan(ids []string) error {
	getl := func(id string, reqbody map[string]interface{}) (int, error) {
		url, err := makeurl(fmt.Sprintf("/internal/index/%v?count=true", id))
		if err != nil {
			return 0, err
		}
		data, _ := json.Marshal(reqbody)
		req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
		if err != nil {
			return 0, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		log.Printf("Status : %v\n", resp.Status)

		var result interface{}

		respbody, _ := ioutil.ReadAll(resp.Body)
		if len(respbody) == 0 {
			return 0, nil
		}
		err = json.Unmarshal(respbody, &result)
		if err != nil {
			return 0, err
		}
		if count, ok := result.(float64); ok {
			return int(count), nil
		}
		return 0, nil
	}

	log.Println("COUNT missing index")
	getl("123", reqcount)

	// first count
	log.Println("COUNT cities - none")
	reqbody := restful_clonebody(reqcount)
	reqbody["inclusion"] = "none"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "ok"
	count, err := getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", count)
	docScanResults := du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 0)
	if count != len(docScanResults) {
		return fmt.Errorf("failed first count")
	}

	// second count
	log.Println("COUNT cities -low")
	reqbody = restful_clonebody(reqcount)
	reqbody["inclusion"] = "low"
	reqbody["limit"] = 1000000
	count, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", count)
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 1)
	if count != len(docScanResults) {
		return fmt.Errorf("failed second count")
	}

	// third count
	log.Println("COUNT cities -high")
	reqbody = restful_clonebody(reqcount)
	reqbody["inclusion"] = "high"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "ok"
	count, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", count)
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 2)
	if count != len(docScanResults) {
		return fmt.Errorf("failed third count")
	}

	// fourth count
	log.Println("COUNT cities - both")
	reqbody = restful_clonebody(reqcount)
	reqbody["inclusion"] = "both"
	reqbody["limit"] = 1000000
	reqbody["stale"] = "false"
	count, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", count)
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "A", "z", 3)
	if count != len(docScanResults) {
		return fmt.Errorf("failed fourth count")
	}

	// fifth
	log.Println("COUNT missing cities")
	reqbody = restful_clonebody(reqcount)
	reqbody["startkey"] = `["0"]`
	reqbody["endkey"] = `["9"]`
	delete(reqbody, "limit")
	reqbody["stale"] = "false"
	count, err = getl(ids[0], reqbody)
	if err != nil {
		return err
	}
	log.Printf("number of entries %v\n", count)
	docScanResults = du.ExpectedScanResponse_string(
		docs, "address.city", "0", "9", 3)
	if count != len(docScanResults) {
		return fmt.Errorf("failed fifth count")
	}
	return nil
}

func restful_clonebody(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{})
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func restful_checkstatus(status string) bool {
	st := strings.ToLower(status)
	x := strings.Contains(st, strconv.Itoa(http.StatusBadRequest))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusInternalServerError))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusMethodNotAllowed))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusNotFound))
	return x
}

func validateEntries(expected tc.ScanResponse, entries []interface{}) error {
	out := make(tc.ScanResponse)
	for _, entry := range entries {
		m := entry.(map[string]interface{})
		out[m["docid"].(string)] = m["key"].([]interface{})
	}
	if err := tv.Validate(expected, out); err != nil {
		return err
	}
	return nil
}

func waitforindexes(ids []string, timeout time.Duration) error {
	period := 1 * time.Second
	for _, id := range ids {
	loop:
		for {
			err, ok := waitforindex(id)
			if err != nil {
				return err
			} else if !ok {
				time.Sleep(period)
				timeout -= period
				if timeout <= 0 {
					return fmt.Errorf("index %v not active", id)
				}
				continue
			}
			break loop
		}
	}
	return nil
}

func waitforindex(id string) (error, bool) {
	indexes, err := restful_getall()
	if err != nil {
		return err, false
	}
	indexi, ok := indexes[id]
	if !ok {
		return fmt.Errorf("index %d is not found", id), false
	}

	index := indexi.(map[string]interface{})
	defn := index["definitions"].(map[string]interface{})
	name := defn["name"].(string)
	if insts := index["instances"].([]interface{}); len(insts) > 0 {
		inst := insts[0].(map[string]interface{})
		state := inst["state"]
		log.Printf("index %v in %v\n", name, state)
		if state == "INDEX_STATE_ACTIVE" {
			return nil, true
		}
		return nil, false
	} else {
		return fmt.Errorf("instances not found for %v", name), false
	}
}

func getscans(id string, body map[string]interface{}) ([]interface{}, error) {
	url, err := makeurl(fmt.Sprintf("/internal/index/%v?multiscan=true", id))
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	log.Printf("getscans status : %v\n", resp.Status)

	dec := json.NewDecoder(resp.Body)
	entries := make([]interface{}, 0)
	for {
		var result interface{}
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			return nil, err
		} else if result == nil {
			break
		}
		switch resval := result.(type) {
		case []interface{}:
			// log.Printf("GOT CHUNK: %v\n", len(resval))
			entries = append(entries, resval...)
		default:
			err := fmt.Errorf("ERROR CHUNK: %v\n", result)
			return nil, err
		}
	}
	return entries, nil
}

func getscanscount(id string, body map[string]interface{}) (int, error) {
	url, err := makeurl(fmt.Sprintf("/internal/index/%v?multiscancount=true", id))
	if err != nil {
		return 0, err
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	log.Printf("Status : %v\n", resp.Status)

	var result interface{}

	respbody, _ := ioutil.ReadAll(resp.Body)
	if len(respbody) == 0 {
		return 0, nil
	}
	err = json.Unmarshal(respbody, &result)
	if err != nil {
		return 0, err
	}
	if count, ok := result.(float64); ok {
		return int(count), nil
	}
	return 0, nil
}

var reqcreate = map[string]interface{}{
	"name":      "myindex",
	"bucket":    "default",
	"exprType":  "N1QL",
	"partnExpr": "",
	"whereExpr": "",
	"secExprs":  []string{"address.city"},
	"isPrimary": false,
	"with":      nil,
}

var reqlookup = map[string]interface{}{
	"equal":    `["a"]`,
	"distinct": false,
	"limit":    1000000,
	"stale":    "ok",
}

var reqrange = map[string]interface{}{
	"startkey":  `["A"]`,
	"endkey":    `["z"]`,
	"inclusion": "both",
	"distinct":  false,
	"limit":     1000000,
	"stale":     "ok",
}

var reqscanall = map[string]interface{}{
	"limit": 1000000,
	"stale": "ok",
}

var reqcount = map[string]interface{}{
	"startkey":  `["A"]`,
	"endkey":    `["z"]`,
	"inclusion": "both",
	"limit":     1000000,
	"stale":     "ok",
}

var reqscans = map[string]interface{}{
	"scans":      `[{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]},{"Seek":null,"Filter":[{"Low":"S","High":"V","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]}]`,
	"projection": `{"EntryKeys":[0],"PrimaryKey":false}`,
	"distinct":   false,
	"limit":      1000000,
	"reverse":    false,
	"offset":     int64(0),
	"stale":      "ok",
}

var reqscanscount = map[string]interface{}{
	"scans":      `[{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]},{"Seek":null,"Filter":[{"Low":"S","High":"V","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]}]`,
	"projection": `{"EntryKeys":[0],"PrimaryKey":false}`,
	"distinct":   false,
	"limit":      1000000,
	"reverse":    false,
	"offset":     int64(0),
	"stale":      "ok",
}
