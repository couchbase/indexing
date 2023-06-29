package backuprestore

import (
	"bytes"
	"encoding/json"
	ioutil "io/ioutil"
	"log"
	http "net/http"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/security"
)

// Get NodeUUID of Indexer Node
func GetNodeUUID(addr string) (string, error) {
	log.Printf("In GetNodeUUID() %v", addr)

	url := addr + "/nodeuuid"

	var resp *http.Response
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	nodeuuid := string(respbody[:])
	log.Printf("Response GetNodeUUID for %v : %v", addr, nodeuuid)
	return nodeuuid, nil
}

// Get BucketUUID of bucket
func GetBucketUUID(bucket, user, passwd, indexManagementAddress string) (string, error) {
	log.Printf("In GetBucketUUID()")

	addr := "http://" + user + ":" + passwd + "@" + indexManagementAddress + "/pools/default/buckets/" + bucket
	var resp *http.Response
	resp, err := http.Get(addr)
	if err != nil {
		return "", err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var f interface{}
	json.Unmarshal(respbody, &f)
	m := f.(map[string]interface{})
	resp_ := m["uuid"].(string)
	log.Printf("Response GetBucketUUID for %v : %v", addr, resp_)
	return resp_, nil
}

func Backup(indexerUrl, bucket, scope, collection string, include bool) ([]byte, error) {
	log.Printf("In Backup()")
	urlstr := "/api/v1/bucket/" + bucket + "/backup"
	if scope != "" && collection != "" {
		filter := "include"
		if !include {
			filter = "exclude"
		}
		urlstr += "?" + filter + "=" + scope + "." + collection
	}

	var resp *http.Response
	resp, err := http.Get(indexerUrl + urlstr)
	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var f interface{}
	json.Unmarshal(respbody, &f)
	m := f.(map[string]interface{})
	resp_ := m["result"]

	b, err := json.Marshal(resp_)
	if err != nil {
		panic(err)
	}
	log.Printf("doBackup: SUCCESS for %v", urlstr)

	return b, nil
}

func Restore(indexerUrl, bucket, scope, collection string, body []byte, include bool) ([]byte, error) {
	log.Printf("In Restore()")
	urlstr := "/api/v1/bucket/" + bucket + "/backup"
	if scope != "" && collection != "" {
		filter := "include"
		if !include {
			filter = "exclude"
		}
		urlstr += "?" + filter + "=" + scope + "." + collection
	}

	var resp *http.Response
	resp, err := http.Post(indexerUrl+urlstr, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	log.Printf("doRestore: %v", string(respbody[:]))
	return respbody, nil
}

func MakeScheduleCreateRequest(idxDefn *c.IndexDefn, addr, indexerId string) error {
	url := addr + "/postScheduleCreateRequest"
	req := &client.ScheduleCreateRequest{
		Definition: *idxDefn,
		Plan:       map[string]interface{}{},
		IndexerId:  c.IndexerId(indexerId),
	}

	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bytesBuf := bytes.NewBuffer(buf)
	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}

	var resp *http.Response
	resp, err = security.PostWithAuth(url, "application/json", bytesBuf, params)
	if err != nil {
		log.Printf("MakeScheduleCreateRequest: error in PostWithAuth: %v, for index (%v, %v, %v, %v)",
			err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		ioutil.ReadAll(resp.Body)
	} else {
		log.Printf("MakeScheduleCreateRequest: unexpected http status: %v, for index (%v, %v, %v, %v)",
			resp.StatusCode, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)

		var msg interface{}
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			log.Printf("MakeScheduleCreateRequest: error in reading response body: %v, for index (%v, %v, %v, %v)",
				err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
			return err
		}

		if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
			log.Printf("MakeScheduleCreateRequest: error in unmarshalling response body: %v, for index (%v, %v, %v, %v)",
				err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
			return err
		}
	}
	return nil
}
