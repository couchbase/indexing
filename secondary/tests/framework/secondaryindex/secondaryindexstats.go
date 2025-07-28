package secondaryindex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"maps"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth/metakv"
	c "github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	manager "github.com/couchbase/indexing/secondary/manager"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

type IndexProperties struct {
	HostNode      string
	HttpPort      string
	Bucket        string
	IndexFilePath string
}

func GetIndexerNodesHttpAddresses(hostaddress string) ([]string, error) {
	clusterURL, err := c.ClusterAuthUrl(hostaddress)
	if err != nil {
		return nil, err
	}

	cinfo, err := c.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return nil, err
	}

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	node_ids := cinfo.GetNodeIdsByServiceType(c.INDEX_HTTP_SERVICE)
	indexNodes := []string{}
	for _, node_id := range node_ids {
		addr, _ := cinfo.GetServiceAddress(node_id, c.INDEX_HTTP_SERVICE, true)
		indexNodes = append(indexNodes, addr)
	}

	return indexNodes, nil
}

func GetIndexerNodesHttpAddressForNode(hostaddress string) (string, error) {
	clusterURL, err := c.ClusterAuthUrl(hostaddress)
	if err != nil {
		return "", err
	}

	cinfo, err := c.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return "", err
	}

	if err := cinfo.Fetch(); err != nil {
		return "", err
	}

	return cinfo.TranslatePort(hostaddress, c.MGMT_SERVICE, c.INDEX_HTTP_SERVICE)

}

func GetIndexerNodes(clusterAddr string) ([]couchbase.Node, error) {
	clusterUrl, err := c.ClusterAuthUrl(clusterAddr)
	if err != nil {
		return nil, err
	}

	cinfo, err := c.NewClusterInfoCache(clusterUrl, "default")
	if err != nil {
		return nil, err
	}

	if err = cinfo.Fetch(); err != nil {
		return nil, err
	}

	return cinfo.GetActiveIndexerNodes(), nil
}

func GetStatsForIndexerHttpAddress(indexerHttpAddr, serverUserName, serverPassword string) map[string]interface{} {
	client := &http.Client{}
	address := "http://" + indexerHttpAddr + "/stats?async=false"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)

	if resp != nil && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get stats failed\n")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Stats")
	defer resp.Body.Close()

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		tc.HandleError(err, "Get Bucket :: Unmarshal of response body")
	}

	return response
}

func GetIndexStats(indexName, bucketName, serverUserName, serverPassword, hostaddress string) map[string]interface{} {
	indexNodes, _ := GetIndexerNodesHttpAddresses(hostaddress)
	indexStats := make(map[string]interface{})

	for _, indexNode := range indexNodes {
		stats := GetStatsForIndexerHttpAddress(indexNode, serverUserName, serverPassword)
		for statKey := range stats {
			if strings.Contains(statKey, bucketName+":"+indexName) {
				accumulate(indexStats, stats, statKey)
			}
		}
	}
	return indexStats
}

func GetIndexStats2(indexName, bucketName, scopeName, collectionName, serverUserName, serverPassword, hostaddress string) map[string]interface{} {
	indexNodes, _ := GetIndexerNodesHttpAddresses(hostaddress)
	indexStats := make(map[string]interface{})

	for _, indexNode := range indexNodes {
		stats := GetStatsForIndexerHttpAddress(indexNode, serverUserName, serverPassword)
		for statKey := range stats {
			if collectionName == "_default" {
				if strings.Contains(statKey, bucketName+":"+indexName) {
					accumulate(indexStats, stats, statKey)
				}
			} else {
				if strings.Contains(statKey, bucketName+":"+scopeName+":"+collectionName+":"+indexName) {
					accumulate(indexStats, stats, statKey)
				}
			}
		}
	}
	return indexStats
}

func GetStats(serverUserName, serverPassword, hostaddress string) map[string]interface{} {
	indexNodes, _ := GetIndexerNodesHttpAddresses(hostaddress)
	indexStats := make(map[string]interface{})

	for _, indexNode := range indexNodes {
		stats := GetStatsForIndexerHttpAddress(indexNode, serverUserName, serverPassword)
		for statKey := range stats {
			accumulate(indexStats, stats, statKey)
		}
	}
	return indexStats
}

func GetPerPartnStatsForIndexerHttpAddress(indexerHttpAddr, serverUserName, serverPassword string) map[string]interface{} {
	client := &http.Client{}
	address := "http://" + indexerHttpAddr + "/stats?partition=true&async=false"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get stats failed\n")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Stats")
	defer resp.Body.Close()

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		tc.HandleError(err, "Get Bucket :: Unmarshal of response body")
	}

	return response
}

func GetPerPartnStats(serverUserName, serverPassword, hostaddress string) map[string]interface{} {
	indexNodes, _ := GetIndexerNodesHttpAddresses(hostaddress)
	indexStats := make(map[string]interface{})

	for _, indexNode := range indexNodes {
		stats := GetPerPartnStatsForIndexerHttpAddress(indexNode, serverUserName, serverPassword)
		for statKey := range stats {
			indexStats[statKey] = stats[statKey]
		}
	}
	return indexStats
}

func ResetAllIndexerStats(serverUserName, serverPassword, hostaddress string) {
	indexNodes, _ := GetIndexerNodesHttpAddresses(hostaddress)

	for _, indexNode := range indexNodes {
		ResetIndexerStats(indexNode, serverUserName, serverPassword)
	}
}

func ResetIndexerStats(indexerHttpAddr, serverUserName, serverPassword string) {
	client := &http.Client{}
	address := "http://" + indexerHttpAddr + "/stats/reset"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Reset stats failed\n")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Reset Stats")
	defer resp.Body.Close()
}

// Accumulate all stats of type float64 for partitioned indexes
func accumulate(indexStats, stats map[string]interface{}, statKey string) {
	if val, ok := indexStats[statKey]; ok {
		switch val.(type) {
		case float64:
			value := val.(float64)
			value += stats[statKey].(float64)
			indexStats[statKey] = value
			return
		}
	}
	indexStats[statKey] = stats[statKey]
}

func WaitForIndexerActive(username, password, hostaddress string) error {

	indexerNodes, err := GetIndexerNodesHttpAddresses(hostaddress)
	if err != nil {
		log.Printf("Error observed while retrieving indexer nodes http addr, err: %v", err)
		return err
	}

	for _, indexerNode := range indexerNodes {
		timer := time.NewTimer(time.Duration(2 * time.Minute))
	loop:
		for {
			select {
			case <-timer.C:
				return fmt.Errorf("Indexer: %v is not active after 2 min", indexerNode)
			default:
				stats := GetStatsForIndexerHttpAddress(indexerNode, username, password)
				if val, ok := stats["indexer_state"]; !ok || (val != nil && val.(string) != "Active") {
					log.Printf("Waiting for indexer: %v to go active", indexerNode)
					time.Sleep(5 * time.Second)
				} else {
					log.Printf("Indexer: %v is active", indexerNode)
					break loop
				}
			}
		}
	}

	return nil
}

// Don't use this for per node settings
func ChangeIndexerSettings(configKey string, configValue interface{}, serverUserName, serverPassword, hostaddress string) error {

	// Wait for some time to prevent the possibility of golang
	// re-using a connection which is about to close
	time.Sleep(100 * time.Millisecond)
	qpclient, err := GetOrCreateClient(hostaddress, "2i_settings")
	if err != nil {
		return err
	}
	nodes, err := qpclient.Nodes()
	if err != nil {
		return err
	}

	var adminurl string
	for _, indexer := range nodes {
		adminurl = indexer.Adminport
		break
	}

	host, sport, _ := net.SplitHostPort(adminurl)
	iport, _ := strconv.Atoi(sport)

	if host == "" || iport == 0 {
		log.Printf("ChangeIndexerSettings: Host %v Port %v Nodes %+v", host, iport, nodes)
	}

	client := http.Client{}
	// hack, fix this
	ihttp := iport + 2
	url := "http://" + host + ":" + strconv.Itoa(ihttp) + "/internal/settings"

	if len(configKey) > 0 {
		log.Printf("Changing config key %v to value %v\n", configKey, configValue)
		jbody := make(map[string]interface{})
		jbody[configKey] = configValue
		pbody, err := json.Marshal(jbody)
		if err != nil {
			return err
		}
		preq, err := http.NewRequest("POST", url, bytes.NewBuffer(pbody))
		preq.SetBasicAuth(serverUserName, serverPassword)

		resp, err := client.Do(preq)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		ioutil.ReadAll(resp.Body)
	}

	return nil
}

func doRetriedSettingChangeWithValidation(configs map[string]interface{}, req *http.Request) error {
	var client = http.Client{}
	var body []byte

	return c.NewRetryHelper(8, 1*time.Millisecond, 2, func(attempt int, lastError error) error {
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err = ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 && err == nil {
			return fmt.Errorf("indexer returned error code %v with body %v", resp.Status, string(body))
		} else if err != nil {
			log.Printf("WARN failed to read resp body on %v with error %v", req.URL, err)
		}

		if err != nil {
			return err
		}

		time.Sleep(200 * time.Millisecond)

		var metakvConfigBytes []byte
		metakvConfigBytes, _, err = metakv.Get(c.IndexingSettingsMetaPath)

		if err != nil {
			log.Printf("WARN metakv get fail error %v", err)
			return err
		}

		var metakvConfig = c.SystemConfig
		if err = metakvConfig.Update(metakvConfigBytes); err != nil {
			log.Printf("WARN update fail err - %v mkcb - %v", err, metakvConfigBytes)
			return err
		}

		for k, v := range configs {
			metaV, ok := metakvConfig[k]
			if !ok {
				return fmt.Errorf("config %v value does not match; expected %v actual N/A", k, v)
			}
			if isAnyNumberType(v) && isAnyNumberType(metaV.Value) && toFloat64(v) == toFloat64(metaV.Value) {
				continue
			}
			if reflect.TypeOf(v) == reflect.TypeOf(map[string]interface{}{}) {
				strV, _ := json.Marshal(v)
				strMetaV, _ := json.Marshal(metaV.Value)
				if string(strV) != string(strMetaV) {
					return fmt.Errorf("config %v value does not match; expected %v (%T) actual %v (%T)", k, v, v, metaV.Value, metaV.Value)
				}
			} else if !reflect.DeepEqual(v, metaV.Value) {
				return fmt.Errorf("config %v value does not match; expected %v (%T) actual %v (%T)", k, v, v, metaV.Value, metaV.Value)
			}
		}

		if attempt != 0 {
			log.Printf("all configs changed successfully")
		}

		return nil
	}).Run()
}

func isAnyNumberType(v interface{}) bool {
	switch v.(type) {
	case uint64, int, float64, uint32, uint16, uint8, int32, int16, int8, int64, float32:
		return true
	}
	return false
}

func toFloat64(v interface{}) float64 {
	switch v := v.(type) {
	case uint64:
		return float64(v)
	case int:
		return float64(v)
	case float64:
		return v
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case string:
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return i
	}
	return 0
}

func ChangeMultipleIndexerSettings(configs map[string]interface{}, serverUserName, serverPassword, hostaddress string) error {

	// Wait for some time to prevent the possibility of golang
	// re-using a connection which is about to close
	addr := GetIndexHttpAddrOnNode(serverUserName, serverPassword, hostaddress)
	if len(addr) == 0 {
		return fmt.Errorf("failed to get indexer port for node %v", hostaddress)
	}

	if len(configs) > 0 {
		jbody := make(map[string]interface{})
		jThisNodeBody := make(map[string]interface{})
		for configKey, configValue := range configs {
			log.Printf("Changing config key %v to value %v\n", configKey, configValue)
			if strings.Contains(configKey, "thisNodeOnly") {
				jThisNodeBody[configKey] = configValue
			} else {
				jbody[configKey] = configValue
			}
		}
		pbody, err := json.Marshal(jbody)
		pThisNodeBody, err1 := json.Marshal(jThisNodeBody)
		if err != nil {
			return err
		} else if err1 != nil {
			return err1
		}

		if len(jbody) > 0 {
			url := "http://" + addr + "/internal/settings"

			preq, _ := http.NewRequest("POST", url, bytes.NewBuffer(pbody))
			preq.SetBasicAuth(serverUserName, serverPassword)

			if err = doRetriedSettingChangeWithValidation(jbody, preq); err != nil {
				return err
			}
		}

		if len(jThisNodeBody) > 0 {
			// sleep to wait for above settings to propogate
			time.Sleep(1 * time.Second)
			url := "http://" + addr + "/settings/thisNodeOnly"

			pThisNodeReq, _ := http.NewRequest("POST", url, bytes.NewBuffer(pThisNodeBody))
			pThisNodeReq.SetBasicAuth(serverUserName, serverPassword)

			var client = http.Client{}
			return c.NewRetryHelper(8, 1*time.Millisecond, 2, func(attempt int, lastError error) error {
				resp, err := client.Do(pThisNodeReq)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				var body []byte
				body, err = ioutil.ReadAll(resp.Body)
				if resp.StatusCode != 200 && err == nil {
					return fmt.Errorf("indexer returned error code %v with body %v", resp.Status, string(body))
				} else if err != nil {
					log.Printf("WARN failed to read resp body on %v with error %v", url, err)
				}

				if err != nil {
					return err
				}

				time.Sleep(100 * time.Millisecond)

				pThisNodeGet, _ := http.NewRequest("GET", url, nil)
				pThisNodeGet.SetBasicAuth(serverUserName, serverPassword)

				var respThisNodeOnly *http.Response
				respThisNodeOnly, err = client.Do(pThisNodeGet)
				if err != nil {
					return err
				}
				defer respThisNodeOnly.Body.Close()

				config := make(map[string]interface{})
				respB, _ := io.ReadAll(respThisNodeOnly.Body)
				json.Unmarshal(respB, &config)
				if !maps.Equal[map[string]interface{}](config, jThisNodeBody) {
					return fmt.Errorf("failed to change node setting to %v. Current value is %v", jThisNodeBody, config)
				}

				if attempt != 0 {
					log.Printf("all configs changed successfully")
				}
				return nil
			}).Run()
		}

	}

	return nil
}

func GetIndexHostNode(indexName, bucketName, serverUserName, serverPassword, hostaddress string) (string, error) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/indexStatus"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get indexStatus failed")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Stats")
	defer resp.Body.Close()

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)

	if err != nil {
		tc.HandleError(err, "Get IndexStatus :: Unmarshal of response body")
		return "", nil
	}

	c, e := GetOrCreateClient(hostaddress, "2itest")
	if e != nil {
		return "", e
	}

	defnID, _ := GetDefnID(c, bucketName, indexName)

	indexes := response["indexes"].([]interface{})
	for _, index := range indexes {
		i := index.(map[string]interface{})
		if i["id"].(float64) == float64(defnID) {
			hosts := i["hosts"].([]interface{})
			return hosts[0].(string), nil
		}
	}

	return "", errors.New("Index not found in /indexStatus")
}

func GetIndexStatus(serverUserName, serverPassword, hostaddress string) (map[string]interface{}, error) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/indexStatus"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get indexStatus failed")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Stats")
	defer resp.Body.Close()

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)

	if err != nil {
		tc.HandleError(err, "Get IndexStatus :: Unmarshal of response body")
		return nil, nil
	}
	return response, nil
}

func GetIndexHttpAddrOnNode(serverUserName, serverPassword, hostaddress string) string {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/nodeServices"

	// Get host, port of the hostaddr
	host, port, err := net.SplitHostPort(hostaddress)
	if err != nil {
		return ""
	}

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("ERR - Req for pools endpoint failed with err %v", err)
		return ""
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get indexStatus failed")
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Stats")
	defer resp.Body.Close()

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)

	if err != nil {
		tc.HandleError(err, "Get nodeServices :: Unmarshal of response body")
		return ""
	}

	nodesExt := response["nodesExt"].([]interface{})
	for _, node := range nodesExt {
		nodeExt := node.(map[string]interface{})
		services := nodeExt["services"].(map[string]interface{})
		if fmt.Sprintf("%v", services["mgmt"].(float64)) == port {
			return fmt.Sprintf("%v:%v", host, services["indexHttp"].(float64))
		}
	}

	return ""
}

func GetIndexLocalMetadata(serverUserName, serverPassword, nodeAddr string) (*manager.LocalIndexMetadata, error) {
	indexerAddr := GetIndexHttpAddrOnNode(serverUserName, serverPassword, nodeAddr)
	if indexerAddr == "" {
		return nil, fmt.Errorf("indexerAddr is empty for nodeAddr: %v", nodeAddr)
	}

	client := &http.Client{}
	address := "http://" + indexerAddr + "/getLocalIndexMetadata"
	log.Printf("for GetIndexLocalMetadata %v", address)
	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%v, %v", "Error in call GET /getLocalIndexMetadata", err)
	} else if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf("For %v GET /getLocalIndexMetadata failed", address)
		return nil, fmt.Errorf("For %v, %v ", address, "GET /getLocalIndexMetadata failed")
	}

	localMeta := new(manager.LocalIndexMetadata)
	body, _ := ioutil.ReadAll(resp.Body)
	if err = json.Unmarshal(body, &localMeta); err != nil {
		tc.HandleError(err, "GET /getLocalIndexMetadata :: Unmarshal of response body")
		return nil, fmt.Errorf("Error unmarshal response %v %v", address, err)
	}
	return localMeta, nil
}

func GetNumIndexesOnNode(serverUserName, serverPassword, addr string) (int, error) {
	localIndexMetadata, err := GetIndexLocalMetadata(serverUserName, serverPassword, addr)
	if err != nil {
		return -1, err
	}

	return len(localIndexMetadata.IndexDefinitions), nil
}
