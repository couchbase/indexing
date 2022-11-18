package kvutility

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	common "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

func SetKeyValuesForCollection(keyValues tc.KeyValues, bucketName, collectionID, password, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	b, err := common.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	for key, value := range keyValues {
		// The vb mapping for a key is independent of collections.
		// Pass key and collectionID separately for correct vb:key mapping
		for i := 0; i < 5; i++ {
			err = b.SetC(key, collectionID, 0, value)
			if err == nil {
				break
			}
			log.Printf("Retrying to set key: %v, collectionId: %v", key, collectionID)
			time.Sleep(1 * time.Second)
		}
		tc.HandleError(err, "set")
	}
}

func GetFromCollection(key string, rv interface{}, bucketName, collectionID, password, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	b, err := common.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	err = b.GetC(key, collectionID, &rv)
	tc.HandleError(err, "get")
}

func DeleteFromCollection(key string, bucketName, collectionID, password, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	b, err := common.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	err = b.DeleteC(key, collectionID)
	tc.HandleError(err, "delete")
}

func DeleteKeysFromCollection(keyValues tc.KeyValues, bucketName, collectionID, password, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	b, err := common.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	for key, _ := range keyValues {
		err = b.DeleteC(key, collectionID)
		tc.HandleError(err, "delete")
	}
}

func GetManifest(bucketName string, serverUserName, serverPassword, hostaddress string) *collections.CollectionManifest {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/scopes"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("GetCollectionsManifest failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "GetCollectionsManifest")
	defer resp.Body.Close()

	manifest := &collections.CollectionManifest{}
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, manifest)
	if err != nil {
		tc.HandleError(err, fmt.Sprintf("GetCollectionsManifest :: Unmarshal of response body: %q", body))
	}
	return manifest
}

func GetScopes(bucketName, serverUserName, serverPassword, hostaddress string) []collections.CollectionScope {
	manifest := GetManifest(bucketName, serverUserName, serverPassword, hostaddress)
	return manifest.Scopes
}

func createScope(bucketName, scopeName, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/scopes"
	data := url.Values{"name": {scopeName}}
	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Create scope failed for bucket %v, scopeName: %v \n", bucketName, scopeName)

	}
	// todo : error out if response is error
	tc.HandleError(err, "Create scope "+address)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	log.Printf("Create scope succeeded for bucket %v, scopeName: %v, body: %s \n", bucketName, scopeName, body)

}

func createCollection(bucketName, scopeName, collectionName, serverUserName, serverPassword, hostaddress string) map[string]interface{} {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/scopes/" + scopeName + "/collections"
	data := url.Values{"name": {collectionName}}
	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Create colleciton failed for bucket %v, scopeName: %v, collection: %v \n",
			bucketName, scopeName, collectionName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Create Collection "+address)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	manifest := make(map[string]interface{})
	err = json.Unmarshal(body, &manifest)
	if err != nil {
		tc.HandleError(err, "createCollection - Error unmarshalling response")
	}

	log.Printf("Created collection succeeded for bucket: %v, scope: %v, collection: %v, body: %s", bucketName, scopeName, collectionName, body)
	return manifest
}

func CreateCollection(bucketName, scope, collection, serverUsername, serverPassword, hostaddress string) map[string]interface{} {
	// 1. get scopes for the bucket
	scopes := GetScopes(bucketName, serverUsername, serverPassword, hostaddress)
	present := false
	for _, s := range scopes {
		if scope == s.Name {
			present = true
			break
		}
	}
	// 2. Scope does not exist. Create the scope
	if !present {
		log.Printf("Creating scope: %v for bucket: %v as it does not exist", scope, bucketName)
		createScope(bucketName, scope, serverUsername, serverPassword, hostaddress)
	}
	return createCollection(bucketName, scope, collection, serverUsername, serverPassword, hostaddress)
}

func DropScope(bucketName, scopeName, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/scopes/" + scopeName
	req, _ := http.NewRequest("DELETE", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Drop scope failed for bucket %v, scope: %v \n", bucketName, scopeName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Drop scope "+address)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Dropped scope %v for bucket: %v, body: %s", scopeName, bucketName, body)
}

func DropCollection(bucketName, scopeName, collectionName, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/scopes/" + scopeName + "/collections/" + collectionName
	req, _ := http.NewRequest("DELETE", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Drop collection failed for bucket %v, scope: %v, collection: %v \n", bucketName, scopeName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Drop scope "+address)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Dropped collection %v for bucket: %v, scope: %v, body: %s", collectionName, bucketName, scopeName, body)
}

func GetCollectionID(bucketName, scopeName, collectionName, serverUserName, serverPassword, hostaddress string) string {
	manifest := GetManifest(bucketName, serverUserName, serverPassword, hostaddress)
	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			for _, collection := range scope.Collections {
				if collection.Name == collectionName {
					return collection.UID
				}
			}
		}
	}
	return ""
}

func GetScopeID(bucketName, scopeName, serverUserName, serverPassword, hostaddress string) string {
	manifest := GetManifest(bucketName, serverUserName, serverPassword, hostaddress)
	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			return scope.UID
		}
	}
	return ""
}

func DropAllScopesAndCollections(bucketName, serverUserName, serverPassword, hostaddress string, dropDefaultCollection bool) {

	manifest := GetManifest(bucketName, serverUserName, serverPassword, hostaddress)
	for _, scope := range manifest.Scopes {

		if scope.Name != common.DEFAULT_SCOPE {
			DropScope(bucketName, scope.Name, serverUserName, serverPassword, hostaddress)
		} else {
			for _, collection := range scope.Collections {
				if collection.Name != common.DEFAULT_COLLECTION {
					DropCollection(bucketName, scope.Name, collection.Name, serverUserName, serverPassword, hostaddress)
				} else {
					if dropDefaultCollection {
						DropCollection(bucketName, scope.Name, collection.Name, serverUserName, serverPassword, hostaddress)
					}
				}
			}
		}
	}
}

func ensureManifest(bucket, serverUserName, serverPassword, hostaddress string, manifest map[string]interface{}) {
	if _, ok := manifest["uid"]; ok {
		uid := manifest["uid"].(string)
		client := &http.Client{}
		for i := 0; i < 30; i++ {
			address := "http://" + hostaddress + "/pools/default/buckets/" + bucket + "/scopes/@ensureManifest/" + uid
			req, _ := http.NewRequest("POST", address, nil)
			req.SetBasicAuth(serverUserName, serverPassword)
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
			resp, err := client.Do(req)
			if resp.StatusCode != http.StatusOK {
				log.Printf(address)
				log.Printf("%v", req)
				log.Printf("%v", resp)
				log.Printf("Ensure manifest, response is NOT OK bucket: %v, uid: %v \n", bucket, uid)
				time.Sleep(1 * time.Second)
			} else {
				// todo : error out if response is error
				tc.HandleError(err, "ensure manifest "+address)
				log.Printf("Received OK response from ensureManifest, bucket: %v, uid: %v", bucket, uid)
				defer resp.Body.Close()
				ioutil.ReadAll(resp.Body)
				return
			}
		}
	}
}

// Checks all ns_server nodes if the collection is populated or not
func WaitForCollectionCreation(bucketName, scopeName, collectionName, serverUserName, serverPassword string, hostaddresses []string, manifest map[string]interface{}) string {
	cids := make([]string, len(hostaddresses))
	for i, hostaddress := range hostaddresses {
		log.Printf("WaitForCollectionCreation: Checking collection creation for host: %v, bucket: %v, scope: %v, collection: %v", hostaddress, bucketName, scopeName, collectionName)
		for j := 0; j < 30; j++ {
			cid := GetCollectionID(bucketName, scopeName, collectionName, serverUserName, serverPassword, hostaddress)
			if cid == "" {
				time.Sleep(1 * time.Second)
			} else {
				cids[i] = cid
				break
			}
		}
	}
	ensureManifest(bucketName, serverUserName, serverPassword, hostaddresses[0], manifest)
	return cids[0]
}
