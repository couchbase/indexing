package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
)

type IndexRequest struct {
	Version uint64           `json:"version,omitempty"`
	Type    string           `json:"type,omitempty"`
	Index   common.IndexDefn `json:"index,omitempty"`
}

//
// main function
//
func main() {
	var curl string
	var cmd string
	var meta string
	var host string
	var defnId uint64

	flag.StringVar(&curl, "curl", "", "cluster url")
	flag.StringVar(&cmd, "cmd", "", "command=getIndexStatus/getIndexMetadata/restoreIndexMetadata/dropIndex")
	flag.StringVar(&meta, "meta", "", "metadata to restore")
	flag.StringVar(&host, "host", "", "host for index defnition id")
	flag.Uint64Var(&defnId, "id", 0, "index definition id")
	flag.Parse()

	cinfo, err := common.NewClusterInfoCache(curl, "default")
	if err != nil {
		log.Printf("%v", err)
		return
	}

	if err := cinfo.Fetch(); err != nil {
		log.Printf("%v", err)
		return
	}

	nodes := cinfo.GetNodeIdsByServiceType("indexHttp")
	if len(nodes) == 0 {
		log.Printf("There is no couchbase server running with indexer service")
		return
	}

	indexHttp, err := cinfo.GetServiceAddress(nodes[0], "indexHttp", false)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	if cmd == "getIndexStatus" || cmd == "getIndexMetadata" {

		resp, err := http.Get("http://" + indexHttp + "/" + cmd)
		if err != nil {
			log.Printf("%v", err)
			return
		}

		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			log.Printf("%v", err)
			return
		}

		log.Printf("%v", string(buf.Bytes()))

	} else if cmd == "restoreIndexMetadata" {

		bodybuf := bytes.NewBuffer([]byte(meta))
		resp, err := http.Post("http://"+indexHttp+"/"+cmd, "application/json", bodybuf)
		if err != nil {
			log.Printf("%v", err)
			return
		}

		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			log.Printf("%v", err)
			return
		}

		log.Printf("%v", string(buf.Bytes()))

	} else if cmd == "dropIndex" {

		for _, id := range nodes {
			indexHttp, err := cinfo.GetServiceAddress(id, "indexHttp", false)
			if err != nil {
				log.Printf("%v", err)
				return
			}

			if strings.HasPrefix(indexHttp, host) {

				defn := common.IndexDefn{DefnId: common.IndexDefnId(defnId)}
				request := &IndexRequest{Type: "drop", Index: defn}

				body, err := json.Marshal(&request)
				if err != nil {
					log.Printf("%v", err)
					return
				}

				log.Printf("dialing http://" + indexHttp + "/" + cmd)

				bodybuf := bytes.NewBuffer(body)
				resp, err := http.Post("http://"+indexHttp+"/"+cmd, "application/json", bodybuf)
				if err != nil {
					log.Printf("%v", err)
					return
				}

				buf := new(bytes.Buffer)
				if _, err := buf.ReadFrom(resp.Body); err != nil {
					log.Printf("%v", err)
					return
				}

				log.Printf("%v", string(buf.Bytes()))
				return
			}
		}

		log.Printf("Cannot find matching host %d", host)
	}
}
