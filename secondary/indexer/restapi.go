package indexer

import "encoding/json"
import "io/ioutil"
import "net/http"
import "strings"
import "strconv"
import "fmt"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import log "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/query/parser/n1ql"
import "github.com/couchbase/query/expression"

type restServer struct {
	cluster string
	client  *qclient.GsiClient
	config  c.Config
}

const UnboundedLiteral = "~[]{}UnboundedTruenilNA~"

func NewRestServer(cluster string) (*restServer, Message) {
	log.Infof("%v starting RESTful services", cluster)

	// get the singleton-client
	config, err := c.GetSettingsConfig(c.SystemConfig)
	if err != nil {
		return nil, nil
	}
	qconf := config.SectionConfig("queryport.client.", true /*trim*/)

	client, _ := qclient.NewGsiClient(cluster, qconf)
	restapi := &restServer{cluster: cluster, client: client, config: qconf}

	if err != nil {
		return restapi, &MsgError{
			err: Error{
				category: INDEXER,
				cause:    err,
				severity: FATAL,
			}}
	}
	restapi.config = config

	http.HandleFunc("/api/indexes", restapi.handleIndexes)
	http.HandleFunc("/api/index/", restapi.handleIndex)
	return restapi, nil
}

func (api *restServer) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (api *restServer) validateAuth(w http.ResponseWriter, r *http.Request) bool {
	valid, err := c.IsAuthValid(r, api.config["indexer.clusterAddr"].String())
	if err != nil {
		api.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

// GET  /api/indexes
// POST /api/indexes?create=true
// PUT  /api/indexes?build=true
func (api *restServer) handleIndexes(
	w http.ResponseWriter, request *http.Request) {

	if !api.validateAuth(w, request) {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	q := request.URL.Query()
	if _, ok := q["create"]; ok {
		if request.Method == "POST" {
			api.doCreate(w, request)
		} else {
			msg := `invalid method, expected POST`
			http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
		}
	} else if _, ok := q["build"]; ok {
		if request.Method == "PUT" {
			api.doBuildMany(w, request)
		} else {
			msg := `invalid method, expected PUT`
			http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
		}
	} else if request.Method == "GET" {
		api.doGetAll(w, request)
	} else {
		msg := `query parameter should be either create or build`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
	}
}

//PUT    /api/index/{id}?build=true
//DELETE /api/index/{id}
//GET    /api/index/{id}
//GET    /api/index/{id}?lookup=true
//GET    /api/index/{id}?range=true
//GET    /api/index/{id}?scanall=true
//GET    /api/index/{id}?count=true
func (api *restServer) handleIndex(
	w http.ResponseWriter, request *http.Request) {

	if !api.validateAuth(w, request) {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	segs := strings.Split(request.URL.Path, "/")
	if len(segs) == 4 {
		q, _ := request.URL.Query(), segs[3]
		if _, ok := q["build"]; ok {
			if request.Method == "PUT" {
				api.doBuildOne(w, request)
			} else {
				msg := `invalid method, expected PUT`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if _, ok := q["lookup"]; ok {
			if request.Method == "GET" || request.Method == "POST" {
				api.doLookup(w, request)
			} else {
				msg := `invalid method, expected GET`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if _, ok := q["range"]; ok {
			if request.Method == "GET" || request.Method == "POST" {
				api.doRange(w, request)
			} else {
				msg := `invalid method, expected GET`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if _, ok := q["multiscan"]; ok {
			if request.Method == "GET" || request.Method == "POST" {
				api.doMultiScan(w, request)
			} else {
				msg := `invalid method, expected GET`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if _, ok := q["scanall"]; ok {
			if request.Method == "GET" || request.Method == "POST" {
				api.doScanall(w, request)
			} else {
				msg := `invalid method, expected GET`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if _, ok := q["count"]; ok {
			if request.Method == "GET" || request.Method == "POST" {
				api.doCount(w, request)
			} else {
				msg := `invalid method, expected GET`
				http.Error(w, jsonstr(msg), http.StatusMethodNotAllowed)
			}
		} else if request.Method == "GET" {
			api.doGet(w, request)
		} else if request.Method == "DELETE" {
			api.doDrop(w, request)
		} else {
			msg := `invalid request, missing api argument`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
		}
	} else {
		msg := `invalid url path, expected index id `
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
	}
}

// POST /api/indexes?create=true
func (api *restServer) doCreate(w http.ResponseWriter, request *http.Request) {

	var params map[string]interface{}

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := `invalid request body (%v), unmarshal failed %v`
		http.Error(w, jsonstr(msg, string(bytes), err), http.StatusBadRequest)
		return
	}

	var indexname, bucket string
	var secExprs []string
	var with []byte

	using, exprtype, with, isPrimary := "gsi", "N1QL", nil, false

	if value, ok := params["name"]; !ok {
		msg := `missing field name`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	} else if indexname = value.(string); indexname == "" {
		msg := `empty field name`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}

	if value, ok := params["bucket"]; !ok {
		msg := `missing field bucket`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	} else if bucket = value.(string); bucket == "" {
		msg := `empty field bucket`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}

	value, ok := params["secExprs"]
	if !ok {
		msg := `missing field secExprs`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	exprs, ok := value.([]interface{})
	if exprs == nil || len(exprs) == 0 {
		msg := `empty field secExprs`
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	} else {
		for _, item := range exprs {
			expr, err := n1ql.ParseExpression(item.(string))
			if err != nil {
				msg := "invalid secondary expression (%v) %v\n"
				http.Error(w, jsonstr(msg, item, err), http.StatusBadRequest)
				return
			}
			secExprs = append(secExprs, expression.NewStringer().Visit(expr))
		}
	}

	if value, ok := params["using"]; ok && value != nil {
		using = value.(string)
	}

	if value, ok := params["exprType"]; ok && value != nil {
		exprtype = strings.ToUpper(value.(string))
	}

	if value, ok := params["isPrimary"]; ok && value != nil {
		isPrimary = value.(bool)
	}

	if value, ok := params["with"]; ok && value != nil {
		if withstr, ok := value.(string); ok && withstr != "" {
			with = []byte(withstr)
		}
	}

	var partnExpr, whereExpr string

	if value, ok := params["partnExpr"]; ok && value != nil {
		partnExpr = value.(string)
	}
	if value, ok := params["whereExpr"]; ok && value != nil {
		whereExpr = value.(string)
	}

	defnId, err := api.client.CreateIndex(
		indexname, bucket, using, exprtype, partnExpr, whereExpr, secExprs,
		isPrimary, with)
	if err != nil {
		http.Error(w, jsonstr("%v", err), http.StatusInternalServerError)
		return
	}

	data := fmt.Sprintf(`{"id": "%v"}`, defnId)
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(data)))
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, data)
}

// PUT  /api/indexes?build=true
func (api *restServer) doBuildMany(
	w http.ResponseWriter, request *http.Request) {

	var params []interface{}

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := `invalid request body, unmarshal failed %v`
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	defnIDs := make([]uint64, 0)
	for _, s := range params {
		id, err := strconv.ParseUint(s.(string), 10, 64)
		if err != nil {
			msg := `invalid index id, ParseUint failed %v`
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
		defnIDs = append(defnIDs, id)
	}

	err = api.client.BuildIndexes(defnIDs)

	// make response
	if err != nil {
		http.Error(w, jsonstr("%v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
}

//PUT    /api/index/{id}?build=true
func (api *restServer) doBuildOne(w http.ResponseWriter, request *http.Request) {
	defnId, err := urlPath2IndexId(request.URL.Path)
	if err != nil {
		msg := `invalid index id, ParseUint failed %v`
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	err = api.client.BuildIndexes([]uint64{defnId})

	// make response
	if err != nil {
		http.Error(w, jsonstr("%v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
}

//GET    /api/index/{id}
func (api *restServer) doGetAll(w http.ResponseWriter, request *http.Request) {
	indexes, err := api.client.Refresh()
	if err != nil {
		msg := `cannot refresh metadata: %v`
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	results := make(map[string]interface{})
	for _, index := range indexes {
		result := map[string]interface{}{
			"definitions": index.Definition,
		}
		instances := make([]map[string]interface{}, 0)
		for _, inst := range index.Instances {
			instance := map[string]interface{}{
				"instId":    fmt.Sprintf("%v", inst.InstId),
				"state":     fmt.Sprintf("%v", inst.State),
				"buildTime": inst.BuildTime,
				"indexerId": fmt.Sprintf("%v", inst.IndexerId),
				"endpoints": inst.Endpts,
			}
			instances = append(instances, instance)
		}
		result["instances"] = instances
		idstr := fmt.Sprintf("%v", index.Definition.DefnId)
		results[idstr] = result
	}

	data, err := json.Marshal(results)
	if err != nil {
		msg := jsonstr(`unable to marshal result: %v`, err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//DELETE /api/index/{id}
func (api *restServer) doDrop(w http.ResponseWriter, request *http.Request) {
	defnId, err := urlPath2IndexId(request.URL.Path)
	if err != nil {
		msg := `invalid index id, ParseUint failed %v`
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	err = api.client.DropIndex(defnId)

	// make response
	if err != nil {
		http.Error(w, jsonstr(`"%v"`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
}

//GET    /api/index/{id}
func (api *restServer) doGet(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	result := map[string]interface{}{
		"definitions": index.Definition,
	}
	instances := make([]map[string]interface{}, 0)
	for _, inst := range index.Instances {
		instance := map[string]interface{}{
			"instId":    fmt.Sprintf("%v", inst.InstId),
			"state":     fmt.Sprintf("%v", inst.State),
			"buildTime": inst.BuildTime,
			"indexerId": fmt.Sprintf("%v", inst.IndexerId),
			"endpoints": inst.Endpts,
		}
		instances = append(instances, instance)
	}
	result["instances"] = instances

	data, err := json.Marshal(result)
	if err != nil {
		msg := jsonstr(`unable to marshal result: %v`, err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//GET    /api/index/{id}?lookup=true
func (api *restServer) doLookup(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	var params map[string]interface{}
	var ts *qclient.TsConsistency
	distinct, limit, stale := false, int64(100), "ok"

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := `invalid request body, unmarshal failed %v`
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	value, ok := params["equal"]
	if !ok {
		msg := `"missing field equal"`
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	matchequal := []byte(value.(string))

	if value, ok = params["distinct"]; ok {
		distinct = value.(bool)
	}

	if value, ok = params["limit"]; ok {
		limit = int64(value.(float64))
	}

	if value, ok = params["stale"]; ok {
		stale = value.(string)
	}

	if value, ok = params["timestamp"]; stale == "partial" {
		if !ok {
			msg := `missing field timestamp for stale="partial"`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
			return
		}
		ts, err = vector2tsconsistency(value.(map[string][]string))
		if err != nil {
			msg := "invalid timestamp, ParseUint failed %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	equal, err := equal2Key(matchequal)
	if err != nil {
		msg := "invalid equal key: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	equals := []c.SecondaryKey{c.SecondaryKey(equal)}

	cons := stale2consistency(stale)

	var skeys []c.SecondaryKey
	var pkeys [][]byte

	w.WriteHeader(http.StatusOK)

	empty := true
	err = nil
	e := api.client.Lookup(
		uint64(index.Definition.DefnId), "", equals, distinct, limit, cons, ts,
		func(res qclient.ResponseReader) bool {
			if err = res.Error(); err != nil {
				return false
			} else if skeys, pkeys, err = res.GetEntries(); err != nil {
				return false
			}
			//nil means no more data
			if skeys != nil {
				empty = false
				data, err := api.makeEntries(skeys, pkeys)
				if err != nil {
					w.Write([]byte(api.makeError(err)))
				}
				w.Write([]byte(data))
				w.(http.Flusher).Flush()
			}
			return true
		})
	if err == nil {
		err = e
	}
	if err != nil {
		w.Write([]byte(api.makeError(err)))
	} else if empty {
		w.Write([]byte("[]"))
	}
}

//GET    /api/index/{id}?range=true
func (api *restServer) doRange(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	var params map[string]interface{}
	var ts *qclient.TsConsistency
	distinct, limit, stale, inclusion := false, int64(100), "ok", "both"

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := "invalid request body, unmarshal failed %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	value, ok := params["startkey"]
	if !ok {
		msg := "missing field ``startkey``"
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	startkey := []byte(value.(string))

	value, ok = params["endkey"]
	if !ok {
		msg := "missing field ``endkey``"
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	endkey := []byte(value.(string))

	if value, ok = params["stale"]; ok && value != nil {
		stale = value.(string)
	}

	if value, ok = params["distinct"]; ok && value != nil {
		distinct = value.(bool)
	}

	if value, ok = params["limit"]; ok && value != nil {
		limit = int64(value.(float64))
	}

	if value, ok = params["inclusion"]; ok && value != nil {
		inclusion = value.(string)
	}

	if value, ok = params["timestamp"]; stale == "partial" {
		if !ok {
			msg := `missing field timestamp for stale="partial"`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
			return
		}
		ts, err = vector2tsconsistency(value.(map[string][]string))
		if err != nil {
			msg := "invalid timestamp, ParseUint failed %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	begin, err := equal2Key(startkey)
	if err != nil {
		msg := "invalid startkey: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	end, err := equal2Key(endkey)
	if err != nil {
		msg := "invalid endkey: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	low, high := c.SecondaryKey(begin), c.SecondaryKey(end)
	cons := stale2consistency(stale)

	var skeys []c.SecondaryKey
	var pkeys [][]byte

	w.WriteHeader(http.StatusOK)

	empty := true
	err = nil
	e := api.client.Range(
		uint64(index.Definition.DefnId), "", low, high,
		incl2incl(inclusion), distinct, limit,
		cons, ts,
		func(res qclient.ResponseReader) bool {
			if err = res.Error(); err != nil {
				return false
			} else if skeys, pkeys, err = res.GetEntries(); err != nil {
				return false
			}
			//nil means no more data
			if skeys != nil {
				empty = false
				data, err := api.makeEntries(skeys, pkeys)
				if err != nil {
					w.Write([]byte(api.makeError(err)))
				}
				w.Write([]byte(data))
				w.(http.Flusher).Flush()
			}
			return true
		})
	if err == nil {
		err = e
	}
	if err != nil {
		w.Write([]byte(api.makeError(err)))
	} else if empty {
		w.Write([]byte("[]"))
	}
}

//GET    /api/index/{id}?multiscan=true
func (api *restServer) doMultiScan(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	var params map[string]interface{}
	var ts *qclient.TsConsistency
	distinct, limit, offset, stale, reverse := false, int64(100), int64(0), "ok", false
	var projection *qclient.IndexProjection

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := "invalid request body, unmarshal failed %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	value, ok := params["scans"]
	if !ok {
		msg := "missing field ``scans``"
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	scansParam := []byte(value.(string))

	if value, ok = params["projection"]; ok && value != nil {
		projection, err = getProjection([]byte(value.(string)))
		if err != nil {
			msg := "invalid projection: %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	if value, ok = params["reverse"]; ok && value != nil {
		reverse = value.(bool)
	}

	if value, ok = params["stale"]; ok && value != nil {
		stale = value.(string)
	}

	if value, ok = params["distinct"]; ok && value != nil {
		distinct = value.(bool)
	}

	if value, ok = params["offset"]; ok && value != nil {
		offset = int64(value.(float64))
	}

	if value, ok = params["limit"]; ok && value != nil {
		limit = int64(value.(float64))
	}

	if value, ok = params["timestamp"]; stale == "partial" {
		if !ok {
			msg := `missing field timestamp for stale="partial"`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
			return
		}
		ts, err = vector2tsconsistency(value.(map[string][]string))
		if err != nil {
			msg := "invalid timestamp, ParseUint failed %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	scans, err := getScans(scansParam)
	if err != nil {
		msg := "invalid scans: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	cons := stale2consistency(stale)

	var skeys []c.SecondaryKey
	var pkeys [][]byte

	w.WriteHeader(http.StatusOK)

	empty := true
	err = nil
	e := api.client.MultiScan(
		uint64(index.Definition.DefnId), "", scans, reverse,
		distinct, projection, offset, limit,
		cons, ts,
		func(res qclient.ResponseReader) bool {
			if err = res.Error(); err != nil {
				return false
			} else if skeys, pkeys, err = res.GetEntries(); err != nil {
				return false
			}
			//nil means no more data
			if skeys != nil {
				empty = false
				data, err := api.makeEntries(skeys, pkeys)
				if err != nil {
					w.Write([]byte(api.makeError(err)))
				}
				w.Write([]byte(data))
				w.(http.Flusher).Flush()
			}
			return true
		})
	if err == nil {
		err = e
	}
	if err != nil {
		w.Write([]byte(api.makeError(err)))
	} else if empty {
		w.Write([]byte("[]"))
	}
}

//GET    /api/index/{id}?scanall=true
func (api *restServer) doScanall(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	var params map[string]interface{}
	var ts *qclient.TsConsistency
	limit, stale := int64(100), "ok"

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := "invalid request body, unmarshal failed %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	if value, ok := params["limit"]; ok && value != nil {
		limit = int64(value.(float64))
	}

	if value, ok := params["stale"]; ok && value != nil {
		stale = value.(string)
	}

	if value, ok := params["timestamp"]; stale == "partial" {
		if !ok {
			msg := `missing field timestamp for stale="partial"`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
			return
		}
		ts, err = vector2tsconsistency(value.(map[string][]string))
		if err != nil {
			msg := "invalid timestamp, ParseUint failed %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	cons := stale2consistency(stale)

	var skeys []c.SecondaryKey
	var pkeys [][]byte

	w.WriteHeader(http.StatusOK)

	empty := true
	err = nil
	e := api.client.ScanAll(
		uint64(index.Definition.DefnId), "", limit, cons, ts,
		func(res qclient.ResponseReader) bool {
			if err = res.Error(); err != nil {
				return false
			} else if skeys, pkeys, err = res.GetEntries(); err != nil {
				return false
			}
			//nil means no more data
			if skeys != nil {
				empty = false
				data, err := api.makeEntries(skeys, pkeys)
				if err != nil {
					w.Write([]byte(api.makeError(err)))
				}
				w.Write([]byte(data))
				w.(http.Flusher).Flush()
			}
			return true
		})
	if err == nil {
		err = e
	}
	if err != nil {
		w.Write([]byte(api.makeError(err)))
	} else if empty {
		w.Write([]byte("[]"))
	}
}

//GET    /api/index/{id}?count=true
func (api *restServer) doCount(w http.ResponseWriter, request *http.Request) {
	index, errmsg := api.getIndex(request.URL.Path)
	if errmsg != "" && strings.Contains(errmsg, "not found") {
		http.Error(w, errmsg, http.StatusNotFound)
		return
	} else if errmsg != "" {
		http.Error(w, errmsg, http.StatusBadRequest)
		return
	}

	var params map[string]interface{}
	var ts *qclient.TsConsistency
	stale, inclusion := "ok", "both"

	bytes, err := ioutil.ReadAll(request.Body)
	if err := json.Unmarshal(bytes, &params); err != nil {
		msg := "invalid request body, unmarshal failed %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}

	value, ok := params["startkey"]
	if !ok {
		msg := "missing field ``startkey``"
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	startkey := []byte(value.(string))

	value, ok = params["endkey"]
	if !ok {
		msg := "missing field ``endkey``"
		http.Error(w, jsonstr(msg), http.StatusBadRequest)
		return
	}
	endkey := []byte(value.(string))

	if value, ok = params["inclusion"]; ok {
		inclusion = value.(string)
	}

	if value, ok := params["stale"]; ok {
		stale = value.(string)
	}

	if value, ok := params["timestamp"]; stale == "partial" {
		if !ok {
			msg := `missing field timestamp for stale="partial"`
			http.Error(w, jsonstr(msg), http.StatusBadRequest)
			return
		}
		ts, err = vector2tsconsistency(value.(map[string][]string))
		if err != nil {
			msg := "invalid timestamp, ParseUint failed %v"
			http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
			return
		}
	}

	begin, err := equal2Key(startkey)
	if err != nil {
		msg := "invalid startkey: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	end, err := equal2Key(endkey)
	if err != nil {
		msg := "invalid endkey: %v"
		http.Error(w, jsonstr(msg, err), http.StatusBadRequest)
		return
	}
	low, high := c.SecondaryKey(begin), c.SecondaryKey(end)
	cons := stale2consistency(stale)

	w.WriteHeader(http.StatusOK)

	count, err := api.client.CountRange(
		uint64(index.Definition.DefnId), "", low, high,
		incl2incl(inclusion), cons, ts)
	if err != nil {
		w.Write([]byte(api.makeError(err)))
		return
	}

	data := []byte(strconv.Itoa(int(count)))
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (api *restServer) getIndex(path string) (*mclient.IndexMetadata, string) {
	var index *mclient.IndexMetadata
	defnId, err := urlPath2IndexId(path)
	if err != nil {
		return nil, jsonstr(`invalid index id, ParseUint failed %v`, err)
	}

	indexes, err := api.client.Refresh()
	if err != nil {
		return nil, jsonstr(`cannot refresh metadata: %v`, err)
	}
	for _, index = range indexes {
		if index.Definition.DefnId == c.IndexDefnId(defnId) {
			if len(index.Instances) == 0 {
				return nil, `"instance not found"`
			}
			inst := index.Instances[0]
			if inst.State != c.INDEX_STATE_ACTIVE {
				return nil, jsonstr(`index not found in %v state`, inst.State)
			}
			return index, ""
		}
	}
	return nil, `"index not found"`
}

func (api *restServer) makeError(err error) string {
	return fmt.Sprintf(`{"error":"%v"}`, err)
}

func (api *restServer) makeEntries(
	skeys []c.SecondaryKey, pkeys [][]byte) (string, error) {

	entries := []string{}
	for i, skey := range skeys {
		data, err := json.Marshal(skey)
		if err != nil {
			return "", err
		}
		s := fmt.Sprintf(`{"key":%v,"docid":"%v"}`, string(data), string(pkeys[i]))
		entries = append(entries, s)
	}
	return "[" + strings.Join(entries, ",\n") + "]", nil
}

func urlPath2IndexId(path string) (uint64, error) {
	segs := strings.Split(path, "/")
	id := segs[3]
	return strconv.ParseUint(id, 10, 64)
}

func vector2tsconsistency(
	ts map[string][]string) (*qclient.TsConsistency, error) {

	vbnos, seqnos := make([]uint16, 0), make([]uint64, 0)
	vbuuids := make([]uint64, 0)
	for vbno_s, val := range ts {
		vbno, err := strconv.ParseUint(vbno_s, 10, 64)
		if err != nil {
			return nil, err
		}
		vbnos = append(vbnos, uint16(vbno))

		vbuuid, err := strconv.ParseUint(val[0], 10, 64)
		if err != nil {
			return nil, err
		}
		vbuuids = append(vbuuids, vbuuid)

		seqno, err := strconv.ParseUint(val[1], 10, 64)
		if err != nil {
			return nil, err
		}
		seqnos = append(seqnos, seqno)
	}
	return qclient.NewTsConsistency(vbnos, seqnos, vbuuids), nil
}

func equal2Key(arg []byte) ([]interface{}, error) {
	var key []interface{}
	if err := json.Unmarshal(arg, &key); err != nil {
		return nil, err
	}
	return key, nil
}

func getScans(arg []byte) (qclient.Scans, error) {
	var scans qclient.Scans
	if err := json.Unmarshal(arg, &scans); err != nil {
		return nil, err
	}
	for _, sc := range scans {
		for _, filter := range sc.Filter {
			if filter.Low == UnboundedLiteral {
				filter.Low = c.MinUnbounded
			}
			if filter.High == UnboundedLiteral {
				filter.High = c.MaxUnbounded
			}
		}
	}
	return scans, nil
}

func getProjection(arg []byte) (*qclient.IndexProjection, error) {
	var proj qclient.IndexProjection
	if err := json.Unmarshal(arg, &proj); err != nil {
		return &proj, err
	}
	return &proj, nil
}

var mstale2consistency = map[string]c.Consistency{
	"ok":      c.AnyConsistency,
	"false":   c.SessionConsistency,
	"partial": c.QueryConsistency,
}

func stale2consistency(stale string) c.Consistency {
	return mstale2consistency[stale]
}

func incl2incl(inclusion string) qclient.Inclusion {
	switch inclusion {
	case "both":
		return qclient.Both
	case "low":
		return qclient.Low
	case "high":
		return qclient.High
	}
	return qclient.Neither
}

func jsonstr(msg string, args ...interface{}) string {
	s := fmt.Sprintf(msg, args...)
	data, _ := json.Marshal(s)
	return string(data)
}
