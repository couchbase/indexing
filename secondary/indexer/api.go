package indexer

import "net/http"
import "strings"
import re "regexp"
import "path/filepath"
import "fmt"

import log "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/cbauth"

type target struct {
	version   string
	level     string
	resource  string
	skipEmpty bool
	partition bool
	pretty    bool
}

type restServer struct {
	statsMgr *statsManager
}

type request struct {
	w       http.ResponseWriter
	r       *http.Request
	creds   cbauth.Creds
	version string
	url     string
}

type reqHandler func(req request)

var staticRoutes map[string]reqHandler
var versionRx *re.Regexp

const defaultVersion = "v1"

func initHandlers(api *restServer) {
	versionRx = re.MustCompile("v\\d+")
	staticRoutes = make(map[string]reqHandler)
	staticRoutes["stats"] = api.statsHandler
}

func NewRestServer(cluster string, stMgr *statsManager) (*restServer, Message) {
	log.Infof("%v starting RESTful services", cluster)
	restapi := &restServer{statsMgr: stMgr}
	initHandlers(restapi)
	http.HandleFunc("/api/", restapi.routeRequest)
	return restapi, nil
}

func (api *restServer) routeRequest(
	w http.ResponseWriter, r *http.Request) {
	/* Currently, we are using manual RegEx based routing.
	 * If in the future, we end up with too many endpoints,
	 * we should consider using a third party router that
	 * implements something sophisticated like a Radix Tree
	 * Example URL: _/api/{v1}/stats/{...} { } - optional
	 */

	creds, ok := api.validateAuth(w, r)
	if !ok {
		return
	}
	url := filepath.Clean(r.URL.Path) // Remove trailing space
	segs := strings.Split(url, "/")
	var req request
	var handler reqHandler
	if len(segs) < 3 {
		http.Error(w, r.URL.Path, 404)
		return
	} else if versionRx.MatchString(segs[2]) && len(segs) > 3 {
		if fun, ok := staticRoutes[segs[3]]; ok {
			// Strip version from URL
			version := segs[2]
			segs = append(segs[:2], segs[3:]...)
			url = strings.Join(segs, "/")
			req = request{
				w:       w,
				r:       r,
				creds:   creds,
				version: version,
				url:     url,
			}
			handler = fun
		}
	} else { // Use the default version
		segs := strings.Split(url, "/")
		if fun, ok := staticRoutes[segs[2]]; ok {
			req = request{
				w:       w,
				r:       r,
				creds:   creds,
				version: defaultVersion,
				url:     url,
			}
			handler = fun
		}
	}
	if req.r != nil {
		handler(req)
	} else {
		http.Error(w, r.URL.Path, 404)
	}
}

func (api *restServer) statsHandler(req request) {
	// Example: _/api/stats/bucket/index (_ is a blank)
	if req.r.Method == "GET" {
		var partition, pretty, skipEmpty bool
		if req.r.URL.Query().Get("partition") == "true" {
			partition = true
		}
		if req.r.URL.Query().Get("pretty") == "true" {
			pretty = true
		}
		if req.r.URL.Query().Get("skipEmpty") == "true" {
			skipEmpty = true
		}
		stats := api.statsMgr.stats.Get()
		segs := strings.Split(req.url, "/")
		t := &target{version: req.version, skipEmpty:skipEmpty,
			partition:partition, pretty:pretty,}
		switch req.version {
		case "v1":
			if len(segs) == 3 { // Indexer node level stats
				t.level = "indexer"
			} else if len(segs) == 4 { // Bucket level stats
				errStr := fmt.Sprintf("Bucket-Level statistics are unavailable" +
					"\nPlease retry at Indexer Node or Index granularity \n%s", req.r.URL.Path)
				http.Error(req.w, errStr, 404)
				return
			} else if len(segs) == 5 { // Index level stats
				t.level = "index"
				t.resource = segs[4]
			} else {
				http.Error(req.w, req.r.URL.Path, 404)
				return
			}
			if !api.authorizeStats(req, t) {
				return
			}
			if bytes, err := stats.VersionedJSON(t); err != nil {
				if err.Error() == "404" {
					http.Error(req.w, req.r.URL.Path, 404)
				} else {
					http.Error(req.w, err.Error(), 500)
				}
			} else {
				req.w.Header().Set("Content-Type", "application/json; charset=utf-8")
				req.w.WriteHeader(200)
				req.w.Write(bytes)
			}
		default:
			http.Error(req.w, req.r.URL.Path, 404)
		}
	} else {
		http.Error(req.w, "Unsupported method", 405)
	}
}

func (api *restServer) authorizeStats(req request, t *target) bool {

	permissions := ([]string)(nil)

	switch t.level {
	case "indexer":
		permissions = append(permissions, "cluster.n1ql.meta!read")
	case "bucket":
		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", t.resource)
		permissions = append(permissions, permission)
		break
	case "index":
		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", t.resource)
		permissions = append(permissions, permission)
		break
	default:
		http.Error(req.w, req.r.URL.Path, 404)
		return false
	}

	return c.IsAllAllowed(req.creds, permissions, req.w)
}

func (api *restServer) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (api *restServer) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := c.IsAuthValid(r)
	if err != nil {
		api.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return creds, valid
}
