package indexer

import (
	"net/http"
	"strings"
	"sync/atomic"

	"fmt"
	re "regexp"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/audit"
	c "github.com/couchbase/indexing/secondary/common"
	log "github.com/couchbase/indexing/secondary/logging"
)

var apiRouter atomic.Value // stores func(http.ResponseWriter,*http.Request)

type target struct {
	version    string
	level      string
	bucket     string
	index      string
	scope      string
	collection string
	skipEmpty  bool
	partition  bool
	pretty     bool
	redact     bool
	creds      cbauth.Creds
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
	staticRoutes["bucket"] = bucketHandler
}

func NewRestServer(cluster string, stMgr *statsManager) (*restServer, Message) {
	log.Infof("%v starting RESTful services", cluster)
	restapi := &restServer{statsMgr: stMgr}
	initHandlers(restapi)
	apiRouter.Store(restapi.routeRequest)
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

	url := strings.TrimSpace(r.URL.Path) // Remove trailing space.
	if url[len(url)-1] == byte('/') {
		url = url[:len(url)-1]
	}
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
		var partition, pretty, skipEmpty, redact bool
		if req.r.URL.Query().Get("partition") == "true" {
			partition = true
		}
		if req.r.URL.Query().Get("pretty") == "true" {
			pretty = true
		}
		if req.r.URL.Query().Get("skipEmpty") == "true" {
			skipEmpty = true
		}
		if req.r.URL.Query().Get("redact") == "true" {
			redact = true
		}
		stats := api.statsMgr.stats.Get()
		segs := strings.Split(req.url, "/")
		t := &target{version: req.version, skipEmpty: skipEmpty,
			partition: partition, pretty: pretty, redact: redact, creds: req.creds}
		switch req.version {
		case "v1":
			if len(segs) == 3 { // Indexer node level stats
				t.level = "indexer"
			} else {
				// bucket or scope or collection or index level stats
				if len(segs) == 4 || len(segs) == 5 {
					bucket := "[a-zA-Z0-9_\\-.%]+" // A bucket can containing "." should be enclosed in back-tick(`) characters
					bucketScopeColl := "[a-zA-Z0-9_\\-%]+"
					keyspaceName := fmt.Sprintf("^(\\`(%s)\\`|(%s))($|\\.(%s)$|\\.(%s)\\.(%s)$)", bucket,
						bucketScopeColl, bucketScopeColl, bucketScopeColl, bucketScopeColl)
					exp := re.MustCompile(keyspaceName)
					substrings := exp.FindStringSubmatch(segs[3]) // On a successful match, substrings list will have 8 strings

					if len(substrings) == 0 || len(substrings) != 8 {
						http.Error(req.w, genErrStr(segs[3]), 404)
						return
					} else {
						// Extract bucket name
						if substrings[2] != "" {
							t.bucket = substrings[2]
						} else {
							t.bucket = substrings[3]
						}
						if substrings[7] == "" { // collection name is empty
							t.scope = substrings[5]
						} else {
							t.scope = substrings[6]
							t.collection = substrings[7]
						}
					}
					if len(segs) == 5 {
						if t.scope == "" && t.collection == "" { // Return from default scope and default collection
							t.scope = c.DEFAULT_SCOPE
							t.collection = c.DEFAULT_COLLECTION
						} else if t.scope == "" || t.collection == "" {
							// It is invalid to specify only scope or only collection name for index level stats
							http.Error(req.w, genErrStr(segs[3]), 404)
							return
						}
						t.level = "index"
						t.index = segs[4]
					} else {
						if t.scope == "" {
							t.level = "bucket"
						} else if t.collection == "" {
							t.level = "scope"
						} else {
							t.level = "collection"
						}
					}
				} else {
					http.Error(req.w, req.r.URL.Path, 404)
					return
				}
			}
			if t.level != "indexer" && !api.authorizeStats(req, t) {
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

// Dont use this function for indexer level stats. For indexer level stats
// we must check permissions for every index.
func (api *restServer) authorizeStats(req request, t *target) bool {

	permissions := ([]string)(nil)

	switch t.level {
	case "indexer":
		permissions = append(permissions, "cluster.n1ql.meta!read")
	case "bucket":
		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", t.bucket)
		permissions = append(permissions, permission)
		break
	case "scope":
		permission := fmt.Sprintf("cluster.scope[%s:%s].n1ql.index!list", t.bucket, t.scope)
		permissions = append(permissions, permission)
		break
	case "collection":
		permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!list", t.bucket, t.scope, t.collection)
		permissions = append(permissions, permission)
		break
	case "index":
		permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!list", t.bucket, t.scope, t.collection)
		permissions = append(permissions, permission)
		break
	default:
		http.Error(req.w, req.r.URL.Path, 404)
		return false
	}

	return c.IsAllAllowed(req.creds, permissions, req.r, req.w, "restServer::authorizeStats")
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
		audit.Audit(c.AUDIT_UNAUTHORIZED, r, "restServer::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(c.HTTP_STATUS_UNAUTHORIZED)
	}
	return creds, valid
}

func genErrStr(keyspaceName string) string {
	errStr := fmt.Sprintf("The keyspace name: %s does not qualify for a valid keyspace name.\n"+
		"Rules for specifying keyspace name: \n"+
		"a. The bucket/scope/collection names should be valid as per the naming semantics\n"+
		"b. When bucket name contains '.' character, it has to be enclosed in back-tick(`) characters in the URL\n"+
		"   E.g., api/v1/stats/`test.1`.test_scope/\n"+
		"c. The keyspace name should be either <bucket_name> (or) <bucket_name>.<scope_name> (or) <bucket_name>.<scope_name>.<collection_name>\n"+
		"   E.g., api/v1/stats/test_bucket\n"+
		"    	  api/v1/stats/test_bucket.test_scope\n"+
		"		  api/v1/stats/test_bucket.test_scope.test_coll are valid\n"+
		"d. When retrieving index level stats, both scope name and collection name are to be specified (except for default scope and default collection)\n"+
		"Specifying only one is not valid\n"+
		"   E.g., api/v1/stats/test_bucket.test_scope/idx_1 is invalid\n"+
		"         api/vi/stats/test_bucket/idx_1 will return index stats from default scope and default collection\n"+
		"         api/vi/stats/test_bucket.test_scope.test_collection/idx_1 will return index stats from  test_scope and test_collection",
		keyspaceName)
	return errStr
}

//
// Bucket Handler. All bucket level APIs called using /api/v1/bucket
// will be handled by this handler. Expected format for the url is
//
// /api/v1/bucket/<bucket-name>/<function-name>
//
// Following is the list of supported APIs.
// /api/v1/bucket/<bucket-name>/backup
//
func bucketHandler(req request) {
	BucketRequestHandler(req.w, req.r, req.creds)
}
