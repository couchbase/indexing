package couchbase

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common/collections"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 256
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

// PoolSize is the size of each connection pool (per host).
var PoolSize = 64

// Timeout value for HTTP requests
var HttpRequestTimeout = time.Duration(120) * time.Second

// PoolOverflow is the number of overflow connections allowed in a
// pool.
var PoolOverflow = PoolSize

// AuthHandler is a callback that gets the auth username and password
// for the given bucket.
type AuthHandler interface {
	GetCredentials() (string, string)
}

// RestPool represents a single pool returned from the pools REST API.
type RestPool struct {
	Name         string `json:"name"`
	StreamingURI string `json:"streamingUri"`
	URI          string `json:"uri"`
}

// Pools represents the collection of pools as returned from the REST API.
type Pools struct {
	ComponentsVersion     map[string]string `json:"componentsVersion,omitempty"`
	ImplementationVersion string            `json:"implementationVersion"`
	IsAdmin               bool              `json:"isAdminCreds"`
	UUID                  string            `json:"uuid"`
	Pools                 []RestPool        `json:"pools"`
	IsIPv6                bool              `json:"isIPv6,omitempty"`
}

// A Node is a computer in a cluster running the couchbase software.
type Node struct {
	ClusterCompatibility int                `json:"clusterCompatibility"`
	ClusterMembership    string             `json:"clusterMembership"`
	CouchAPIBase         string             `json:"couchApiBase"`
	Hostname             string             `json:"hostname"`
	InterestingStats     map[string]float64 `json:"interestingStats,omitempty"`
	MCDMemoryAllocated   float64            `json:"mcdMemoryAllocated"`
	MCDMemoryReserved    float64            `json:"mcdMemoryReserved"`
	MemoryFree           float64            `json:"memoryFree"`
	MemoryTotal          float64            `json:"memoryTotal"`
	OS                   string             `json:"os"`
	Ports                map[string]int     `json:"ports"`
	Status               string             `json:"status"`
	Uptime               int                `json:"uptime,string"`
	Version              string             `json:"version"`
	ThisNode             bool               `json:"thisNode,omitempty"`
	Services             []string           `json:"services,omitempty"`
	NodeUUID             string             `json:"nodeUUID,omitempty"`
	AddressFamily        string             `json:"addressFamily,omitempty"`
	NodeHash             int                `json:"nodeHash,omitempty"`
	ServerGroup          string             `json:"serverGroup,omitempty"`
}

type BucketName struct {
	Name string `json:"bucketName"`
	UUID string `json:"uuid"`
}

// A Pool of nodes and buckets.
type Pool struct {
	BucketMap map[string]Bucket
	Nodes     []Node

	// Bucket -> Collections manifest for the bucket
	Manifest map[string]*collections.CollectionManifest

	BucketURL       map[string]string `json:"buckets"`
	ServerGroupsUri string            `json:"serverGroupsUri"`
	BucketNames     []BucketName      `json:"bucketNames"`

	client Client
}

type SaslBucket struct {
	Buckets []Bucket `json:"buckets"`
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type VBucketServerMap struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap"`
}

// Bucket is the primary entry point for most data operations.
type Bucket struct {
	connPools        unsafe.Pointer // *[]*connectionPool
	vBucketServerMap unsafe.Pointer // *VBucketServerMap
	nodeList         unsafe.Pointer // *[]Node

	Capabilities        []string               `json:"bucketCapabilities"`
	CapabilitiesVersion string                 `json:"bucketCapabilitiesVer"`
	Type                string                 `json:"bucketType"`
	Name                string                 `json:"name"`
	NodeLocator         string                 `json:"nodeLocator"`
	Quota               map[string]float64     `json:"quota,omitempty"`
	Replicas            int                    `json:"replicaNumber"`
	URI                 string                 `json:"uri"`
	StreamingURI        string                 `json:"streamingUri"`
	LocalRandomKeyURI   string                 `json:"localRandomKeyUri,omitempty"`
	UUID                string                 `json:"uuid"`
	BasicStats          map[string]interface{} `json:"basicStats,omitempty"`
	Controllers         map[string]interface{} `json:"controllers,omitempty"`
	StorageBackend      string                 `json:"storageBackend,omitempty"`

	CollectionManifestUID string `json:"collectionsManifestUid,omitempty"`

	// These are used for JSON IO, but isn't used for processing
	// since it needs to be swapped out safely.
	VBSMJson  VBucketServerMap `json:"vBucketServerMap"`
	NodesJSON []Node           `json:"nodes"`

	Rev      int            `json:"rev"`
	NodesExt []NodeServices `json:"nodesExt"`

	pool        *Pool
	commonSufix string
}

// PoolServices is all the bucket-independent services in a pool
type PoolServices struct {
	Rev      int            `json:"rev"`
	NodesExt []NodeServices `json:"nodesExt"`
}

// NodeServices is all the bucket-independent services running on
// a node (given by Hostname)
type NodeServices struct {
	Services map[string]int `json:"services,omitempty"`
	Hostname string         `json:"hostname"`
	ThisNode bool           `json:"thisNode"`
}

type ServerGroups struct {
	Groups []ServerGroup `json:"groups"`
}

type ServerGroup struct {
	Name  string `json:"name"`
	Nodes []Node `json:"nodes"`
}

// VBServerMap returns the current VBucketServerMap.
func (b *Bucket) VBServerMap() *VBucketServerMap {
	return (*VBucketServerMap)(atomic.LoadPointer(&(b.vBucketServerMap)))
}

func (b *Bucket) GetVBmap(addrs []string) (map[string][]uint16, error) {
	vbmap := b.VBServerMap()
	servers := vbmap.ServerList
	if addrs == nil {
		addrs = vbmap.ServerList
	}

	m := make(map[string][]uint16)
	for _, addr := range addrs {
		m[addr] = make([]uint16, 0)
	}
	for vbno, idxs := range vbmap.VBucketMap {
		if len(idxs) == 0 {
			return nil, fmt.Errorf("vbmap: No KV node no for vb %d", vbno)
		} else if idxs[0] < 0 || idxs[0] >= len(servers) {
			return nil, fmt.Errorf("vbmap: Invalid KV node no %d for vb %d", idxs[0], vbno)
		}
		addr := servers[idxs[0]]
		if _, ok := m[addr]; ok {
			m[addr] = append(m[addr], uint16(vbno))
		}
	}
	return m, nil
}

// Nodes returns the current list of nodes servicing this bucket.
func (b *Bucket) Nodes() []Node {
	return *(*[]Node)(atomic.LoadPointer(&b.nodeList))
}

func (b *Bucket) getConnPools() []*connectionPool {
	return *(*[]*connectionPool)(atomic.LoadPointer(&b.connPools))
}

func (b *Bucket) replaceConnPools(with []*connectionPool) {
	for {
		old := atomic.LoadPointer(&b.connPools)
		if atomic.CompareAndSwapPointer(&b.connPools, old, unsafe.Pointer(&with)) {
			if old != nil {
				for _, pool := range *(*[]*connectionPool)(old) {
					if pool != nil {
						pool.Close()
					}
				}
			}
			return
		}
	}
}

func (b *Bucket) getConnPool(i int) *connectionPool {

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("bucket(%v) getConnPool crashed: %v\n", b.Name, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if i < 0 {
		return nil
	}

	p := b.getConnPools()
	if len(p) > i {
		return p[i]
	}
	return nil
}

func (b *Bucket) getMasterNode(i int) string {

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("bucket(%v) getMasterNode crashed: %v\n", b.Name, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if i < 0 {
		return ""
	}

	p := b.getConnPools()
	if len(p) > i && p[i] != nil {
		return p[i].host
	}
	return ""
}

func (b *Bucket) authHandler() (ah AuthHandler) {
	if b.pool != nil {
		ah = b.pool.client.ah
	}
	if ah == nil {
		ah = &basicAuth{b.Name, ""}
	}
	return
}

// NodeAddresses gets the (sorted) list of memcached node addresses
// (hostname:port).
func (b *Bucket) NodeAddresses() []string {
	vsm := b.VBServerMap()
	rv := make([]string, len(vsm.ServerList))
	copy(rv, vsm.ServerList)
	sort.Strings(rv)
	return rv
}

// CommonAddressSuffix finds the longest common suffix of all
// host:port strings in the node list.
func (b *Bucket) CommonAddressSuffix() string {
	input := []string{}
	for _, n := range b.Nodes() {
		input = append(input, n.Hostname)
	}
	return FindCommonSuffix(input)
}

// A Client is the starting point for all services across all buckets
// in a Couchbase cluster.
type Client struct {
	BaseURL   *url.URL
	ah        AuthHandler
	Info      Pools
	UserAgent string
}

func (c *Client) SetUserAgent(userAgent string) {
	c.UserAgent = userAgent
}

func maybeAddAuth(req *http.Request, ah AuthHandler) {
	if ah != nil {
		user, pass := ah.GetCredentials()
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
}

// queryRestAPIOnLocalhost is only used for communication with ns_server
// And ns_server communication is always on localhost so skipping TLS for the
// Get calls here. This used only in client.parseURLResponse and bucket.parseURLResponse
func queryRestAPIOnLocalhost(
	baseURL *url.URL,
	path string,
	authHandler AuthHandler,
	out interface{},
	reqParams *security.RequestParams) error {
	u := *baseURL
	u.User = nil
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	res, err := security.GetWithAuthNonTLS(u.String(), reqParams)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	bodyBytes, _ := ioutil.ReadAll(res.Body)
	responseBody := ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	d := json.NewDecoder(responseBody)
	if err = d.Decode(&out); err != nil {
		logging.Errorf("queryRestAPI: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bodyBytes), err)
		return err
	}
	return nil
}

// Pool streaming API based observe-callback wrapper
func (c *Client) RunObservePool(pool string, callb func(interface{}) error, cancel chan bool) error {

	path := "/poolsStreaming/" + pool
	decoder := func(bs []byte) (interface{}, error) {
		var pool Pool
		var err error
		if err = json.Unmarshal(bs, &pool); err != nil {
			logging.Errorf("RunObservePool: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &pool, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

// NodeServices streaming API based observe-callback wrapper
func (c *Client) RunObserveNodeServices(pool string, callb func(interface{}) error, cancel chan bool) error {

	path := "/pools/" + pool + "/nodeServicesStreaming"
	decoder := func(bs []byte) (interface{}, error) {
		var ps PoolServices
		var err error
		if err = json.Unmarshal(bs, &ps); err != nil {
			logging.Errorf("RunObserveNodeServices: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &ps, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

func (c *Client) RunObserveCollectionManifestChanges(pool, bucket string, callb func(interface{}) error, cancel chan bool) error {

	path := "/pools/" + pool + "/bs/" + bucket
	decoder := func(bs []byte) (interface{}, error) {
		var b Bucket
		var err error
		if err = json.Unmarshal(bs, &b); err != nil {
			logging.Errorf("RunObserveCollectionManifestChanges: Error while decoding the response from path: %s, response body: %s, err: %v", path, string(bs), err)
		}
		return &b, err
	}

	return c.runObserveStreamingEndpoint(path, decoder, callb, cancel)
}

// Helper for observing and calling back streaming endpoint
func (c *Client) runObserveStreamingEndpoint(path string,
	decoder func([]byte) (interface{}, error),
	callb func(interface{}) error,
	cancel chan bool) error {

	u := *c.BaseURL
	u.User = nil
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	res, err := security.GetWithAuthNonTLS(u.String(), nil)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	reader := bufio.NewReader(res.Body)
	defer res.Body.Close()
	for {
		if cancel != nil {
			select {
			case <-cancel:
				break
			default:
			}
		}

		bs, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		if len(bs) == 1 && bs[0] == '\n' {
			continue
		}

		object, err := decoder(bs)
		if err != nil {
			return err
		}

		err = callb(object)
		if err != nil {
			return err
		}
	}

	return nil
}

// parseURLResponse is used for communication with ns_server and we always talk to ns_server
// on localhost we use queryRestAPIOnLocalhost which will skip TLS.
func (c *Client) parseURLResponse(path string, out interface{}) error {
	params := &security.RequestParams{
		Timeout:   HttpRequestTimeout,
		UserAgent: c.UserAgent,
	}
	return queryRestAPIOnLocalhost(c.BaseURL, path, c.ah, out, params)

}

// Note: This function is not being used currently. queryRestAPIOnLocalhost in this function does
// not use TLS.
func (b *Bucket) parseURLResponse(path string, out interface{}) error {
	nodes := b.Nodes()
	if len(nodes) == 0 {
		return errors.New("no couch rest URLs")
	}

	// Pick a random node to start querying.
	startNode := rand.Intn(len(nodes))
	maxRetries := len(nodes)
	for i := 0; i < maxRetries; i++ {
		node := nodes[(startNode+i)%len(nodes)] // Wrap around the nodes list.
		// Skip non-healthy nodes.
		if node.Status != "healthy" {
			continue
		}

		url := &url.URL{
			Host:   node.Hostname,
			Scheme: "http",
		}

		err := queryRestAPIOnLocalhost(url, path, b.pool.client.ah, out, &security.RequestParams{Timeout: HttpRequestTimeout})
		if err == nil {
			return err
		}
	}
	return errors.New("all nodes failed to respond")
}

type basicAuth struct {
	u, p string
}

func (b *basicAuth) GetCredentials() (string, string) {
	return b.u, b.p
}

func basicAuthFromURL(us string) (ah AuthHandler) {
	u, err := ParseURL(us)
	if err != nil {
		return
	}
	if user := u.User; user != nil {
		pw, _ := user.Password()
		ah = &basicAuth{user.Username(), pw}
	}
	return
}

// ConnectWithAuth connects to a couchbase cluster with the given
// authentication handler.
func ConnectWithAuth(baseU string, ah AuthHandler) (c Client, err error) {
	c.BaseURL, err = ParseURL(baseU)
	if err != nil {
		return
	}
	c.ah = ah

	return c, c.parseURLResponse("/pools", &c.Info)
}

// Connect to a couchbase cluster.  An authentication handler will be
// created from the userinfo in the URL if provided.
func Connect(baseU string) (Client, error) {
	return ConnectWithAuth(baseU, basicAuthFromURL(baseU))
}

//Get SASL buckets
type BucketInfo struct {
	Name string // name of bucket
}

func GetBucketList(baseU string) (bInfo []BucketInfo, err error) {

	c := &Client{}
	c.BaseURL, err = ParseURL(baseU)
	if err != nil {
		return
	}
	c.ah = basicAuthFromURL(baseU)

	var buckets []Bucket
	err = c.parseURLResponse("/pools/default/buckets", &buckets)
	if err != nil {
		return
	}
	bInfo = make([]BucketInfo, 0)
	for _, bucket := range buckets {
		bucketInfo := BucketInfo{Name: bucket.Name}
		bInfo = append(bInfo, bucketInfo)
	}
	return bInfo, err
}

func (b *Bucket) Refresh() error {
	pool := b.pool
	tmpb := &Bucket{}

	// Unescape the b.URI as it pool.client.parseURLResponse will again escape it.
	bucketURI, err1 := url.PathUnescape(b.URI)
	if err1 != nil {
		return fmt.Errorf("Malformed bucket URI path %v, error %v", b.URI, err1)
	}

	err := pool.client.parseURLResponse(bucketURI, tmpb)
	if err != nil {
		return err
	}
	b.init(tmpb)

	return nil
}

func (b *Bucket) Init(hostport string) {
	connHost, _, _ := net.SplitHostPort(hostport)
	for i := range b.NodesJSON {
		b.NodesJSON[i].Hostname = NormalizeHost(connHost, b.NodesJSON[i].Hostname)
	}

	newcps := make([]*connectionPool, len(b.VBSMJson.ServerList))
	for i := range newcps {
		b.VBSMJson.ServerList[i] = NormalizeHost(connHost, b.VBSMJson.ServerList[i])
		newcps[i] = newConnectionPool(
			b.VBSMJson.ServerList[i],
			b.authHandler(), PoolSize, PoolOverflow)
	}

	for i, ns := range b.NodesExt {
		h := ns.Hostname
		if h == "" {
			h = connHost
		}
		p := fmt.Sprintf("%d", ns.Services["mgmt"])
		hp := net.JoinHostPort(h, p)
		b.NodesExt[i].Hostname = hp
	}

	b.replaceConnPools(newcps)
	atomic.StorePointer(&b.vBucketServerMap, unsafe.Pointer(&b.VBSMJson))
	atomic.StorePointer(&b.nodeList, unsafe.Pointer(&b.NodesJSON))
}

func (b *Bucket) init(nb *Bucket) {
	connHost, _, _ := net.SplitHostPort(b.pool.client.BaseURL.Host)
	for i := range nb.NodesJSON {
		nb.NodesJSON[i].Hostname = NormalizeHost(connHost, nb.NodesJSON[i].Hostname)
	}

	newcps := make([]*connectionPool, len(nb.VBSMJson.ServerList))
	for i := range newcps {
		nb.VBSMJson.ServerList[i] = NormalizeHost(connHost, nb.VBSMJson.ServerList[i])
		newcps[i] = newConnectionPool(
			nb.VBSMJson.ServerList[i],
			b.authHandler(), PoolSize, PoolOverflow)
	}
	b.replaceConnPools(newcps)
	atomic.StorePointer(&b.vBucketServerMap, unsafe.Pointer(&nb.VBSMJson))
	atomic.StorePointer(&b.nodeList, unsafe.Pointer(&nb.NodesJSON))
}

func (p *Pool) getVersion() uint32 {
	// Compute the minimum version among all the nodes
	version := (uint32)(math.MaxUint32)
	for _, n := range p.Nodes {
		v := uint32(n.ClusterCompatibility / 65536)
		if v < version {
			version = v
		}
	}
	return version
}

func (p *Pool) getTerseBucket(bucketn string) (bool, *Bucket, error) {
	retry := false
	nb := &Bucket{}
	err := p.client.parseURLResponse(p.BucketURL["terseBucketsBase"]+bucketn, nb)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		// bucket might have got deleted.
		if strings.Contains(err.Error(), "HTTP error 404") {
			retry = true
			return retry, nil, err
		}
		return retry, nil, err
	}
	return retry, nb, nil
}

// Note: Call this only when cluster version is atleast 7.0
// It is allowed to query the collections endpoint only if all
// the nodes in the cluster are upgraded to 7.0 version or later
func (p *Pool) getCollectionManifest(bucketn string) (retry bool,
	manifest *collections.CollectionManifest, err error) {
	manifest = &collections.CollectionManifest{}
	err = p.client.parseURLResponse("pools/default/buckets/"+bucketn+"/scopes", manifest)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		if strings.Contains(err.Error(), "HTTP error 404") {
			retry = true
			return
		}
		return
	}
	return
}

func (c *Client) GetTerseBucket(terseBucketsBase, bucketn string) (retry bool, nb *Bucket, err error) {
	nb = &Bucket{}
	err = c.parseURLResponse(terseBucketsBase+bucketn, nb)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		// bucket might have got deleted.
		if strings.Contains(err.Error(), "HTTP error 404") {
			retry = true
			return
		}
		return
	}
	return
}

//This function always uses default pool
// Note: Call this only when cluster version is atleast 7.0
// It is allowed to query the collections endpoint only if all
// the nodes in the cluster are upgraded to 7.0 version or later
func (c *Client) GetCollectionManifest(bucketn string) (retry bool,
	manifest *collections.CollectionManifest, err error) {
	manifest = &collections.CollectionManifest{}
	err = c.parseURLResponse("pools/default/buckets/"+bucketn+"/scopes", manifest)
	if err != nil {
		// bucket list is out of sync with cluster bucket list
		if strings.Contains(err.Error(), "HTTP error 404") {
			retry = true
			return
		}
		return
	}
	return
}

func (c *Client) GetIndexScopeLimit(bucketn, scope string) (uint32, error) {
	retryCount := 0
loop:
	retry, manifest, err := c.GetCollectionManifest(bucketn)
	if retry && retryCount <= 5 {
		retryCount++
		logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying for GetIndexScopeLimit..", bucketn)
		time.Sleep(500 * time.Millisecond)
		goto loop
	}
	if err != nil {
		return 0, err
	}
	return manifest.GetIndexScopeLimit(scope), nil
}

// refreshBucket only calls terseBucket endpoint to fetch the bucket info.
func (p *Pool) RefreshBucket(bucketn string, resetBucketMap bool) error {
	if resetBucketMap {
		p.BucketMap = make(map[string]Bucket)
	}
	retryCount := 0
loop:
	retry, nb, err := p.getTerseBucket(bucketn)
	if retry {
		retryCount++
		if retryCount > 5 {
			return err
		}
		logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying to getTerseBucket. retry count %v", bucketn, retryCount)
		time.Sleep(5 * time.Millisecond)
		goto loop
	}
	if err != nil {
		return err
	}
	nb.pool = p
	nb.init(nb)
	p.BucketMap[nb.Name] = *nb

	return nil
}

// Refresh calls pools/default/buckets to get data and list of buckets
// calls terseBucket and scopes endpoint for bucket info and manifest.
func (p *Pool) Refresh() (err error) {
	p.BucketMap = make(map[string]Bucket)
	p.Manifest = make(map[string]*collections.CollectionManifest)

	// Compute the minimum version among all the nodes
	version := p.getVersion()

loop:
	buckets := []Bucket{}
	err = p.client.parseURLResponse(p.BucketURL["uri"], &buckets)
	if err != nil {
		return err
	}
	for _, b := range buckets {
		retry, nb, err := p.getTerseBucket(b.Name)
		if retry {
			logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying for getTerseBucket..", b.Name)
			time.Sleep(5 * time.Millisecond)
			goto loop
		}
		if err != nil {
			return err
		}
		b.pool = p
		b.init(nb)
		p.BucketMap[b.Name] = b

		if version >= 7 {
			retry, manifest, err := p.getCollectionManifest(b.Name)
			if retry {
				logging.Warnf("cluster_info: Out of sync for bucket %s. Retrying for getBucketManifest..", b.Name)
				time.Sleep(5 * time.Millisecond)
				goto loop
			}
			if err != nil {
				return err
			}
			p.Manifest[b.Name] = manifest
		}
	}

	return nil
}

func (p *Pool) RefreshManifest(bucket string, resetManifestMap bool) error {
	retryCount := 0
	if resetManifestMap {
		p.Manifest = make(map[string]*collections.CollectionManifest)
	}
	// Compute the minimum version among all the nodes
	version := p.getVersion()
retry:
	if version >= 7 {
		retry, manifest, err := p.getCollectionManifest(bucket)
		if retry && retryCount <= 5 {
			retryCount++
			logging.Warnf("cluster_info: Retrying to getBucketManifest for bucket %s", bucket)
			time.Sleep(1 * time.Millisecond)
			goto retry
		}
		if err != nil {
			return err
		}
		p.Manifest[bucket] = manifest
	}
	return nil
}

func (p *Pool) GetServerGroups() (groups ServerGroups, err error) {

	// ServerGroupsUri is empty in CE
	if p.ServerGroupsUri == "" {
		return ServerGroups{}, nil
	}

	err = p.client.parseURLResponse(p.ServerGroupsUri, &groups)
	return

}

func getVersionHashFromURL(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("Unable to parse URL: %v", urlStr)
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("Unable to extract version hash from URL: %v", urlStr)
	}
	return m["v"][0], nil
}

// GetBucketURLVersionHash will parse the p.BucketURI and extract version hash from it
// Parses /pools/default/buckets?v=<$ver>&uuid=<$uid> and returns $ver
func (p *Pool) GetBucketURLVersionHash() (string, error) {
	b := p.BucketURL["uri"]
	return getVersionHashFromURL(b)
}

func (p *Pool) GetServerGroupsVersionHash() (string, error) {

	// ServerGroupsUri is empty in CE
	if p.ServerGroupsUri == "" {
		return "", nil
	}

	return getVersionHashFromURL(p.ServerGroupsUri)
}

// GetPoolWithBucket gets a pool from pool URI and calls ns_server terseBucket endpoint
// API to get info for only given bucket
func (c *Client) GetPoolWithBucket(name string, bucketn string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}

	if err = c.parseURLResponse(poolURI, &p); err != nil {
		return
	}

	p.client = *c

	err = p.RefreshBucket(bucketn, true)
	return
}

func (c *Client) GetPoolWithoutRefresh(name string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}

	if err = c.parseURLResponse(poolURI, &p); err != nil {
		return
	}

	p.client = *c
	return
}

// GetPool gets a pool from within the couchbase cluster (usually
// "default").
func (c *Client) GetPool(name string) (p Pool, err error) {
	var poolURI string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolURI = p.URI
		}
	}
	if poolURI == "" {
		return p, errors.New("No pool named " + name)
	}

	if err = c.parseURLResponse(poolURI, &p); err != nil {
		return
	}

	p.client = *c

	err = p.Refresh()
	return
}

// GetPoolServices returns all the bucket-independent services in a pool.
// (See "Exposing services outside of bucket context" in http://goo.gl/uuXRkV)
func (c *Client) GetPoolServices(name string) (ps PoolServices, err error) {
	var poolName string
	for _, p := range c.Info.Pools {
		if p.Name == name {
			poolName = p.Name
		}
	}
	if poolName == "" {
		return ps, errors.New("No pool named " + name)
	}

	poolURI := "/pools/" + poolName + "/nodeServices"
	err = c.parseURLResponse(poolURI, &ps)

	return
}

// Close marks this bucket as no longer needed, closing connections it
// may have open.
func (b *Bucket) Close() {
	if b.connPools != nil {
		for _, c := range b.getConnPools() {
			if c != nil {
				c.Close()
			}
		}
		b.connPools = nil
	}
}

func bucketFinalizer(b *Bucket) {
	if b.connPools != nil {
		logging.Warnf("Warning: Finalizing a bucket with active connections.")
	}
}

// GetBucket gets a bucket from within this pool.
func (p *Pool) GetBucket(name string) (*Bucket, error) {
	rv, ok := p.BucketMap[name]
	if !ok {
		return nil, errors.New("No bucket named " + name)
	}
	runtime.SetFinalizer(&rv, bucketFinalizer)
	return &rv, nil
}

func (p *Pool) GetCollectionID(bucket, scope, collection string) string {
	if manifest, ok := p.Manifest[bucket]; ok {
		return manifest.GetCollectionID(scope, collection)
	}
	return collections.COLLECTION_ID_NIL
}

func (p *Pool) GetScopeID(bucket, scope string) string {
	if manifest, ok := p.Manifest[bucket]; ok {
		return manifest.GetScopeID(scope)
	}
	return collections.SCOPE_ID_NIL
}

func (p *Pool) GetScopeAndCollectionID(bucket, scope, collection string) (string, string) {
	if manifest, ok := p.Manifest[bucket]; ok {
		return manifest.GetScopeAndCollectionID(scope, collection)
	}
	return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL
}

// GetPool gets the pool to which this bucket belongs.
func (b *Bucket) GetPool() *Pool {
	return b.pool
}

// GetClient gets the client from which we got this pool.
func (p *Pool) GetClient() *Client {
	return &p.client
}

// GetBucket is a convenience function for getting a named bucket from
// a URL
func GetBucket(endpoint, poolname, bucketname string) (*Bucket, error) {
	var err error
	client, err := Connect(endpoint)
	if err != nil {
		return nil, err
	}

	pool, err := client.GetPool(poolname)
	if err != nil {
		return nil, err
	}

	return pool.GetBucket(bucketname)
}

// Make hostnames comparable for terse-buckets info and old buckets info
func NormalizeHost(ch, h string) string {
	host, port, _ := net.SplitHostPort(h)
	if host == "$HOST" {
		host = ch
	}
	return net.JoinHostPort(host, port)
}

func (b *Bucket) GetDcpConn(name DcpFeedName, host string) (*memcached.Client, error) {
	for _, sconn := range b.getConnPools() {
		if sconn.host == host {
			return sconn.GetDcpConn(name)
		}
	}

	return nil, fmt.Errorf("no pool found")
}

func (b *Bucket) GetMcConn(host string) (*memcached.Client, error) {
	for _, sconn := range b.getConnPools() {
		if sconn.host == host {
			// Get connection without doing DCP_OPEN
			mc, err := sconn.Get()
			if err != nil {
				return nil, err
			}
			return mc, nil
		}
	}

	return nil, fmt.Errorf("no pool found")
}
