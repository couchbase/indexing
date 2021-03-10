package common

import (
	"errors"
	"expvar"
	"fmt"
	"hash/crc64"
	"io"
	"io/ioutil"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/cbauthimpl"
	"github.com/couchbase/indexing/secondary/common/collections"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

const IndexNamePattern = "^[A-Za-z0-9#_-]+$"

const (
	MAX_AUTH_RETRIES = 10
)

var ErrInvalidIndexName = fmt.Errorf("Invalid index name")

// ExcludeStrings will exclude strings in `excludes` from `strs`. preserves the
// order of `strs` in the result.
func ExcludeStrings(strs []string, excludes []string) []string {
	cache := make(map[string]bool)
	for _, s := range excludes {
		cache[s] = true
	}
	ss := make([]string, 0, len(strs))
	for _, s := range strs {
		if _, ok := cache[s]; ok == false {
			ss = append(ss, s)
		}
	}
	return ss
}

// CommonStrings returns intersection of two set of strings.
func CommonStrings(xs []string, ys []string) []string {
	ss := make([]string, 0, len(xs))
	cache := make(map[string]bool)
	for _, x := range xs {
		cache[x] = true
	}
	for _, y := range ys {
		if _, ok := cache[y]; ok {
			ss = append(ss, y)
		}
	}
	return ss
}

// HasString does membership check for a string.
func HasString(str string, strs []string) bool {
	for _, s := range strs {
		if str == s {
			return true
		}
	}
	return false
}

// ExcludeUint32 remove items from list.
func ExcludeUint32(xs []uint32, from []uint32) []uint32 {
	fromSubXs := make([]uint32, 0, len(from))
	for _, num := range from {
		if HasUint32(num, xs) == false {
			fromSubXs = append(fromSubXs, num)
		}
	}
	return fromSubXs
}

// ExcludeUint64 remove items from list.
func ExcludeUint64(xs []uint64, from []uint64) []uint64 {
	fromSubXs := make([]uint64, 0, len(from))
	for _, num := range from {
		if HasUint64(num, xs) == false {
			fromSubXs = append(fromSubXs, num)
		}
	}
	return fromSubXs
}

// RemoveUint32 delete `item` from list `xs`.
func RemoveUint32(item uint32, xs []uint32) []uint32 {
	ys := make([]uint32, 0, len(xs))
	for _, x := range xs {
		if x == item {
			continue
		}
		ys = append(ys, x)
	}
	return ys
}

// RemoveUint16 delete `item` from list `xs`.
func RemoveUint16(item uint16, xs []uint16) []uint16 {
	ys := make([]uint16, 0, len(xs))
	for _, x := range xs {
		if x == item {
			continue
		}
		ys = append(ys, x)
	}
	return ys
}

// RemoveString delete `item` from list `xs`.
func RemoveString(item string, xs []string) []string {
	ys := make([]string, 0, len(xs))
	for _, x := range xs {
		if x == item {
			continue
		}
		ys = append(ys, x)
	}
	return ys
}

// HasUint32 does membership check for a uint32 integer.
func HasUint32(item uint32, xs []uint32) bool {
	for _, x := range xs {
		if x == item {
			return true
		}
	}
	return false
}

// HasUint64 does membership check for a uint32 integer.
func HasUint64(item uint64, xs []uint64) bool {
	for _, x := range xs {
		if x == item {
			return true
		}
	}
	return false
}

// FailsafeOp can be used by gen-server implementors to avoid infinitely
// blocked API calls.
func FailsafeOp(
	reqch, respch chan []interface{},
	cmd []interface{},
	finch chan bool) ([]interface{}, error) {

	select {
	case reqch <- cmd:
		if respch != nil {
			select {
			case resp := <-respch:
				return resp, nil
			case <-finch:
				return nil, ErrorClosed
			}
		}
	case <-finch:
		return nil, ErrorClosed
	}
	return nil, nil
}

// FailsafeOpAsync is same as FailsafeOp that can be used for
// asynchronous operation, that is, caller does not wait for response.
func FailsafeOpAsync(
	reqch chan []interface{}, cmd []interface{}, finch chan bool) error {

	select {
	case reqch <- cmd:
	case <-finch:
		return ErrorClosed
	}
	return nil
}

// FailsafeOpAsync2 is same as FailsafeOpAsync that can be used for
// asynchronous operation, that is, caller does not wait for response.
// This method aborts send if abortCh is closed on callers side
func FailsafeOpAsync2(
	reqch chan []interface{}, cmd []interface{}, finch, abortCh chan bool) error {

	select {
	case reqch <- cmd:
	case <-finch:
		return ErrorClosed
	case <-abortCh:
		return ErrorAborted
	}
	return nil
}

// FailsafeOpNoblock is same as FailsafeOpAsync that can be used for
// non-blocking operation, that is, if `reqch` is full caller does not block.
func FailsafeOpNoblock(
	reqch chan []interface{}, cmd []interface{}, finch chan bool) error {

	select {
	case reqch <- cmd:
	case <-finch:
		return ErrorClosed
	default:
		return ErrorChannelFull
	}
	return nil
}

// OpError suppliments FailsafeOp used by gen-servers.
func OpError(err error, vals []interface{}, idx int) error {
	if err != nil {
		return err
	} else if vals != nil {
		if vals[idx] != nil {
			return vals[idx].(error)
		} else {
			return nil
		}
	}
	return nil
}

// cbauth admin authentication helper
// Uses default cbauth env variables internally to provide auth creds
type CbAuthHandler struct {
	Hostport string
	Bucket   string
}

func (ah *CbAuthHandler) GetCredentials() (string, string) {

	var u, p string

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("CbAuthHandler::GetCredentials error=%v Retrying (%d)", err, r)
		}

		u, p, err = cbauth.GetHTTPServiceAuth(ah.Hostport)
		return err
	}

	rh := NewRetryHelper(MAX_AUTH_RETRIES, time.Second, 2, fn)
	err := rh.Run()
	if err != nil {
		panic(err)
	}

	return u, p
}

func (ah *CbAuthHandler) AuthenticateMemcachedConn(host string, conn *memcached.Client) error {

	var u, p string

	fn := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("CbAuthHandler::AuthenticateMemcachedConn error=%v Retrying (%d)", err, r)
		}

		u, p, err = cbauth.GetMemcachedServiceAuth(host)
		return err
	}

	rh := NewRetryHelper(MAX_AUTH_RETRIES, time.Second*3, 1, fn)
	err := rh.Run()
	if err != nil {
		return err
	}

	_, err = conn.Auth(u, p)
	_, err = conn.SelectBucket(ah.Bucket)
	return err
}

// GetKVAddrs gather the list of kvnode-address based on the latest vbmap.
func GetKVAddrs(cluster, pooln, bucketn string) ([]string, error) {
	b, err := ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		return nil, err
	}
	defer b.Close()

	if err := b.Refresh(); err != nil {
		logging.Errorf("GetKVAddrs, error during bucket.Refresh() for bucket: %v, err: %v", b.Name, err)
		return nil, err
	}

	m, err := b.GetVBmap(nil)
	if err != nil {
		return nil, err
	}

	kvaddrs := make([]string, 0, len(m))
	for kvaddr := range m {
		kvaddrs = append(kvaddrs, kvaddr)
	}
	return kvaddrs, nil
}

// IsIPLocal return whether `ip` address is loopback address or
// compares equal with local-IP-address.
func IsIPLocal(ip string) bool {
	netIP := net.ParseIP(ip)

	// if loopback address, return true
	if netIP.IsLoopback() {
		return true
	}

	// compare with the local ip
	if localIP, err := GetLocalIP(); err == nil {
		if localIP.Equal(netIP) {
			return true
		}
	}
	return false
}

// GetLocalIP return the first external-IP4 configured for the first
// interface connected to this node.
func GetLocalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range interfaces {
		if (iface.Flags & net.FlagUp) == 0 {
			continue // interface down
		}
		if (iface.Flags & net.FlagLoopback) != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && !ip.IsLoopback() {
				if ip = ip.To4(); ip != nil {
					return ip, nil
				}
			}
		}
	}
	return nil, errors.New("cannot find local IP address")
}

// ExitOnStdinClose is exit handler to be used with ns-server.
func ExitOnStdinClose() {
	buf := make([]byte, 4)
	for {
		_, err := os.Stdin.Read(buf)
		if err != nil {
			if err == io.EOF {
				time.Sleep(1 * time.Second)
				os.Exit(0)
			}

			panic(fmt.Sprintf("Stdin: Unexpected error occured %v", err))
		}
	}
}

// GetColocatedHost find the server addr for localhost and return the same.
func GetColocatedHost(cluster string) (string, error) {
	// get vbmap from bucket connection.
	bucket, err := ConnectBucket(cluster, "default", "default")
	if err != nil {
		return "", err
	}
	defer bucket.Close()

	hostports := bucket.NodeAddresses()
	serversM := make(map[string]string)
	servers := make([]string, 0)
	for _, hostport := range hostports {
		host, _, err := net.SplitHostPort(hostport)
		if err != nil {
			return "", err
		}
		serversM[host] = hostport
		servers = append(servers, host)
	}

	for _, server := range servers {
		addrs, err := net.LookupIP(server)
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			if IsIPLocal(addr.String()) {
				return serversM[server], nil
			}
		}
	}
	return "", errors.New("unknown host")
}

func CrashOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func ClusterAuthUrl(cluster string) (string, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return "", err
		}
		cluster = u.Host
	}

	adminUser, adminPasswd, err := cbauth.GetHTTPServiceAuth(cluster)
	if err != nil {
		return "", err
	}

	clusterUrl := url.URL{
		Scheme: "http",
		Host:   cluster,
		User:   url.UserPassword(adminUser, adminPasswd),
	}

	return clusterUrl.String(), nil
}

func ClusterUrl(cluster string) string {
	host := cluster
	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			panic(err) // TODO: should we panic ?
		}
		host = u.Host
	}
	clusterUrl := url.URL{
		Scheme: "http",
		Host:   host,
	}

	return clusterUrl.String()
}

func MaybeSetEnv(key, value string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	os.Setenv(key, value)
	return value
}

func EquivalentIP(
	raddr string,
	raddrs []string) (this string, other string, err error) {

	host, port, err := net.SplitHostPort(raddr)
	if err != nil {
		return "", "", err
	}

	if host == "localhost" {
		host = GetLocalIpAddr(IsIpv6())
	}

	netIP := net.ParseIP(host)

	for _, raddr1 := range raddrs {
		host1, port1, err := net.SplitHostPort(raddr1)
		if err != nil {
			return "", "", err
		}

		if host1 == "localhost" {
			host1 = GetLocalIpAddr(IsIpv6())
		}
		netIP1 := net.ParseIP(host1)
		// check whether ports are same.
		if port != port1 {
			continue
		}
		// check whether both are local-ip.
		if IsIPLocal(host) && IsIPLocal(host1) {
			return net.JoinHostPort(host, port),
				net.JoinHostPort(host1, port), nil // raddr => raddr1
		}
		// check whether they are coming from the same remote.
		if netIP.Equal(netIP1) {
			return net.JoinHostPort(host, port),
				net.JoinHostPort(host1, port1), nil // raddr == raddr1
		}
	}
	return net.JoinHostPort(host, port),
		net.JoinHostPort(host, port), nil
}

//---------------------
// SDK bucket operation
//---------------------

// ConnectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func ConnectBucket(cluster, pooln, bucketn string) (*couchbase.Bucket, error) {
	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return nil, err
		}
		cluster = u.Host
	}

	ah := &CbAuthHandler{
		Hostport: cluster,
		Bucket:   bucketn,
	}

	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		return nil, err
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		return nil, err
	}
	return bucket, err
}

// ConnectBucket2 will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket. It also returns clusterVersion
func ConnectBucket2(cluster, pooln, bucketn string) (*couchbase.Bucket,
	int, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return nil, 0, err
		}
		cluster = u.Host
	}

	ah := &CbAuthHandler{
		Hostport: cluster,
		Bucket:   bucketn,
	}

	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		return nil, 0, err
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		return nil, 0, err
	}

	clusterCompat := math.MaxInt32
	for _, node := range pool.Nodes {
		if node.ClusterCompatibility < clusterCompat {
			clusterCompat = node.ClusterCompatibility
		}
	}

	clusterVersion := 0
	if clusterCompat != math.MaxInt32 {
		version := clusterCompat / 65536
		minorVersion := clusterCompat - (version * 65536)
		clusterVersion = int(GetVersion(uint32(version), uint32(minorVersion)))
	}

	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		return nil, 0, err
	}
	return bucket, clusterVersion, err
}

// MaxVbuckets return the number of vbuckets in bucket.
func MaxVbuckets(bucket *couchbase.Bucket) (int, error) {
	count := 0
	m, err := bucket.GetVBmap(nil)
	if err == nil {
		for _, vbnos := range m {
			count += len(vbnos)
		}
	}
	return count, err
}

// BucketTs return bucket timestamp for all vbucket.
func BucketTs(bucket *couchbase.Bucket, maxvb int) (seqnos, vbuuids []uint64, err error) {
	seqnos = make([]uint64, maxvb)
	vbuuids = make([]uint64, maxvb)
	stats, err := bucket.GetStats("vbucket-details")
	// for all nodes in cluster
	for _, nodestat := range stats {
		// for all vbuckets
		for i := 0; i < maxvb; i++ {
			vbno_str := strconv.Itoa(i)
			vbstatekey := "vb_" + vbno_str
			vbhseqkey := "vb_" + vbno_str + ":high_seqno"
			vbuuidkey := "vb_" + vbno_str + ":uuid"
			vbstate, ok := nodestat[vbstatekey]
			highseqno_s, hseq_ok := nodestat[vbhseqkey]
			vbuuid_s, uuid_ok := nodestat[vbuuidkey]
			if ok && hseq_ok && uuid_ok && vbstate == "active" {
				if uuid, err := strconv.ParseUint(vbuuid_s, 10, 64); err == nil {
					vbuuids[i] = uuid
				}
				if s, err := strconv.ParseUint(highseqno_s, 10, 64); err == nil {
					if s > seqnos[i] {
						seqnos[i] = s
					}
				}
			}
		}
	}
	return seqnos, vbuuids, err
}

func IsAuthValid(r *http.Request) (cbauth.Creds, bool, error) {

	creds, err := cbauth.AuthWebCreds(r)
	if err != nil {
		if strings.Contains(err.Error(), cbauthimpl.ErrNoAuth.Error()) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return creds, true, nil
}

func SetNumCPUs(percent int) int {
	ncpu := percent / 100
	if ncpu == 0 {
		ncpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(ncpu)
	return ncpu
}

func IndexStatement(def IndexDefn, numPartitions int, numReplica int, printNodes bool) string {
	var stmt string
	primCreate := "CREATE PRIMARY INDEX `%s` ON `%s`"
	primCreateCollection := "CREATE PRIMARY INDEX `%s` ON `%s`.`%s`.`%s`"
	secCreate := "CREATE INDEX `%s` ON `%s`(%s)"
	secCreateCollection := "CREATE INDEX `%s` ON `%s`.`%s`.`%s`(%s)"
	where := " WHERE %s"
	partition := " PARTITION BY hash(%s)"

	getPartnStmt := func() string {
		if len(def.PartitionKeys) > 0 {
			exprs := ""
			for _, exp := range def.PartitionKeys {
				if exprs != "" {
					exprs += ","
				}
				exprs += exp
			}
			return fmt.Sprintf(partition, exprs)
		}
		return ""
	}

	if def.IsPrimary {
		if def.IndexOnCollection() {
			stmt = fmt.Sprintf(primCreateCollection, def.Name, def.Bucket, def.Scope, def.Collection)
		} else {
			stmt = fmt.Sprintf(primCreate, def.Name, def.Bucket)
		}

		stmt += getPartnStmt()

	} else {
		exprs := ""
		for i, exp := range def.SecExprs {
			if exprs != "" {
				exprs += ","
			}
			exprs += exp
			if def.Desc != nil && def.Desc[i] {
				exprs += " DESC"
			}
		}

		if def.IndexOnCollection() {
			stmt = fmt.Sprintf(secCreateCollection, def.Name, def.Bucket, def.Scope, def.Collection, exprs)
		} else {
			stmt = fmt.Sprintf(secCreate, def.Name, def.Bucket, exprs)
		}

		stmt += getPartnStmt()

		if def.WhereExpr != "" {
			stmt += fmt.Sprintf(where, def.WhereExpr)
		}
	}

	withExpr := ""
	/*
		if def.Immutable {
			withExpr += "\"immutable\":true"
		}
	*/

	if def.Deferred {
		if len(withExpr) != 0 {
			withExpr += ","
		}

		withExpr += " \"defer_build\":true"
	}

	if def.RetainDeletedXATTR {
		if len(withExpr) != 0 {
			withExpr += ","
		}

		withExpr += " \"retain_deleted_xattr\":true"
	}

	if printNodes && len(def.Nodes) != 0 {
		if len(withExpr) != 0 {
			withExpr += ","
		}
		withExpr += " \"nodes\":[ "

		for i, node := range def.Nodes {
			withExpr += "\"" + node + "\""
			if i < len(def.Nodes)-1 {
				withExpr += ","
			}
		}

		withExpr += " ]"
	}

	if numReplica == -1 {
		numReplica = def.GetNumReplica()
	}

	if numReplica != 0 {
		if len(withExpr) != 0 {
			withExpr += ","
		}

		withExpr += fmt.Sprintf(" \"num_replica\":%v", numReplica)
	}

	if IsPartitioned(def.PartitionScheme) {
		if len(withExpr) != 0 {
			withExpr += ","
		}

		withExpr += fmt.Sprintf(" \"num_partition\":%v", numPartitions)
	}

	if len(withExpr) != 0 {
		stmt += fmt.Sprintf(" WITH { %s }", withExpr)
	}

	return stmt
}

func LogRuntime() string {
	n := runtime.NumCPU()
	v := runtime.Version()
	m := runtime.GOMAXPROCS(-1)
	fmsg := "%v %v; cpus: %v; GOMAXPROCS: %v; version: %v"
	return fmt.Sprintf(fmsg, runtime.GOARCH, runtime.GOOS, n, m, v)
}

func LogOs() string {
	gid := os.Getgid()
	uid := os.Getuid()
	hostname, _ := os.Hostname()
	return fmt.Sprintf("uid: %v; gid: %v; hostname: %v", uid, gid, hostname)
}

//
// This method fetch the bucket UUID.  If this method return an error,
// then it means that the node is not able to connect in order to fetch
// bucket UUID.
//
func GetBucketUUID(cluster, bucket string) (string, error) {

	url, err := ClusterAuthUrl(cluster)
	if err != nil {
		return BUCKET_UUID_NIL, err
	}

	cinfo, err := NewClusterInfoCache(url, "default")
	if err != nil {
		return BUCKET_UUID_NIL, err
	}
	cinfo.SetUserAgent("GetBucketUUID")

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		return BUCKET_UUID_NIL, err
	}

	return cinfo.GetBucketUUID(bucket), nil
}

//
// This method will fetch the collectionID.  If this method returns an error,
// then it means that the node is not able to connect in order to fetch
// the collectionID
func GetCollectionID(cluster, bucket, scope, collection string) (string, error) {
	url, err := ClusterAuthUrl(cluster)
	if err != nil {
		return collections.COLLECTION_ID_NIL, err
	}

	cinfo, err := NewClusterInfoCache(url, "default")
	if err != nil {
		return collections.COLLECTION_ID_NIL, err
	}
	cinfo.SetUserAgent("GetCollectionID")

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		return collections.COLLECTION_ID_NIL, err
	}

	return cinfo.GetCollectionID(bucket, scope, collection), nil
}

//
// This method will fetch the scopeID.  If this method returns an error,
// then it means that the node is not able to connect in order to fetch
//
func GetScopeID(cluster, bucket, scope string) (string, error) {
	url, err := ClusterAuthUrl(cluster)
	if err != nil {
		return collections.SCOPE_ID_NIL, err
	}

	cinfo, err := NewClusterInfoCache(url, "default")
	if err != nil {
		return collections.SCOPE_ID_NIL, err
	}

	cinfo.SetUserAgent("GetScopeID")
	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		return collections.SCOPE_ID_NIL, err
	}

	return cinfo.GetScopeID(bucket, scope), nil
}

//
// This method will fetch the scope and collection ID.  If this method
// returns an error, then it means that the node is not able to connect
// in order to fetch
//
func GetScopeAndCollectionID(cluster, bucket, scope, collection string) (string, string, error) {
	url, err := ClusterAuthUrl(cluster)
	if err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
	}

	cinfo, err := NewClusterInfoCache(url, "default")
	if err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
	}

	cinfo.SetUserAgent("GetScopeAndCollectionID")

	cinfo.Lock()
	defer cinfo.Unlock()

	if err := cinfo.Fetch(); err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
	}

	sid, cid := cinfo.GetScopeAndCollectionID(bucket, scope, collection)
	return sid, cid, nil
}

func FileSize(name string) (int64, error) {
	f, err := os.Open(name)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

// HashVbuuid return crc64 value of list of 64-bit vbuuids.
func HashVbuuid(vbuuids []uint64) uint64 {
	var bytes []byte
	vbuuids_sl := (*reflect.SliceHeader)(unsafe.Pointer(&vbuuids))
	bytes_sl := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	bytes_sl.Data = vbuuids_sl.Data
	bytes_sl.Len = vbuuids_sl.Len * 8
	bytes_sl.Cap = vbuuids_sl.Cap * 8
	return crc64.Checksum(bytes, crc64.MakeTable(crc64.ECMA))
}

func IsValidIndexName(n string) error {
	valid, _ := regexp.MatchString(IndexNamePattern, n)
	if !valid {
		return ErrInvalidIndexName
	}

	return nil
}

func ComputeAvg(lastAvg, lastValue, currValue int64) int64 {
	if lastValue == 0 {
		return 0
	}

	diff := currValue - lastValue
	// Compute avg for first time
	if lastAvg == 0 {
		return diff
	}

	return (diff + lastAvg) / 2
}

// Write to the admin console
func Console(clusterAddr string, format string, v ...interface{}) error {
	msg := fmt.Sprintf(format, v...)
	values := url.Values{"message": {msg}, "logLevel": {"info"}, "component": {"indexing"}}
	reader := strings.NewReader(values.Encode())

	if !strings.HasPrefix(clusterAddr, "http://") {
		clusterAddr = "http://" + clusterAddr
	}
	clusterAddr += "/_log"

	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}
	res, err := security.PostWithAuth(clusterAddr, "application/x-www-form-urlencoded", reader, params)
	if err == nil {
		ioutil.ReadAll(res.Body)
		res.Body.Close()
	}

	return err
}

func CopyFile(dest, source string) (err error) {
	var sf, df *os.File

	defer func() {
		if sf != nil {
			sf.Close()
		}
		if df != nil {
			df.Close()
		}
	}()

	if sf, err = os.Open(source); err != nil {
		return err
	} else if IsPathExist(dest) {
		return nil
	} else if df, err = os.Create(dest); err != nil {
		return err
	} else if _, err = io.Copy(df, sf); err != nil {
		return err
	}

	var info os.FileInfo
	if info, err = os.Stat(source); err != nil {
		return err
	} else if err = os.Chmod(dest, info.Mode()); err != nil {
		return err
	}
	return
}

// CopyDir compose destination path based on source and,
//   - if dest is file, and path is reachable, it is a no-op.
//   - if dest is file, and path is not reachable, create and copy.
//   - if dest is dir, and path is reachable, recurse into the dir.
//   - if dest is dir, and path is not reachable, create and recurse into the dir.
func CopyDir(dest, source string) error {
	var created bool

	if fi, err := os.Stat(source); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("source not a directory")
	} else if IsPathExist(dest) == false {
		created = true
		if err := os.MkdirAll(dest, fi.Mode()); err != nil {
			return err
		}
	}

	var err error
	defer func() {
		// if copy failed in the middle and directory was created by us, clean.
		if err != nil && created {
			os.RemoveAll(dest)
		}
	}()

	var entries []os.FileInfo
	if entries, err = ioutil.ReadDir(source); err != nil {
		return err
	} else {
		for _, entry := range entries {
			s := filepath.Join(source, entry.Name())
			d := filepath.Join(dest, entry.Name())
			if entry.IsDir() {
				if err = CopyDir(d, s); err != nil {
					return err
				}
			} else if err = CopyFile(d, s); err != nil {
				return err
			}
		}
	}
	return nil
}

func IsPathExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return !os.IsNotExist(err)
	}
	return true
}

func DiskUsage(dir string) (int64, error) {
	var sz int64
	err := filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !fi.IsDir() {
			sz += fi.Size()
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return sz, nil
}

func GenNextBiggerKey(b []byte, isPrimary bool) []byte {
	var x big.Int
	if !isPrimary {
		// Remove last 1 byte terminator encoding
		x.SetBytes(b[:len(b)-1])
	} else {
		x.SetBytes(b[:len(b)])
	}
	x.Add(&x, big.NewInt(1))
	return x.Bytes()
}

func IsAllowed(creds cbauth.Creds, permissions []string, w http.ResponseWriter) bool {

	allow := false
	err := error(nil)

	for _, permission := range permissions {
		allow, err = creds.IsAllowed(permission)
		if allow && err == nil {
			break
		}
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return false
	}

	if !allow {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(http.StatusText(http.StatusUnauthorized)))
		return false
	}

	return true
}

func IsAllAllowed(creds cbauth.Creds, permissions []string, w http.ResponseWriter) bool {

	allow := true
	err := error(nil)

	for _, permission := range permissions {
		allow, err = creds.IsAllowed(permission)
		if !allow || err != nil {
			break
		}
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return false
	}

	if !allow {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(http.StatusText(http.StatusUnauthorized)))
		return false
	}

	return true
}

func ComputePercent(a, b int64) int64 {
	if a+b > 0 {
		return a * 100 / (a + b)
	}

	return 0
}

func SetIpv6(isIpv6 bool) {
	security.SetIpv6(isIpv6)
}

func IsIpv6() bool {
	return security.IsIpv6()
}

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	_, valid, err := IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

func GetHTTPReqInfo(r *http.Request) string {
	return fmt.Sprintf("Method %v, Host %v, ContentLength %v, UserAgent %v, RemoteAddr %v",
		r.Method, r.Host, r.ContentLength, r.UserAgent(), r.RemoteAddr)
}

func GrHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	hndlr := pprof.Handler("goroutine")
	hndlr.ServeHTTP(rw, r)
}

func BlockHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	hndlr := pprof.Handler("block")
	hndlr.ServeHTTP(rw, r)
}

func HeapHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	hndlr := pprof.Handler("heap")
	hndlr.ServeHTTP(rw, r)
}

func TCHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	hndlr := pprof.Handler("threadcreate")
	hndlr.ServeHTTP(rw, r)
}

func PProfHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	pprof.Index(rw, r)
}

func ProfileHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	pprof.Profile(rw, r)
}

func CmdlineHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	pprof.Cmdline(rw, r)
}

func SymbolHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	pprof.Symbol(rw, r)
}

func TraceHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	pprof.Trace(rw, r)
}

func ExpvarHandler(rw http.ResponseWriter, r *http.Request) {

	valid := validateAuth(rw, r)
	if !valid {
		return
	}

	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(rw, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(rw, ",\n")
		}
		first = false
		fmt.Fprintf(rw, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(rw, "\n}\n")
}

func GetCollectionDefaults(scope, collection string) (string, string) {
	if scope == "" {
		scope = DEFAULT_SCOPE
	}

	if collection == "" {
		collection = DEFAULT_COLLECTION
	}

	return scope, collection
}

// Mapping of major and minor versions to indexer's
// internal representation of server versions
func GetVersion(version, minorVersion uint32) uint64 {
	if version < 5 {
		return INDEXER_45_VERSION
	}
	if version == 5 {
		if minorVersion < 5 {
			return INDEXER_50_VERSION
		}
		if minorVersion >= 5 {
			return INDEXER_55_VERSION
		}
	}
	if version == 6 {
		if minorVersion >= 5 {
			return INDEXER_65_VERSION
		}
		return INDEXER_55_VERSION // For 6.0, return 5.5 as indexer version
	}
	if version == 7 {
		return INDEXER_70_VERSION
	}
	return INDEXER_CUR_VERSION
}
