package common

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"hash/crc32"
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
	"sync"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/cbauthimpl"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/golang/snappy"
)

const IndexNamePattern = "^[A-Za-z0-9#_-]+$"
const FLAG_COMPRESSED = byte(1)             // compressed flag
const DEFAULT_BIN_SIZE = 2560 * 1024 * 1024 // 2.5G

const (
	MAX_AUTH_RETRIES = 10
)

var ErrInvalidIndexName = fmt.Errorf("Invalid index name")

// Create the 2KB ECMA input table for crc64 checksums once ever,
// instead of every time a checksum is computed.
var crc64ECMA *crc64.Table

func init() {
	crc64ECMA = crc64.MakeTable(crc64.ECMA)
}

// Crc64Checksum computes the CRC64 checksum of a byte
// slice using the highly recommended ECMA polynomial.
func Crc64Checksum(bytes []byte) uint64 {
	return crc64.Checksum(bytes, crc64ECMA)
}

// Crc64Update incrementally updates an existing CRC64 checksum with more bytes.
// The order in which original checksum and updates are made affects the result.
func Crc64Update(checksum uint64, bytes []byte) uint64 {
	return crc64.Update(checksum, crc64ECMA, bytes)
}

// ChecksumAndCompress checksums and optionally compresses a byte slice, prepending an 8-byte
// header comprised of:
//
//	header[0] - flags:
//	  bit 0    - compressed?
//	  bits 1-7 - unused bits
//	header[1-4] - crc32 checksum written in big-endian order
//	header[5-7] - unused bytes
func ChecksumAndCompress(byteSlice []byte, compress bool) []byte {
	header := make([]byte, 8)
	if compress {
		header[0] |= FLAG_COMPRESSED
		byteSlice = snappy.Encode(nil, byteSlice)
	}
	checkSum := crc32.ChecksumIEEE(byteSlice)
	binary.BigEndian.PutUint32(header[1:5], checkSum)

	return append(header, byteSlice...)
}

func ChecksumAndUncompress(byteSlice []byte) ([]byte, error) {
	header := byteSlice[:8]
	checksumFromHeader := binary.BigEndian.Uint32(header[1:5])
	data := byteSlice[8:]
	computedChecksum := crc32.ChecksumIEEE(data)
	if computedChecksum != checksumFromHeader {
		return nil, errors.New("corrupted payload detected")
	}
	if header[0] == FLAG_COMPRESSED {
		// data is compressed
		var err error
		data, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("Couldn't decode payload. err: %v", err)
		}
	}
	return data, nil
}

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
	case <-abortCh:
		return ErrorAborted
	default:
	}

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

// GetLocalIP returns the first external IP configured for the first
// interface connected to this node. This may be IPv4 or IPv6.
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
				if security.IsIpv6() {
					if ip = ip.To16(); ip != nil {
						return ip, nil
					}
				} else {
					if ip = ip.To4(); ip != nil {
						return ip, nil
					}
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

	var adminUser, adminPasswd, scheme string
	var err error

	scheme = "http"

	if security.IsToolsConfigUsed() {
		user, passwd := security.GetToolsCreds()
		scheme = "https"
		adminUser = user
		adminPasswd = passwd
	} else {
		adminUser, adminPasswd, err = cbauth.GetHTTPServiceAuth(cluster)
		if err != nil {
			return "", err
		}
	}

	clusterUrl := url.URL{
		Scheme: scheme,
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

	pool, err := couch.GetPoolWithBucket(pooln, bucketn)
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
	pool, err := couch.GetPoolWithBucket(pooln, bucketn)
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
		if strings.Contains(err.Error(), cbauthimpl.ErrNoAuth.Error()) ||
			err.Error() == "no web credentials found in request" {
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

func ConstructIndexExprs(defn *IndexDefn) ([]string, string) {
	exprStmt := ""
	secExprs, desc, vectorAttr, _ := GetUnexplodedExprs(defn.SecExprs, defn.Desc, defn.HasVectorAttr)
	for i, exp := range secExprs {
		if exprStmt != "" {
			exprStmt += ","
		}
		exprStmt += exp
		if desc != nil && desc[i] {
			exprStmt += " DESC"
		}

		if vectorAttr != nil && vectorAttr[i] {
			exprStmt += " VECTOR"
		}

		if i == 0 && defn.IndexMissingLeadingKey { // Missing is implicit for non leading keys
			_, _, isFlatten, _ := queryutil.IsArrayExpression(exp) // For flatten MISSING is present in exp
			if !isFlatten {
				exprStmt += " INCLUDE MISSING"
			}
		}
	}
	return secExprs, exprStmt
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
		if def.ExprStmt != "" {
			exprs = def.ExprStmt
		} else {
			_, exprs = ConstructIndexExprs(&def)
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

	if GetDeploymentModel() != SERVERLESS_DEPLOYMENT {
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
	}

	if len(withExpr) != 0 {
		stmt += fmt.Sprintf(" WITH { %s }", withExpr)
	}

	return stmt
}

// For flattened array, returns the list of secExprs and
// the desc array before explosion
func GetUnexplodedExprs(secExprs []string, desc []bool, hasVectorAttr []bool) ([]string, []bool, []bool, bool) {

	var isArray, isFlatten bool
	skipFlattenExprsTillPos := 0
	origSecExprs := make([]string, 0)
	origDesc := make([]bool, 0)
	origHasVectorAttr := make([]bool, 0)
	if desc == nil {
		desc = make([]bool, len(secExprs))
		for i, _ := range secExprs {
			desc[i] = false
		}
	}

	if hasVectorAttr == nil {
		hasVectorAttr = make([]bool, len(secExprs))
		for i, _ := range secExprs {
			hasVectorAttr[i] = false
		}
	}

	for i, exp := range secExprs {
		if isArray && isFlatten && i < skipFlattenExprsTillPos {
			continue
		}

		isArray, _, isFlatten, _ = queryutil.IsArrayExpression(exp)
		if isArray && isFlatten {
			numFlattenKeys, _ := queryutil.NumFlattenKeys(exp)
			skipFlattenExprsTillPos = i + numFlattenKeys
			// Always populate "false" as DESC for flattened array expression
			// as it is not allowed to have DESC key outside of flattened key
			// expression i.e. DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age) DESC
			// is an invalid statement.
			//
			// A valid statement would be
			// DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age DESC)
			origDesc = append(origDesc, false)
			origHasVectorAttr = append(origHasVectorAttr, false)
		} else {
			origDesc = append(origDesc, desc[i])
			origHasVectorAttr = append(origHasVectorAttr, hasVectorAttr[i])
		}

		origSecExprs = append(origSecExprs, exp)

	}

	return origSecExprs, origDesc, origHasVectorAttr, isFlatten
}

func PopulateUnExplodedExprs(defn *IndexDefn) {
	defn.UnexplodedSecExprs, defn.ExprStmt = ConstructIndexExprs(defn)
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

// This method fetch the bucket UUID.  If this method return an error,
// then it means that the node is not able to connect in order to fetch
// bucket UUID.
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

func GetNumVBuckets(cluster, bucketn string) (int, error) {
	pooln := "default"

	binfo, err := NewBucketInfo(cluster, pooln, bucketn)
	if err != nil {
		fmsg := "NewBucketInfo(`%v`, `%v`, `%v`): %v\n"
		e := fmt.Errorf(fmsg, cluster, pooln, bucketn, err)
		return 0, e
	}

	return binfo.GetNumVBuckets(bucketn)
}

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

// This method will fetch the scopeID.  If this method returns an error,
// then it means that the node is not able to connect in order to fetch
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

// This method will fetch the scope and collection ID.  If this method
// returns an error, then it means that the node is not able to connect
// in order to fetch
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
	f, err := iowrap.Os_Open(name)
	if err != nil {
		return 0, err
	}
	defer iowrap.File_Close(f)

	fi, err := iowrap.File_Stat(f)
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
	return crc64.Checksum(bytes, crc64ECMA)
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

// Console writes a message to the admin console (Logs page of UI). It does not pop up a UI toaster.
// Console messages will also be written to ns_server's info.log and debug.log but not indexer.log.
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
			iowrap.File_Close(sf)
		}
		if df != nil {
			iowrap.File_Close(df)
		}
	}()

	if sf, err = iowrap.Os_Open(source); err != nil {
		return err
	} else if IsPathExist(dest) {
		return nil
	} else if df, err = iowrap.Os_Create(dest); err != nil {
		return err
	} else if _, err = iowrap.Io_Copy(df, sf); err != nil {
		return err
	}

	var info os.FileInfo
	if info, err = iowrap.Os_Stat(source); err != nil {
		return err
	} else if err = iowrap.Os_Chmod(dest, info.Mode()); err != nil {
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

	if fi, err := iowrap.Os_Stat(source); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("source not a directory")
	} else if IsPathExist(dest) == false {
		created = true
		if err := iowrap.Os_MkdirAll(dest, fi.Mode()); err != nil {
			return err
		}
	}

	var err error
	defer func() {
		// if copy failed in the middle and directory was created by us, clean.
		if err != nil && created {
			iowrap.Os_RemoveAll(dest)
		}
	}()

	var entries []os.FileInfo
	if entries, err = iowrap.Ioutil_ReadDir(source); err != nil {
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
	if _, err := iowrap.Os_Stat(path); err != nil {
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

// IsAllowed checks if the user creds have ANY of the specified permissions.
// calledBy is "Class::Method" of calling function for auditing.
func IsAllowed(creds cbauth.Creds, permissions []string, r *http.Request,
	w http.ResponseWriter, calledBy string) bool {

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
		audit.Audit(AUDIT_FORBIDDEN, r, "util::IsAllowed", "Called by "+calledBy)
		w.WriteHeader(http.StatusForbidden)
		w.Write(HTTP_STATUS_FORBIDDEN)
		return false
	}

	return true
}

// IsAllAllowed checks if the user creds have ALL of the specified permissions.
// calledBy is "Class::Method" of calling function for auditing.
func IsAllAllowed(creds cbauth.Creds, permissions []string, r *http.Request,
	w http.ResponseWriter, calledBy string) bool {

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
		audit.Audit(AUDIT_FORBIDDEN, r, "util::IsAllAllowed", "Called by "+calledBy)
		w.WriteHeader(http.StatusForbidden)
		w.Write(HTTP_STATUS_FORBIDDEN)
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

func GetIPv6FromParam(ipv4, ipv6 string) (bool, error) {
	//
	// Validate ipv4 and ipv6 parameters
	// Projector / Indexer only listen on required ip:port and NOT on optional ip:port
	// So, if ipv4 is required and ipv6 is (optional or off), isIPv6 will
	//     be set to false
	// Similarly, if ipv6 is required and ipv4 is (optional or off), isIPv6
	//     will be set to true
	//

	var isIPv6 bool

	// Backward compatibility check
	if ipv4 == "" {
		if ipv6 == "true" {
			return true, nil
		}

		if ipv6 == "false" {
			return false, nil
		}

		return false, fmt.Errorf("Invalid input")
	}

	// Input validation
	if ipv4 != "required" && ipv6 != "required" {
		return false, fmt.Errorf("Invalid input")
	}

	if ipv4 == "required" && ipv6 == "required" {
		return false, fmt.Errorf("Invalid input")
	}

	if ipv6 == "required" {
		isIPv6 = true
	}

	return isIPv6, nil
}

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	creds, valid, err := IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		audit.Audit(AUDIT_UNAUTHORIZED, r, "util::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(HTTP_STATUS_UNAUTHORIZED)
	} else if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!read")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false
		} else if !allowed {
			logging.Verbosef("util::validateAuth not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(HTTP_STATUS_FORBIDDEN)
			return false
		}
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

// Enum values for lockType arguments of TraceXxxLock/Unlock funcs.
const (
	LOCK_READ  = iota // acquire lock for read
	LOCK_WRITE        // acquire lock for write
)
const LOCK_LOG_DUR = 500 * time.Millisecond // minimum lock wait/hold duration before logging it

// TraceMutexLOCK assists logging how long it took to lock a Mutex. This logs how long it took
// iff >= LOCK_LOG_DUR. It returns the time at which the lock was acquired.
//
// Arguments
//
//	lock -- pointer to the mutex to lock
//	lockName -- name of the mutex (for logging)
//	caller -- name of method doing this action (usually fully qualified, like "class::method:")
//	parent -- name of caller's caller for disambiguation of low-level calls, else ""
func TraceMutexLOCK(lock *sync.Mutex, lockName, caller, parent string) time.Time {

	lockTryTime := time.Now()
	lock.Lock()
	lockGotTime := time.Now()

	if lockWaitDur := lockGotTime.Sub(lockTryTime); lockWaitDur >= LOCK_LOG_DUR {
		traceLockLog(LOCK_WAIT, lockWaitDur, LOCK_WRITE, lockName, caller, parent)
	}
	return lockGotTime
}

// TraceMutexUNLOCK assists logging how long a Mutex was held. This logs the hold time
// iff >= LOCK_LOG_DUR.
//
// Arguments
//
//	lockGotTime -- time the lock was originally acquired (return value from TraceMutexLOCK)
//	lock -- pointer to the mutex to unlock
//	lockName -- name of the mutex (for logging)
//	caller -- name of method doing this action (usually fully qualified, like "class::method:")
//	parent -- name of caller's caller for disambiguation of low-level calls, else ""
func TraceMutexUNLOCK(lockGotTime time.Time, lock *sync.Mutex, lockName, caller, parent string) {

	lock.Unlock()
	lockReleasedTime := time.Now()

	if lockHoldDur := lockReleasedTime.Sub(lockGotTime); lockHoldDur >= LOCK_LOG_DUR {
		traceLockLog(LOCK_HOLD, lockHoldDur, LOCK_WRITE, lockName, caller, parent)
	}
}

// TraceRWMutexLOCK assists logging how long it took to lock an RWMutex. This logs how long it took
// iff >= LOCK_LOG_DUR. It returns the time at which the lock was acquired.
//
// Arguments
//
//	lockType -- LOCK_READ or LOCK_WRITE
//	lock -- pointer to the mutex to lock
//	lockName -- name of the mutex (for logging)
//	caller -- name of method doing this action (usually fully qualified, like "class::method:")
//	parent -- name of caller's caller for disambiguation of low-level calls, else ""
func TraceRWMutexLOCK(lockType int, lock *sync.RWMutex, lockName, caller, parent string) time.Time {

	lockTryTime := time.Now()
	if lockType == LOCK_READ {
		lock.RLock()
	} else { // LOCK_WRITE
		lock.Lock()
	}
	lockGotTime := time.Now()

	if lockWaitDur := lockGotTime.Sub(lockTryTime); lockWaitDur >= LOCK_LOG_DUR {
		traceLockLog(LOCK_WAIT, lockWaitDur, lockType, lockName, caller, parent)
	}
	return lockGotTime
}

// TraceRWMutexUNLOCK assists logging how long an RWMutex was held. This logs the hold time
// iff >= LOCK_LOG_DUR.
//
// Arguments
//
//	lockGotTime -- time the lock was originally acquired (return value from TraceRWMutexLOCK)
//	lockType -- LOCK_READ or LOCK_WRITE
//	lock -- pointer to the mutex to unlock
//	lockName -- name of the mutex (for logging)
//	caller -- name of method doing this action (usually fully qualified, like "class::method:")
//	parent -- name of caller's caller for disambiguation of low-level calls, else ""
func TraceRWMutexUNLOCK(lockGotTime time.Time,
	lockType int, lock *sync.RWMutex, lockName, caller, parent string) {

	if lockType == LOCK_READ {
		lock.RUnlock()
	} else { // LOCK_WRITE
		lock.Unlock()
	}
	lockReleasedTime := time.Now()

	if lockHoldDur := lockReleasedTime.Sub(lockGotTime); lockHoldDur >= LOCK_LOG_DUR {
		traceLockLog(LOCK_HOLD, lockHoldDur, lockType, lockName, caller, parent)
	}
}

// Enum values for lockAction argument of traceLockLog function.
const (
	LOCK_HOLD = iota // held mutex
	LOCK_WAIT        // waited for mutex
)

// traceLockLog is a helper for the Trace[RW]MutexLOCK/UNLOCK functions to log long waits/holds.
// Examples:
//
//	util::traceLockLog: Long hold of myMutex.Lock() 784ms in myClass::myMethod:
//	  called by parentClass::parentMethod:
//	util::traceLockLog: Long wait for myMutex.RLock() 511ms in myClass::myMethod:
//	  called by parentClass::parentMethod:
//
// Arguments
//
//	lockAction -- LOCK_WAIT or LOCK_HOLD
//	lockDur -- duration of the lock hold or wait
//	lockType -- LOCK_READ or LOCK_WRITE
//	lockName -- name of the mutex (for logging)
//	caller -- name of method doing this action (usually fully qualified, like "class::method:")
//	parent -- name of caller's caller for disambiguation of low-level calls, else ""
func traceLockLog(lockAction int, lockDur time.Duration, lockType int,
	lockName, caller, parent string) {
	const method = "util::traceLockLog:" // for logging

	var bld strings.Builder
	bldPtr := &bld

	fmt.Fprintf(bldPtr, "%v Long ", method)
	if lockAction == LOCK_HOLD {
		fmt.Fprintf(bldPtr, "hold of %v.", lockName)
	} else { // LOCK_WAIT
		fmt.Fprintf(bldPtr, "wait for %v.", lockName)
	}
	if lockType == LOCK_READ {
		fmt.Fprintf(bldPtr, "RLock")
	} else { // LOCK_WRITE
		fmt.Fprintf(bldPtr, "Lock")
	}
	fmt.Fprintf(bldPtr, "() %v in %v", lockDur, caller)
	if parent != "" {
		fmt.Fprintf(bldPtr, " called by %v", parent)
	}

	logging.Warnf(bld.String())
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

// WriteFileWithSync simulates iowrap.Ioutil_WriteFile but also syncs (forces bytes
// to disk) before closing. Returns the first error, if any.
func WriteFileWithSync(path string, data []byte, perm os.FileMode) error {
	fd, err := iowrap.Os_OpenFile(path, os.O_WRONLY|os.O_CREATE, perm)
	if err == nil { // opened, so must close
		_, err = iowrap.File_Write(fd, data)
		if err == nil {
			err = iowrap.File_Sync(fd)
		}
		err2 := iowrap.File_Close(fd)
		if err == nil {
			err = err2
		}
	}
	return err
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
		if minorVersion < 1 {
			return INDEXER_70_VERSION
		} else if minorVersion == 1 {
			return INDEXER_71_VERSION
		} else if minorVersion == 2 {
			return INDEXER_72_VERSION
		} else if minorVersion == 5 {
			return INDEXER_75_VERSION
		} else if minorVersion >= 6 {
			return INDEXER_76_VERSION
		}
	}

	if version == 8 {
		return INDEXER_80_VERSION
	}

	return INDEXER_CUR_VERSION
}

// ServerPriority - priority of the server to be elected as master when rebalance runs
// string should be in the format:
// MAJOR_VERSION.MINOR_VERSION.PATCH_VERSION-MPXX
// version can only range from [0,100)
type ServerPriority string

func (sv ServerPriority) GetVersion() uint64 {
	if len(sv) == 0 {
		return 0
	}
	var ver uint64 = 0
	vs := strings.ToUpper(string(sv))
	if strings.Contains(vs, "-MP") {
		var mp string
		var ok bool

		vs, mp, ok = strings.Cut(vs, "-MP")
		if !ok {
			return 0
		}
		mpv, err := strconv.Atoi(mp)
		if err != nil {
			return 0
		}
		ver += uint64(mpv % 100)
	}
	splits := strings.Split(vs, ".")
	multiplier := uint64(1000_000)
	for _, strVer := range splits {
		mv, err := strconv.Atoi(strVer)
		if err != nil {
			return 0
		}
		ver += uint64(mv%100) * multiplier
		multiplier /= 100
	}
	return ver
}

func (sv ServerPriority) String() string {
	return string(sv)
}

// ShouldMaintainShardAfffinty - if shard affinity is enabled, cluster is on or ahead of 7.6
// and storage mode is Plasma.
// Should only be called from *indexer* and not from client/metadata_provider
func ShouldMaintainShardAffinity(config Config) bool {
	clusterVer := GetClusterVersion()
	isShardAffinityEnabled := config.GetDeploymentAwareShardAffinity()
	isStoragePlasma := GetClusterStorageMode() == PLASMA
	return clusterVer >= INDEXER_76_VERSION && isShardAffinityEnabled && isStoragePlasma
}

// CanMaintainShardAffinity - if shard affinity is enabled and storage mode is Plasma
// Should only be called from *indexer* and not from client/metadata_provider
func CanMaintanShardAffinity(config Config) bool {
	isShardAffinityEnabled := config.GetDeploymentAwareShardAffinity()
	isStoragePlasma := GetClusterStorageMode() == PLASMA
	return isShardAffinityEnabled && isStoragePlasma
}

func GetBinSize(config Config) uint64 {
	if binSize, ok := config["planner.internal.binSize"]; ok && binSize.Uint64() > 0 {
		return binSize.Uint64()
	}
	return DEFAULT_BIN_SIZE
}

func ComputeSHA256ForFloat32Array(vecs []float32) []byte {
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&vecs[0])), len(vecs)*4)
	sum := sha256.Sum256(bytes)
	// The Sum256 method returns a slice of fixed size (32 bytes).
	// It is much better to have variable length slices instead of using
	// fixed length slices so that future changes can be accommodated.
	// sum[:] does the conversion for us
	return sum[:]
}
