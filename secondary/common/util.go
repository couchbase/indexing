package common

import "errors"
import "fmt"
import "io"
import "net"
import "net/url"
import "os"

import "github.com/couchbase/cbauth"
import "github.com/couchbase/indexing/secondary/dcp"
import "github.com/couchbase/indexing/secondary/dcp/transport/client"

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
	} else if vals[idx] == nil {
		return nil
	}
	return vals[idx].(error)
}

// cbauth admin authentication helper
// Uses default cbauth env variables internally to provide auth creds
type cbAuthHandler struct {
	hostport string
	bucket   string
}

func (ah *cbAuthHandler) GetCredentials() (string, string) {
	u, p, err := cbauth.GetHTTPServiceAuth(ah.hostport)
	if err != nil {
		panic(err)
	}

	return u, p
}

func (ah *cbAuthHandler) AuthenticateMemcachedConn(host string, conn *memcached.Client) error {
	u, p, err := cbauth.GetMemcachedServiceAuth(host)
	if err != nil {
		panic(err)
	}
	_, err = conn.Auth(u, p)
	_, err = conn.SelectBucket(ah.bucket)
	return err
}

// ConnectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func ConnectBucket(cluster, pooln, bucketn string) (*couchbase.Bucket, error) {
	ah := &cbAuthHandler{
		hostport: cluster,
		bucket:   bucketn,
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

// GetKVAddrs gather the list of kvnode-address based on the latest vbmap.
func GetKVAddrs(cluster, pooln, bucketn string) ([]string, error) {
	b, err := ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		return nil, err
	}
	defer b.Close()

	b.Refresh()
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
	clusterUrl := url.URL{
		Scheme: "http",
		Host:   cluster,
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
