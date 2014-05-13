package common

import (
	"github.com/couchbaselabs/go-couchbase"
)

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

// FailsafeOp can be used by gen-server implementors to avoid infinitely
// blocked API calls.
func FailsafeOp(reqch, respch chan []interface{}, cmd []interface{}, finch chan bool) ([]interface{}, error) {
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

// OpError suppliments FailsafeOp used by gen-servers.
func OpError(err error, vals []interface{}, idx int) error {
	if err != nil {
		return err
	} else if vals[idx] == nil {
		return nil
	}
	return vals[idx].(error)
}

// ConnectBucket will instantiate a couchbase-bucket instance with kvaddr.
func ConnectBucket(kvaddr, pooln, bucketn string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect("http://" + kvaddr)
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
