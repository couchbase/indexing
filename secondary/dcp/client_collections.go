/*
Provides collections enabled go client
*/
package couchbase

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/couchbase/indexing/secondary/common/collections"
	"time"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
)

// General-purpose value setter for collections
//
// The SetC, AddC and DeleteC methods are just wrappers around this.  The
// interpretation of `v` depends on whether the `Raw` option is
// given. If it is, v must be a byte array or nil. (A nil value causes
// a delete.) If `Raw` is not given, `v` will be marshaled as JSON
// before being written. It must be JSON-marshalable and it must not
// be nil.
func (b *Bucket) WriteC(k, cid string, flags, exp int, v interface{},
	opt WriteOptions) (err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) {
			ClientOpCallback(fmt.Sprintf("WriteC(%v)", opt), k, t, err)
		}(time.Now())
	}

	var data []byte
	if opt&Raw == 0 {
		data, err = json.Marshal(v)
		if err != nil {
			return err
		}
	} else if v != nil {
		data = v.([]byte)
	}

	encBytes, err := collections.PrependLEB128EncStrKey(([]byte)(k), cid)
	if err != nil {
		return err
	}
	encK := fmt.Sprintf("%s", encBytes)
	var res *transport.MCResponse
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {

		if err := mc.EnableCollections("WriteC-Client"); err != nil {
			return err
		}

		if opt&AddOnly != 0 {
			res, err = memcached.UnwrapMemcachedError(
				mc.Add(vb, encK, flags, exp, data))
			if err == nil && res.Status != transport.SUCCESS {
				if res.Status == transport.KEY_EEXISTS {
					err = ErrKeyExists
				} else {
					err = res
				}
			}
		} else if opt&Append != 0 {
			res, err = mc.Append(vb, encK, data)
		} else if data == nil {
			res, err = mc.Del(vb, encK)
		} else {
			res, err = mc.Set(vb, encK, flags, exp, data)
		}
		return err
	})

	if err == nil && (opt&(Persist|Indexable) != 0) {
		err = b.WaitForPersistence(encK, res.Cas, data == nil)
	}

	return err
}

// Set a value in this collection for key `k`
// The value will be serialized into a JSON document.
func (b *Bucket) SetC(k, cid string, exp int, v interface{}) error {
	return b.WriteC(k, cid, 0, exp, v, 0)
}

// SetRawC sets a value in this collection for key `k`
// without JSON encoding it.
func (b *Bucket) SetRawC(k, cid string, exp int, v []byte) error {
	return b.WriteC(k, cid, 0, exp, v, Raw)
}

// Add adds a value to this bucket for encoded key
// `encKey` is  is cid+key. `k` is used to compute vb
func (b *Bucket) AddC(k, cid string, exp int, v interface{}) (added bool, err error) {
	err = b.WriteC(k, cid, 0, exp, v, AddOnly)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

// AddRaw adds a value to this collection for key `k`
func (b *Bucket) AddRawC(k, cid string, exp int, v []byte) (added bool, err error) {
	err = b.WriteC(k, cid, 0, exp, v, AddOnly|Raw)
	if err == ErrKeyExists {
		return false, nil
	}
	return (err == nil), err
}

// Append appends raw data to an existing item for key `k` to this collection
func (b *Bucket) AppendC(k, cid string, data []byte) error {
	return b.WriteC(k, cid, 0, 0, data, Append|Raw)
}

// GetsRaw gets a raw value from this bucket including its CAS
// counter and flags from this collection for key `k`
func (b *Bucket) GetsRawC(k, cid string) (data []byte, flags int,
	cas uint64, err error) {

	if ClientOpCallback != nil {
		defer func(t time.Time) { ClientOpCallback("GetsRaw", k, t, err) }(time.Now())
	}

	encBytes, err := collections.PrependLEB128EncStrKey(([]byte)(k), cid)
	if err != nil {
		return nil, 0, 0, err
	}

	encK := fmt.Sprintf("%s", encBytes)
	err = b.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, encK)
		if err != nil {
			return err
		}
		cas = res.Cas
		if len(res.Extras) >= 4 {
			flags = int(binary.BigEndian.Uint32(res.Extras))
		}
		data = res.Body
		return nil
	})
	return
}

// Gets gets a value from this bucket, including its CAS counter from
// this collection for key `k`
//  The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) GetsC(k, cid string, rv interface{}, caso *uint64) error {
	data, _, cas, err := b.GetsRawC(k, cid)
	if err != nil {
		return err
	}
	if caso != nil {
		*caso = cas
	}
	return json.Unmarshal(data, rv)
}

// Get a value from this collection for key `k`
// The value is expected to be a JSON stream and will be deserialized
// into rv.
func (b *Bucket) GetC(k, cid string, rv interface{}) error {
	return b.GetsC(k, cid, rv, nil)
}

// GetRawC gets a raw value from this colleciton for key `k`
// No marshaling is performed.
func (b *Bucket) GetRawC(k, cid string) ([]byte, error) {
	d, _, _, err := b.GetsRawC(k, cid)
	return d, err
}

// DeleteC delets `k` from this collection
func (b *Bucket) DeleteC(k, cid string) error {
	return b.WriteC(k, cid, 0, 0, nil, Raw)
}
