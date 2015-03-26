// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
)

// Key is an array of JSON objects, per encoding/json
type Key struct {
	raw     []byte //raw key received from KV
	encoded []byte //collatejson byte representation of the key
}

// Value is the primary key of the relavent document
type Value []byte

var codecPool = sync.Pool{New: newCodec}
var codecBufPool = sync.Pool{New: newCodecBuf}

func newCodec() interface{} {
	return collatejson.NewCodec(16)
}

func newCodecBuf() interface{} {
	return make([]byte, 0, MAX_SEC_KEY_BUFFER_LEN)
}

func NewKey(data []byte) (Key, error) {
	var err error
	var key Key

	if len(data) > MAX_SEC_KEY_LEN {
		return key, errors.New("Key Too Long")
	}

	key.raw = data

	if bytes.Compare([]byte("[]"), data) == 0 || len(data) == 0 {
		key.encoded = nil
		return key, nil
	}

	jsoncodec := codecPool.Get().(*collatejson.Codec)
	defer codecPool.Put(jsoncodec)
	//TODO collatejson needs 3x buffer size. see if that can
	//be reduced. Also reuse buffer.
	buf := codecBufPool.Get().([]byte)
	defer codecBufPool.Put(buf)
	if buf, err = jsoncodec.Encode(data, buf); err != nil {
		return key, err
	}

	key.encoded = append([]byte(nil), buf...)
	return key, nil
}

func NewValue(docid []byte) (Value, error) {

	val := Value(docid)
	return val, nil
}

func NewKeyFromEncodedBytes(encoded []byte) (Key, error) {

	var k Key
	k.encoded = encoded
	return k, nil

}

func NewValueFromEncodedBytes(b []byte) (Value, error) {

	val := Value(b)
	return val, nil

}

func (k *Key) Compare(than Key) int {

	b1 := k.encoded
	b2 := than.Encoded()
	return bytes.Compare(b1, b2)
}

func (k *Key) Encoded() []byte {

	return k.encoded
}

func (k *Key) Raw() []byte {

	var err error
	if k.raw == nil && k.encoded != nil {
		jsoncodec := collatejson.NewCodec(16)
		// TODO: Refactor to reuse tmp buffer
		buf := make([]byte, 0, MAX_SEC_KEY_LEN)
		if buf, err = jsoncodec.Decode(k.encoded, buf); err != nil {
			logging.Errorf("KV::Raw Error Decoding Key %v, Err %v", k.encoded,
				err)
			return nil
		}
		k.raw = append([]byte(nil), buf...)
	}

	return k.raw
}

func (k *Key) IsNull() bool {
	return k.encoded == nil
}

func (k *Key) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%v", string(k.raw)))
	return buf.String()
}

func (v *Value) Encoded() []byte {
	return []byte(*v)
}

func (v *Value) Raw() []byte {
	return []byte(*v)
}

func (v *Value) Docid() []byte {
	return []byte(*v)
}

func (v *Value) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Docid:%s ", string(*v)))
	return buf.String()
}
