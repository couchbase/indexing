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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/logging"
)

// Key is an array of JSON objects, per encoding/json
type Key struct {
	raw     []byte //raw key received from KV
	encoded []byte //collatejson byte representation of the key
}

// Value is the primary key of the relavent document
type Value struct {
	raw     Valuedata
	encoded []byte
}

type Valuedata struct {
	Docid []byte
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

	jsoncodec := collatejson.NewCodec(16)
	//TODO collatejson needs 3x buffer size. see if that can
	//be reduced. Also reuse buffer.
	buf := make([]byte, 0, MAX_SEC_KEY_BUFFER_LEN)
	if buf, err = jsoncodec.Encode(data, buf); err != nil {
		return key, err
	}

	key.encoded = append([]byte(nil), buf...)
	return key, nil
}

func NewValue(docid []byte) (Value, error) {

	var val Value

	val.raw.Docid = docid

	var err error
	if val.encoded, err = json.Marshal(val.raw); err != nil {
		return val, err
	}
	return val, nil
}

func NewKeyFromEncodedBytes(encoded []byte) (Key, error) {

	var k Key
	k.encoded = encoded
	return k, nil

}

func NewValueFromEncodedBytes(b []byte) (Value, error) {

	var val Value
	var err error
	if b != nil {
		err = json.Unmarshal(b, &val.raw)
	}
	val.encoded = b
	return val, err

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

func (k *Key) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%v", string(k.raw)))
	return buf.String()
}

func (v *Value) Encoded() []byte {
	return v.encoded
}

func (v *Value) Raw() Valuedata {
	return v.raw
}

func (v *Value) Docid() []byte {
	return v.raw.Docid
}

func (v *Value) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Docid:%v ", v.raw.Docid))
	return buf.String()
}
