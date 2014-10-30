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
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
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
	Docid   []byte
	Vbucket Vbucket //useful for debugging, can be removed to optimize space
	Seqno   Seqno   //useful for debugging, can be removed to optimize space
}

var KEY_SEPARATOR []byte = []byte{0xff, 0xff, 0xff, 0xff}

func NewKey(data []byte) (Key, error) {

	var err error
	var key Key
	var code []byte

	key.raw = data

	jsoncodec := collatejson.NewCodec(16)
	//convert key to its collatejson encoded byte representation
	buf := new(bytes.Buffer)
	bufcode := make([]byte, MAX_SEC_KEY_LEN)
	if code, err = jsoncodec.Encode(data, bufcode); err != nil {
		return key, err
	}
	if _, err = buf.Write(code); err != nil {
		return key, err
	}

	key.encoded = buf.Bytes()

	return key, nil

}

func NewValue(docid []byte, vbucket Vbucket, seqno Seqno) (Value, error) {

	var val Value

	val.raw.Docid = docid
	val.raw.Vbucket = vbucket
	val.raw.Seqno = seqno

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
	if k.raw == nil {
		jsoncodec := collatejson.NewCodec(16)
		buf := make([]byte, MAX_SEC_KEY_LEN)
		if buf, err = jsoncodec.Decode(k.encoded, buf); err != nil {
			common.Errorf("KV::Raw Error Decoding Key %v, Err %v", k.encoded,
				err)
			return nil
		}
		k.raw = buf
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
	buf.WriteString(fmt.Sprintf("Vbucket:%d ", v.raw.Vbucket))
	buf.WriteString(fmt.Sprintf("Seqno:%d", v.raw.Seqno))
	return buf.String()
}
