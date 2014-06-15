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
	"github.com/prataprc/collatejson"
)

// Key is an array of JSON objects, per encoding/json
type Key struct {
	raw     keydata
	encoded []byte //collatejson byte representation of the key
}

type keydata struct {
	keybytes Keybytes
	docid    []byte
}

// Value is the primary key of the relavent document
type Value struct {
	raw     valuedata
	encoded []byte
}

type valuedata struct {
	Keybytes Keybytes
	Docid    []byte
	Vbucket  Vbucket
	Seqno    Seqno
}

type Keybytes [][]byte

var KEY_SEPARATOR []byte = []byte{0xff, 0xff, 0xff, 0xff}

func NewKey(data [][]byte, docid []byte) (Key, error) {

	var err error
	var key Key
	key.raw.keybytes = data
	key.raw.docid = docid

	jsoncodec := collatejson.NewCodec()
	//convert key to its collatejson encoded byte representation
	buf := new(bytes.Buffer)
	for _, k := range key.raw.keybytes {
		if _, err = buf.Write(jsoncodec.Encode(k)); err != nil {
			return key, err
		}
		if _, err = buf.Write(KEY_SEPARATOR); err != nil {
			return key, err
		}
	}
	//write the docid in the end
	if _, err = buf.Write(key.raw.docid); err != nil {
		return key, err
	}

	key.encoded = buf.Bytes()

	return key, nil

}

func NewValue(data [][]byte, docid []byte, vbucket Vbucket, seqno Seqno) (Value, error) {

	var val Value

	val.raw.Keybytes = data
	val.raw.Docid = docid
	val.raw.Vbucket = vbucket
	val.raw.Seqno = seqno

	var err error
	if val.encoded, err = json.Marshal(val.raw); err != nil {
		return val, err
	}
	return val, nil
}

func NewKeyFromEncodedBytes(b []byte) (Key, error) {

	var k Key
	//TODO Add decoding for bytes for k.raw
	k.encoded = b
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

	//strip the docid before bytewise comparison
	i1 := bytes.LastIndex(k.encoded, KEY_SEPARATOR)
	b1 := k.encoded[0:i1]

	b2 := than.EncodedBytes()
	i2 := bytes.LastIndex(b2, KEY_SEPARATOR)
	b2 = b2[0:i2]

	return bytes.Compare(b1, b2)
}

func (k *Key) EncodedBytes() []byte {

	return k.encoded
}

func (k *Key) String() string {
	var buf bytes.Buffer
	buf.WriteString("Key:[")
	for i, key := range k.raw.keybytes {
		buf.WriteString(fmt.Sprintf("%v", string(key)))
		if i < len(k.raw.keybytes)-1 {
			buf.WriteString(" ")
		}
	}
	buf.WriteString("]")
	if k.raw.docid != nil {
		buf.WriteString(fmt.Sprintf(" Docid:%v ", k.raw.docid))
	}
	return buf.String()
}

func (v *Value) EncodedBytes() []byte {

	return v.encoded

}

func (v *Value) KeyBytes() Keybytes {

	return v.raw.Keybytes
}

func (v *Value) Docid() []byte {

	return v.raw.Docid
}

func (v *Value) String() string {
	var buf bytes.Buffer
	buf.WriteString("Key:[")
	for i, key := range v.raw.Keybytes {
		buf.WriteString(fmt.Sprintf("%v", string(key)))
		if i < len(v.raw.Keybytes)-1 {
			buf.WriteString(" ")
		}
	}
	buf.WriteString("]")
	buf.WriteString(fmt.Sprintf("Docid:%v ", v.raw.Docid))
	buf.WriteString(fmt.Sprintf("Vbucket:%d ", v.raw.Vbucket))
	buf.WriteString(fmt.Sprintf("Seqno:%d", v.raw.Seqno))
	return buf.String()
}
