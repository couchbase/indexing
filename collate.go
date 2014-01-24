//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Encoding and Decoding function to transform JSON text into binary
// representation without loosing information. That is,
//
// * binary representation should preserve the sort order such that, sorting
// binary encoded json documents much match sorting by functions that parse
// and compare JSON documents.
//
// * it must be possible to get back the original document, in semantically
// correct form, from its binary representation.
//
// Notes:
//
// * items in a property object are sorted by its property name before they
// are compared with other property object.
package collatejson

import (
	"bytes"
	"fmt"
	"github.com/couchbaselabs/dparval"
	"math"
	"sort"
	"strconv"
)

// While encoding JSON data-element, both basic and composite, encoded string
// is prefixed with a type-byte. TERMINATOR terminates encoded datum.
const (
	TERMINATOR byte = iota
	TYPE_MISSING
	TYPE_NULL
	TYPE_FALSE
	TYPE_TRUE
	TYPE_NUMBER
	TYPE_STRING
	TYPE_ARRAY
	TYPE_OBJ
)

// Codec.
type Codec struct {
	arrayLenPrefix    bool // if true, first sort arrays based on its length.
	propertyLenPrefix bool // if true, first sort properties based on length.
}

// create a new codec object and return a reference to it.
func NewCodec() *Codec {
	return &Codec{
		arrayLenPrefix:    true,
		propertyLenPrefix: true,
	}
}

// sort by array length before sorting by array elements. use `false` to sort
// only by array elements
func (codec *Codec) SortbyArrayLen(what bool) {
	codec.arrayLenPrefix = what
}

// sort by property length before sorting by property items. use `false`
// to sort only by proprety items
func (codec *Codec) SortbyPropertyLen(what bool) {
	codec.propertyLenPrefix = what
}

// Encode json documents to order preserving binary representation.
func (codec *Codec) Encode(rawjson []byte) []byte {
	doc := dparval.NewValueFromBytes(rawjson)
	code := json2code(codec, doc.Value())
	return code
}

func (codec *Codec) Decode(code []byte) []byte {
	json, _, _ := code2json(codec, code)
	return json
}

// local function that encodes basic json types to binary representation.
// composite types recursively call this function.
func json2code(codec *Codec, val interface{}) []byte {
	var code []byte
	if val == nil {
		return []byte{TYPE_NULL, TERMINATOR}
	}
	switch value := val.(type) {
	case bool:
		if !value {
			code = []byte{TYPE_FALSE}
		} else {
			code = []byte{TYPE_TRUE}
		}
		return append(code, TERMINATOR)
	case float64:
		fvalue := strconv.FormatFloat(value, 'e', -1, 64)
		code = EncodeFloat([]byte(fvalue))
		return append(joinBytes([]byte{TYPE_NUMBER}, code), TERMINATOR)
	case int:
		code = EncodeInt([]byte(strconv.Itoa(value)))
		return append(joinBytes([]byte{TYPE_NUMBER}, code), TERMINATOR)
	case uint64:
		return json2code(codec, float64(value))
	case string:
		return append(joinBytes([]byte{TYPE_STRING}, []byte(value)), TERMINATOR)
	case []interface{}:
		res := make([][]byte, 0)
		res = append(res, []byte{TYPE_ARRAY})
		if codec.arrayLenPrefix {
			res = append(res, json2code(codec, len(value)))
		}
		for _, val := range value {
			res = append(res, json2code(codec, val))
		}
		return append(bytes.Join(res, []byte{}), TERMINATOR)
	case map[string]interface{}:
		res := make([][]byte, 0)
		res = append(res, []byte{TYPE_OBJ})
		if codec.propertyLenPrefix {
			res = append(res, json2code(codec, len(value)))
		}
		keys := sortProps(value)
		for _, key := range keys {
			res = append(
				res, json2code(codec, key), json2code(codec, value[key]))
		}
		return append(bytes.Join(res, []byte{}), TERMINATOR)
	}
	panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", val, val))
}

func code2json(codec *Codec, code []byte) ([]byte, []byte, error) {
	var datum, json, tmp []byte
	var err error

	if len(code) == 0 {
		return []byte{}, code, nil
	}
	switch code[0] {
	case TYPE_NULL:
		datum, code = getDatum(code)
		return []byte("null"), code, nil
	case TYPE_TRUE:
		datum, code = getDatum(code)
		json = []byte("true")
		return json, code, nil
	case TYPE_FALSE:
		datum, code = getDatum(code)
		json = []byte("false")
		return json, code, nil
	case TYPE_NUMBER:
		var fvalue float64
		datum, code = getDatum(code)
		datum = datum[1:] // remove type encoding TYPE_NUMBER
		ftext := DecodeFloat(datum)
		fvalue, err = strconv.ParseFloat(string(ftext), 64)
		if math.Trunc(fvalue) == fvalue {
			json = []byte(fmt.Sprintf("%v", int64(fvalue)))
		} else {
			json = []byte(fmt.Sprintf("%v", fvalue))
		}
		return json, code, err
	case TYPE_STRING:
		datum, code = getDatum(code)
		datum = datum[1:] // remove type encoding TYPE_STRING
		json = joinBytes([]byte("\""), datum, []byte("\""))
		return json, code, nil
	case TYPE_ARRAY:
		var l int
		code = code[1:] // remove type encoding TYPE_ARRAY
		if codec.arrayLenPrefix {
			datum, code = getDatum(code)
			datum = datum[1:] // remove TYPE_NUMBER for len encoding
			l, err = strconv.Atoi(string(DecodeInt(datum)))
		}
		json = []byte("[")
		comma := []byte{}
		for code[0] != TERMINATOR {
			tmp, code, err = code2json(codec, code)
			json = joinBytes(json, comma, tmp)
			comma = []byte(", ")
			l -= 1
		}
		code = code[1:] // remove TERMINATOR
		if l != 0 {
			err = fmt.Errorf("Can decode %v elements in array", l)
		}
		json = joinBytes(json, []byte("]"))
		return json, code, err
	case TYPE_OBJ:
		var l int
		code = code[1:] // remove type encoding TYPE_OBJ
		if codec.propertyLenPrefix {
			datum, code = getDatum(code)
			datum = datum[1:] // Remove TYPE_NUMBER for len encoding
			l, err = strconv.Atoi(string(DecodeInt(datum)))
		}
		json = []byte("{")
		comma, name, value := []byte{}, []byte{}, []byte{}
		for code[0] != TERMINATOR {
			name, code, err = code2json(codec, code)
			value, code, err = code2json(codec, code)
			json = joinBytes(json, comma, name, []byte(": "), value)
			comma = []byte(", ")
			l -= 1
		}
		code = code[1:]
		if l != 0 {
			err = fmt.Errorf("Can decode %v elements in property", l)
		}
		json = joinBytes(json, []byte("}"))
		return json, code, err
	}
	panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", code))
}

// local function that sorts JSON property objects based on property names.
func sortProps(props map[string]interface{}) sort.StringSlice {
	// collect all the keys
	allkeys := make(sort.StringSlice, 0)
	for k, _ := range props {
		allkeys = append(allkeys, k)
	}
	// sort the keys
	allkeys.Sort()
	return allkeys
}

// Get the encoded datum (basic datatype) based on TERMINATOR and return a
// tuple of, `encoded-datum`, `remaining-code`, where remaining-code starts
// after the TERMINATOR
func getDatum(code []byte) ([]byte, []byte) {
	var i int
	var b byte
	for i, b = range code {
		if b == TERMINATOR {
			break
		}
	}
	return code[:i], code[i+1:]
}
