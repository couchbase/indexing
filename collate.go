//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Package collatejson supplies Encoding and Decoding function to transform
// JSON text into binary representation without loosing information. That is,
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
	"code.google.com/p/go.text/collate"
	"code.google.com/p/go.text/collate/colltab"
	"code.google.com/p/go.text/language"
	"code.google.com/p/go.text/unicode/norm"
	"fmt"
	"github.com/couchbaselabs/dparval"
	"math"
	"sort"
	"strconv"
)

// While encoding JSON data-element, both basic and composite, encoded string
// is prefixed with a type-byte. `Terminator` terminates encoded datum.
const (
	Terminator byte = iota
	TypeMissing
	TypeNull
	TypeFalse
	TypeTrue
	TypeNumber
	TypeString
	TypeArray
	TypeObj
)

// Codec structure
type Codec struct {
	arrayLenPrefix    bool // if true, first sort arrays based on its length.
	propertyLenPrefix bool // if true, first sort properties based on length.
	strength          colltab.Level
	alternate         collate.AlternateHandling
	backwards         bool
	hiraganaQ         bool
	caseLevel         bool
	numeric           bool
	language          language.Tag
	nfkd              bool
	utf8              bool
}

// NewCodec creates a new codec object and returns a reference to it.
func NewCodec() *Codec {
	return &Codec{
		arrayLenPrefix:    true,
		propertyLenPrefix: true,
		strength:          colltab.Quaternary,
		backwards:         false,
		hiraganaQ:         false,
		caseLevel:         true,
		numeric:           false,
		language:          language.English,
	}
}

// SortbyArrayLen sorts array by length before sorting by array elements. Use
// `false` to sort only by array elements
func (codec *Codec) SortbyArrayLen(what bool) {
	codec.arrayLenPrefix = what
}

// SortbyPropertyLen sorts property by length before sorting by property items.
// Use `false` to sort only by proprety items
func (codec *Codec) SortbyPropertyLen(what bool) {
	codec.propertyLenPrefix = what
}

// UnicodeCollationPriority sets collate.Collator properties for unicode
// collation.
func (codec *Codec) UnicodeCollationPriority(
	strength colltab.Level, alternate collate.AlternateHandling,
	backwards, hiraganaQ, caseLevel, numeric bool) {
	codec.strength = strength
	codec.alternate = alternate
	codec.backwards = backwards
	codec.hiraganaQ = hiraganaQ
	codec.caseLevel = caseLevel
	codec.numeric = numeric
}

// SetLanguage uses language tag while doing unicode collation.
func (codec *Codec) SetLanguage(l language.Tag) {
	codec.language = l
}

// SortbyNFKD will enable an alternate collation using NFKD unicode standard.
func (codec *Codec) SortbyNFKD(what bool) {
	codec.nfkd = what
}

// SortbyUTF8 will do plain binary comparision for strings.
func (codec *Codec) SortbyUTF8(what bool) {
	codec.utf8 = what
}

// Encode json documents to order preserving binary representation.
func (codec *Codec) Encode(rawjson []byte) []byte {
	doc := dparval.NewValueFromBytes(rawjson)
	code := json2code(codec, doc.Value())
	return code
}

// Decode a slice of byte into json string and return them as slice of byte.
func (codec *Codec) Decode(code []byte) []byte {
	json, _, _ := code2json(codec, code)
	return json
}

// local function that encodes basic json types to binary representation.
// composite types recursively call this function.
func json2code(codec *Codec, val interface{}) []byte {
	var code []byte
	if val == nil {
		return []byte{TypeNull, Terminator}
	}
	switch value := val.(type) {
	case bool:
		if !value {
			code = []byte{TypeFalse}
		} else {
			code = []byte{TypeTrue}
		}
		return append(code, Terminator)
	case float64:
		fvalue := strconv.FormatFloat(value, 'e', -1, 64)
		code = EncodeFloat([]byte(fvalue))
		return append(joinBytes([]byte{TypeNumber}, code), Terminator)
	case int:
		code = EncodeInt([]byte(strconv.Itoa(value)))
		return append(joinBytes([]byte{TypeNumber}, code), Terminator)
	case uint64:
		return json2code(codec, float64(value))
	case string:
		code = suffixEncodeString(codec.EncodeString(value))
		res := joinBytes([]byte{TypeString}, code)
		return append(res, Terminator)
	case []interface{}:
		res := make([][]byte, 0)
		res = append(res, []byte{TypeArray})
		if codec.arrayLenPrefix {
			res = append(res, json2code(codec, len(value)))
		}
		for _, val := range value {
			res = append(res, json2code(codec, val))
		}
		return append(bytes.Join(res, []byte{}), Terminator)
	case map[string]interface{}:
		res := make([][]byte, 0)
		res = append(res, []byte{TypeObj})
		if codec.propertyLenPrefix {
			res = append(res, json2code(codec, len(value)))
		}
		keys := sortProps(value)
		for _, key := range keys {
			res = append(
				res, json2code(codec, key), json2code(codec, value[key]))
		}
		return append(bytes.Join(res, []byte{}), Terminator)
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
	case TypeNull:
		datum, code = getDatum(code)
		return []byte("null"), code, nil
	case TypeTrue:
		datum, code = getDatum(code)
		json = []byte("true")
		return json, code, nil
	case TypeFalse:
		datum, code = getDatum(code)
		json = []byte("false")
		return json, code, nil
	case TypeNumber:
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
	case TypeString:
		var s string
		s, code = suffixDecodeString(code[1:])
		json = joinBytes([]byte("\""), []byte(s), []byte("\""))
		return json, code, nil
	case TypeArray:
		var l int
		code = code[1:] // remove type encoding TYPE_ARRAY
		if codec.arrayLenPrefix {
			datum, code = getDatum(code)
			datum = datum[1:] // remove TYPE_NUMBER for len encoding
			l, err = strconv.Atoi(string(DecodeInt(datum)))
		}
		json = []byte("[")
		comma := []byte{}
		for code[0] != Terminator {
			tmp, code, err = code2json(codec, code)
			json = joinBytes(json, comma, tmp)
			comma = []byte(", ")
			l--
		}
		code = code[1:] // remove Terminator
		if l != 0 {
			err = fmt.Errorf("can decode %v elements in array", l)
		}
		json = joinBytes(json, []byte("]"))
		return json, code, err
	case TypeObj:
		var l int
		code = code[1:] // remove type encoding TYPE_OBJ
		if codec.propertyLenPrefix {
			datum, code = getDatum(code)
			datum = datum[1:] // Remove TYPE_NUMBER for len encoding
			l, err = strconv.Atoi(string(DecodeInt(datum)))
		}
		json = []byte("{")
		comma, name, value := []byte{}, []byte{}, []byte{}
		for code[0] != Terminator {
			name, code, err = code2json(codec, code)
			value, code, err = code2json(codec, code)
			json = joinBytes(json, comma, name, []byte(": "), value)
			comma = []byte(", ")
			l--
		}
		code = code[1:]
		if l != 0 {
			err = fmt.Errorf("can decode %v elements in property", l)
		}
		json = joinBytes(json, []byte("}"))
		return json, code, err
	}
	panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", code, code))
}

// local function that sorts JSON property objects based on property names.
func sortProps(props map[string]interface{}) sort.StringSlice {
	// collect all the keys
	allkeys := make(sort.StringSlice, 0)
	for k := range props {
		allkeys = append(allkeys, k)
	}
	// sort the keys
	allkeys.Sort()
	return allkeys
}

// Get the encoded datum (basic datatype) based on Terminator and return a
// tuple of, `encoded-datum`, `remaining-code`, where remaining-code starts
// after the Terminator
func getDatum(code []byte) ([]byte, []byte) {
	var i int
	var b byte
	for i, b = range code {
		if b == Terminator {
			break
		}
	}
	return code[:i], code[i+1:]
}

// EncodeString encodes string in utf8 encoding to binary sequence based on
// UTF8, NFKD or go.text/collate algorithms.
func (codec *Codec) EncodeString(value string) (code []byte) {
	bs := []byte(value)
	if codec.utf8 {
		code = []byte(bs)
	} else if codec.nfkd {
		code = norm.NFKD.Bytes([]byte(bs)) // canonical decomposed
	} else {
		// TODO: Try to understand the performance implication of collate.Buffer
		// object
		buf := &collate.Buffer{}
		c := collate.New(codec.language)
		c.Strength = codec.strength
		c.Alternate = codec.alternate
		c.Backwards = codec.backwards
		c.HiraganaQuaternary = codec.hiraganaQ
		c.CaseLevel = codec.caseLevel
		c.Numeric = codec.numeric
		code = c.Key(buf, []byte(bs))
	}
	return code
}
