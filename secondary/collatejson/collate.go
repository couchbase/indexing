//  Copyright (c) 2013 Couchbase, Inc.

// Package collatejson supplies Encoding and Decoding function to transform
// JSON text into binary representation without loosing information. That is,
//
// * binary representation should preserve the sort order such that, sorting
//   binary encoded json documents much match sorting by functions that parse
//   and compare JSON documents.
// * it must be possible to get back the original document, in semantically
//   correct form, from its binary representation.
//
// Notes:
//
// * items in a property object are sorted by its property name before they
//   are compared with property's value.
package collatejson

import "bytes"
import "encoding/json"
import "errors"
import "strings"
import "sort"
import "fmt"
import "strconv"
import "sync"
import n1ql "github.com/couchbase/query/value"

var bufPool *sync.Pool

const bufSize = 1024

func init() {
	bufPool = &sync.Pool{
		New: func() interface{} {
			b := make([]byte, bufSize, bufSize)
			return &b
		},
	}
}

var _ = fmt.Sprintf("dummy print")

// ErrorNumberType means configured number type is not supported by codec.
var ErrorNumberType = errors.New("collatejson.numberType")

// ErrorOutputLen means output buffer has insufficient length.
var ErrorOutputLen = errors.New("collatejson.outputLen")

// Length is an internal type used for prefixing length
// of arrays and properties.
type Length int64

// Missing denotes a special type for an item that evaluates
// to _nothing_.
type Missing string

// MissingLiteral is special string to denote missing item.
// IMPORTANT: we are assuming that MissingLiteral will not
// occur in the keyspace.
const MissingLiteral = Missing("~[]{}falsenilNA~")

// MinBufferSize for target buffer to encode or decode.
const MinBufferSize = 16

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
	TypeLength
	TypeArray
	TypeObj
)

// Codec structure
type Codec struct {
	arrayLenPrefix    bool        // if true, first sort arrays based on its length
	propertyLenPrefix bool        // if true, first sort properties based on length
	doMissing         bool        // if true, handle missing values (for N1QL)
	numberType        interface{} // "float64" | "int64" | "decimal"
	//-- unicode
	//backwards        bool
	//hiraganaQ        bool
	//caseLevel        bool
	//numeric          bool
	//nfkd              bool
	//utf8              bool
	//strength          colltab.Level
	//alternate         collate.AlternateHandling
	//language          language.Tag
}

// NewCodec creates a new codec object and returns a reference to it.
func NewCodec(propSize int) *Codec {
	return &Codec{
		arrayLenPrefix:    false,
		propertyLenPrefix: true,
		doMissing:         true,
		numberType:        float64(0.0),
	}
}

// SortbyArrayLen sorts array by length before sorting by array
// elements. Use `false` to sort only by array elements.
// Default is `true`.
func (codec *Codec) SortbyArrayLen(what bool) {
	codec.arrayLenPrefix = what
}

// SortbyPropertyLen sorts property by length before sorting by
// property items. Use `false` to sort only by proprety items.
// Default is `true`.
func (codec *Codec) SortbyPropertyLen(what bool) {
	codec.propertyLenPrefix = what
}

// UseMissing will interpret special string MissingLiteral and
// encode them as TypeMissing.
// Default is `true`.
func (codec *Codec) UseMissing(what bool) {
	codec.doMissing = what
}

// NumberType chooses type of encoding / decoding for JSON
// numbers. Can be "float64", "int64", "decimal".
// Default is "float64"
func (codec *Codec) NumberType(what string) {
	switch what {
	case "float64":
		codec.numberType = float64(0.0)
	case "int64":
		codec.numberType = int64(0)
	case "decimal":
		codec.numberType = "0"
	}
}

// Encode json documents to order preserving binary representation.
// `code` is the output buffer for encoding and expected to have
// enough capacity, atleast 3x of input `text` and > MinBufferSize.
func (codec *Codec) Encode(text, code []byte) ([]byte, error) {
	code = code[:0]
	if cap(code) < (3*len(text)) || cap(code) < MinBufferSize {
		return nil, ErrorOutputLen
	} else if len(text) == 0 {
		return code, nil
	}
	var m interface{}
	if err := json.Unmarshal(text, &m); err != nil {
		return nil, err
	}
	return codec.json2code(m, code)
}

// Decode a slice of byte into json string and return them as
// slice of byte. `text` is the output buffer for decoding and
// expected to have enough capacity, atleast 3x of input `code`
// and > MinBufferSize.
func (codec *Codec) Decode(code, text []byte) ([]byte, error) {
	text = text[:0]
	if cap(text) < len(code) || cap(text) < MinBufferSize {
		return nil, ErrorOutputLen
	}
	text, _, err := codec.code2json(code, text)
	return text, err
}

// local function that encodes basic json types to binary representation.
// composite types recursively call this function.
func (codec *Codec) json2code(val interface{}, code []byte) ([]byte, error) {
	if val == nil {
		code = append(code, TypeNull, Terminator)
	}

	var cs []byte
	var err error

	switch value := val.(type) {
	case bool:
		if value {
			code = append(code, TypeTrue, Terminator)
		} else {
			code = append(code, TypeFalse, Terminator)
		}

	case float64:
		code = append(code, TypeNumber)
		cs, err = codec.normalizeFloat(value, code[1:])
		if err == nil {
			code = code[:len(code)+len(cs)]
			code = append(code, Terminator)
		}

	case int:
		code = append(code, TypeNumber)
		cs = EncodeInt([]byte(strconv.Itoa(value)), code[1:])
		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)

	case Length:
		code = append(code, TypeLength)
		cs = EncodeInt([]byte(strconv.Itoa(int(value))), code[1:])
		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)

	case string:
		if codec.doMissing && MissingLiteral.Equal(value) {
			code = append(code, TypeMissing)
			code = append(code, Terminator)
		} else {
			code = append(code, TypeString)
			cs = suffixEncodeString([]byte(value), code[1:])
			code = code[:len(code)+len(cs)]
			code = append(code, Terminator)
		}

	case []interface{}:
		code = append(code, TypeArray)
		if codec.arrayLenPrefix {
			arrlen := Length(len(value))
			if cs, err = codec.json2code(arrlen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}
		if err == nil {
			for _, val := range value {
				l := len(code)
				cs, err = codec.json2code(val, code[l:])
				if err == nil {
					code = code[:l+len(cs)]
					continue
				}
				break
			}
			code = append(code, Terminator)
		}

	case map[string]interface{}:
		code = append(code, TypeObj)
		if codec.propertyLenPrefix {
			proplen := Length(len(value))
			if cs, err = codec.json2code(proplen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}

		if err == nil {
			keys := codec.sortProps(value)
			for _, key := range keys {
				l := len(code)
				// encode key
				if cs, err = codec.json2code(key, code[l:]); err != nil {
					break
				}
				code = code[:l+len(cs)]
				l = len(code)
				// encode value
				if cs, err = codec.json2code(value[key], code[l:]); err != nil {
					break
				}
				code = code[:l+len(cs)]
			}
			code = append(code, Terminator)
		}
	}
	return code, err
}

var null = []byte("null")
var boolTrue = []byte("true")
var boolFalse = []byte("false")

func (codec *Codec) code2json(code, text []byte) ([]byte, []byte, error) {
	if len(code) == 0 {
		return text, code, nil
	}

	var ts, remaining, datum []byte
	var err error

	switch code[0] {
	case Terminator:
		remaining = code

	case TypeMissing:
		datum, remaining = getDatum(code)
		text = append(text, '"')
		text = append(text, MissingLiteral...)
		text = append(text, '"')

	case TypeNull:
		datum, remaining = getDatum(code)
		text = append(text, null...)

	case TypeTrue:
		datum, remaining = getDatum(code)
		text = append(text, boolTrue...)

	case TypeFalse:
		datum, remaining = getDatum(code)
		text = append(text, boolFalse...)

	case TypeLength:
		datum, remaining = getDatum(code)
		_, ts = DecodeInt(datum[1:], text)
		text = text[:len(text)+len(ts)]

	case TypeNumber:
		datum, remaining = getDatum(code)
		ts = DecodeFloat(datum[1:], text)
		ts, err = codec.denormalizeFloat(ts)
		ts = bytes.TrimLeft(ts, "+")
		text = append(text, ts...)

	case TypeString:
		var strb []byte
		tmp := bufPool.Get().(*[]byte)
		strb, remaining, err = suffixDecodeString(code[1:], (*tmp)[:0])
		if err == nil {
			text, err = encodeString(strb, text)
			bufPool.Put(tmp)
		}

	case TypeArray:
		var l int
		text = append(text, '[')
		if codec.arrayLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text[1:])
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					ln := len(text)
					ts, code, err = codec.code2json(code, text[ln:])
					if err != nil {
						break
					}
					text = text[:ln+len(ts)]
					if l > 1 {
						text = append(text, ',')
					}
				}
			}
		} else {
			comma := false
			code = code[1:]
			for code[0] != Terminator {
				if comma {
					text = append(text, ',')
				}
				ln := len(text)
				ts, code, err = codec.code2json(code, text[ln:])
				if err != nil {
					break
				}
				text = text[:ln+len(ts)]
				comma = true
			}
		}
		remaining = code[1:] // remove Terminator
		text = append(text, ']')

	case TypeObj:
		var l int
		var key, value []byte
		text = append(text, '{')
		if codec.propertyLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text[1:])
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					// decode key
					ln := len(text)
					key, code, err = codec.code2json(code, text[ln:])
					if err != nil {
						break
					}
					text = text[:ln+len(key)]
					text = append(text, ':')
					// decode value
					ln = len(text)
					key, code, err = codec.code2json(code, text[ln:])
					if err != nil {
						break
					}
					text = text[:ln+len(key)]
					if l > 1 {
						text = append(text, ',')
					}
				}
			}
		} else {
			comma := false
			code = code[1:]
			for code[0] != Terminator {
				if comma {
					text = append(text, ',')
				}
				// decode key
				ln := len(text)
				key, code, err = codec.code2json(code, text[ln:])
				if err != nil {
					break
				}
				text = text[:ln+len(key)]
				text = append(text, ':')
				// decode value
				ln = len(text)
				value, code, err = codec.code2json(code, text[ln:])
				if err != nil {
					break
				}
				text = text[:ln+len(value)]
				comma = true
			}
		}
		remaining = code[1:] // remove Terminator
		text = append(text, '}')
	}
	return text, remaining, err
}

// local function that sorts JSON property objects based on property names.
func (codec *Codec) sortProps(props map[string]interface{}) []string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	ss := sort.StringSlice(keys)
	ss.Sort()
	return keys
}

// get the encoded datum (basic JSON datatype) based on Terminator and return a
// tuple of, `encoded-datum`, `remaining-code`, where remaining-code starts
// after the Terminator
func getDatum(code []byte) (datum []byte, remaining []byte) {
	var i int
	var b byte
	for i, b = range code {
		if b == Terminator {
			break
		}
	}
	return code[:i], code[i+1:]
}

func (codec *Codec) normalizeFloat(value float64, code []byte) ([]byte, error) {
	switch codec.numberType.(type) {
	case float64:
		cs := EncodeFloat([]byte(strconv.FormatFloat(value, 'e', -1, 64)), code)
		return cs, nil

	case int64:
		return EncodeInt([]byte(strconv.Itoa(int(value))), code), nil

	case string:
		cs := EncodeFloat([]byte(strconv.FormatFloat(value, 'e', -1, 64)), code)
		return cs, nil
	}
	return nil, ErrorNumberType
}

func (codec *Codec) denormalizeFloat(text []byte) ([]byte, error) {
	var err error
	var f float64

	switch codec.numberType.(type) {
	case float64:
		return text, nil

	case int64:
		f, err = strconv.ParseFloat(string(text), 64)
		return []byte(strconv.Itoa(int(f))), nil

	case string:
		f, err = strconv.ParseFloat(string(text), 64)
		if err == nil {
			return []byte(strconv.FormatFloat(f, 'f', -1, 64)), nil
		}

	default:
		return text, nil
	}
	return nil, ErrorNumberType
}

// Equal checks wether n is MissingLiteral
func (m Missing) Equal(n string) bool {
	s := string(m)
	if len(n) == len(s) && n[0] == '~' && n[1] == '[' {
		return s == n
	}
	return false
}

func (codec *Codec) n1ql2code(val n1ql.Value, code []byte) ([]byte, error) {
	var cs []byte
	var err error

	switch val.Type() {
	case n1ql.NULL:
		code = append(code, TypeNull, Terminator)
	case n1ql.BOOLEAN:
		act := val.Actual().(bool)
		if act {
			code = append(code, TypeTrue, Terminator)
		} else {
			code = append(code, TypeFalse, Terminator)
		}
	case n1ql.NUMBER:
		act := val.Actual().(float64)
		code = append(code, TypeNumber)
		cs, err = codec.normalizeFloat(act, code[1:])
		if err == nil {
			code = code[:len(code)+len(cs)]
			code = append(code, Terminator)
		}
	case n1ql.STRING:
		code = append(code, TypeString)
		act := val.Actual().(string)
		cs = suffixEncodeString([]byte(act), code[1:])
		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)
	case n1ql.MISSING:
		code = append(code, TypeMissing)
		code = append(code, Terminator)
	case n1ql.ARRAY:
		act := val.Actual().([]interface{})
		code = append(code, TypeArray)
		if codec.arrayLenPrefix {
			arrlen := Length(len(act))
			if cs, err = codec.json2code(arrlen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}
		if err == nil {
			for _, val := range act {
				l := len(code)
				cs, err = codec.n1ql2code(n1ql.NewValue(val), code[l:])
				if err == nil {
					code = code[:l+len(cs)]
					continue
				}
				break
			}
			code = append(code, Terminator)
		}
	case n1ql.OBJECT:
		act := val.Actual().(map[string]interface{})
		code = append(code, TypeObj)
		if codec.propertyLenPrefix {
			proplen := Length(len(act))
			if cs, err = codec.json2code(proplen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}

		if err == nil {
			keys := codec.sortProps(val.Actual().(map[string]interface{}))
			for _, key := range keys {
				l := len(code)
				// encode key
				if cs, err = codec.n1ql2code(n1ql.NewValue(key), code[l:]); err != nil {
					break
				}
				code = code[:l+len(cs)]
				l = len(code)
				// encode value
				if cs, err = codec.n1ql2code(n1ql.NewValue(act[key]), code[l:]); err != nil {
					break
				}
				code = code[:l+len(cs)]
			}
			code = append(code, Terminator)
		}
	}

	return code, err
}

// Caller is responsible for providing sufficiently sized buffer
// Otherwise it may panic
func (codec *Codec) EncodeN1QLValue(val n1ql.Value, buf []byte) (bs []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				err = ErrorOutputLen
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	return codec.n1ql2code(val, buf)
}
