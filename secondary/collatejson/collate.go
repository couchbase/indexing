//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// Package collatejson supplies Encoding and Decoding function to transform
// JSON text into binary representation without loosing information. That is,
//
//   - binary representation should preserve the sort order such that, sorting
//     binary encoded json documents much match sorting by functions that parse
//     and compare JSON documents.
//   - it must be possible to get back the original document, in semantically
//     correct form, from its binary representation.
//
// Notes:
//
//   - items in a property object are sorted by its property name before they
//     are compared with property's value.
package collatejson

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	json "github.com/couchbase/indexing/secondary/common/json"

	n1ql "github.com/couchbase/query/value"
)

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

// ErrInvalidKeyTypeInObject means key of the object is not string
var ErrInvalidKeyTypeInObject = errors.New("collatejson.invalidKeyTypeInObject")

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

const TerminatorSuffix byte = 1

// Codec structure
type Codec struct {
	arrayLenPrefix    bool        // if true, first sort arrays based on its length
	propertyLenPrefix bool        // if true, first sort properties based on length
	doMissing         bool        // if true, handle missing values (for N1QL)
	numberType        interface{} // "float64" | "int64" | "decimal"

	// If >= 0, Codec will return the position of the field in encoded key
	fieldPos   int
	encodedPos []int32 // List of positions where the field is encoded

	nestLevel int
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
		fieldPos:          -1,
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
func (codec *Codec) Decode(code, text []byte) (out []byte, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				retErr = ErrorOutputLen
			} else {
				retErr = fmt.Errorf("%v", r)
			}
		}
	}()

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

	case int64:
		code = append(code, TypeNumber)
		var intStr string
		var number Integer
		intStr, err = number.ConvertToScientificNotation(value)
		cs = EncodeFloat([]byte(intStr), code[1:])
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
		var scratch [128]byte
		datum, remaining = getDatum(code)
		_, ts = DecodeInt(datum[1:], scratch[:0])
		text = append(text, ts...)

	case TypeNumber:
		var scratch [128]byte
		datum, remaining = getDatum(code)
		ts = DecodeFloat(datum[1:], scratch[:0])
		ts, err = codec.denormalizeFloat(ts)
		ts = bytes.TrimLeft(ts, "+")
		var number Integer
		ts, _, _ = number.TryConvertFromScientificNotation(ts, false)
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
		var scratch [128]byte
		text = append(text, '[')
		if codec.arrayLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], scratch[:0])
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					text, code, err = codec.code2json(code, text)
					if err != nil {
						break
					}
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
				text, code, err = codec.code2json(code, text)
				if err != nil {
					break
				}
				comma = true
			}
		}
		remaining = code[1:] // remove Terminator
		text = append(text, ']')

	case TypeObj:
		var scratch [128]byte
		var l int
		text = append(text, '{')
		if codec.propertyLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], scratch[:0])
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					// decode key
					text, code, err = codec.code2json(code, text)
					if err != nil {
						break
					}
					text = append(text, ':')
					// decode value
					text, code, err = codec.code2json(code, text)
					if err != nil {
						break
					}
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
				text, code, err = codec.code2json(code, text)
				if err != nil {
					break
				}
				text = append(text, ':')
				// decode value
				text, code, err = codec.code2json(code, text)
				if err != nil {
					break
				}
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
		act := val.ActualForIndex().(bool)
		if act {
			code = append(code, TypeTrue, Terminator)
		} else {
			code = append(code, TypeFalse, Terminator)
		}
	case n1ql.NUMBER:
		act := val.ActualForIndex()
		code = append(code, TypeNumber)
		var cs []byte
		switch act.(type) {
		case float64:
			cs, err = codec.normalizeFloat(act.(float64), code[1:])
		case int64:
			var intStr string
			var number Integer
			intStr, err = number.ConvertToScientificNotation(act.(int64))
			cs = EncodeFloat([]byte(intStr), code[1:])
		}
		if err == nil {
			code = code[:len(code)+len(cs)]
			code = append(code, Terminator)
		}
	case n1ql.STRING:
		code = append(code, TypeString)
		act := val.ActualForIndex().(string)
		cs = suffixEncodeString([]byte(act), code[1:])
		code = code[:len(code)+len(cs)]
		code = append(code, Terminator)
	case n1ql.MISSING:
		code = append(code, TypeMissing)
		code = append(code, Terminator)
	case n1ql.ARRAY:
		act := val.ActualForIndex().([]interface{})
		code = append(code, TypeArray)
		codec.nestLevel++

		if codec.arrayLenPrefix {
			arrlen := Length(len(act))
			if cs, err = codec.json2code(arrlen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}
		if err == nil {
			for i, val := range act {
				l := len(code)
				cs, err = codec.n1ql2code(n1ql.NewValue(val), code[l:])
				if err == nil {

					if codec.nestLevel == 1 && i == codec.fieldPos {
						codec.encodedPos = append(codec.encodedPos, int32(l))
					}
					code = code[:l+len(cs)]
					continue
				}
				break
			}

			codec.nestLevel--
			code = append(code, Terminator)
		}
	case n1ql.OBJECT:
		act := val.ActualForIndex().(map[string]interface{})
		code = append(code, TypeObj)
		if codec.propertyLenPrefix {
			proplen := Length(len(act))
			if cs, err = codec.json2code(proplen, code[1:]); err == nil {
				code = code[:len(code)+len(cs)]
			}
		}

		if err == nil {
			keys := codec.sortProps(val.ActualForIndex().(map[string]interface{}))
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

type Integer struct{}

// Formats an int64 to scientic notation. Example:
// 75284 converts to 7.5284e+04
// 1200000 converts to 1.2e+06
// -612988654 converts to -6.12988654e+08
// This is used in encode path
func (i *Integer) ConvertToScientificNotation(val int64) (string, error) {

	// For integers that can be represented precisely with
	// float64, return using FormatFloat
	if val < 9007199254740991 && val > -9007199254740991 {
		return strconv.FormatFloat(float64(val), 'e', -1, 64), nil
	}

	intStr := strconv.FormatInt(val, 10)
	if len(intStr) == 0 {
		return "", nil
	}

	format := func(str string) string {
		var first, rem string
		first = str[0:1]
		if len(str) >= 2 {
			rem = str[1:]
		}
		if len(rem) == 0 { // The integer is a single digit number
			return first + ".e+00"
		} else {
			return first + "." + strings.TrimRight(rem, "0") + "e+" + strconv.Itoa(len(rem))
		}
	}

	var enotation string
	sign := ""
	if intStr[0:1] == "-" || intStr[0:1] == "+" { // The integer has a sign
		sign = intStr[0:1]
		enotation = format(intStr[1:])
	} else {
		enotation = format(intStr)
	}

	return sign + enotation, nil
}

// This function has been retained to support unit tests.
// Regular code path should use ConvertToScientificNotation.
func (i *Integer) ConvertToScientificNotation_TestOnly(val int64) (string, error) {
	intStr := strconv.FormatInt(val, 10)
	if len(intStr) == 0 {
		return "", nil
	}

	format := func(str string) string {
		var first, rem string
		first = str[0:1]
		if len(str) >= 2 {
			rem = str[1:]
		}
		if len(rem) == 0 { // The integer is a single digit number
			return first + ".e+00"
		} else {
			return first + "." + rem + "e+" + strconv.Itoa(len(rem))
		}
	}

	var enotation string
	sign := ""
	if intStr[0:1] == "-" || intStr[0:1] == "+" { // The integer has a sign
		sign = intStr[0:1]
		enotation = format(intStr[1:])
	} else {
		enotation = format(intStr)
	}

	return sign + enotation, nil
}

// If float, return e notation
// If integer, convert from e notation to standard notation
// This is used in decode path
func (i *Integer) TryConvertFromScientificNotation(val []byte,
	getInt64 bool) (ret []byte, isInt64 bool, i64 int64) {
	defer func() {
		if r := recover(); r != nil {
			ret = val
			isInt64 = false
		}
	}()

	number := string(val)
	sign := ""
	if number[0:1] == "-" || number[0:1] == "+" {
		sign = number[0:1]
		number = number[1:]
	}

	decimalPos := strings.Index(number, ".")
	ePos := strings.Index(number, "e")
	characteristic := number[0:decimalPos]
	mantissa := number[decimalPos+1 : ePos]

	exp, err := strconv.ParseInt(number[ePos+1:], 10, 64)
	if err != nil {
		return val, false, 0 // error condition, return input format
	}

	// If the exponent is more than 20, the number won't fit in int64 number
	// format. So, treat it as float.
	if exp > 20 {
		return val, false, 0
	}

	if exp > 0 && (int(exp) >= len(mantissa)) { // It is possibly an integer
		if int(exp) > len(mantissa) {
			mantissa = mantissa + strings.Repeat("0", int(exp)-len(mantissa))
		}
		if characteristic == "0" {
			// strconv.ParseInt is a single loop iteration over the input
			// and hence is not a bad choice
			if getInt64 {
				i64, err = strconv.ParseInt(sign+mantissa, 10, 64)
				if err != nil {
					// May not be an integer. In case of malformed input
					// further call to ParseFloat will fail too.
					return val, false, 0
				}
			}
			return []byte(sign + mantissa), true, i64
		} else {
			if getInt64 {
				i64, err = strconv.ParseInt(sign+characteristic+mantissa, 10, 64)
				if err != nil {
					// May not be an integer. In case of malformed input
					// further call to ParseFloat will fail too.
					return val, false, 0
				}
			}
			return []byte(sign + characteristic + mantissa), true, i64
		}
	}

	return val, false, 0
}

// Caller is responsible for providing sufficiently sized buffer
// Otherwise it may panic
func (codec *Codec) DecodeN1QLValue(code, buf []byte) (val n1ql.Value, err error) {

	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				err = ErrorOutputLen
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	val, _, err = codec.code2n1ql(code, buf, true)
	return val, err
}

func (codec *Codec) code2n1ql(code, text []byte, decode bool) (n1ql.Value, []byte, error) {
	if len(code) == 0 {
		return nil, nil, nil
	}

	var ts, remaining, datum []byte
	var err error
	var n1qlVal n1ql.Value

	switch code[0] {
	case Terminator:
		remaining = code

	case TypeMissing:
		datum, remaining = getDatum(code)
		if decode {
			n1qlVal = n1ql.NewMissingValue()
		}

	case TypeNull:
		datum, remaining = getDatum(code)
		if decode {
			n1qlVal = n1ql.NewNullValue()
		}

	case TypeTrue:
		datum, remaining = getDatum(code)
		if decode {
			n1qlVal = n1ql.TRUE_VALUE
		}

	case TypeFalse:
		datum, remaining = getDatum(code)
		if decode {
			n1qlVal = n1ql.FALSE_VALUE
		}

	case TypeLength:
		datum, remaining = getDatum(code)
		if decode {
			var val int64
			_, ts = DecodeInt(datum[1:], text)
			val, err = strconv.ParseInt(string(ts), 10, 64)
			if err == nil {
				n1qlVal = n1ql.NewValue(val)
			}
		}

	case TypeNumber:
		datum, remaining = getDatum(code)
		if decode {
			ts = DecodeFloat(datum[1:], text)
			ts, err = codec.denormalizeFloat(ts)
			ts = bytes.TrimLeft(ts, "+")
			var number Integer
			var isInt64 bool
			var i64 int64
			ts, isInt64, i64 = number.TryConvertFromScientificNotation(ts, true)

			if isInt64 {
				n1qlVal = n1ql.NewValue(i64)
			} else {
				var val float64
				val, err = strconv.ParseFloat(string(ts), 64)
				if err == nil {
					n1qlVal = n1ql.NewValue(val)
				}
			}
		}

	case TypeString:
		var strb []byte
		strb, remaining, err = suffixDecodeString(code[1:], text)
		if decode && err == nil {
			n1qlVal = n1ql.NewValue(string(strb))
		}

	case TypeArray:
		var l int
		var tv n1ql.Value
		arrval := make([]interface{}, 0)
		if codec.arrayLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text)
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					ln := len(text)
					lnc := len(code)
					tv, code, err = codec.code2n1ql(code, text[ln:], decode)
					if err != nil {
						break
					}
					text = text[:ln+lnc-len(code)]
					if decode {
						arrval = append(arrval, tv.ActualForIndex())
					}
				}
				if decode {
					n1qlVal = n1ql.NewValue(arrval)
				}
			}
		} else {
			code = code[1:]
			for code[0] != Terminator {
				ln := len(text)
				lnc := len(code)
				tv, code, err = codec.code2n1ql(code, text[ln:], decode)
				if err != nil {
					break
				}
				text = text[:ln+lnc-len(code)]
				if decode {
					arrval = append(arrval, tv.ActualForIndex())
				}
			}
			if decode {
				n1qlVal = n1ql.NewValue(arrval)
			}
		}
		remaining = code[1:] // remove Terminator

	case TypeObj:
		var l int
		var key, value n1ql.Value
		objval := make(map[string]interface{})
		if codec.propertyLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text)
			l, err = strconv.Atoi(string(ts))
			if err == nil {
				for ; l > 0; l-- {
					// decode key
					ln := len(text)
					lnc := len(code)
					key, code, err = codec.code2n1ql(code, text[ln:], decode)
					if err != nil {
						break
					}
					text = text[:ln+lnc-len(code)]
					// decode value
					ln = len(text)
					lnc = len(code)
					value, code, err = codec.code2n1ql(code, text[ln:], decode)
					if err != nil {
						break
					}
					text = text[:ln+lnc-len(code)]
					if decode {
						if keystr, ok := key.ActualForIndex().(string); ok {
							objval[keystr] = value.ActualForIndex()
						} else {
							err = ErrInvalidKeyTypeInObject
							break
						}
					}
				}
				if decode {
					n1qlVal = n1ql.NewValue(objval)
				}
			}
		} else {
			code = code[1:]
			for code[0] != Terminator {
				// decode key
				ln := len(text)
				lnc := len(code)
				key, code, err = codec.code2n1ql(code, text[ln:], decode)
				if err != nil {
					break
				}
				text = text[:ln+lnc-len(code)]
				// decode value
				ln = len(text)
				lnc = len(code)
				value, code, err = codec.code2n1ql(code, text[ln:], decode)
				if err != nil {
					break
				}
				text = text[:ln+lnc-len(code)]
				if decode {
					if keystr, ok := key.ActualForIndex().(string); ok {
						objval[keystr] = value.ActualForIndex()
					} else {
						err = ErrInvalidKeyTypeInObject
						break
					}
				}
			}
			if decode {
				n1qlVal = n1ql.NewValue(objval)
			}
		}
		remaining = code[1:] // remove Terminator
	}
	return n1qlVal, remaining, err
}

// DecodeN1QLValues takes collatejson encoded data as input, and returns
// the rows in n1ql.Values format, as required by n1ql.
// Note: Caller is responsible for providing sufficiently sized buffer
// Otherwise it may panic
func (codec *Codec) DecodeN1QLValues(code, buf []byte) (vals n1ql.Values, err error) {
	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				err = ErrorOutputLen
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	if len(code) == 0 {
		return nil, nil
	}

	// Assume that its an array.
	if code[0] != TypeArray {
		return nil, ErrNotAnArray
	}

	code = code[1:]

	// There aren't any in-product users of arrayLenPrefix
	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	text := buf
	var tv n1ql.Value

	vals = make(n1ql.Values, 0)
	for code[0] != Terminator {
		ln := len(text)
		lnc := len(code)
		tv, code, err = codec.code2n1ql(code, text[ln:], true)
		if err != nil {
			break
		}
		text = text[:ln+lnc-len(code)]
		vals = append(vals, tv)
	}
	return vals, err
}

// FixEncodedInt is a special purpose method only to address MB-28956.
// Do not use otherwise.
func (codec *Codec) FixEncodedInt(code, buf []byte) ([]byte, error) {

	var fixed []byte
	var err error

	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				err = ErrorOutputLen
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	fixed, _, err = codec.fixEncodedInt(code, buf)
	return fixed, err
}

// fixEncodedInt is a special purpose method only to address MB-28956.
// Do not use otherwise.
func (codec *Codec) fixEncodedInt(code, text []byte) ([]byte, []byte, error) {
	if len(code) == 0 {
		return text, code, nil
	}

	var ts, remaining, datum []byte
	var err error

	switch code[0] {
	case Terminator:
		remaining = code

	case TypeMissing, TypeNull, TypeTrue, TypeFalse, TypeLength:
		datum, remaining = getDatum(code)
		text = append(text, datum...)
		text = append(text, Terminator)

	case TypeNumber:
		text = append(text, TypeNumber)

		datum, remaining = getDatum(code)
		ts = DecodeFloat(datum[1:], text[1:])
		ts, err = codec.denormalizeFloat(ts)
		ts = bytes.TrimLeft(ts, "+")

		var number Integer
		var isInt64 bool
		var intval int64
		ts, isInt64, intval = number.TryConvertFromScientificNotation(ts, true)

		if isInt64 {
			var intStr string
			intStr, err = number.ConvertToScientificNotation(intval)
			ts = EncodeFloat([]byte(intStr), text[1:])
		} else {
			var val float64
			val, err = strconv.ParseFloat(string(ts), 64)
			ts, err = codec.normalizeFloat(val, text[1:])
		}

		text = append(text, ts...)
		text = append(text, Terminator)

	case TypeString:
		datum, remaining, err = getStringDatum(code)
		text = append(text, datum...)
		text = append(text, Terminator)

	case TypeArray:
		var l int
		text = append(text, TypeArray)
		if codec.arrayLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text[1:])
			l, err = strconv.Atoi(string(ts))

			text = append(text, datum...)
			text = append(text, Terminator)
			if err == nil {
				for ; l > 0; l-- {
					ln := len(text)
					ts, code, err = codec.fixEncodedInt(code, text[ln:])
					if err != nil {
						break
					}
					text = append(text, ts...)
				}
			}
		} else {
			code = code[1:]
			for code[0] != Terminator {
				ln := len(text)
				ts, code, err = codec.fixEncodedInt(code, text[ln:])
				if err != nil {
					break
				}
				text = append(text, ts...)
			}
		}
		remaining = code[1:] // remove Terminator
		text = append(text, Terminator)

	case TypeObj:
		var l int
		var key, value []byte
		text = append(text, TypeObj)
		if codec.propertyLenPrefix {
			datum, code = getDatum(code[1:])
			_, ts := DecodeInt(datum[1:], text[1:])
			l, err = strconv.Atoi(string(ts))

			text = append(text, datum...)
			text = append(text, Terminator)
			if err == nil {
				for ; l > 0; l-- {
					// decode key
					ln := len(text)
					key, code, err = codec.fixEncodedInt(code, text[ln:])
					if err != nil {
						break
					}
					text = append(text, key...)
					// decode value
					ln = len(text)
					value, code, err = codec.fixEncodedInt(code, text[ln:])
					if err != nil {
						break
					}
					text = append(text, value...)
				}
			}
		} else {
			code = code[1:]
			for code[0] != Terminator {
				// decode key
				ln := len(text)
				key, code, err = codec.fixEncodedInt(code, text[ln:])
				if err != nil {
					break
				}
				text = append(text, key...)
				// decode value
				ln = len(text)
				value, code, err = codec.fixEncodedInt(code, text[ln:])
				if err != nil {
					break
				}
				text = append(text, value...)
			}
		}
		remaining = code[1:] // remove Terminator
		text = append(text, Terminator)
	}
	return text, remaining, err
}

func getStringDatum(code []byte) ([]byte, []byte, error) {
	for i := 0; i < len(code); i++ {
		x := code[i]
		if x == Terminator {
			i++
			switch x = code[i]; x {
			case TerminatorSuffix:
				//keep moving
			case Terminator:
				if i == (len(code)) {
					return nil, nil, nil
				}
				return code[:i], code[i+1:], nil
			default:
				return nil, nil, ErrorSuffixDecoding
			}
			continue
		}
	}
	return nil, nil, ErrorSuffixDecoding
}

func (codec *Codec) SetFieldPos(pos int) {
	codec.fieldPos = pos
}

func (codec *Codec) GetEncodedPos() []int32 {
	return codec.encodedPos
}
