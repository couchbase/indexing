package collatejson

import (
	"bytes"
	"fmt"
	"github.com/couchbaselabs/dparval"
	"sort"
	"strconv"
)

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

type Codec struct {
	arrayLenPrefix    bool
	propertyLenPrefix bool
}

func NewCodec() *Codec {
	return &Codec{
		arrayLenPrefix:    true,
		propertyLenPrefix: true,
	}
}

func (codec *Codec) DoArrayLenPrefix(what bool) {
	codec.arrayLenPrefix = what
}

func (codec *Codec) DoPropertyLenPrefix(what bool) {
	codec.propertyLenPrefix = what
}

func (codec *Codec) Encode(rawjson []byte) []byte {
	doc := dparval.NewValueFromBytes(rawjson)
	code := json2code(codec, doc.Value())
	return code
}

//func Decode(code []byte) []byte {
//    json := code2json(code)
//    return json
//}

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
		return append(EncodeInt([]byte(strconv.Itoa(value))), TERMINATOR)
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
				res, []byte{TYPE_STRING}, []byte(key),
				json2code(codec, value[key]))
		}
		return append(bytes.Join(res, []byte{}), TERMINATOR)
	}
	panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", val, val))
}

//func code2json(code []byte) ([]byte, []byte, error) {
//    var val []byte
//    if len(code) == 0 {
//        return []byte{}
//    }
//    switch code[0] {
//    case TYPE_TRUE:
//        val = []byte("true")
//    case TYPE_FALSE:
//        val = []byte("false")
//    case float64:
//        fvalue := strconv.FormatFloat(value, 'f', -1, 64)
//        if !strings.Contains(fvalue, ".") {
//            fvalue = fvalue + ".0"
//        }
//        if value > -1.0 && value < 1.0 {
//            code = EncodeSD([]byte(fvalue))
//        } else {
//            code = EncodeLD([]byte(fvalue))
//        }
//        return joinBytes([]byte{TYPE_NUMBER}, code)
//    case int:
//        return EncodeInt([]byte(strconv.Itoa(value)))
//    case uint64:
//        return json2code(float64(value))
//    case string:
//        return joinBytes([]byte{TYPE_STRING}, []byte(value))
//    case []interface{}:
//        res := make([][]byte, 0)
//        res = append(res, []byte{TYPE_ARRAY}, json2code(len(value)))
//        for _, val := range value {
//            res = append(res, json2code(val))
//        }
//        return bytes.Join(res, []byte{})
//    case map[string]interface{}:
//        res := make([][]byte, 0)
//        res = append(res, []byte{TYPE_OBJ}, json2code(len(value)))
//        keys := sortProps(value)
//        for _, key := range keys {
//            res = append(
//                res, []byte{TYPE_STRING}, []byte(key), json2code(value[key]))
//        }
//        return bytes.Join(res, []byte{})
//    }
//    panic(fmt.Sprintf("collationType doesn't understand %+v of type %T", val, val))
//}

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
