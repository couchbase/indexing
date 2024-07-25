package collatejson

import (
	"fmt"
	"strconv"

	"github.com/couchbase/indexing/secondary/logging"
)

// ReverseCollate reverses the bits in an encoded byte stream based
// on the fields specified as desc. Calling reverse on an already
// reversed stream gives back the original stream.
func (codec *Codec) ReverseCollate(code []byte, desc []bool) (rev []byte, e error) {

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("ReverseCollate:: recovered panic - %s \n %s", r, logging.StackTrace())
			e = fmt.Errorf("%v", r)
		}
	}()

	for i, d := range desc {
		if d {
			field, _, _ := codec.extractEncodedField(code, i+1)
			flipBits(field)
		}
	}
	return code, nil
}

// flips the bits of the given byte slice
func flipBits(code []byte) {

	for i, b := range code {
		code[i] = ^b
	}
	return

}

// get the encoded datum based on Terminator and return a
// tuple of, `encoded-datum`, `remaining-code`, where remaining-code starts
// after the Terminator
func getEncodedDatum(code []byte) (datum []byte, remaining []byte) {
	var i int
	var b byte
	for i, b = range code {
		if b == Terminator || b == ^Terminator {
			break
		}
	}
	return code[:i+1], code[i+1:]
}

func getEncodedString(code []byte) ([]byte, []byte, error) {
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
				return code[:i+1], code[i+1:], nil
			default:
				return nil, nil, ErrorSuffixDecoding
			}
			continue
		} else if x == ^Terminator {
			i++
			switch x = code[i]; x {
			case ^TerminatorSuffix:
				//keep moving
			case ^Terminator:
				if i == (len(code)) {
					return nil, nil, nil
				}
				return code[:i+1], code[i+1:], nil
			default:
				return nil, nil, ErrorSuffixDecoding
			}
			continue
		}
	}
	return nil, nil, ErrorSuffixDecoding
}

// extracts a given field from the encoded byte stream
func (codec *Codec) extractEncodedField(code []byte, fieldPos int) ([]byte, []byte, error) {
	if len(code) == 0 {
		return code, code, nil
	}

	var ts, remaining, datum, orig []byte
	var err error

	orig = code

	switch code[0] {
	case Terminator:
		remaining = code

	case TypeMissing, ^TypeMissing:
		datum, remaining = getEncodedDatum(code)

	case TypeNull, ^TypeNull:
		datum, remaining = getEncodedDatum(code)

	case TypeTrue, ^TypeTrue:
		datum, remaining = getEncodedDatum(code)

	case TypeFalse, ^TypeFalse:
		datum, remaining = getEncodedDatum(code)

	case TypeLength, ^TypeLength:
		datum, remaining = getEncodedDatum(code)

	case TypeNumber, ^TypeNumber:
		datum, remaining = getEncodedDatum(code)

	case TypeString, ^TypeString:
		datum, remaining, err = getEncodedString(code)

	case TypeArray, ^TypeArray:
		var l, currField, currFieldStart int
		if codec.arrayLenPrefix {
			tmp := bufPool.Get().(*[]byte)
			datum, code = getEncodedDatum(code[1:])

			if datum[0] == ^TypeLength {
				flipBits(datum[1:])
			}

			_, ts := DecodeInt(datum[1:len(datum)-1], (*tmp)[:0])
			l, err = strconv.Atoi(string(ts))
			bufPool.Put(tmp)

			if datum[0] == ^TypeLength {
				flipBits(datum[1:])
			}

			currFieldStart = 1
			if err == nil {
				currField = 1
				for ; l > 0; l-- {
					ts, code, err = codec.extractEncodedField(code, 0)
					if err != nil {
						break
					}
					if currField == fieldPos {
						return orig[currFieldStart : currFieldStart+len(ts)], nil, nil
					}
					currField++
					currFieldStart = currFieldStart + len(ts)
				}
			}
		} else {
			code = code[1:]
			currField = 1
			currFieldStart = 1
			for code[0] != Terminator && code[0] != ^Terminator {
				ts, code, err = codec.extractEncodedField(code, 0)
				if err != nil {
					break
				}
				if currField == fieldPos {
					return orig[currFieldStart : currFieldStart+len(ts)], nil, nil
				}
				currField++
				currFieldStart = currFieldStart + len(ts)
				datum = append(datum, ts...)

			}
		}
		remaining = code[1:]             // remove Terminator
		datum = orig[0 : len(datum)+1+1] //1 for Terminator and 1 for Type

	case TypeObj, ^TypeObj:
		var l int
		var key, value []byte
		if codec.propertyLenPrefix {
			tmp := bufPool.Get().(*[]byte)
			datum, code = getEncodedDatum(code[1:])

			if datum[0] == ^TypeLength {
				flipBits(datum[1:])
			}

			_, ts := DecodeInt(datum[1:len(datum)-1], (*tmp)[:0])
			l, err = strconv.Atoi(string(ts))
			bufPool.Put(tmp)

			if datum[0] == ^TypeLength {
				flipBits(datum[1:])
			}

			if err == nil {
				for ; l > 0; l-- {
					key, code, err = codec.extractEncodedField(code, 0)
					if err != nil {
						break
					}
					value, code, err = codec.extractEncodedField(code, 0)
					if err != nil {
						break
					}
					datum = append(datum, key...)
					datum = append(datum, value...)
				}
			}
		} else {
			code = code[1:]
			for code[0] != Terminator && code[0] != ^Terminator {
				key, code, err = codec.extractEncodedField(code, 0)
				if err != nil {
					break
				}
				value, code, err = codec.extractEncodedField(code, 0)
				if err != nil {
					break
				}
				datum = append(datum, key...)
				datum = append(datum, value...)
			}
		}
		remaining = code[1:]             // remove Terminator
		datum = orig[0 : len(datum)+1+1] //1 for Terminator and 1 for Type
	}
	return datum, remaining, err
}

// ReplaceEncodedFieldInArray takes replaceFieldPos i.e. 0 based index into given collatejson encoded code of array
// and replaces that field with dataToReplace (which is expected to be code of new field). It uses newCodeBuf to
// place the results and return the same if newCodeBuf is of lesser capacity than result it will return error.
func (codec *Codec) ReplaceEncodedFieldInArray(code []byte, replaceFieldPos int, dataToReplace, newCodeBuf []byte) ([]byte, error) {
	if len(code) == 0 {
		return code, nil
	}

	var ts, orig []byte
	var err error

	orig = code

	switch code[0] {
	case TypeArray, ^TypeArray:
		var currField, currFieldStart int
		if codec.arrayLenPrefix {
			return nil, ErrNotImplemented
		} else {
			code = code[1:]
			currField = 0
			currFieldStart = 1
			for code[0] != Terminator && code[0] != ^Terminator {
				ts, code, err = codec.extractEncodedField(code, 0)
				if err != nil {
					return nil, err
				}
				if currField == replaceFieldPos {
					newCodeLen := len(orig[:currFieldStart]) + len(dataToReplace) + len(orig[currFieldStart+len(ts):])
					if cap(newCodeBuf) < newCodeLen {
						return nil, ErrorOutputLen
					}
					newCodeBuf = append(newCodeBuf, orig[:currFieldStart]...)
					newCodeBuf = append(newCodeBuf, dataToReplace...)
					newCodeBuf = append(newCodeBuf, orig[currFieldStart+len(ts):]...)
					return newCodeBuf, nil
				}
				currField++
				currFieldStart = currFieldStart + len(ts)
			}
		}
	default:
		return nil, ErrInvalidInput
	}
	return nil, ErrInvalidInput
}
