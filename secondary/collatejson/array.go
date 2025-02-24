package collatejson

import (
	"errors"
	"fmt"
	"strings"

	"github.com/couchbase/indexing/secondary/logging"
	n1ql "github.com/couchbase/query/value"
)

var ErrNotAnArray = errors.New("not an array")
var ErrLenPrefixUnsupported = errors.New("arrayLenPrefix is unsupported")

func (codec *Codec) ExplodeArray(code []byte, tmp []byte) (arr [][]byte, e error) {

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("ExplodeArray:: recovered panic - %s \n %s", r, logging.StackTrace())
			e = fmt.Errorf("%v", r)
		}
	}()

	var err error

	array := make([][]byte, 0)

	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code
	for code[0] != Terminator {
		_, code, err = codec.code2json(code, tmp)
		if err != nil {
			break
		}

		if size := len(elemBuf) - len(code); size > 0 {
			array = append(array, elemBuf[:size])
			elemBuf = code
		}
	}

	return array, err
}

// Explodes an encoded array, returns encoded parts as well as
// decoded parts. Also takes an array of explode positions and
// decode positions to determine what to explode and what to decode
func (codec *Codec) ExplodeArray2(code []byte, tmp, decbuf []byte, cktmp, dktmp [][]byte,
	explodePos, decPos []bool, explodeUpto int) ([][]byte, [][]byte, error) {
	var err error
	var text []byte

	if codec.arrayLenPrefix {
		return nil, nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code

	pos := 0
	for code[0] != Terminator {
		text, code, err = codec.code2json(code, tmp)
		if err != nil {
			break
		}

		if dktmp != nil {
			if decPos[pos] {
				copy(decbuf, text)
				x := decbuf[:len(text)]
				dktmp[pos] = x
				decbuf = decbuf[len(text):]
			} else {
				dktmp[pos] = nil
			}
		}

		if size := len(elemBuf) - len(code); size > 0 {
			if explodePos[pos] {
				cktmp[pos] = elemBuf[:size]
			}
			elemBuf = code
		}

		pos++
		if pos > explodeUpto {
			return cktmp, dktmp, err
		}
	}

	return cktmp, dktmp, err
}

// Explodes an encoded array, returns encoded parts as well as
// decoded parts. Also takes an array of explode positions and
// decode positions to determine what to explode and what to decode
func (codec *Codec) ExplodeArray3(code []byte, tmp []byte, cktmp [][]byte,
	dktmp n1ql.Values, explodePos, decPos []bool, explodeUpto int) (enc [][]byte,
	dec n1ql.Values, e error) {

	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				e = ErrorOutputLen
			} else {
				logging.Fatalf("ExplodeArray3:: recovered panic - %s \n %s", r, logging.StackTrace())
				e = fmt.Errorf("%v", r)
			}
		}
	}()

	var err error
	var val n1ql.Value
	var decode bool

	if codec.arrayLenPrefix {
		return nil, nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code

	pos := 0
	for code[0] != Terminator {

		if decPos != nil {
			decode = decPos[pos]
		}

		val, code, err = codec.code2n1ql(code, tmp, decode)
		if err != nil {
			break
		}

		if dktmp != nil {
			dktmp[pos] = val
		}

		if size := len(elemBuf) - len(code); size > 0 {
			if explodePos[pos] {
				cktmp[pos] = elemBuf[:size]
			}
			elemBuf = code
		}

		pos++
		if pos > explodeUpto {
			return cktmp, dktmp, err
		}
	}

	return cktmp, dktmp, err
}

// ExplodeArray5 is same as ExplodeArray3 except that it uses "SVPool"
// to allocate n1ql values instead of creating new values
func (codec *Codec) ExplodeArray5(code []byte, tmp []byte, cktmp [][]byte,
	dktmp n1ql.Values, explodePos, decPos []bool, explodeUpto int, svPool *n1ql.StringValuePoolForIndex) (enc [][]byte,
	dec n1ql.Values, e error) {

	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				e = ErrorOutputLen
			} else {
				logging.Fatalf("ExplodeArray3:: recovered panic - %s \n %s", r, logging.StackTrace())
				e = fmt.Errorf("%v", r)
			}
		}
	}()

	var err error
	var val n1ql.Value
	var decode bool

	if codec.arrayLenPrefix {
		return nil, nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code

	pos := 0
	for code[0] != Terminator {

		if decPos != nil {
			decode = decPos[pos]
		}

		val, code, err = codec.code2n1qlpooled(code, tmp, decode, pos, svPool)
		if err != nil {
			break
		}

		if dktmp != nil {
			dktmp[pos] = val
		}

		if size := len(elemBuf) - len(code); size > 0 {
			if explodePos[pos] {
				cktmp[pos] = elemBuf[:size]
			}
			elemBuf = code
		}

		pos++
		if pos > explodeUpto {
			return cktmp, dktmp, err
		}
	}

	return cktmp, dktmp, err
}

// Explodes an encoded array, returns all encoded parts
// without decoding any part of the array
func (codec *Codec) ExplodeArray4(code []byte, tmp []byte) (arr [][]byte, e error) {

	defer func() {
		if r := recover(); r != nil {
			if strings.Contains(fmt.Sprint(r), "slice bounds out of range") {
				e = ErrorOutputLen
			} else {
				logging.Fatalf("ExplodeArray4:: recovered panic - %s \n %s", r, logging.StackTrace())
				e = fmt.Errorf("%v", r)
			}
		}
	}()

	var err error
	array := make([][]byte, 0)

	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code

	for code[0] != Terminator {
		_, code, err = codec.code2n1ql(code, tmp, false)
		if err != nil {
			break
		}

		if size := len(elemBuf) - len(code); size > 0 {
			array = append(array, elemBuf[:size])
			elemBuf = code
		}
	}

	return array, err
}

func (codec *Codec) JoinArray(vals [][]byte, code []byte) (arr []byte, e error) {

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("JoinArray:: recovered panic - %s \n %s", r, logging.StackTrace())
			e = fmt.Errorf("%v", r)
		}
	}()

	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	code = append(code, TypeArray)
	for _, val := range vals {
		code = append(code, val...)
	}
	code = append(code, Terminator)

	return code, nil
}
