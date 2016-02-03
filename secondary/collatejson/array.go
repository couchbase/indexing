package collatejson

import "errors"

var ErrNotAnArray = errors.New("not an array")
var ErrLenPrefixUnsupported = errors.New("arrayLenPrefix is unsupported")

func (codec *Codec) ExplodeArray(code []byte, tmp []byte) ([][]byte, error) {
	var ts []byte
	var err error

	var array [][]byte

	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code
	for code[0] != Terminator {
		ln := len(tmp)
		ts, code, err = codec.code2json(code, tmp[ln:])
		if err != nil {
			break
		}
		tmp = tmp[:ln+len(ts)]

		if size := len(elemBuf) - len(code); size > 0 {
			array = append(array, elemBuf[:size])
			elemBuf = code
		}
	}

	return array, err
}

func (codec *Codec) JoinArray(vals [][]byte, code []byte) ([]byte, error) {
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
