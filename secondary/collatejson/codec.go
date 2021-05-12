//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package collatejson

import "bytes"
import "errors"
import "strconv"

// error codes
var ErrorSuffixDecoding = errors.New("collatejson.suffixDecoding")

// Constants used in text representation of basic data types.
const (
	PLUS  = 43
	MINUS = 45
	LT    = 60
	GT    = 62
	DOT   = 46
	ZERO  = 48
)

// Constants used to represent positive and negative numbers while encoding
// them.
const (
	negPrefix = byte(MINUS)
	posPrefix = byte(GT)
)

// Negative integers, in its text representation are 10's complement. This map
// provides the lookup table to generate the complements.
var negIntLookup = map[byte]byte{
	48: 57, // 0 - 9
	49: 56, // 1 - 8
	50: 55, // 2 - 7
	51: 54, // 3 - 6
	52: 53, // 4 - 5
	53: 52, // 5 - 4
	54: 51, // 6 - 3
	55: 50, // 7 - 2
	56: 49, // 8 - 1
	57: 48, // 9 - 0
}

// A simple lookup table to flip prefixes.
var prefixOpp = map[byte]byte{
	posPrefix: negPrefix,
	negPrefix: posPrefix,
}

// EncodeInt encodes integer such that their natural order is preserved as a
// lexicographic order of their representation. Additionally it must be
// possible to get back the natural representation from its lexical
// representation.
//
// Input `text` is also in textual representation, that is, strconv.Atoi(text)
// is the actual integer that is encoded.
//
// Zero is encoded as '0'
func EncodeInt(text, code []byte) []byte { // text -> code
	if len(text) == 0 { // empty input
		return code
	}
	if code, ok := isZero(text, code); ok {
		return code
	}

	switch text[0] {
	case PLUS: // positive int
		code = encodePosInt(text[1:], code)
	case MINUS: // negative int
		code = encodeNegInt(text[1:], code)
	default:
		code = encodePosInt(text, code)
	}
	return code
}

// encode positive integer, local function gets called by EncodeInt
//  encoding 7,          >7
//  encoding 123,        >>3123
//  encoding 1234567890, >>>2101234567890
func encodePosInt(text []byte, code []byte) []byte {
	code = append(code, posPrefix)
	if len(text) > 1 {
		// TODO: replace Itoa with more efficient function.
		c := encodePosInt([]byte(strconv.Itoa(len(text))), code[1:])
		code = code[:1+len(c)]
	}
	code = append(code, text...)
	return code
}

// encode positive integer, local function gets called by EncodeInt
//  encoding -1,         -8
//  encoding -2,         -7
//  encoding -9,         -0
//  encoding -10,        --789
//  encoding -11,        --788
//  encoding -1234567891 ---7898765432108
//  encoding -1234567890 ---7898765432109
//  encoding -1234567889 ---7898765432110
func encodeNegInt(text []byte, code []byte) []byte {
	code = append(code, negPrefix)
	if len(text) > 1 {
		// TODO: replace Itoa with more efficient function.
		c := encodeNegInt([]byte(strconv.Itoa(len(text))), code[1:])
		code = code[:1+len(c)]
	}
	for _, x := range text {
		code = append(code, negIntLookup[x])
	}
	return code
}

// Check whether input is a representation of zero, like,
//    0 +0 -0
func isZero(text, code []byte) ([]byte, bool) {
	// Handle different forms of zero.
	if (len(text) == 1) && (text[0] == ZERO) {
		code = append(code, ZERO)
		return code, true

	} else if (len(text) == 2) && ((text[0] == PLUS) || (text[0] == MINUS)) &&
		(text[1] == ZERO) {

		code = append(code, ZERO)
		return code, true
	}
	return code, false
}

// DecodeInt complements EncodeInt, it returns integer in text that can be
// converted to integer value using strconv.AtoI(return_value)
func DecodeInt(code, text []byte) (int, []byte) { // code -> text
	if len(code) == 0 { // empty input
		return 0, text
	}
	if code[0] == ZERO {
		text = append(text, ZERO)
		return 1, text
	}

	var skip int
	var final []byte

	switch code[0] {
	case posPrefix:
		text = append(text, PLUS)
		skip, final = doDecodeInt(code[1:], text[1:])
		text = append(text, final...)

	case negPrefix:
		text = append(text, MINUS)
		skip, final = doDecodeInt(code[1:], text[1:])
		for _, x := range final {
			text = append(text, negIntLookup[x])
		}
	}
	return skip + len(text), text
}

// local function called by DecodeInt
func doDecodeInt(code, text []byte) (int, []byte) {
	var skip int

	if code[0] == posPrefix {
		skip, text = doDecodeInt(code[1:], text)
		s := skip + len(text) + 1
		l, _ := strconv.Atoi(string(text))
		text = append(text[:0], code[s:s+l]...)
		return s, text

	} else if code[0] == negPrefix {
		skip, text = doDecodeInt(code[1:], text)
		for i, x := range text {
			text[i] = negIntLookup[x]
		}
		s := skip + len(text) + 1
		l, _ := strconv.Atoi(string(text))
		text = append(text[:0], code[s:s+l]...)
		return s, text

	} else {
		text = append(text, code[0])
		return 0, text
	}
}

// EncodeFloat encodes floating point number such that their natural order is
// preserved as lexicographic order of their representation. Additionally it
// must be possible to get back the natural representation from its lexical
// representation.
//
// A floating point number f takes a mantissa m ∈ [1/10 , 1) and an integer
// exponent e such that f = (10^e) * ±m.
//
//  encoding −0.1 × 10^11    - --7888+
//  encoding −0.1 × 10^10    - --7898+
//  encoding -1.4            - -885+
//  encoding -1.3            - -886+
//  encoding -1              - -88+
//  encoding -0.123          - 0876+
//  encoding -0.0123         - +1876+
//  encoding -0.001233       - +28766+
//  encoding -0.00123        - +2876+
//  encoding 0               0
//  encoding +0.00123        + -7123-
//  encoding +0.001233       + -71233-
//  encoding +0.0123         + -8123-
//  encoding +0.123          + 0123-
//  encoding +1              + +11-
//  encoding +1.3            + +113-
//  encoding +1.4            + +114-
//  encoding +0.1 × 10^10    + ++2101-
//  encoding +0.1 × 10^11    + ++2111-
func EncodeFloat(text, code []byte) []byte {
	if len(text) == 0 { // empty input
		return code
	}
	if val, e := strconv.ParseFloat(string(text), 64); e == nil && val == 0 {
		code = append(code, ZERO)
		return code
	}

	prefix, text := signPrefix(text)
	code = append(code, prefix)

	var exp, mant []byte
	for i, x := range text {
		if x == 'e' {
			mant = text[:i]
			exp = text[i+1:]
			break
		}
	}

	x := [128]byte{}
	mantissa := append(x[:0], prefix, '0', '.')
	for _, x := range mant {
		if x == '.' {
			continue
		}
		mantissa = append(mantissa, x)
	}

	expi, _ := strconv.Atoi(string(exp))
	if prefix == negPrefix {
		exp = []byte(strconv.Itoa(-(expi + 1)))
	} else {
		exp = []byte(strconv.Itoa(expi + 1))
	}

	// Encode integer.
	code = append(code, EncodeInt(exp, code[1:])...)
	// Encode and adjust the decimal part.
	code = append(code, EncodeSD(mantissa, code[len(code):])[1:]...)
	return code
}

var flipmap = map[byte]byte{PLUS: MINUS, MINUS: PLUS}

// DecodeFloat complements EncodeFloat, it returns `exponent` and `mantissa`
// in text format.
func DecodeFloat(code, text []byte) []byte {
	if len(code) == 0 { // empty input
		return text
	} else if len(code) == 1 && code[0] == ZERO {
		text = append(text, ZERO)
		return text
	}

	msign := code[0]

	var skip int
	var final, exponent []byte

	x := [128]byte{}
	switch code[1] {
	case negPrefix, posPrefix:
		skip, exponent = DecodeInt(code[1:], x[:0])

	default:
		exponent = append(x[:0], PLUS)
		skip, final = DecodeInt(code[1:], text)
		exponent = append(exponent, final...)
	}

	if msign == negPrefix {
		exponent[0] = flipmap[exponent[0]]
	}

	y := [128]byte{}
	mantissa := append(y[:0], msign)
	mantissa = append(mantissa, code[1+skip:]...)
	text = append(text, DecodeSD(mantissa, text)...)
	text = append(text, 'e')
	text = append(text, exponent...)
	return text
}

// EncodeSD encodes small-decimal, values that are greater than -1.0 and less
// than +1.0,such that their natural order is preserved as lexicographic order
// of their representation. Additionally it must be possible to get back the
// natural representation from its lexical representation.
//
// Small decimals is greater than -1.0 and less than 1.0
//
// Input `text` is also in textual representation, that is,
// strconv.ParseFloat(text, 64) is the actual integer that is encoded.
//
//  encoding -0.9995    -0004>
//  encoding -0.999     -000>
//  encoding -0.0123    -9876>
//  encoding -0.00123   -99876>
//  encoding -0.0001233 -9998766>
//  encoding -0.000123  -999876>
//  encoding +0.000123  >000123-
//  encoding +0.0001233 >0001233-
//  encoding +0.00123   >00123-
//  encoding +0.0123    >0123-
//  encoding +0.999     >999-
//  encoding +0.9995    >9995-
//
// Caveats:
//  -0.0, 0.0 and +0.0 must be filtered out as integer ZERO `0`.
func EncodeSD(text, code []byte) []byte {
	if len(text) == 0 { // empty input
		return code
	}

	var prefix byte
	prefix, text = signPrefix(text)
	code = append(code, prefix)

	// Remove decimal point and all zeros before that.
	for i, x := range text {
		if x == '.' {
			text = text[i+1:]
			break
		}
	}
	if prefix == negPrefix { // Do inversion if negative number
		for _, x := range text {
			code = append(code, negIntLookup[x])
		}
	} else { // if positive number just copy the text
		code = append(code, text...)
	}
	code = append(code, prefixOpp[prefix])
	return code
}

// DecodeSD complements EncodeSD, it returns integer in text that can be
// converted to integer type using strconv.ParseFloat(return_value, 64).
func DecodeSD(code, text []byte) []byte {
	if len(code) == 0 {
		return text
	}

	prefix, sign := code[0], prefixSign(code)
	text = append(text, []byte{sign, ZERO, DOT}...)

	// If negative number invert the digits.
	if prefix == negPrefix {
		for _, x := range code[1 : len(code)-1] {
			text = append(text, negIntLookup[x])
		}
	} else {
		text = append(text, code[1:len(code)-1]...)
	}
	return text
}

// EncodeLD encodes large-decimal, values that are greater than or equal to
// +1.0 and less than or equal to -1.0, such that their natural order is
// preserved as a lexicographic order of their representation. Additionally
// it must be possible to get back the natural representation from its lexical
// representation.
//
// Input `text` is also in textual representation, that is,
// strconv.ParseFloat(text, 64) is the actual integer that is encoded.
//
//  encoding -100.5         --68994>
//  encoding -10.5          --7>
//  encoding -3.145         -3854>
//  encoding -3.14          -385>
//  encoding -1.01          -198>
//  encoding -1             -1>
//  encoding -0.0001233     -09998766>
//  encoding -0.000123      -0999876>
//  encoding +0.000123      >0000123-
//  encoding +0.0001233     >00001233-
//  encoding +1             >1-
//  encoding +1.01          >101-
//  encoding +3.14          >314-
//  encoding +3.145         >3145-
//  encoding +10.5          >>2105-
//  encoding +100.5         >>31005-
func EncodeLD(text, code []byte) []byte {
	if len(text) == 0 { // empty input
		return code
	}

	prefix, _ := signPrefix(text)

	// Encode integer and decimal part.
	texts := bytes.Split(text, []byte{DOT})
	if len(texts[0]) == 1 && texts[0][0] == ZERO {
		code = append(code, prefix, ZERO)
	} else if len(texts[0]) == 2 {
		if s := texts[0][0]; (s == PLUS || s == MINUS) && texts[0][1] == ZERO {
			code = append(code, prefix, ZERO)
		} else {
			l := len(EncodeInt(texts[0], code))
			code = code[:l]
		}
	} else {
		l := len(EncodeInt(texts[0], code))
		code = code[:l]
	}

	var codedec []byte
	x := [128]byte{}
	if len(texts) == 2 {
		text = joinBytes([]byte{text[0], ZERO, DOT}, texts[1])
		codedec = EncodeSD(text, x[:0])[1:]
	} else {
		// TODO: This is constant, optimize away.
		text = []byte{text[0], ZERO, DOT, ZERO}
		codedec = EncodeSD(text, x[:0])[1:]
	}

	// Adjust the decimal part
	codedec[len(codedec)-1] = prefixOpp[prefix]
	if len(codedec) == 2 && codedec[0] == ZERO {
		codedec = codedec[1:]
	}
	code = append(code, codedec...)
	return code
}

// DecodeLD complements EncodeLD, it returns integer in text that can be
// converted to integer type using strconv.ParseFloat(return_value, 64).
func DecodeLD(code, text []byte) []byte {
	if len(code) == 0 { // empty input
		return text
	}

	prefix, sign := code[0], prefixSign(code)
	text = append(text, sign)
	skip, textint := doDecodeInt(code[1:], text[1:])
	l := len(textint) + 1
	text = text[:l]

	// If negative number invert the digits.
	if sign == MINUS {
		for i, x := range text[1:] {
			text[i+1] = negIntLookup[x]
		}
	}

	code = code[skip+l:]
	textdec := DecodeSD(joinBytes([]byte{prefix}, code), text[len(text):])[2:]
	text = append(text, textdec...)
	return text
}

// local function that check the sign of the input number and returns the
// encoding-prefix and remaining input. Used while encoding numbers.
func signPrefix(text []byte) (byte, []byte) {
	switch text[0] {
	case PLUS:
		return posPrefix, text[1:]
	case MINUS:
		return negPrefix, text[1:]
	default:
		return posPrefix, text
	}
}

// local function that check the encoded sign of the coded number and returns
// the actual sign of the number. Used while decoding numbers.
func prefixSign(code []byte) byte {
	var sign byte
	switch code[0] {
	case posPrefix:
		sign = PLUS
	case negPrefix:
		sign = MINUS
	}
	return sign
}

// join byte slices into single byte slice.
func joinBytes(args ...[]byte) []byte {
	return bytes.Join(args, []byte{})
}

func suffixEncodeString(s []byte, code []byte) []byte {
	text := []byte(s)
	for _, x := range text {
		code = append(code, x)
		if x == Terminator {
			code = append(code, TerminatorSuffix)
		} else if x == ^Terminator {
			code = append(code, ^TerminatorSuffix)
		}
	}
	code = append(code, Terminator)
	return code
}

func suffixDecodeString(code []byte, text []byte) ([]byte, []byte, error) {
	for i := 0; i < len(code); i++ {
		x := code[i]
		if x == Terminator {
			i++
			switch x = code[i]; x {
			case TerminatorSuffix:
				text = append(text, 0)
			case Terminator:
				if i == (len(code) - 1) {
					return text, nil, nil
				}
				return text, code[i+1:], nil
			default:
				return nil, nil, ErrorSuffixDecoding
			}
			continue
		} else if x == ^Terminator {
			switch x = code[i+1]; x {
			case ^TerminatorSuffix:
				i++ //skip the suffix
				text = append(text, ^Terminator)
			case ^Terminator:
				i++ //skip the terminator
				if i == (len(code) - 1) {
					return text, nil, nil
				}
				return text, code[i+1:], nil
			default:
				//support single ^terminator
				//for backward compatibility
				text = append(text, ^Terminator)
			}
			continue
		}
		text = append(text, x)
	}
	return nil, nil, ErrorSuffixDecoding
}
