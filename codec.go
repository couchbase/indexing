//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package collatejson

import (
    "bytes"
    "fmt"
    "strconv"
)

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
func EncodeInt(text []byte) []byte {
    if len(text) == 0 { // empty input
        return []byte{}
    }
    if val := isZero(text); val != nil {
        return val
    }

    // Handle positive or negative integers
    var acc [][]byte
    switch text[0] {
    case PLUS:
        acc = encodePosInt(text[1:], make([][]byte, 0, 16))
    case MINUS:
        acc = encodeNegInt(text[1:], make([][]byte, 0, 16))
    default:
        acc = encodePosInt(text, make([][]byte, 0, 16))
    }
    return bytes.Join(acc, []byte{})
}

// encode positive integer, local function gets called by EncodeInt
//  encoding 7,          >7
//  encoding 123,        >>3123
//  encoding 1234567890, >>>2101234567890
func encodePosInt(text []byte, acc [][]byte) [][]byte {
    acc = append(acc, []byte{posPrefix})
    if len(text) > 1 {
        acc = encodePosInt([]byte(strconv.Itoa(len(text))), acc)
    }
    acc = append(acc, text)
    return acc
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
func encodeNegInt(text []byte, acc [][]byte) [][]byte {
    acc = append(acc, []byte{negPrefix})
    if len(text) > 1 {
        acc = encodeNegInt([]byte(strconv.Itoa(len(text))), acc)
    }
    invert := make([]byte, 0, len(text))
    for _, x := range text {
        invert = append(invert, negIntLookup[x])
    }
    acc = append(acc, invert)
    return acc
}

// DecodeInt complements EncodeInt, it returns integer in text that can be
// converted to integer value using strconv.AtoI(return_value)
func DecodeInt(code []byte) []byte {
    if len(code) == 0 { // empty input
        return []byte{}
    } else if code[0] == ZERO {
        return []byte{ZERO}
    }

    val, text := doDecodeInt(code)
    if code[0] == posPrefix {
        return joinBytes([]byte{PLUS}, text[:val])
    }
    invtext := make([]byte, 0, len(code))
    for i := 0; i < val; i++ {
        invtext = append(invtext, negIntLookup[text[i]])
    }
    return joinBytes([]byte{MINUS}, invtext)
}

// local function called by DecodeInt
func doDecodeInt(code []byte) (int, []byte) {
    var val int
    code = code[1:]
    if code[0] == posPrefix {
        val, code = doDecodeInt(code)
        vali, _ := strconv.Atoi(string(code[:val]))
        return vali, code[val:]
    } else if code[0] == negPrefix {
        val, code = doDecodeInt(code)
        codeLen := make([]byte, 0, val)
        for _, x := range code[:val] {
            codeLen = append(codeLen, negIntLookup[x])
        }
        vali, _ := strconv.Atoi(string(codeLen))
        return vali, code[val:]
    } else {
        return 1, code
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
func EncodeFloat(text []byte) []byte {
    if len(text) == 0 { // empty input
        return []byte{}
    }
    if val, e := strconv.ParseFloat(string(text), 64); e == nil && val == 0 {
        return []byte{ZERO}
    }

    prefix, text := signPrefix(text)
    exp, mantissa := parseFloat(prefix, text)
    // Encode integer and decimal part.
    codeint, codedec := EncodeInt(exp), EncodeSD(mantissa)
    // Adjust the decimal part
    codedec = codedec[1:]
    return joinBytes([]byte{prefix}, codeint, codedec)
}

// DecodeFloat complements EncodeFloat, it returns `exponent` and `mantissa`
// in text format.
func DecodeFloat(code []byte) []byte {
    if len(code) == 0 { // empty input
        return []byte{}
    } else if len(code) == 1 && code[0] == ZERO {
        return code
    }

    msign := code[0]

    var val int
    var icode, e []byte

    if code[1] != negPrefix && code[1] != posPrefix {
        val, icode = 0, code[2:]
        e = joinBytes([]byte{PLUS}, DecodeInt(code[1:]))
    } else {
        val, icode = doDecodeInt(code[1:])
        e = DecodeInt(code[1:])
    }
    flipmap := map[byte]byte{PLUS: MINUS, MINUS: PLUS}
    if msign == negPrefix {
        e[0] = flipmap[e[0]]
    }

    m := DecodeSD(joinBytes([]byte{msign}, icode[val:]))
    return joinBytes(m, []byte("e"), e)
}

func parseFloat(prefix byte, text []byte) ([]byte, []byte) {
    var exp string
    parts := bytes.Split(text, []byte("e"))
    m := parseMantissa(parts[0])
    expi, _ := strconv.Atoi(string(parts[1]))
    if prefix == negPrefix {
        exp = strconv.Itoa(-(expi + 1))
    } else {
        exp = strconv.Itoa(expi + 1)
    }
    mantissa := joinBytes([]byte{prefix}, []byte(m))
    return []byte(exp), mantissa
}

func parseMantissa(text []byte) []byte {
    parts := bytes.Split(text, []byte("."))
    switch len(parts) {
    case 1:
        return joinBytes([]byte("0."), parts[0])
    case 2:
        return joinBytes([]byte("0."), parts[0], parts[1])
    default:
        panic(fmt.Errorf("invalid mantissa %v", string(text)))
    }
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
func EncodeSD(text []byte) []byte {
    if len(text) == 0 { // empty input
        return []byte{}
    }

    prefix, text := signPrefix(text)

    // Remove decimal point and all zeros before that.
    text = bytes.Split(text, []byte{DOT})[1]
    code := make([]byte, 0, len(text))
    if prefix == negPrefix { // Do inversion if negative number
        for _, x := range text {
            code = append(code, negIntLookup[x])
        }
    } else { // if positive number just copy the text
        code = code[:len(text)]
        copy(code, text)
    }
    return wrapSmallDec(prefix, code)
}

// DecodeSD complements EncodeSD, it returns integer in text that can be
// converted to integer type using strconv.ParseFloat(return_value, 64).
func DecodeSD(code []byte) []byte {
    if len(code) == 0 {
        return []byte{}
    }

    prefix, sign := code[0], prefixSign(code)
    code = code[1 : len(code)-1]
    text := make([]byte, 0, len(code))

    // If negative number invert the digits.
    if prefix == negPrefix {
        for _, x := range code {
            text = append(text, negIntLookup[x])
        }
    } else {
        text = code
    }
    return joinBytes([]byte{sign, ZERO, DOT}, text)
}

// local function
func wrapSmallDec(prefix byte, code []byte) []byte {
    return joinBytes([]byte{prefix}, code, []byte{prefixOpp[prefix]})
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
func EncodeLD(text []byte) []byte {
    if len(text) == 0 { // empty input
        return []byte{}
    }

    var codeint, codedec []byte
    prefix, _ := signPrefix(text)
    // Encode integer and decimal part.
    texts := bytes.Split(text, []byte{DOT})
    if len(texts[0]) == 1 && texts[0][0] == ZERO {
        codeint = []byte{prefix, ZERO}
    } else if len(texts[0]) == 2 {
        if (texts[0][0] == PLUS || texts[0][0] == MINUS) && texts[0][1] == ZERO {
            codeint = []byte{prefix, ZERO}
        } else {
            codeint = EncodeInt(texts[0])
        }
    } else {
        codeint = EncodeInt(texts[0])
    }
    if len(texts) == 2 {
        codedec = EncodeSD(joinBytes([]byte{text[0], ZERO, DOT}, texts[1]))
    } else {
        codedec = EncodeSD(joinBytes([]byte{text[0], ZERO, DOT, ZERO}))
    }
    // Adjust the decimal part
    codedec = codedec[1:]
    codedec[len(codedec)-1] = prefixOpp[prefix]
    if len(codedec) == 2 && codedec[0] == ZERO {
        codedec = codedec[1:]
    }
    return joinBytes(codeint, codedec)
}

// DecodeLD complements EncodeLD, it returns integer in text that can be
// converted to integer type using strconv.ParseFloat(return_value, 64).
func DecodeLD(code []byte) []byte {
    if len(code) == 0 { // empty input
        return []byte{}
    }

    prefix, sign := code[0], prefixSign(code)
    val, code := doDecodeInt(code)
    textint := make([]byte, 0, len(code))

    // If negative number invert the digits.
    if sign == MINUS {
        for i := 0; i < val; i++ {
            textint = append(textint, negIntLookup[code[i]])
        }
        textint = joinBytes([]byte{sign}, textint)
    } else {
        textint = joinBytes([]byte{sign}, code[:val])
    }

    textdec := DecodeSD(joinBytes([]byte{prefix}, code[val:]))
    return joinBytes(textint, textdec[2:])
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
    default:
        panic("encoded json number must always have sign prefix")
    }
    return sign
}

// join byte slices into single byte slice.
func joinBytes(args ...[]byte) []byte {
    return bytes.Join(args, []byte{})
}

// Check whether input is a representation of zero, like,
//    0 +0 -0
// return the normalized form of zero, which is just `0`.
func isZero(text []byte) []byte {
    // Handle different forms of zero.
    if len(text) == 1 && text[0] == ZERO {
        return []byte{ZERO}
    } else if len(text) == 2 && text[0] == PLUS && text[1] == ZERO {
        return []byte{ZERO}
    } else if len(text) == 2 && text[0] == MINUS && text[1] == ZERO {
        return []byte{ZERO}
    }
    return nil
}
