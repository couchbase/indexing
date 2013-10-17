package collatejson
import (
    "bytes"
    "fmt"
    "strconv"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

const (
    PLUS = 43
    MINUS = 45
    LT = 60
    GT = 62
    DOT = 46
    ZERO = 48
)

var negPrefix = byte(MINUS)
var posPrefix = byte(GT)

var negIntLookup = map[byte]byte {
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
var prefixOpp = map[byte]byte {
    posPrefix: negPrefix,
    negPrefix: posPrefix,
}

// encode integer
func encodeInt(text []byte) []byte{
    var acc [][]byte
    if len(text) == 0 { // empty input
        return []byte{}
    }

    // Handle different forms of zero.
    if len(text) == 1 && text[0] == ZERO {
        return []byte{ZERO}
    }
    if len(text) == 2 && text[0] == PLUS && text[1] == ZERO {
        return []byte{ZERO}
    }
    if len(text) == 2 && text[0] == MINUS && text[1] == ZERO {
        return []byte{ZERO}
    }

    // Handle positive or negative integers
    switch text[0] {
    case PLUS:
        acc = _encodePosInt(text[1:], make([][]byte, 0, 10))
    case MINUS:
        acc = _encodeNegInt(text[1:], make([][]byte, 0, 10))
    default:
        acc = _encodePosInt(text, make([][]byte, 0, 10))
    }
    return bytes.Join(acc, []byte{})
}

func _encodePosInt(text []byte, acc [][]byte) [][]byte {
    acc = append(acc, []byte{posPrefix})
    if len(text) > 1 {
        acc = _encodePosInt([]byte(strconv.Itoa(len(text))), acc)
    }
    acc = append(acc, text)
    return acc
}

func _encodeNegInt(text []byte, acc [][]byte) [][]byte {
    acc = append(acc, []byte{negPrefix})
    if len(text) > 1 {
        acc = _encodeNegInt([]byte(strconv.Itoa(len(text))), acc)
    }
    invert := make([]byte, 0, len(text))
    for _, x := range text {
        invert = append(invert, negIntLookup[x])
    }
    acc = append(acc, invert)
    return acc
}

// decode integers
func decodeInt(code []byte) []byte {
    if len(code) == 1 && code[0] == ZERO {
        return []byte{ZERO}
    } else if len(code) == 2 {
        if (code[0] == posPrefix || code[0] == negPrefix) && code[1] == ZERO {
            sign, _ := signPrefix(code)
            return []byte{sign, ZERO}
        }
    }
    val, text := _decodeInt(code)
    if code[0] == posPrefix {
        return joinBytes([]byte{PLUS}, text[:val])
    } else {
        invtext := make([]byte, 0, len(code))
        for i := 0; i < val; i++ {
            invtext = append(invtext, negIntLookup[text[i]])
        }
        return joinBytes([]byte{MINUS}, invtext)
    }
}

func _decodeInt(code []byte) (int, []byte) {
    var val int
    code = code[1:]
    if code[0] == posPrefix {
        val, code = _decodeInt(code)
        vali, _ := strconv.Atoi(string(code[:val]))
        return vali, code[val:]
    } else if code[0] == negPrefix {
        val, code = _decodeInt(code)
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


// small-decimal conversions
//
// Caveats:
//  -0.0, 0.0 and +0.0 must be filtered out as integer ZERO `0`.
func encodeSD(text []byte) []byte {
    prefix, text := signPrefix(text)
    //fmt.Println("sd", string(text))

    // Remove decimal point and all zeros before that.
    text = bytes.Split(text, []byte{DOT})[1]
    code := make([]byte, 0, len(text))
    //fmt.Println("prefix", prefix == negPrefix, prefix, negPrefix)
    if prefix == negPrefix { // Do inversion if negative number
        for _, x := range text {
            code = append(code, negIntLookup[x])
        }
    } else { // if positive number just copy the text
        code = code[:len(text)]
        copy(code, text)
    }
    return _wrapSmallDec(prefix, code)
}

func decodeSD(code []byte) []byte {
    prefix, sign := code[0], prefixSign(code)
    code = code[1:len(code)-1]
    text := make([]byte, 0, len(code))

    // If negative number invert the digits.
    if prefix == negPrefix {
        for _, x := range code {
            text = append(text, negIntLookup[x])
        }
    } else {
        text = code
    }
    return joinBytes([]byte{sign,ZERO,DOT}, text)
}

func _wrapSmallDec(prefix byte, code []byte) []byte {
    return joinBytes([]byte{prefix}, code, []byte{prefixOpp[prefix]})
}


// large decimal conversions
func encodeLD(text []byte) []byte {
    var codeint []byte
    prefix, _ := signPrefix(text)
    // Encode integer and decimal part.
    texts := bytes.Split(text, []byte{DOT})
    if len(texts[0]) == 1 && texts[0][0] == ZERO {
        codeint = []byte{prefix, ZERO}
    } else if len(texts[0]) == 2 {
        if (texts[0][0] == PLUS || texts[0][0] == MINUS) && texts[0][1] == ZERO {
            codeint = []byte{prefix, ZERO}
        } else {
            codeint = encodeInt(texts[0])
        }
    } else {
        codeint = encodeInt(texts[0])
    }
    codedec := encodeSD(joinBytes([]byte{text[0],ZERO,DOT}, texts[1]))
    // Adjust the decimal part
    codedec = codedec[1:]
    codedec[len(codedec)-1] = prefixOpp[prefix]
    return joinBytes(codeint, codedec)
}

func decodeLD(code []byte) []byte {
    prefix, sign := code[0], prefixSign(code)
    val, code := _decodeInt(code)
    textint := make([]byte, 0, len(code))

    // If negative number invert the digits.
    if  sign == MINUS {
        for i := 0; i < val; i++ {
            textint = append(textint, negIntLookup[code[i]])
        }
        textint = joinBytes([]byte{sign}, textint)
    } else {
        textint = joinBytes([]byte{sign}, code[:val])
    }

    textdec := decodeSD(joinBytes([]byte{prefix}, code[val:]))
    return joinBytes(textint, textdec[2:])
}

// float conversions
func encodeFloat(exp []byte, mantissa []byte) []byte {
    prefix, _ := signPrefix(mantissa)
    // Encode integer and decimal part.
    codeint := encodeInt(exp)
    codedec := encodeSD(mantissa)
    // Adjust the decimal part
    codedec = codedec[1:]
    return joinBytes([]byte{prefix}, codeint, codedec)
}

func decodeFloat(code []byte) ([]byte, []byte) {
    sign := prefixSign(code)
    val, code := _decodeInt(code[1:])
    textint := joinBytes([]byte{prefixSign(code[1:])}, code[:val])
    textdec := decodeSD(joinBytes([]byte{sign}, code[val:]))
    return textint, textdec
}


func signPrefix(text []byte) (byte, []byte) {
    //fmt.Println("sing", string(text))
    switch text[0] {
    case PLUS:
        return posPrefix, text[1:]
    case MINUS:
        return negPrefix, text[1:]
    default:
        return posPrefix, text
    }
}

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

func joinBytes(args ...[]byte) []byte {
    return bytes.Join(args, []byte{})
}
