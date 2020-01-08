package collections

import (
	"errors"
	"strconv"
)

var ErrorEmptyCollectionId = errors.New("Empty collection ID")

// Returns the leb128 encoded value
func LEB128Enc(cid uint32) []byte {
	enc := make([]byte, 0)

	for {
		b := (byte)(cid & 0x7f) // Lower order 7 bits of CID
		cid >>= 7

		if cid != 0 { // More bytes to come
			b |= 0x80 // Set higher order bit
		}
		enc = append(enc, b) // emit byte

		if cid == 0 {
			break
		}
	}
	return enc
}

// Returns the leb128 encoded value
// Takes hexa-decimal string as input
func LEB128EncFrmStr(cidS string) ([]byte, error) {

	if cidS == "" {
		return nil, ErrorEmptyCollectionId
	}

	cidUint64, err := strconv.ParseUint(cidS, 16, 32) // Hexa decimal string with 32 bits
	if err != nil {
		return nil, err
	}
	cid := (uint32)(cidUint64)

	return LEB128Enc(cid), nil
}

// Prepends the leb128 encoded value to input byte stream
// and returns the prepended value
// Takes byte stream and uint32 value as input
func PrependLEB128EncKey(key []byte, cid uint32) []byte {
	enc := LEB128Enc(cid)
	return append(enc, key...)
}

// Prepends the leb128 encoded value to input byte stream
// and returns the prepended value
// Takes byte stream and hexa-decimal string as input
func PrependLEB128EncStrKey(key []byte, cid string) ([]byte, error) {
	enc, err := LEB128EncFrmStr(cid)
	if err != nil {
		return nil, err
	}
	return append(enc, key...), nil
}

// Decodes the encoded value according to LEB128 uint32 scheme
// Returns the decoded key as byte stream, collectionID as uint32 value
func LEB128Dec(data []byte) ([]byte, uint32) {

	if len(data) == 0 {
		return data, 0
	}

	cid := (uint32)(data[0] & 0x7f)
	end := 1
	if data[0]&0x80 == 0x80 {
		shift := 7
		for end = 1; end < len(data); end++ {
			cid |= ((uint32)(data[end]&0x7f) << (uint32)(shift))
			if data[end]&0x80 == 0 {
				break
			}
			shift += 7
		}
		end++
	}
	return data[end:], cid
}

// Decodes the encoded value according to LEB128 uint32 scheme
// Returns the decoded key as byte stream, collectionID as hexa decimal string
func LEB128DecToStr(data []byte) ([]byte, string) {

	if len(data) == 0 {
		return data, ""
	}

	key, cid := LEB128Dec(data)
	return key, strconv.FormatUint((uint64)(cid), 16) // Base-16 string
}
