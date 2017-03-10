package indexer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
)

var (
	ErrSecKeyNil     = errors.New("Secondary key array is empty")
	ErrSecKeyTooLong = errors.New(fmt.Sprintf("Secondary key is too long (> %d)", MAX_SEC_KEY_LEN))
	ErrDocIdTooLong  = errors.New(fmt.Sprintf("DocID is too long (>%d)", MAX_DOCID_LEN))
)

// Special index keys
var (
	MinIndexKey = &NilIndexKey{cmp: -1, pcmp: -1}
	MaxIndexKey = &NilIndexKey{cmp: 1, pcmp: 1}
	NilJsonKey  = []byte("[]")
)

var (
	jsonEncoder     *collatejson.Codec
	encBufPool      *common.BytesBufPool
	arrayEncBufPool *common.BytesBufPool
)

const (
	maxIndexEntrySize = MAX_SEC_KEY_BUFFER_LEN + MAX_DOCID_LEN + 2
)

var (
	maxArrayKeyLength       = common.SystemConfig["indexer.settings.max_array_seckey_size"].Int()
	maxArrayKeyBufferLength = maxArrayKeyLength * 3
	maxArrayIndexEntrySize  = maxArrayKeyBufferLength + MAX_DOCID_LEN + 2
)

func init() {
	jsonEncoder = collatejson.NewCodec(16)
	encBufPool = common.NewByteBufferPool(maxIndexEntrySize + ENCODE_BUF_SAFE_PAD)
}

// Generic index entry abstraction (primary or secondary)
// Represents a row in the index
type IndexEntry interface {
	ReadDocId([]byte) ([]byte, error)
	ReadSecKey([]byte) ([]byte, error)
	Count() int
	Bytes() []byte
	String() string
}

// Generic index key abstraction (primary or secondary)
// Represents a key supplied by the user for scan operation
type IndexKey interface {
	Compare(IndexEntry) int
	ComparePrefixFields(IndexEntry) int
	CompareIndexKey(IndexKey) int
	ComparePrefixIndexKey(IndexKey) int
	Bytes() []byte
	String() string
}

// Storage encoding for primary index entry
// Raw docid bytes are stored as the key
type primaryIndexEntry []byte

func NewPrimaryIndexEntry(docid []byte) (primaryIndexEntry, error) {
	if isDocIdLarge(docid) {
		return nil, ErrDocIdTooLong
	}

	e := primaryIndexEntry(docid)
	return e, nil
}

func BytesToPrimaryIndexEntry(b []byte) (*primaryIndexEntry, error) {
	e := primaryIndexEntry(b)
	return &e, nil
}

func (e *primaryIndexEntry) ReadDocId(buf []byte) ([]byte, error) {
	buf = append(buf, []byte(*e)...)
	return buf, nil
}

func (e *primaryIndexEntry) ReadSecKey(buf []byte) ([]byte, error) {
	return buf, nil
}

func (e *primaryIndexEntry) Count() int {
	return 1
}

func (e *primaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *primaryIndexEntry) String() string {
	return string(*e)
}

// Storage encoding for secondary index entry
// Format:
// [collate_json_encoded_sec_key][raw_docid_bytes][optional_count_2_bytes][len_of_docid_2_bytes]
// The MSB of right byte of docid length indicates whether count is encoded or not
type secondaryIndexEntry []byte

func NewSecondaryIndexEntry(key []byte, docid []byte, isArray bool, count int, desc []bool, buf []byte) (secondaryIndexEntry, error) {
	var err error
	var offset int

	if isNilJsonKey(key) {
		return nil, ErrSecKeyNil
	}

	if key[0] == '[' { // JSON
		if isArray {
			if isArraySecKeyLarge(key) {
				return nil, errors.New(fmt.Sprintf("Secondary array key is too long (> %d)", maxArrayKeyLength))
			}
		} else if isSecKeyLarge(key) {
			return nil, ErrSecKeyTooLong
		}
		if buf, err = jsonEncoder.Encode(key, buf); err != nil {
			return nil, err
		}
	} else { // Encoded
		if isArray {
			if len(key) > maxArrayKeyBufferLength {
				return nil, errors.New(fmt.Sprintf("Encoded secondary array key is too long (> %d)", maxArrayKeyBufferLength))
			}
		} else if len(key) > MAX_SEC_KEY_BUFFER_LEN {
			return nil, errors.New(fmt.Sprintf("Encoded secondary key is too long (> %d)", MAX_SEC_KEY_BUFFER_LEN))
		}
		buf = append(buf, key...)
	}

	if desc != nil {
		buf = jsonEncoder.ReverseCollate(buf, desc)
	}

	buf = append(buf, docid...)

	if count > 1 {
		buf = buf[:len(buf)+2]
		offset = len(buf) - 2
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(count))
	}

	buf = buf[:len(buf)+2]
	offset = len(buf) - 2
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(docid)))
	if count > 1 {
		buf[offset+1] |= byte(uint8(1) << 7)
	}

	e := secondaryIndexEntry(buf)
	return e, nil
}

func BytesToSecondaryIndexEntry(b []byte) (*secondaryIndexEntry, error) {
	e := secondaryIndexEntry(b)
	return &e, nil
}

func (e *secondaryIndexEntry) lenDocId() int {
	rbuf := []byte(*e)
	offset := len(rbuf) - 2
	l := binary.LittleEndian.Uint16(rbuf[offset : offset+2])
	len := l & 0x7fff // Length & 0111111 11111111 (as MSB of length is used to indicate presence of count)
	return int(len)
}

func (e *secondaryIndexEntry) lenKey() int {
	if e.isCountEncoded() == true {
		return len(*e) - e.lenDocId() - 4
	}
	return len(*e) - e.lenDocId() - 2
}

func (e *secondaryIndexEntry) isCountEncoded() bool {
	rbuf := []byte(*e)
	offset := len(rbuf) - 1 // Decode length byte to see if count is encoded
	return (rbuf[offset] & 0x80) == 0x80
}

func (e secondaryIndexEntry) ReadDocId(buf []byte) ([]byte, error) {
	docidlen := e.lenDocId()
	var offset int
	if e.isCountEncoded() == true {
		offset = len(e) - docidlen - 4
	} else {
		offset = len(e) - docidlen - 2
	}
	buf = append(buf, e[offset:offset+docidlen]...)
	return buf, nil
}

func (e secondaryIndexEntry) Count() int {
	rbuf := []byte(e)
	if e.isCountEncoded() {
		offset := len(rbuf) - 4
		count := int(binary.LittleEndian.Uint16(rbuf[offset : offset+2]))
		return count
	} else {
		return 1
	}
}

func (e secondaryIndexEntry) ReadSecKey(buf []byte) ([]byte, error) {
	var err error
	var encoded []byte
	doclen := e.lenDocId()
	if e.isCountEncoded() {
		encoded = e[0 : len(e)-doclen-4]
	} else {
		encoded = e[0 : len(e)-doclen-2]
	}

	if buf, err = jsonEncoder.Decode(encoded, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (e *secondaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *secondaryIndexEntry) String() string {
	buf := make([]byte, MAX_SEC_KEY_LEN*4)
	buf, _ = e.ReadSecKey(buf)
	buf = append(buf, ':')
	buf, _ = e.ReadDocId(buf)
	return string(buf)
}

type NilIndexKey struct {
	cmp  int
	pcmp int
}

func (k *NilIndexKey) Compare(entry IndexEntry) int {
	return k.cmp
}

func (k *NilIndexKey) ComparePrefixFields(entry IndexEntry) int {
	return k.pcmp
}

func (k *NilIndexKey) CompareIndexKey(k1 IndexKey) int {
	key, ok := k1.(*NilIndexKey)
	if ok {
		if k.cmp < key.cmp {
			return -1
		} else if k.cmp == key.cmp {
			return 0
		} else if k.cmp > key.cmp {
			return 1
		}
	}
	return k.cmp
}

func (k *NilIndexKey) ComparePrefixIndexKey(k1 IndexKey) int {
	key, ok := k1.(*NilIndexKey)
	if ok {
		if k.pcmp < key.pcmp {
			return -1
		} else if k.pcmp == key.pcmp {
			return 0
		} else if k.pcmp > key.pcmp {
			return 1
		}
	}
	return k.pcmp
}

func (k *NilIndexKey) Bytes() []byte {
	return nil
}

func (k *NilIndexKey) String() string {
	return "nil"
}

type primaryKey []byte

func NewPrimaryKey(docid []byte) (IndexKey, error) {
	if len(docid) == 0 {
		return &NilIndexKey{}, nil
	}
	k := primaryKey(docid)
	return &k, nil
}

func (k *primaryKey) Compare(entry IndexEntry) int {
	return bytes.Compare(*k, entry.Bytes())
}

// This function will be never called since do not support prefix equality
// for primary keys.
func (k *primaryKey) ComparePrefixFields(entry IndexEntry) (r int) {
	panic("prefix compare is not implemented for primary key")
}

func (k *primaryKey) CompareIndexKey(k1 IndexKey) (r int) {
	key, ok := k1.(*NilIndexKey)
	if ok {
		return key.cmp * -1
	}
	return bytes.Compare(*k, k1.Bytes())
}

func (k *primaryKey) ComparePrefixIndexKey(k1 IndexKey) (r int) {
	key, ok := k1.(*NilIndexKey)
	if ok {
		return key.pcmp * -1
	}
	return bytes.Compare(*k, k1.Bytes())
}

func (k *primaryKey) Bytes() []byte {
	return *k
}

func (k *primaryKey) String() string {
	return string(*k)
}

type secondaryKey []byte

func NewSecondaryKey(key []byte, buf []byte) (IndexKey, error) {
	if isNilJsonKey(key) {
		return &NilIndexKey{}, nil
	}

	if isSecKeyLarge(key) {
		return nil, ErrSecKeyTooLong
	}

	var err error
	if buf, err = jsonEncoder.Encode(key, buf); err != nil {
		return nil, err
	}

	buf = append([]byte(nil), buf[:len(buf)]...)

	k := secondaryKey(buf)
	return &k, nil
}

func (k *secondaryKey) Compare(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)

	secEntry := entry.(*secondaryIndexEntry)
	entryKeylen := secEntry.lenKey()
	if klen > entryKeylen {
		klen = entryKeylen
	}

	return bytes.Compare(kbytes[:klen], entry.Bytes()[:klen])
}

// A compound secondary index entry would be an array
// of indexed fields. A compound index on N fields
// also should be equivalent to other indexes with 1 to N
// prefix fields.
// When a user supplies a low or high key constraints,
// indexing systems should check the number of fields
// in the user supplied key and treat target index as
// an index created on that n prefix fields.
//
// Collatejson encoding puts a terminator character at the
// end of encoded bytes to represent termination of json array.
// Since we want to match partial secondary keys against fullset
// compound index entries, we would remove the last byte from the
// encoded user supplied secondary key to match prefixes.
func (k *secondaryKey) ComparePrefixFields(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)
	secEntry := entry.(*secondaryIndexEntry)

	prefixLen := klen - 1 // Ignore last byte
	entryKeylen := secEntry.lenKey()
	// Compare full secondary entry
	if klen > entryKeylen {
		prefixLen = entryKeylen
	}
	return bytes.Compare(kbytes[:prefixLen], entry.Bytes()[:prefixLen])
}

func (k *secondaryKey) CompareIndexKey(k1 IndexKey) int {
	key, ok := k1.(*NilIndexKey)
	if ok {
		return key.cmp * -1
	}

	return bytes.Compare(k.Bytes(), k1.Bytes())
}

func (k *secondaryKey) ComparePrefixIndexKey(k1 IndexKey) int {
	key, ok := k1.(*NilIndexKey)
	if ok {
		return key.pcmp * -1
	}

	kbytes := []byte(*k)
	klen := len(kbytes)

	k1bytes := k1.Bytes()
	k1len := len(k1bytes)

	kprefixLen := klen - 1 // Ignore last byte
	k1prefixLen := k1len - 1
	if kprefixLen > k1prefixLen {
		kprefixLen = k1prefixLen
	}

	return bytes.Compare(kbytes[:kprefixLen], k1bytes[:kprefixLen])
}

func (k *secondaryKey) Bytes() []byte {
	return *k
}

func (k *secondaryKey) String() string {
	buf := make([]byte, 0, MAX_SEC_KEY_LEN)
	buf, _ = jsonEncoder.Decode(*k, buf)
	return string(buf)
}

func isNilJsonKey(k []byte) bool {
	return bytes.Equal(NilJsonKey, k) || len(k) == 0
}

func isSecKeyLarge(k []byte) bool {
	return len(k) > MAX_SEC_KEY_LEN
}

func isArraySecKeyLarge(k []byte) bool {
	return len(k) > maxArrayKeyLength
}

func isDocIdLarge(k []byte) bool {
	return len(k) > MAX_DOCID_LEN
}

func IndexEntrySize(key []byte, docid []byte) int {
	return len(key) + len(docid) + 2
}

func GetIndexEntryBytes2(key []byte, docid []byte,
	isPrimary bool, isArray bool, count int, desc []bool, buf []byte) (bs []byte, err error) {

	if isPrimary {
		bs, err = NewPrimaryIndexEntry(docid)
	} else {
		bs, err = NewSecondaryIndexEntry(key, docid, isArray, count, desc, buf)
		if err == ErrSecKeyNil {
			return nil, nil
		}
	}

	return bs, err
}

func GetIndexEntryBytes(key []byte, docid []byte,
	isPrimary bool, isArray bool, count int, desc []bool) (entry []byte, err error) {

	var bufPool *common.BytesBufPool
	var bufPtr *[]byte
	var buf []byte

	if isArray {
		bufPool = arrayEncBufPool
	} else if !isPrimary {
		bufPool = encBufPool
	}

	if bufPool != nil {
		bufPtr = bufPool.Get()
		buf = (*bufPtr)[:0]

		defer func() {
			bufPool.Put(bufPtr)
		}()
	}

	entry, err = GetIndexEntryBytes2(key, docid, isPrimary, isArray, count, desc, buf)
	return append([]byte(nil), entry...), err
}
