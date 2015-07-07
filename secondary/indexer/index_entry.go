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
	jsonEncoder *collatejson.Codec
	encBufPool  *common.BytesBufPool
)

const (
	maxIndexEntrySize = MAX_SEC_KEY_BUFFER_LEN + MAX_DOCID_LEN + 2
)

func init() {
	jsonEncoder = collatejson.NewCodec(16)
	encBufPool = common.NewByteBufferPool(maxIndexEntrySize)
}

// Generic index entry abstraction (primary or secondary)
// Represents a row in the index
type IndexEntry interface {
	ReadDocId([]byte) ([]byte, error)
	ReadSecKey([]byte) ([]byte, error)

	Bytes() []byte
	String() string
}

// Generic index key abstraction (primary or secondary)
// Represents a key supplied by the user for scan operation
type IndexKey interface {
	Compare(IndexEntry) int
	ComparePrefixFields(IndexEntry) int
	Bytes() []byte
	String() string
}

// Storage encoding for primary index entry
// Raw docid bytes are stored as the key
type primaryIndexEntry []byte

func NewPrimaryIndexEntry(docid []byte) (*primaryIndexEntry, error) {
	if isDocIdLarge(docid) {
		return nil, ErrDocIdTooLong
	}

	buf := append([]byte(nil), docid...)
	e := primaryIndexEntry(buf)
	return &e, nil
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

func (e *primaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *primaryIndexEntry) String() string {
	return string(*e)
}

// Storage encoding for secondary index entry
// Format:
// [collate_json_encoded_sec_key][raw_docid_bytes][len_of_docid_2_bytes]
type secondaryIndexEntry []byte

func NewSecondaryIndexEntry(key []byte, docid []byte) (*secondaryIndexEntry, error) {
	var err error
	var buf []byte

	if isNilJsonKey(key) {
		return nil, ErrSecKeyNil
	}

	if isSecKeyLarge(key) {
		return nil, ErrSecKeyTooLong
	}

	poolBuf := encBufPool.Get()
	defer encBufPool.Put(poolBuf)
	if buf, err = jsonEncoder.Encode(key, (*poolBuf)[:0]); err != nil {
		return nil, err
	}

	buf = append(buf, docid...)
	buf = buf[:len(buf)+2]
	offset := len(buf) - 2
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(docid)))

	buf = append([]byte(nil), buf[:len(buf)]...)
	e := secondaryIndexEntry(buf)
	return &e, nil
}

func BytesToSecondaryIndexEntry(b []byte) (*secondaryIndexEntry, error) {
	e := secondaryIndexEntry(b)
	return &e, nil
}

func (e *secondaryIndexEntry) lenDocId() int {
	rbuf := []byte(*e)
	offset := len(rbuf) - 2
	l := binary.LittleEndian.Uint16(rbuf[offset : offset+2])
	return int(l)
}

func (e secondaryIndexEntry) ReadDocId(buf []byte) ([]byte, error) {
	doclen := e.lenDocId()
	offset := len(e) - doclen - 2
	buf = append(buf, e[offset:offset+doclen]...)

	return buf, nil
}

func (e secondaryIndexEntry) ReadSecKey(buf []byte) ([]byte, error) {
	var err error
	doclen := e.lenDocId()

	encoded := e[0 : len(e)-doclen-2]
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

func (k *primaryKey) ComparePrefixFields(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)

	if klen > len(entry.Bytes()) {
		return 1
	}

	return bytes.Compare(*k, entry.Bytes()[:klen])
}

func (k *primaryKey) Bytes() []byte {
	return *k
}

func (k *primaryKey) String() string {
	return string(*k)
}

type secondaryKey []byte

func NewSecondaryKey(key []byte) (IndexKey, error) {
	if isNilJsonKey(key) {
		return &NilIndexKey{}, nil
	}

	if isSecKeyLarge(key) {
		return nil, ErrSecKeyTooLong
	}

	var err error
	buf := make([]byte, 0, MAX_SEC_KEY_BUFFER_LEN)
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

	if klen > len(entry.Bytes()) {
		return 1
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
	if klen > len(entry.Bytes()) {
		return 1
	}
	return bytes.Compare(kbytes[:klen-1], entry.Bytes()[:klen-1])
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

func isDocIdLarge(k []byte) bool {
	return len(k) > MAX_DOCID_LEN
}
