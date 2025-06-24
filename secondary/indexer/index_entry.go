package indexer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
)

var (
	ErrSecKeyNil    = errors.New("Secondary key array is empty")
	ErrDocIdTooLong = errors.New(fmt.Sprintf("DocID is too long (>%d)", MAX_DOCID_LEN))
)

// Special index keys
var (
	MinIndexKey = &NilIndexKey{cmp: -1, pcmp: -1}
	MaxIndexKey = &NilIndexKey{cmp: 1, pcmp: 1}
	NilJsonKey  = []byte("[]")
)

var (
	jsonEncoder *collatejson.Codec
)

type keySizeConfig struct {
	maxArrayKeyLength       int
	maxArrayKeyBufferLength int
	maxArrayIndexEntrySize  int

	maxSecKeyLen       int
	maxSecKeyBufferLen int
	maxIndexEntrySize  int

	allowLargeKeys bool
}

func init() {

	jsonEncoder = collatejson.NewCodec(16)

	//0 - based on projector version, 1 - force enable, 2 - force disable
	gEncodeCompatMode = EncodeCompatMode(common.SystemConfig["indexer.encoding.encode_compat_mode"].Int())
}

// Generic index entry abstraction (primary or secondary)
// Represents a row in the index
type IndexEntry interface {
	ReadDocId([]byte) ([]byte, error)
	ReadSecKey([]byte) ([]byte, error)
	Count() int
	Bytes() []byte
	String() string
	MeteredByteLen() int
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

func (e *primaryIndexEntry) MeteredByteLen() int {
	return len([]byte(*e))
}

// Storage encoding for secondary index entry
// Format:
// [collate_json_encoded_sec_key][raw_docid_bytes][optional_count_2_bytes][len_of_docid_2_bytes]
// The MSB of right byte of docid length indicates whether count is encoded or not
type secondaryIndexEntry []byte

func NewSecondaryIndexEntry(key []byte, docid []byte, isArray bool, count int,
	desc []bool, buf []byte, meta *MutationMeta, sz keySizeConfig) (secondaryIndexEntry, error) {

	return NewSecondaryIndexEntry2(key, docid, isArray, count, desc, buf, true, meta, sz)
}

func NewSecondaryIndexEntry2(key []byte, docid []byte, isArray bool,
	count int, desc []bool, buf []byte, validateSize bool, meta *MutationMeta,
	sz keySizeConfig) (secondaryIndexEntry, error) {
	var err error
	var offset int

	if isNilJsonKey(key) {
		return nil, ErrSecKeyNil
	}

	if isJSONEncoded(key) {
		if isArray {
			if !sz.allowLargeKeys && validateSize && len(key) > sz.maxArrayKeyLength {
				return nil, errors.New(fmt.Sprintf("Secondary array key is too long (> %d)", sz.maxArrayKeyLength))
			}
		} else if !sz.allowLargeKeys && validateSize && len(key) > sz.maxSecKeyLen {
			return nil, errors.New(fmt.Sprintf("Secondary key is too long (> %d)", sz.maxSecKeyLen))
		}

		// Resize buffer here if needed
		buf = resizeEncodeBuf(buf, len(key)*3, true)
		if buf, err = jsonEncoder.Encode(key, buf); err != nil {
			return nil, err
		}
	} else { // Encoded
		if isArray {
			if !sz.allowLargeKeys && validateSize && len(key) > sz.maxArrayKeyBufferLength {
				return nil, errors.New(fmt.Sprintf("Encoded secondary array key is too long (> %d)", sz.maxArrayKeyBufferLength))
			}
		} else if !sz.allowLargeKeys && validateSize && len(key) > sz.maxSecKeyBufferLen {
			return nil, errors.New(fmt.Sprintf("Encoded secondary key is too long (> %d)", sz.maxSecKeyBufferLen))
		}

		fixed := false
		if meta != nil && gEncodeCompatMode != FORCE_DISABLE {
			if (gEncodeCompatMode == FORCE_ENABLE) || (gEncodeCompatMode == CHECK_VERSION && meta.projVer < common.ProjVer_5_1_1) {
				fixed = true
				if buf, err = jsonEncoder.FixEncodedInt(key, buf); err != nil {
					return nil, err
				}
			}
		}

		if !fixed {
			buf = append(buf, key...)
		}
	}

	if desc != nil {
		buf, err = jsonEncoder.ReverseCollate(buf, desc)
		if err != nil {
			return nil, err
		}
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

// used only in bhive scan path
func NewSecondaryIndexEntry3(key []byte, docid []byte, buf []byte) (secondaryIndexEntry, error) {

	buf = append(buf, key...)
	buf = append(buf, docid...)

	buf = buf[:len(buf)+2]
	offset := len(buf) - 2
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(docid)))

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

func (e *secondaryIndexEntry) MeteredByteLen() int {
	return e.lenKey() + e.lenDocId()
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
		err = fmt.Errorf("Collatejson decode error: %v", err)
		return nil, err
	}
	return buf, nil
}

func (e secondaryIndexEntry) ReadSecKeyCJson() []byte {
	var encoded []byte
	doclen := e.lenDocId()
	if e.isCountEncoded() {
		encoded = e[0 : len(e)-doclen-4]
	} else {
		encoded = e[0 : len(e)-doclen-2]
	}

	return encoded
}

func (e *secondaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *secondaryIndexEntry) String() string {
	buf := make([]byte, len(*e)*4)
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

func NewSecondaryKey(key []byte, buf []byte, allowLargeKeys bool, maxSecKeyLen int, emptyArrAsNil bool) (IndexKey, error) {
	if emptyArrAsNil && isNilJsonKey(key) {
		return &NilIndexKey{}, nil
	}

	if !allowLargeKeys && len(key) > maxSecKeyLen {
		return nil, errors.New(fmt.Sprintf("Secondary key is too long (> %d)", maxSecKeyLen))
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
	klen := len([]byte(*k))
	buf := make([]byte, 0, klen*3)
	buf, _ = jsonEncoder.Decode(*k, buf)
	return string(buf)
}

func isNilJsonKey(k []byte) bool {
	return bytes.Equal(NilJsonKey, k) || len(k) == 0
}

func isDocIdLarge(k []byte) bool {
	return len(k) > MAX_DOCID_LEN
}

func IndexEntrySize(key []byte, docid []byte) int {
	return len(key) + len(docid) + 2
}

// Return encoded key with docid without size check
func GetIndexEntryBytes3(key []byte, docid []byte, isPrimary bool, isArray bool,
	count int, desc []bool, buf []byte, meta *MutationMeta, sz keySizeConfig) (bs []byte, err error) {

	if isPrimary {
		bs, err = NewPrimaryIndexEntry(docid)
	} else {
		bs, err = NewSecondaryIndexEntry2(key, docid, isArray, count, desc, buf, false, meta, sz)
		if err == ErrSecKeyNil {
			return nil, nil
		}
	}

	return bs, err
}

func GetIndexEntryBytes2(key []byte, docid []byte, isPrimary bool, isArray bool,
	count int, desc []bool, buf []byte, meta *MutationMeta, sz keySizeConfig) (bs []byte, err error) {

	if isPrimary {
		bs, err = NewPrimaryIndexEntry(docid)
	} else {
		bs, err = NewSecondaryIndexEntry(key, docid, isArray, count, desc, buf, meta, sz)
		if err == ErrSecKeyNil {
			return nil, nil
		}
	}

	return bs, err
}

func GetIndexEntryBytes(key []byte, docid []byte, isPrimary bool, isArray bool,
	count int, desc []bool, meta *MutationMeta, sz keySizeConfig) (entry []byte, err error) {

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

		if len(key)+MAX_KEY_EXTRABYTES_LEN > cap(*bufPtr) {
			newSize := len(key) + MAX_DOCID_LEN + ENCODE_BUF_SAFE_PAD
			buf = make([]byte, 0, newSize)
			bufPtr = &buf
		}

		defer func() {
			bufPool.Put(bufPtr)
		}()
	}

	entry, err = GetIndexEntryBytes2(key, docid, isPrimary, isArray, count, desc, buf, meta, sz)
	return append([]byte(nil), entry...), err
}

func isJSONEncoded(key []byte) bool {
	return key[0] == '['
}

func getNumShaEncodings(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

func vectorBackEntry2MainEntry(docid []byte, bentry []byte, buf []byte, sz keySizeConfig) []byte {

	// Strip the SHA values from bentry
	l := uint32(len(bentry))
	numShaEncodings := getNumShaEncodings(bentry[l-4:])
	shaEncodingLen := numShaEncodings*sha256.Size + 4

	entry, _ := NewSecondaryIndexEntry2(bentry[:l-shaEncodingLen], docid, false, 1,
		nil, buf, false, nil, sz)

	return entry
}

type bhiveCentroidId []byte

func NewBhiveCentroidId(centroidId uint64) bhiveCentroidId {
	var cid [8]byte
	binary.LittleEndian.PutUint64(cid[:], uint64(centroidId))
	return cid[:]
}

// Compare is not used by bhive iterator. Adding this method only for
// interface compatibility
func (k bhiveCentroidId) Compare(entry IndexEntry) int {
	return 0
}

// ComparePrefixFields is not used by bhive iterator. Adding this method only for
// interface compatibility
func (k bhiveCentroidId) ComparePrefixFields(entry IndexEntry) int {
	return 0
}

func (k bhiveCentroidId) CompareIndexKey(k1 IndexKey) int {
	return bytes.Compare(k.Bytes(), k1.Bytes())
}

// ComparePrefixIndexKey is not used by bhive iterator. Adding this method only for
// interface compatibility
func (k bhiveCentroidId) ComparePrefixIndexKey(k1 IndexKey) int {
	return 0
}

func (k bhiveCentroidId) Bytes() []byte {
	return []byte(k)
}

func (k bhiveCentroidId) String() string {
	val := binary.LittleEndian.Uint64(k.Bytes())
	return strconv.FormatUint(val, 10) // return as decimal string
}
