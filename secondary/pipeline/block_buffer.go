package pipeline

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	blockPool       unsafe.Pointer
	blockBufferSize = 16 * 1024
)

var (
	ErrNoBlockSpace = errors.New("Not enough space in buffer")
	ErrNoMoreItem   = errors.New("No more item to be read from buffer")
)

func SetupBlockPool(sz int) {
	p := &sync.Pool{
		New: func() interface{} {
			b := make([]byte, sz, sz)
			return &b
		},
	}

	atomic.StorePointer(&blockPool, unsafe.Pointer(p))
}

func init() {
	SetupBlockPool(blockBufferSize)
}

func getBlockPool() *sync.Pool {
	return (*sync.Pool)(atomic.LoadPointer(&blockPool))
}

func GetBlock() *[]byte {
	p := getBlockPool()
	return p.Get().(*[]byte)
}

func PutBlock(b *[]byte) {
	p := getBlockPool()
	p.Put(b)
}

type BlockBufferWriter struct {
	buf *[]byte
	cap int
	len int
}

func (b *BlockBufferWriter) Init(buf *[]byte) {
	b.buf = buf
	b.len = 4
	b.cap = cap(*buf)
}

func (b *BlockBufferWriter) Put(itms ...[]byte) error {
	l := 0
	for _, itm := range itms {
		l += len(itm)
	}

	itmLen := 4 + l + 4*len(itms)
	if itmLen > b.cap-b.len {
		return ErrNoBlockSpace
	}

	for _, itm := range itms {
		binary.LittleEndian.PutUint32((*b.buf)[b.len:b.len+4], uint32(len(itm)))
		b.len += 4
		copy((*b.buf)[b.len:], itm)
		b.len += len(itm)
	}
	return nil
}

func (b *BlockBufferWriter) IsEmpty() bool {
	return b.len == 4
}

func (b *BlockBufferWriter) Close() {
	binary.LittleEndian.PutUint32((*b.buf)[0:4], uint32(b.len))
}

type BlockBufferReader struct {
	buf    *[]byte
	len    int
	offset int
}

func (b *BlockBufferReader) Init(buf *[]byte) {
	b.buf = buf
	b.len = int(binary.LittleEndian.Uint32((*buf)[0:4]))
	b.offset = 4
}

func (b *BlockBufferReader) Len() int {
	return b.len
}

func (b *BlockBufferReader) Get() ([]byte, error) {
	if b.offset == b.len {
		return nil, ErrNoMoreItem
	}

	dlen := int(binary.LittleEndian.Uint32((*b.buf)[b.offset : b.offset+4]))
	b.offset += 4
	b.offset += dlen

	return (*b.buf)[b.offset-dlen : b.offset], nil
}
