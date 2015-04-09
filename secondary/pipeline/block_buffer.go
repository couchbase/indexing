package pipeline

import (
	"encoding/binary"
	"errors"
	"sync"
)

var (
	blockPool       *sync.Pool
	blockBufferSize = 16 * 1024
)

var (
	ErrNoBlockSpace = errors.New("Not enough space in buffer")
	ErrNoMoreItem   = errors.New("No more item to be read from buffer")
)

func SetupBlockPool(sz int) {
	blockPool = &sync.Pool{
		New: func() interface{} {
			b := make([]byte, sz, sz)
			return &b
		},
	}
}

func init() {
	SetupBlockPool(blockBufferSize)
}

func GetBlock() *[]byte {
	return blockPool.Get().(*[]byte)
}

func PutBlock(b *[]byte) {
	blockPool.Put(b)
}

type BlockBufferWriter struct {
	buf *[]byte
	cap int
	len int
}

func (b *BlockBufferWriter) Init(buf *[]byte) {
	b.buf = buf
	b.len = 2
	b.cap = cap(*buf)
}

func (b *BlockBufferWriter) Put(itms ...[]byte) error {
	l := 0
	for _, itm := range itms {
		l += len(itm)
	}

	if 2+l+2*len(itms) > b.cap-b.len {
		return ErrNoBlockSpace
	}

	for _, itm := range itms {
		binary.LittleEndian.PutUint16((*b.buf)[b.len:b.len+2], uint16(len(itm)))
		b.len += 2
		copy((*b.buf)[b.len:], itm)
		b.len += len(itm)
	}

	return nil
}

func (b *BlockBufferWriter) IsEmpty() bool {
	return b.len == 2
}

func (b *BlockBufferWriter) Close() {
	binary.LittleEndian.PutUint16((*b.buf)[0:2], uint16(b.len))
}

type BlockBufferReader struct {
	buf    *[]byte
	len    int
	offset int
}

func (b *BlockBufferReader) Init(buf *[]byte) {
	b.buf = buf
	b.len = int(binary.LittleEndian.Uint16((*buf)[0:2]))
	b.offset = 2
}

func (b *BlockBufferReader) Len() int {
	return b.len
}

func (b *BlockBufferReader) Get() ([]byte, error) {
	if b.offset == b.len {
		return nil, ErrNoMoreItem
	}

	dlen := int(binary.LittleEndian.Uint16((*b.buf)[b.offset : b.offset+2]))
	b.offset += 2
	b.offset += dlen

	return (*b.buf)[b.offset-dlen : b.offset], nil
}
