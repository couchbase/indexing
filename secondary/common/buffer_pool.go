package common

import "sync"

// Thread safe byte buffer pool
type BytesBufPool struct {
	pool *sync.Pool
}

func NewByteBufferPool(size int) *BytesBufPool {
	newBufFn := func() interface{} {
		return make([]byte, 0, size)
	}

	return &BytesBufPool{
		pool: &sync.Pool{
			New: newBufFn,
		},
	}
}

func (p *BytesBufPool) Get() []byte {

	return p.pool.Get().([]byte)
}

func (p *BytesBufPool) Put(buf []byte) {
	// Reset len = 0
	buf = buf[:0]
	p.pool.Put(buf)
}
