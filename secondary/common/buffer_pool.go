package common

import "sync"

// Thread safe byte buffer pool
// A buffer pointer received by Get() method should be
// put back using Put() method. This ensures that we not
// need to create a new buf slice with len == 0
type BytesBufPool struct {
	pool *sync.Pool
}

func NewByteBufferPool(size int) *BytesBufPool {
	newBufFn := func() interface{} {
		b := make([]byte, size, size)
		return &b
	}

	return &BytesBufPool{
		pool: &sync.Pool{
			New: newBufFn,
		},
	}
}

func (p *BytesBufPool) Get() *[]byte {

	return p.pool.Get().(*[]byte)
}

func (p *BytesBufPool) Put(buf *[]byte) {
	p.pool.Put(buf)
}
