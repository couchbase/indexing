package pipeline

import "sync"

type ItemWriter struct {
	errLock sync.Mutex
	err     error

	wblock *[]byte
	wchan  chan interface{}
	wr     BlockBufferWriter
	closed bool

	killch chan struct{}
}

func (w *ItemWriter) InitWriter() {
	w.wblock = nil
	w.closed = false
	w.killch = make(chan struct{})
	w.wchan = make(chan interface{}, 1)
}

func (w *ItemWriter) SetNumBuffers(n int) {
	w.wchan = make(chan interface{}, n)
}

func (w *ItemWriter) Shutdown(err error) {
	w.errLock.Lock()
	defer w.errLock.Unlock()
	w.err = err
}

func (w *ItemWriter) Kill() {
	close(w.killch)
}

func (w *ItemWriter) grabBlock() {
	w.wblock = GetBlock()
	w.wr.Init(w.wblock)
}

func (w *ItemWriter) sendBlock() error {
	w.wr.Close()
	select {
	case w.wchan <- w.wblock:
	case <-w.killch:
		return ErrSupervisorKill
	}

	return nil
}

func (w *ItemWriter) ResizeBlockBuffer(itmLen int) {
	if w.wr.IsEmpty() && (itmLen > w.wr.cap-w.wr.len) {
		newBuf := make([]byte, itmLen+4, itmLen+4)
		w.wr.Init(&newBuf)
		w.wblock = &newBuf
	}
}

func (w *ItemWriter) WriteItem(itm ...[]byte) error {
	var err error
	if w.wblock == nil {
		w.grabBlock()
	}

	l := 0
	for _, it := range itm {
		l += len(it)
	}

	itmLen := 4 + l + 4*len(itm)
	w.ResizeBlockBuffer(itmLen)
	if w.wr.Put(itm...) == ErrNoBlockSpace {
		err = w.HasShutdown()
		if err != nil {
			return err
		}
		err = w.sendBlock()
		if err != nil {
			return err
		}
		w.grabBlock()
		itmLen = 4 + l + 4*len(itm)
		w.ResizeBlockBuffer(itmLen)
		return w.wr.Put(itm...)
	}

	return nil
}

func (w *ItemWriter) Channel() chan interface{} {
	return w.wchan
}

func (w *ItemWriter) CloseWrite() error {
	if w.closed {
		return nil
	}

	err := w.HasShutdown()
	if err != nil {
		w.CloseWithError(err)
		return err
	}

	if w.wblock == nil {
	} else if w.wr.IsEmpty() {
		PutBlock(w.wblock)
		w.wblock = nil
	} else {
		w.sendBlock()
	}
	close(w.wchan)
	w.closed = true

	return nil
}

func (w *ItemWriter) HasShutdown() error {
	w.errLock.Lock()
	defer w.errLock.Unlock()

	return w.err
}

func (w *ItemWriter) CloseWithError(err error) {
	if w.closed {
		return
	}

	w.closed = true

	if w.wblock != nil {
		PutBlock(w.wblock)
		w.wblock = nil
	}

	select {
	case w.wchan <- err:
		close(w.wchan)
	case <-w.killch:
	}
}

type ItemReader struct {
	rblock *[]byte
	rchan  chan interface{}
	rr     BlockBufferReader

	killch chan struct{}
}

func (r *ItemReader) SetSource(w Writer) {
	r.rchan = w.Channel()
}

func (r *ItemReader) InitReader() {
	r.rblock = nil
	r.killch = make(chan struct{})
}

func (r *ItemReader) Kill() {
	close(r.killch)
}

func (r *ItemReader) grabBlock() error {
	select {
	case x, ok := <-r.rchan:
		if !ok {
			return ErrNoMoreItem
		}
		switch v := x.(type) {
		case *[]byte:
			r.rblock = v
			r.rr.Init(r.rblock)
		case error:
			return v
		}
	case <-r.killch:
		return ErrSupervisorKill
	}

	return nil
}

func (r *ItemReader) PeekBlock() ([]byte, error) {
	if r.rblock == nil {
		if err := r.grabBlock(); err != nil {
			return nil, err
		}
	}

	return (*r.rblock)[4 : 4+r.rr.Len()-4], nil
}

func (r *ItemReader) FlushBlock() {
	if r.rblock != nil {
		PutBlock(r.rblock)
		r.rblock = nil
	}
}

func (r *ItemReader) ReadItem() ([]byte, error) {
	if r.rblock == nil {
		if err := r.grabBlock(); err != nil {
			return nil, err
		}
	}

	itm, err := r.rr.Get()
	if err == ErrNoMoreItem {
		PutBlock(r.rblock)
		r.rblock = nil
		if err := r.grabBlock(); err != nil {
			return nil, err
		}
		itm, err = r.rr.Get()
		if err != nil {
			return nil, err
		}
	}

	return itm, nil
}

func (r *ItemReader) CloseRead() error {
	if r.rblock != nil {
		PutBlock(r.rblock)
		r.rblock = nil
	}

	return nil
}

type ItemReadWriter struct {
	ItemReader
	ItemWriter
}

func (rw *ItemReadWriter) InitReadWriter() {
	rw.rblock = nil
	rw.closed = false
	rw.InitWriter()
	rw.ItemReader.killch = rw.ItemWriter.killch
}

func (rw *ItemReadWriter) Kill() {
	close(rw.ItemWriter.killch)
}
