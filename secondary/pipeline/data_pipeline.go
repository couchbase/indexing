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
	w.wblock = blockPool.Get().(*[]byte)
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

func (w *ItemWriter) WriteItem(itm ...[]byte) error {
	var err error
	if w.wblock == nil {
		w.grabBlock()
	}

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
		return err
	}

	if w.wr.IsEmpty() {
		blockPool.Put(w.wblock)
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

	if w.wblock != nil {
		blockPool.Put(w.wblock)
	}

	select {
	case w.wchan <- w.err:
		close(w.wchan)
		w.closed = true
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

func (w *ItemReader) InitReader() {
	w.killch = make(chan struct{})
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

func (r *ItemReader) ReadItem() ([]byte, error) {
	if r.rblock == nil {
		if err := r.grabBlock(); err != nil {
			return nil, err
		}
	}

	itm, err := r.rr.Get()
	if err == ErrNoMoreItem {
		blockPool.Put(r.rblock)
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
		blockPool.Put(r.rblock)
	}

	return nil
}

type ItemReadWriter struct {
	ItemReader
	ItemWriter
}

func (rw *ItemReadWriter) InitReadWriter() {
	rw.InitWriter()
	rw.ItemReader.killch = rw.ItemReader.killch
}

func (rw *ItemReadWriter) Kill() {
	close(rw.ItemWriter.killch)
}
