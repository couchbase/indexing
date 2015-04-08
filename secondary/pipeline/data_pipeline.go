package pipeline

type ItemWriter struct {
	wblock *[]byte
	wchan  chan interface{}
	wr     BlockBufferWriter
}

func (w *ItemWriter) Shutdown(err error) {}

func (w *ItemWriter) grabBlock() {
	w.wblock = blockPool.Get().(*[]byte)
	w.wr.Init(w.wblock)
}

func (w *ItemWriter) WriteItem(itm ...[]byte) error {
	if w.wblock == nil {
		w.grabBlock()
	}

	if w.wr.Put(itm...) == ErrNoBlockSpace {
		w.wr.Close()
		w.wchan <- w.wblock
		w.grabBlock()
		return w.wr.Put(itm...)
	}

	return nil
}

func (w *ItemWriter) Channel() chan interface{} {
	return w.wchan
}

func (w *ItemWriter) CloseWrite() error {
	if w.wr.IsEmpty() {
		blockPool.Put(w.wblock)
	} else {
		w.wr.Close()
		w.wchan <- w.wblock
	}
	w.wblock = nil
	close(w.wchan)

	return nil
}

type ItemReader struct {
	rblock *[]byte
	rchan  chan interface{}
	rr     BlockBufferReader
}

func (r *ItemReader) SetSource(w Writer) {
	r.rchan = w.Channel()
}

func (r *ItemReader) grabBlock() error {
	x, ok := <-r.rchan
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
