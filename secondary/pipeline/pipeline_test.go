package pipeline

import "fmt"
import "testing"
import "bytes"
import "time"
import "errors"

var (
	srcSleep, filterSleep, sinkSleep time.Duration
)

type src struct {
	ItemWriter
	count int
	errch chan error
}

func newSrc(c int) *src {
	s := &src{count: c, errch: make(chan error, 1)}
	s.InitWriter()

	return s

}

func (s *src) Routine() error {
	for i := 0; i < s.count; i++ {
		select {
		case err := <-s.errch:
			s.CloseWithError(err)
			return err
		default:
		}
		time.Sleep(srcSleep)
		itm := []byte(fmt.Sprintf("item-%d", i))
		err := s.WriteItem(itm)
		switch err {
		case ErrSupervisorKill:
			return nil
		case nil:
		default:
			s.CloseWithError(err)
			return err
		}
	}
	s.CloseWrite()

	return nil
}

type filter struct {
	ItemReadWriter
	errch chan error
}

func newFilter() *filter {
	f := &filter{errch: make(chan error, 1)}
	f.InitReadWriter()
	return f
}

func (f *filter) Routine() error {
loop:
	for {
		select {
		case err := <-f.errch:
			f.CloseWithError(err)
			return err
		default:
		}
		time.Sleep(filterSleep)
		itm, err := f.ReadItem()
		switch err {
		case ErrSupervisorKill:
			return nil
		case nil:
		case ErrNoMoreItem:
			f.CloseRead()
			f.CloseWrite()
			break loop
		default:
			f.CloseWithError(err)
			return err
		}

		f.WriteItem([]byte(string(itm) + "-filtered"))
	}

	return nil
}

type sink struct {
	ItemReader
	t     *testing.T
	count int
	errch chan error
}

func newSink(t *testing.T, c int) *sink {
	s := &sink{t: t, count: c, errch: make(chan error, 1)}
	s.InitReader()
	return s
}

func (s *sink) Routine() error {
	i := 0
loop:
	for {
		select {
		case err := <-s.errch:
			s.CloseRead()
			return err
		default:
		}
		time.Sleep(sinkSleep)
		itm, err := s.ReadItem()
		switch err {
		case ErrNoMoreItem:
			s.CloseRead()
			break loop
		case nil:
		default:
			s.CloseRead()
			return err
		}

		expected := []byte(fmt.Sprintf("item-%d-filtered", i))
		i++
		if !bytes.Equal(itm, expected) {
			s.t.Fatalf("got %s, expected %s", itm, expected)
		}

	}

	if i != s.count {
		s.t.Errorf("Count: got %v, expected %d", i, s.count)
	}

	return nil
}

func TestSimplePipeline(t *testing.T) {

	tests := []int{0, 100, 500, 799, 10000, 49999}
	for _, i := range tests {
		var p Pipeline
		t.Logf("Running test for %v items\n", i)
		s := newSrc(i)
		f := newFilter()
		si := newSink(t, i)

		f.SetSource(s)
		si.SetSource(f)

		p.AddSource("src", s)
		p.AddFilter("filter", f)
		p.AddSink("sink", si)
		p.Execute()
	}

}

func TestShutdownPipeline(t *testing.T) {
	SetupBlockPool(512)
	srcSleep = time.Millisecond

	tests := []time.Duration{0, 100, 200, 300, 500}
	testErr := errors.New("test_pipeline_error")

	for _, delay := range tests {
		t.Logf("Running test for shutdown delay of %v\n", delay*time.Millisecond)
		var p Pipeline
		i := 10000
		s := newSrc(i)
		f := newFilter()
		si := newSink(t, i)

		f.SetSource(s)
		si.SetSource(f)

		p.AddSource("src", s)
		p.AddFilter("filter", f)
		p.AddSink("sink", si)
		time.AfterFunc(time.Millisecond*delay, func() { s.Shutdown(testErr) })
		err := p.Execute()

		if err != testErr {
			t.Errorf("Expected %v, got %v", testErr, err)
		}
	}
}

func TestErrorPipeline(t *testing.T) {
	SetupBlockPool(512)
	srcSleep = time.Millisecond

	tests := []time.Duration{0, 100, 200, 300, 500}

	testFn := func(who string) {
		for _, delay := range tests {
			t.Logf("Running test for error occuring in %v after delay of %v\n", who, delay*time.Millisecond)
			var p Pipeline
			var errch chan error
			i := 10000
			s := newSrc(i)
			f := newFilter()
			si := newSink(t, i)

			if who == "src" {
				errch = s.errch
			} else if who == "filter" {
				errch = f.errch
			} else {
				errch = si.errch
			}
			f.SetSource(s)
			si.SetSource(f)

			p.AddSource("src", s)
			p.AddFilter("filter", f)
			p.AddSink("sink", si)
			testErr := fmt.Errorf("%s-%d", who, delay)
			time.AfterFunc(time.Millisecond*delay, func() { errch <- testErr })
			err := p.Execute()

			if err != testErr {
				t.Errorf("Expected %v, got %v", testErr, err)
			}
		}
	}

	testFn("src")
	testFn("filter")
	testFn("sink")
}
