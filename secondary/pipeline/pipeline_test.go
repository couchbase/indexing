package pipeline

import "fmt"
import "testing"
import "bytes"

type src struct {
	ItemWriter
	count int
}

func newSrc(c int) *src {
	s := &src{count: c}
	s.wchan = make(chan interface{}, 1)

	return s

}

func (s *src) Routine() error {
	for i := 0; i < s.count; i++ {
		itm := []byte(fmt.Sprintf("item-%d", i))
		s.WriteItem(itm)
	}
	s.CloseWrite()

	return nil
}

type filter struct {
	ItemWriter
	ItemReader
}

func newFilter() *filter {
	f := &filter{}
	f.wchan = make(chan interface{}, 1)
	return f
}

func (f *filter) Routine() error {
	for {
		itm, err := f.ReadItem()
		if err == ErrNoMoreItem {
			f.CloseRead()
			f.CloseWrite()
			return nil
		}

		f.WriteItem([]byte(string(itm) + "-filtered"))
	}

	return nil
}

type sink struct {
	ItemReader
	t     *testing.T
	count int
}

func newSink(t *testing.T, c int) *sink {
	return &sink{t: t, count: c}
}

func (s *sink) Routine() error {
	i := 0
	for {
		itm, err := s.ReadItem()
		if err == ErrNoMoreItem {
			s.CloseRead()
			break
		}

		expected := []byte(fmt.Sprintf("item-%d-filtered", i))
		i++
		if !bytes.Equal(itm, expected) {
			s.t.Fatalf("got %s, expected %s", itm, expected)
		}

	}

	if i != s.count {
		s.t.Errorf("Count: got %v, expected", i, s.count)
	}

	return nil
}

func TestSimplePipeline(t *testing.T) {

	tests := []int{100, 500, 799, 10000, 49999}
	for _, i := range tests {
		var p Pipeline
		t.Logf("Running test for %v\n", i)
		s := newSrc(i)
		f := newFilter()
		si := newSink(t, i)

		f.SetSource(s)
		si.SetSource(f)

		p.AddSource(s)
		p.AddFilter(f)
		p.AddSink(si)
		p.Execute()
	}

}
