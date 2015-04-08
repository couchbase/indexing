package pipeline

import (
	"errors"
)

var (
	ErrSinkExist = errors.New("Sink already exist")
)

type Runnable interface {
	Routine() error
}

type Writer interface {
	Channel() chan interface{}
}

type Source interface {
	Writer
	Shutdown(err error)
	Routine() error
}

type Filter interface {
	Writer
	SetSource(Writer)
	Routine() error
}

type Sink interface {
	SetSource(Writer)
	Routine() error
}

type Pipeline struct {
	sources []Source
	filters []Filter
	sink    Sink
}

func (p *Pipeline) AddSource(s Source) error {
	p.sources = append(p.sources, s)
	return nil
}

func (p *Pipeline) AddFilter(f Filter) error {
	p.filters = append(p.filters, f)
	return nil
}

func (p *Pipeline) AddSink(s Sink) error {
	if p.sink == nil {
		p.sink = s
	} else {
		return ErrSinkExist
	}

	return nil
}

func (p *Pipeline) runIt(r Runnable) {
	go func() {
		err := r.Routine()
		if err != nil {
			for _, src := range p.sources {
				src.Shutdown(err)
			}
		}
	}()
}

func (p *Pipeline) Execute() error {
	for _, src := range p.sources {
		p.runIt(src)
	}

	for _, filter := range p.filters {
		p.runIt(filter)
	}

	return p.sink.Routine()
}
