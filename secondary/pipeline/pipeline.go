package pipeline

import (
	"errors"
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
)

var (
	ErrSinkExist      = errors.New("Sink already exist")
	ErrSupervisorKill = errors.New("Supervisor requested exit")
)

type Runnable interface {
	Routine() error
	Kill()
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
	sources []*pipelineObject
	filters []*pipelineObject
	sink    *pipelineObject
	wg      sync.WaitGroup
}

type pipelineObject struct {
	n string
	r Runnable
}

func (p *Pipeline) AddSource(name string, s Source) error {
	p.sources = append(p.sources, &pipelineObject{n: name, r: s.(Runnable)})
	return nil
}

func (p *Pipeline) AddFilter(name string, f Filter) error {
	p.filters = append(p.filters, &pipelineObject{n: name, r: f.(Runnable)})
	return nil
}

func (p *Pipeline) AddSink(name string, s Sink) error {
	if p.sink == nil {
		p.sink = &pipelineObject{n: name, r: s.(Runnable)}
	} else {
		return ErrSinkExist
	}

	return nil
}

func (p *Pipeline) runIt(o *pipelineObject) {
	p.wg.Add(1)
	go func() {
		err := o.r.Routine()
		if err != nil {
			logging.Errorf("%v exited with error %v", o.n, err)
		}
		p.wg.Done()
	}()
}

func (p *Pipeline) Finalize() {
	for _, src := range p.sources {
		src.r.Kill()
	}

	for _, filter := range p.filters {
		filter.r.Kill()
	}

	p.wg.Wait()
}

func (p *Pipeline) Execute() error {
	for _, src := range p.sources {
		p.runIt(src)
	}

	for _, filter := range p.filters {
		p.runIt(filter)
	}

	err := p.sink.r.Routine()
	p.Finalize()
	return err
}
