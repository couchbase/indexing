//go:build community

package regulator

type Ctx interface{}

type Units uint64

type UnitType uint

type AggregateRecorder struct {
}

func (ar *AggregateRecorder) AddBytes(bytes uint64) error {
	return nil
}

func (ar *AggregateRecorder) State() (metered, pending Units, bytesPending uint64) {
	return
}

func (ar *AggregateRecorder) Commit() (committed Units, err error) {
	return
}

func (ar *AggregateRecorder) Abort() error {
	return nil
}

func (ar *AggregateRecorder) Flush() error {
	return nil
}
