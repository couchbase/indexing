package syncx

import (
	"sync"
	"sync/atomic"
)

// OnceOnSuccess is a synchronization primitive similar to sync.Once,
// but with one key difference: the function is marked only if it succeeds.
//
// The provided function is executed only once, and only if it returns a nil error.
// If the function returns an error, it is not marked as done and can be retried
// by subsequent calls.
//
// OnceOnSuccess guarantees that the function will be executed at most once successfully,
// even when called concurrently.
//
// Hence the name: 'Once' but only on success.
type OnceOnSuccess struct {
	done atomic.Uint32
	mu   sync.Mutex
}

func (o *OnceOnSuccess) Do(f func() error) error {
	if o.done.Load() == 1 {
		// already executed - no error seen
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done.Load() == 0 {
		err := f()
		if err == nil {
			o.done.Store(1)
		}
		return err
	}
	return nil
}
