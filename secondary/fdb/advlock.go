package forestdb

import "runtime/debug"

type advLock struct {
	initialized bool
	ch          chan int
}

func (l *advLock) Lock() {
	if !l.initialized {
		l.ch = make(chan int, 1)
		l.initialized = true
		l.ch <- 1
	}

	printStack := true
loop:
	for {
		select {
		case <-l.ch:
			break loop
		default:
			if printStack {
				Log.Infof("Unable to acquire lock\n%s", string(debug.Stack()))
				printStack = false
			}
		}
	}
}

func (l *advLock) Unlock() {
	l.ch <- 1
}
