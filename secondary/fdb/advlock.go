package forestdb

import "runtime/debug"

type advLock struct {
	initialized bool
	ch          chan int
}

func newAdvLock() advLock {
	l := advLock{}
	l.Init()
	return l
}

func (l *advLock) Init() {
	if !l.initialized {
		l.ch = make(chan int, 1)
		l.ch <- 1
		l.initialized = true
	}
}

func (l *advLock) Destroy() {
	l.initialized = false
	close(l.ch)
}

func (l *advLock) Lock() {
	printStack := true
loop:
	for {
		select {
		case <-l.ch:
			break loop
		default:
			if printStack {
				Log.Debugf("Unable to acquire lock\n%s", string(debug.Stack()))
				printStack = false
			}
		}
	}
}

func (l *advLock) Unlock() {
	l.ch <- 1
}
