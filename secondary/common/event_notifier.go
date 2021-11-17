package common

import (
	"fmt"
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
)

type EventType string

const (
	EVENT_NONE                     EventType = ""
	EVENT_NODEINFO_UPDATED                   = "NODE_INFO"
	EVENT_CLUSTERINFO_UPDATED_BASE           = "CLUSTER_INFO"
)

func getClusterInfoEventType(bucketName string) EventType {
	f := fmt.Sprintf("%s_%s", EVENT_CLUSTERINFO_UPDATED_BASE, bucketName)
	return EventType(f)
}

type eventManager struct {
	mutex            sync.Mutex
	isClosed         bool
	notifiers        map[EventType]([]*notifier)
	eventChannelSize uint
	notifierListSize uint
}

type notifier struct {
	id            string
	notifications chan interface{}
}

//
// Create a new event manager
//
func newEventManager(eventChannelSize uint, numEventTypes uint) (*eventManager, error) {

	r := &eventManager{
		isClosed:         false,
		notifiers:        make(map[EventType]([]*notifier), numEventTypes),
		eventChannelSize: eventChannelSize,
	}
	return r, nil
}

//
// Terminate the eventManager
//
func (e *eventManager) close() {

	defer func() {
		if r := recover(); r != nil {
			logging.Warnf("panic in eventManager.Close() : %s.  Ignored.\n", r)
		}
	}()

	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.isClosed {
		return
	}

	e.isClosed = true

	for _, notifiers := range e.notifiers {
		for _, notifier := range notifiers {
			close(notifier.notifications)
		}
	}
}

//
// Register a new event listener
//
func (e *eventManager) register(id string, evtType EventType) (<-chan interface{}, error) {

	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.isClosed {
		return nil, fmt.Errorf("eventManager is closed id %s event type %v", id, evtType)
	}

	notifiers, ok := e.notifiers[evtType]
	if !ok {
		e.notifiers[evtType] = make([]*notifier, 0)
	}

	for _, notifier := range notifiers {
		if notifier.id == id {
			return nil, fmt.Errorf("notifier with id %s already registered current event type %v", id, evtType)
		}
	}

	notifier := &notifier{
		id:            id,
		notifications: make(chan interface{}, e.eventChannelSize),
	}
	e.notifiers[evtType] = append(e.notifiers[evtType], notifier)

	return notifier.notifications, nil
}

//
// Count of waiting notifiers for an event
//
func (e *eventManager) count(evtType EventType) int {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	notifiers, ok := e.notifiers[evtType]
	if !ok {
		return 0
	}
	return len(notifiers)
}

//
// De-register a event listener
//
func (e *eventManager) unregister(id string, evtType EventType) error {

	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.isClosed {
		return fmt.Errorf("eventManager is closed id %s event type %v", id, evtType)
	}

	notifiers, ok := e.notifiers[evtType]
	if !ok {
		return nil
	}

	for i, notifier := range notifiers {
		if notifier.id == id {
			if i < len(notifiers)-1 {
				e.notifiers[evtType] = append(notifiers[:i], notifiers[i+1:]...)
			} else {
				e.notifiers[evtType] = notifiers[:i]
			}
		}
	}

	return nil
}

//
// Notify
//
func (e *eventManager) notify(evtType EventType, obj interface{}) error {

	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.isClosed {
		return fmt.Errorf("eventManager is closed event type %v", evtType)
	}

	notifiers, ok := e.notifiers[evtType]
	if !ok {
		return nil
	}

	// Note: here is a possibility that the channel is blocked and
	// this function holding onto the mutex
	for _, notifier := range notifiers {
		notifier.notifications <- obj
	}

	return nil
}
