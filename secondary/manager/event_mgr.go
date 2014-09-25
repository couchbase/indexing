package manager

import (
	"sync"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type EventType byte 
const (
	CREATE_INDEX EventType = iota 
	DROP_INDEX   EventType 
)

type eventManager struct {
	mutex			 sync.Mutex
	isClosed		 bool
	notifiers        map[EventType]([]notifier)
}

type notifier struct {
	id				 string
	notifications	 chan interface{}	
}

const DEFAULT_EVT_QUEUE_SIZE uint16 = 20 
const DEFAULT_NOTIFIER_QUEUE_SIZE uint16 = 5 

///////////////////////////////////////////////////////
// Package Local Function 
///////////////////////////////////////////////////////

//
// Create a new event manager
//
func newEventManager() (*eventManager, error) {

	r := &eventManager{isClosed : false}
	return r, nil
}

//
// Terminate the eventManager
//
func (e *eventManager) close() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in eventManager.Close() : %s.  Ignored.\n", r)
		}
	}()
	
	e.mutex.Lock()
	defer e.mutex.Unlock()
	
	if isClosed {
		return
	}
	
	isClosed = true
	
	for _, notifiers := range e.notifiers {
		for _, notifier := range notifiers {
			close(notifier.notifications)
		}
	}		
}

//
// Register a new event listener
//
func (e *eventManager) register(id string, evtType EventType) (<- chan interface{}, err) {

	e.mutex.Lock()
	defer e.mutex.Unlock()
	
	notifiers, ok := e.notifiers[evtType]
	if !ok {
		notifiers = make([]notifier, DEFAULT_NOTIFIER_QUEUE_SIZE)
		e.notifiers[evtType] = notifiers
	} 
				
	for _, notifier := range notifiers {
		if notifier.id == id {
			return nil, NewError(ERROR_EVT_DUPLICATE_NOTIFIER, NORMAL, EVENT_MANAGER, "", 
				fmt.Printf("Notifier %d already registered", id))
		}
	}
				
	notifier := &notifier{id : id, 
						  notifications : make(chan interface{}, DEFAULT_EVT_QUEUE_SIZE)} 
	e.notifiers[evtType] = append(e.notifiers[evtType], notifier)
			
	return notifier.notifications, nil
}

//
// De-register a event listener
//
func (e *eventManager) unregister(id string, evtType EventType) {

	e.mutex.Lock()
	defer e.mutex.Unlock()

	notifiers, ok := e.notifiers[evtType]
	if !ok {
		return
	}
	
	for i, notifier := range notifiers {
		if notifier.id == id {
			if i < len(notifiers) - 1 {
				e.notifiers[evtType] = append(notifier[:i], notifier[i+1:])
			} else {
				e.notifiers[evtType] = notifier[:i]
			}
		}
	}
}

//
// Notify
//
func (e *eventManager) notify(evtType EventType, obj interface{}) {

	e.mutex.Lock()
	defer e.mutex.Unlock()
	
	notifiers, ok := e.notifiers[evtType]
	if !ok {
		return
	}

	// TODO : There is a possibility that the channel is blocked and
	// this function holding onto the mutex	
	for i, notifier := range notifiers {
		notifier.notifications <- obj
	}
}



