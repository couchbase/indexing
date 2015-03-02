package common

import (
	"errors"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
	"time"
)

const (
	notifyWaitTimeout = time.Second * 5
)

var (
	ErrNodeServicesConnect = errors.New("Internal services API connection closed")
	ErrNodeServicesCancel  = errors.New("Cancelled services change notifier")
)

// Implements nodeServices change notifier system
var singletonServicesContainer struct {
	sync.Mutex
	notifiers map[string]*serviceNotifierInstance
}

type serviceNotifierInstance struct {
	sync.Mutex
	id          string
	waiterCount int
	client      couchbase.Client
	waiters     map[int]chan couchbase.PoolServices
}

type ServicesChangeNotifier struct {
	id       int
	instance *serviceNotifierInstance
	ch       chan couchbase.PoolServices
	cancel   chan bool
}

func init() {
	singletonServicesContainer.notifiers = make(map[string]*serviceNotifierInstance)
}

// Initialize change notifier object for a clusterUrl
func NewServicesChangeNotifier(clusterUrl, pool string) (*ServicesChangeNotifier, error) {
	singletonServicesContainer.Lock()
	defer singletonServicesContainer.Unlock()
	id := clusterUrl + "-" + pool

	if _, ok := singletonServicesContainer.notifiers[id]; !ok {
		logging.Infof("servicesChangeNotifier: Creating new notifier instance for %s, %s", clusterUrl, pool)
		client, err := couchbase.Connect(clusterUrl)
		if err != nil {
			return nil, err
		}
		instance := &serviceNotifierInstance{
			id:      id,
			client:  client,
			waiters: make(map[int]chan couchbase.PoolServices),
		}

		callb := func(ps couchbase.PoolServices) error {
			instance.Lock()
			defer instance.Unlock()
			for _, w := range instance.waiters {
				select {
				case w <- ps:
				case <-time.After(notifyWaitTimeout):
					logging.Warnf("servicesChangeNotifier: Consumer for %v took too long to read notification, making the consumer invalid", clusterUrl)
					close(w)
				}
			}

			return nil
		}

		singletonServicesContainer.notifiers[id] = instance

		go func() {
			err = client.RunObserveNodeServices(pool, callb, nil)
			if err != nil {
				logging.Errorf("servicesChangeNotifier: Connection terminated for notifier instance of %s, %s (%v)", clusterUrl, pool, err)
				singletonServicesContainer.Lock()
				for _, w := range instance.waiters {
					close(w)
				}
				delete(singletonServicesContainer.notifiers, instance.id)
				singletonServicesContainer.Unlock()
			}
		}()
	}

	notifier := singletonServicesContainer.notifiers[id]
	notifier.Lock()
	notifier.waiterCount++
	scn := &ServicesChangeNotifier{
		instance: notifier,
		ch:       make(chan couchbase.PoolServices),
		cancel:   make(chan bool),
		id:       notifier.waiterCount,
	}

	notifier.waiters[scn.id] = scn.ch
	notifier.Unlock()

	return scn, nil
}

// Call Get() method to block wait and obtain next services Config
func (sn *ServicesChangeNotifier) Get() (ps couchbase.PoolServices, err error) {
	select {
	case <-sn.cancel:
		err = ErrNodeServicesCancel
	case svs, ok := <-sn.ch:
		if !ok {
			err = ErrNodeServicesConnect
		} else {
			ps = svs
		}
	}
	return
}

// Consumer can cancel and invalidate notifier object by calling Close()
func (sn *ServicesChangeNotifier) Close() {
	sn.instance.Lock()
	defer sn.instance.Unlock()
	close(sn.cancel)
	delete(sn.instance.waiters, sn.id)
}
