package common

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	notifyWaitTimeout = time.Second * 5
)

type NotificationType int

const (
	ServiceChangeNotification NotificationType = iota
	PoolChangeNotification
	BucketsChangeNotification
	CollectionManifestChangeNotification
	PeriodicUpdateNotification
	ForceUpdateNotification
)

var (
	ErrNodeServicesConnect = errors.New("Internal services API connection closed")
	ErrNodeServicesCancel  = errors.New("Cancelled services change notifier")
	ErrNotifierInvalid     = errors.New("Notifier invalidated due to internal error")
)

// Implements nodeServices change notifier system
var singletonServicesContainer struct {
	sync.Mutex
	notifiers map[string]*serviceNotifierInstance
}

type serviceNotifierInstance struct {
	sync.Mutex
	id          string
	clusterUrl  string
	pool        string
	waiterCount int
	client      couchbase.Client
	valid       bool
	waiters     map[string]chan Notification

	buckets map[string]bool

	closeCh chan bool
}

func (instance *serviceNotifierInstance) getNotifyCallback(t NotificationType) func(interface{}) error {
	fn := func(msg interface{}) error {
		instance.Lock()
		defer instance.Unlock()

		if !instance.valid {
			return ErrNotifierInvalid
		}

		notifMsg := Notification{
			Type: t,
			Msg:  msg,
		}

		if t == CollectionManifestChangeNotification {
			switch (msg).(type) {
			case *couchbase.Bucket:
				bucket := (msg).(*couchbase.Bucket)
				logging.Debugf("serviceChangeNotifier: received %s for bucket: %s", notifMsg, bucket.Name)
			default:
				errMsg := "Invalid msg type with CollectionManifestChangeNotification"
				return errors.New(errMsg)
			}
		} else if t == PoolChangeNotification {
			switch (msg).(type) {
			case *couchbase.Pool:
				pool := (msg).(*couchbase.Pool)
				instance.bucketsChangeCallback(pool.BucketNames)
				logging.Infof("serviceChangeNotifier: received %s", notifMsg)
			default:
				errMsg := "Invalid msg type with PoolChangeNotification"
				return errors.New(errMsg)
			}
		} else {
			logging.Infof("serviceChangeNotifier: received %s", notifMsg)
		}

		for id, w := range instance.waiters {
			select {
			case w <- notifMsg:
			case <-time.After(notifyWaitTimeout):
				split := strings.Split(id, "_")
				consumer := split[:len(split)-1]
				logging.Warnf("serviceChangeNotifier: Consumer (%v) for %v took too long to read notification, making the consumer invalid", strings.Join(consumer, "_"), instance.id)
				close(w)
				delete(instance.waiters, id)
			}
		}
		return nil
	}

	return fn
}

// This method gets invoked whenever there is a change to poolsStreaming endpoint
// The serviceChangeNotifier keeps a track of buckets that is it monitoring. When this method is
// invoked, it would check the buckets that have been newly added into the cluster and starts monitoring
// the corresponding buckets streaming endpoints (for manifestUID changes). If a bucket is deleted, then
// the streaming endpoint would get closed with EOF error and the go-routine would terminate. So, there is
// no need to explicitly close the go-routine monitoring the streaming endpoint of a bucket
func (instance *serviceNotifierInstance) bucketsChangeCallback(bucketNames []couchbase.BucketName) {

	// Remove all buckets that are present in instances and not in incoming bucketNames
	for bucket := range instance.buckets {
		present := false
		for _, b := range bucketNames {
			if b.Name == bucket {
				present = true
				break
			}
		}
		if !present {
			logging.Infof("serviceChangeNotifier: Removing the bucket: %v from book-keeping", bucket)
			delete(instance.buckets, bucket)
		}
	}

	for _, b := range bucketNames {
		if _, ok := instance.buckets[b.Name]; !ok {
			// Bucket is newly added to cluster
			logging.Infof("serviceChangeNotifier: Starting to monitor the bucket streaming endpoint for bucket: %v", b.Name)
			go instance.RunObserveCollectionManifestChanges(b.Name)
			instance.buckets[b.Name] = true
		}
	}

	return
}

func (instance *serviceNotifierInstance) RunPoolObserver() {
	poolCallback := instance.getNotifyCallback(PoolChangeNotification)
	err := instance.client.RunObservePool(instance.pool, poolCallback, instance.closeCh)
	if err != nil {
		logging.Warnf("serviceChangeNotifier: Connection terminated for pool notifier instance of %s (%v)", instance.id, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunServicesObserver() {
	servicesCallback := instance.getNotifyCallback(ServiceChangeNotification)
	err := instance.client.RunObserveNodeServices(instance.pool, servicesCallback, instance.closeCh)
	if err != nil {
		logging.Warnf("serviceChangeNotifier: Connection terminated for services notifier instance of %s (%v)", instance.id, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunObserveCollectionManifestChanges(bucket string) {
	collectionChangeCallback := instance.getNotifyCallback(CollectionManifestChangeNotification)
	err := instance.client.RunObserveCollectionManifestChanges(instance.pool, bucket, collectionChangeCallback, instance.closeCh)
	if err != nil {
		logging.Warnf("serviceChangeNotifier: Connection terminated for collection manifest notifier instance of %s, bucket: %s, (%v)", instance.id, bucket, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) cleanup() {
	singletonServicesContainer.Lock()
	defer singletonServicesContainer.Unlock()

	instance.Lock()
	defer instance.Unlock()
	if !instance.valid {
		return
	}

	instance.valid = false
	for _, w := range instance.waiters {
		close(w)
	}
	delete(singletonServicesContainer.notifiers, instance.id)
	close(instance.closeCh)
}

func URLString(baseURL *url.URL) string {
	debugStr := baseURL.Scheme + "://"
	cred := strings.Split(baseURL.User.String(), ":")
	user := cred[0]
	debugStr += user + "@"
	debugStr += baseURL.Host
	return debugStr
}

type Notification struct {
	Type NotificationType
	Msg  interface{}
}

func (n Notification) String() string {
	var t string = "ServiceChangeNotification"
	if n.Type == PoolChangeNotification {
		t = "PoolChangeNotification"
	} else if n.Type == BucketsChangeNotification {
		t = "BucketsChangeNotification"
	} else if n.Type == CollectionManifestChangeNotification {
		t = "CollectionManifestChangeNotification"
	} else if n.Type == PeriodicUpdateNotification {
		t = "PeriodicUpdateNotification"
	} else if n.Type == ForceUpdateNotification {
		t = "ForceUpdateNotification"
	}
	return t
}

type ServicesChangeNotifier struct {
	id       string // A combination of "consumer" name and waiter count
	instance *serviceNotifierInstance
	ch       chan Notification
	cancel   chan bool
}

func init() {
	singletonServicesContainer.notifiers = make(map[string]*serviceNotifierInstance)
}

// Initialize change notifier object for a clusterUrl
func NewServicesChangeNotifier(clusterUrl, pool, consumer string) (*ServicesChangeNotifier, error) {
	singletonServicesContainer.Lock()
	defer singletonServicesContainer.Unlock()

	baseUrl, err := couchbase.ParseURL(clusterUrl)
	if err != nil {
		return nil, err
	}
	id := URLString(baseUrl) + "-" + pool

	if _, ok := singletonServicesContainer.notifiers[id]; !ok {
		client, err := couchbase.Connect(clusterUrl)
		if err != nil {
			return nil, err
		}
		instance := &serviceNotifierInstance{
			id:         id,
			client:     client,
			clusterUrl: clusterUrl,
			pool:       pool,
			valid:      true,
			waiters:    make(map[string]chan Notification),
			buckets:    make(map[string]bool),
			closeCh:    make(chan bool),
		}
		logging.Infof("serviceChangeNotifier: Creating new notifier instance for %s", id)

		singletonServicesContainer.notifiers[id] = instance
		go instance.RunPoolObserver()
		go instance.RunServicesObserver()
	}

	notifier := singletonServicesContainer.notifiers[id]
	notifier.Lock()
	defer notifier.Unlock()
	notifier.waiterCount++
	scn := &ServicesChangeNotifier{
		instance: notifier,
		ch:       make(chan Notification, 1),
		cancel:   make(chan bool),
		id:       fmt.Sprintf("%v_%v", consumer, notifier.waiterCount),
	}

	notifier.waiters[scn.id] = scn.ch

	return scn, nil
}

// Call Get() method to block wait and obtain next services Config
func (sn *ServicesChangeNotifier) Get() (n Notification, err error) {
	select {
	case <-sn.cancel:
		err = ErrNodeServicesCancel
	case msg, ok := <-sn.ch:
		if !ok {
			err = ErrNodeServicesConnect
		} else {
			n = msg
		}
	}
	return
}

func (sn *ServicesChangeNotifier) GetNotifyCh() chan Notification {
	return sn.ch
}

// Consumer can cancel and invalidate notifier object by calling Close()
func (sn *ServicesChangeNotifier) Close() {
	sn.instance.Lock()
	defer sn.instance.Unlock()
	close(sn.cancel)
	delete(sn.instance.waiters, sn.id)
}
