package common

import (
	"errors"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"strings"
	"sync"
	"time"
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
	waiters     map[int]chan Notification

	buckets map[string]bool
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
				logging.Infof("serviceChangeNotifier: received %s for bucket: %s", notifMsg, bucket.Name)
			default:
				errMsg := "Invalid msg type with CollectionManifestChangeNotification"
				return errors.New(errMsg)
			}
		} else {
			logging.Infof("serviceChangeNotifier: received %s", notifMsg)
		}

		for id, w := range instance.waiters {
			select {
			case w <- notifMsg:
			case <-time.After(notifyWaitTimeout):
				logging.Warnf("servicesChangeNotifier: Consumer for %v took too long to read notification, making the consumer invalid", instance.DebugStr())
				close(w)
				delete(instance.waiters, id)
			}
		}
		return nil
	}

	return fn
}

// This method gets invoked whenever there is a change to "pools/default/saslBucketsStreaming" endpoint
// The serviceChangeNotifier keeps a track of buckets that is it monitoring. When this method is
// invoked, it would check the buckets that have been newly added into the cluster and starts monitoring
// the corresponding buckets streaming endpoints (for manifestUID changes). If a bucket is deleted, then
// the streaming endpoing would get closed with EOF error and the go-routine would terminate. So, there is
// no need to explicitly close the go-routine monitoring the streaming endpoint of a bucket
func (instance *serviceNotifierInstance) bucketsChangeCallback(t NotificationType) func(interface{}) error {
	fn := func(msg interface{}) error {

		instance.Lock()
		defer instance.Unlock()

		if !instance.valid {
			return ErrNotifierInvalid
		}

		var saslBucket *couchbase.SaslBucket
		logging.Infof("serviceChangeNotifier: received BucketsChangeNotification")
		switch (msg).(type) {
		case *couchbase.SaslBucket:
			saslBucket = (msg).(*couchbase.SaslBucket)
		default:
			return errors.New("Invalid message type with BucketsChangeNotification")
		}

		// Remove all buckets that are present in instances and not in incoming message
		for bucket, _ := range instance.buckets {
			present := false
			for _, b := range saslBucket.Buckets {
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

		for _, bucket := range saslBucket.Buckets {
			if _, ok := instance.buckets[bucket.Name]; !ok {
				// Bucket is newly added to cluster
				logging.Infof("serviceChangeNotifier: Starting to monitor the bucket streaming endpoint for bucket: %v", bucket.Name)
				go instance.RunObserveCollectionManifestChanges(bucket.Name)
				instance.buckets[bucket.Name] = true
			}
		}

		return nil
	}

	return fn
}

func (instance *serviceNotifierInstance) RunPoolObserver() {
	poolCallback := instance.getNotifyCallback(PoolChangeNotification)
	err := instance.client.RunObservePool(instance.pool, poolCallback, nil)
	if err != nil {
		logging.Warnf("servicesChangeNotifier: Connection terminated for pool notifier instance of %s, %s (%v)", instance.DebugStr(), instance.pool, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunServicesObserver() {
	servicesCallback := instance.getNotifyCallback(ServiceChangeNotification)
	err := instance.client.RunObserveNodeServices(instance.pool, servicesCallback, nil)
	if err != nil {
		logging.Warnf("servicesChangeNotifier: Connection terminated for services notifier instance of %s, %s (%v)", instance.DebugStr(), instance.pool, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunBucketsObserver() {
	bucketCallback := instance.bucketsChangeCallback(BucketsChangeNotification)
	err := instance.client.RunObserveBuckets(instance.pool, bucketCallback, nil)
	if err != nil {
		logging.Warnf("servicesChangeNotifier: Connection terminated for buckets notifier instance of %s, %s (%v)", instance.DebugStr(), instance.pool, err)
	}
	instance.cleanup()
}

func (instance *serviceNotifierInstance) RunObserveCollectionManifestChanges(bucket string) {
	collectionChangeCallback := instance.getNotifyCallback(CollectionManifestChangeNotification)
	err := instance.client.RunObserveCollectionManifestChanges(instance.pool, bucket, collectionChangeCallback, nil)
	if err != nil {
		logging.Warnf("servicesChangeNotifier: Connection terminated for collection manifest notifier instance of %s, %s, bucket: %s, (%v)", instance.DebugStr(), instance.pool, bucket, err)
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
}

func (instance *serviceNotifierInstance) DebugStr() string {
	debugStr := instance.client.BaseURL.Scheme + "://"
	cred := strings.Split(instance.client.BaseURL.User.String(), ":")
	user := cred[0]
	debugStr += user + "@"
	debugStr += instance.client.BaseURL.Host
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
	}
	return t
}

type ServicesChangeNotifier struct {
	id       int
	instance *serviceNotifierInstance
	ch       chan Notification
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
			waiters:    make(map[int]chan Notification),
			buckets:    make(map[string]bool),
		}
		logging.Infof("servicesChangeNotifier: Creating new notifier instance for %s, %s", instance.DebugStr(), pool)

		singletonServicesContainer.notifiers[id] = instance
		go instance.RunPoolObserver()
		go instance.RunServicesObserver()
		go instance.RunBucketsObserver()
	}

	notifier := singletonServicesContainer.notifiers[id]
	notifier.Lock()
	defer notifier.Unlock()
	notifier.waiterCount++
	scn := &ServicesChangeNotifier{
		instance: notifier,
		ch:       make(chan Notification, 1),
		cancel:   make(chan bool),
		id:       notifier.waiterCount,
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
