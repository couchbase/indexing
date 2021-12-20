package common

/*
* ClusterInfoProvider will allow user to get Nodes, Collection and Bucket Info Providers
* ClusterInfoProvider can also provide few API to avoid redirection to NodesInfoProvider
* or CollectionInfoProvider like ValidateBucket, ValidateCollection etc.
* NodesInfoProvider will give information about the nodes like clusterVersion, serviceAddress etc.
* CollectionInfoProvider will provide API to query Collection Manifest information
 */

type ClusterInfoProvider interface {
	// NodesInfoProvider will give API to get nodes info like cluster version and etc.
	GetNodesInfoProvider() (NodesInfoProvider, error)

	// CollectionInfoProvider will give API to query Collections Manifest info like collnId etc.
	GetCollectionInfoProvider(bucketName string) (CollectionInfoProvider, error)

	// BucketInfoProvider will give API to query info about bucket like vbmap and etc.
	GetBucketInfoProvider(bucketName string) (BucketInfoProvider, error)

	// Close and cleanup pointers
	Close()

	// SetUserAgent for log prefix
	SetUserAgent(logPrefix string)

	// Set time between retries of calls to ns_server
	SetRetryInterval(seconds uint32)

	// TODO: Move to this to bucket Info
	GetBucketUUID(bucket string) (uuid string, err error)

	// Stub functions to make clusterInfoClient and clusterInfoCacheLiteClient compatible
	FetchWithLock() error
}

// NewClusterInfoProvider is factory to get ClusterInfoClient or ClusterInfoCacheLiteClient
func NewClusterInfoProvider(lite bool, clusterUrl string, pool string, cfg Config) (
	ClusterInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, cfg)
		if err != nil {
			return nil, err
		}
		return ClusterInfoProvider(cicl), nil
	} else {
		ci, err := NewClusterInfoClient(clusterUrl, pool, cfg)
		if err != nil {
			return nil, err
		}
		return ClusterInfoProvider(ci), nil
	}
}

// NodesInfoProvider is interface to query Nodes, NodesExt and ServerGroups information
type NodesInfoProvider interface {
	// ClusterVersion
	GetClusterVersion() uint64

	// NodeIds are indices into nodesExt or nodeSvs slice so should not be used
	// across instances of NodesInfoProvider
	GetNodesByServiceType(srvc string) (nids []NodeId)

	// Hold the lock if needed and get NodeIds and pass here to get service address.
	GetServiceAddress(nid NodeId, srvc string, useEncryptedPortMap bool) (addr string, err error)

	GetNodeUUID(nid NodeId) string

	GetLocalHostname() (string, error)
	GetLocalNodeUUID() string
	GetLocalServiceAddress(srvc string, useEncryptedPortMap bool) (srvcAddr string, err error)

	// Stub functions to make nodesInfo replaceable with clusterInfoCache
	SetUserAgent(userAgent string)
	FetchNodesAndSvsInfo() (err error)

	RWLockable // Stub to make ClusterInfoCache replaceable with NodesInfo
}

// NewNodesInfoProvider is factory to either get NodesInfo or ClusterInfoCache
func NewNodesInfoProvider(lite bool, clusterUrl string, pool string) (
	NodesInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, nil)
		if err != nil {
			return nil, err
		}
		ni, err := cicl.GetNodesInfo()
		if err != nil {
			return nil, err
		}
		return NodesInfoProvider(ni), nil
	} else {
		ci, err := NewClusterInfoCache(clusterUrl, pool)
		if err != nil {
			return nil, err
		}
		return NodesInfoProvider(ci), nil
	}
}

// CollectionInfoProvider is an interface to query Collection Manifest
type CollectionInfoProvider interface {
	CollectionID(bucket, scope, collection string) string
	ScopeID(bucket, scope string) string
	ScopeAndCollectionID(bucket, scope, collection string) (string, string)

	RWLockable // Stub to make ClusterInfoCache replaceable with CollectionInfo
}

// NewCollectionInfoProvider is factory to either get CollectionInfo or ClusterInfoCache
func NewCollectionInfoProvider(lite bool, clusterUrl, pool, bucketName string) (
	CollectionInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, nil)
		if err != nil {
			return nil, err
		}
		ci, err := cicl.GetCollectionInfo(bucketName)
		if err != nil {
			return nil, err
		}
		return CollectionInfoProvider(ci), nil
	} else {
		ci, err := NewClusterInfoCache(clusterUrl, pool)
		if err != nil {
			return nil, err
		}
		return CollectionInfoProvider(ci), nil
	}
}

type BucketInfoProvider interface {
	// GetLocalVBuckets returns vbs in the current kv node
	GetLocalVBuckets(bucket string) (vbs []uint16, err error)

	// TODO: understand why vbmap check is needed in this API
	//GetBucketUUID(bucket string) (uuid string, err error)

	// Stub
	FetchBucketInfo(bucketName string) error
	FetchWithLock() error

	RWLockable // Stub to make ClusterInfoCache replaceable with BucketInfo
}

// NewBucketInfoProvider is factory to either get BucketInfo or ClusterInfoCache
func NewBucketInfoProvider(lite bool, clusterUrl, pool, bucketName string) (
	BucketInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, nil)
		if err != nil {
			return nil, err
		}
		bi, err := cicl.GetBucketInfo(bucketName)
		if err != nil {
			return nil, err
		}
		return BucketInfoProvider(bi), nil
	} else {
		ci, err := NewClusterInfoCache(clusterUrl, pool)
		if err != nil {
			return nil, err
		}
		return BucketInfoProvider(ci), nil
	}
}

// Stubs to make ClusterInfoCache replaceable with NodesInfo and CollectionInfo
type StubRWMutex struct{}

func (rw *StubRWMutex) Lock()    {}
func (rw *StubRWMutex) Unlock()  {}
func (rw *StubRWMutex) RLock()   {}
func (rw *StubRWMutex) RUnlock() {}

type RWLockable interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}
