package common

import couchbase "github.com/couchbase/indexing/secondary/dcp"

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

	// Set max retries for ns_server calls
	SetMaxRetries(retries uint32)

	// Bucket Info Level Information accessors in client
	GetBucketUUID(bucket string) (uuid string, err error)

	GetBucketNames() []couchbase.BucketName

	GetNumVBuckets(bucket string) (numVBuckets int, err error)

	GetCollectionID(bucket, scope, collection string) string

	IsEphemeral(bucket string) (bool, error)

	IsMagmaStorage(bucketName string) (bool, error)

	ValidateBucket(bucketName string, uuids []string) (resp bool)

	// Node Info Level Information accessors in client
	ClusterVersion() uint64

	// Collection Info Level Information accessors in client
	ValidateCollectionID(bucket, scope, collection, collnID string, retry bool) bool

	// Stub functions to make clusterInfoClient and clusterInfoCacheLiteClient compatible
	// Note:
	// * cinfoProviderRLock if acquired before this call is for acquiring cinfoProvider object
	// * cinfoProvider can be cinfoClient or cinfoLiteClient and lock is used in hot swap
	// * This function will update internal state of cache with lock in case of cinfoClient
	//   currently is a no-op in case of cinfoLiteClient as internal state management is done
	//   with atomic update of pointers
	ForceFetch() error
}

// NewClusterInfoProvider is factory to get ClusterInfoClient or ClusterInfoCacheLiteClient
func NewClusterInfoProvider(lite bool, clusterUrl string, pool, userAgent string, cfg Config) (
	ClusterInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, userAgent, cfg.SectionConfig("cinfo_lite.", true))
		if err != nil {
			return nil, err
		}
		return ClusterInfoProvider(cicl), nil
	} else {
		ci, err := NewClusterInfoClient(clusterUrl, pool, userAgent, cfg)
		if err != nil {
			return nil, err
		}
		ci.SetUserAgent(userAgent)
		return ClusterInfoProvider(ci), nil
	}
}

// NodesInfoProvider is interface to query Nodes, NodesExt and ServerGroups information
type NodesInfoProvider interface {
	// ClusterVersion
	GetClusterVersion() uint64

	// NodeIds are indices into nodesExt or nodeSvs slice so should not be used
	// across instances of NodesInfoProvider
	GetNodeIdsByServiceType(srvc string) (nids []NodeId)

	GetNodesByServiceType(srvc string) (nodes []couchbase.Node)

	// Hold the lock if needed and get NodeIds and pass here to get service address.
	GetServiceAddress(nid NodeId, srvc string, useEncryptedPortMap bool) (addr string, err error)

	GetNodeUUID(nid NodeId) string
	GetNodeIdByUUID(uuid string) (NodeId, bool)

	GetLocalHostname() (string, error)
	GetLocalNodeUUID() string
	GetLocalServiceAddress(srvc string, useEncryptedPortMap bool) (srvcAddr string, err error)
	GetLocalServicePort(srvc string, useEncryptedPortMap bool) (string, error)

	GetActiveIndexerNodes() (nodes []couchbase.Node)
	GetFailedIndexerNodes() (nodes []couchbase.Node)

	GetActiveKVNodes() (nodes []couchbase.Node)
	GetFailedKVNodes() (nodes []couchbase.Node)

	GetActiveQueryNodes() (nodes []couchbase.Node)
	GetFailedQueryNodes() (nodes []couchbase.Node)

	// Stub functions to make nodesInfo replaceable with clusterInfoCache
	SetUserAgent(userAgent string)
	FetchNodesAndSvsInfo() (err error)

	RWLockable // Stub to make ClusterInfoCache replaceable with NodesInfo
}

// NewNodesInfoProvider is factory to either get NodesInfo or ClusterInfoCache
func NewNodesInfoProvider(lite bool, clusterUrl string, pool, userAgent string, config Config) (
	NodesInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, userAgent,
			config.SectionConfig("cinfo_lite", true))
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
		ci.SetUserAgent(userAgent)
		return NodesInfoProvider(ci), nil
	}
}

// CollectionInfoProvider is an interface to query Collection Manifest
type CollectionInfoProvider interface {
	CollectionID(bucket, scope, collection string) string
	ScopeID(bucket, scope string) string
	ScopeAndCollectionID(bucket, scope, collection string) (string, string)
	GetIndexScopeLimit(bucket, scope string) (uint32, error)

	RWLockable // Stub to make ClusterInfoCache replaceable with CollectionInfo
	FetchBucketInfo(bucketName string) error
	FetchManifestInfo(bucketName string) error
}

// NewCollectionInfoProvider is factory to either get CollectionInfo or ClusterInfoCache
func NewCollectionInfoProvider(lite bool, clusterUrl, pool, bucketName, userAgent string, config Config) (
	CollectionInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, userAgent,
			config.SectionConfig("cinfo_lite", true))
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
		ci.SetUserAgent(userAgent)
		return CollectionInfoProvider(ci), nil
	}
}

type BucketInfoProvider interface {
	// GetLocalVBuckets returns vbs in the current kv node
	GetLocalVBuckets(bucket string) (vbs []uint16, err error)

	GetBucketUUID(bucket string) (uuid string)

	IsEphemeral(bucket string) (bool, error)

	GetServiceAddress(nid NodeId, srvc string, useEncryptedPortMap bool) (addr string, err error)

	GetVBuckets(nid NodeId, bucket string) (vbs []uint32, err error)

	GetNodesByBucket(bucket string) (nids []NodeId, err error)

	GetNumVBuckets(bucket string) (numVBuckets int, err error)

	// Stub
	FetchBucketInfo(bucketName string) error
	ForceFetch() error

	RWLockable // Stub to make ClusterInfoCache replaceable with BucketInfo
}

// NewBucketInfoProvider is factory to either get BucketInfo or ClusterInfoCache
func NewBucketInfoProvider(lite bool, clusterUrl, pool, bucketName, userAgent string, config Config) (
	BucketInfoProvider, error) {
	if lite {
		cicl, err := NewClusterInfoCacheLiteClient(clusterUrl, pool, userAgent,
			config.SectionConfig("cinfo_lite", true))
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
		ci.SetUserAgent(userAgent)
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
