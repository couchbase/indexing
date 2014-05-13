package common

// Router defines is per uuid router interface, where uuid might refer to an
// index or similar entities.
//
// TODO:
// - Differentiate between downstream endpoints and coordinator endpoint, so
//   as to optimize the payload size to coordinator endpoint.
type Router interface {
	// Bucket will return the bucket name for which this router instance is
	// applicable.
	Bucket() string

	// UuidEndpoints return a list of endpoints <host:port> that are
	// associated with unique id `uuid`. Uuid can be an index-uuid.
	UuidEndpoints() []string

	// Coordinator endpoint
	CoordinatorEndpoint() string

	// UpsertEndpoints return a list of endpoints <host:port> to which Upserted
	// secondary-key, for this uuid, need to be published.
	UpsertEndpoints(vbno uint16, seqno uint64, docid, key, oldkey []byte) []string

	// UpsertDeletionEndpoints return a list of endpoints <host:port> to which
	// UpsertDeletion secondary-key, for this uuid, need to be published.
	UpsertDeletionEndpoints(vbno uint16, seqno uint64, docid, key, oldkey []byte) []string

	// DeletionEndpoints return a list of endpoints <host:port> to which
	// Deletion docid/secondary-key, for this uuid,  need to be published.
	DeletionEndpoints(vbno uint16, seqno uint64, docid, oldkey []byte) []string
}
