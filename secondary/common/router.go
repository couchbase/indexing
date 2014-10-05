package common

import mc "github.com/couchbase/gomemcached/client"

// Router defines is per instance (aka engine) router
// interface, where an instance might refer to an index
// or similar entities.
type Router interface {
	// Bucket will return the bucket name for which this
	// router instance is applicable.
	Bucket() string

	// Endpoints return full list of endpoints <host:port>
	// that are listening for this instance.
	Endpoints() []string

	// UpsertEndpoints return a list of endpoints <host:port>
	// to which Upsert message will be published.
	// * m.VBucket, m.Seqno, m.Key - carry {vbno, seqno, docid}
	UpsertEndpoints(m *mc.UprEvent, partKey, key, oldKey []byte) []string

	// UpsertDeletionEndpoints return a list of endpoints
	// <host:port> to which UpsertDeletion message will be
	// published.
	// * oldPartKey and oldKey will be computed based on m.OldValue
	// * m.VBucket, m.Seqno, m.Key - carry {vbno, seqno, docid}
	UpsertDeletionEndpoints(m *mc.UprEvent, oldPartKey, key, oldKey []byte) []string

	// DeletionEndpoints return a list of endpoints
	// <host:port> to which Deletion message will be published.
	// * partKey and oldKey will be computed based on m.OldValue
	// * m.VBucket, m.Seqno, m.Key - carry {vbno, seqno, docid}
	DeletionEndpoints(m *mc.UprEvent, oldPartKey, oldKey []byte) []string
}
