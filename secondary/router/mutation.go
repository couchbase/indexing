// APIs to create KeyVersions messages.
// TODO: use slab allocated or memory pool to manage KeyVersions

package mutation

import (
	"github.com/couchbase/indexing/secondary/common"
)

// NewUpsert construct a Upsert message corresponding to KV's MUTATION
// message. Caller's responsibility to initialize `Keys`, `Oldkeys` and
// `Indexids`  as applicable.
func NewUpsert(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.Upsert,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewDeletion construct a Deletion message corresponding to KV's DELETION
// message. Caller's responsibility to initialize `Indexids`.
func NewDeletion(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.Deletion,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewUpsertDeletion construct a UpsertDeletion message. It is locally
// generated message to delete older key version. Caller's responsibility to
// initialize `Keys` and `Indexids`
func NewUpsertDeletion(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.UpsertDeletion,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewSync construct a Sync control message.
func NewSync(vb uint16, vbuuid uint64, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.Sync,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewDropData construct a DropData control message.
func NewDropData(vb uint16, vbuuid uint64, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.DropData,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewStreamBegin construct a StreamBegin control message.
func NewStreamBegin(vb uint16, vbuuid uint64, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.StreamBegin,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewStreamEnd construct a StreamEnd control message.
func NewStreamEnd(vb uint16, vbuuid uint64, seqno uint64) *common.KeyVersions {
	return &common.KeyVersions{
		Command: common.StreamEnd,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}
