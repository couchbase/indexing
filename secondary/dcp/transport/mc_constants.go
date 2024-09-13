// Package transport is binary protocol packet formats and constants.
package transport

import (
	"fmt"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

// CommandCode for memcached packets.
type CommandCode uint8

const (
	GET        = CommandCode(0x00)
	SET        = CommandCode(0x01)
	ADD        = CommandCode(0x02)
	REPLACE    = CommandCode(0x03)
	DELETE     = CommandCode(0x04)
	INCREMENT  = CommandCode(0x05)
	DECREMENT  = CommandCode(0x06)
	QUIT       = CommandCode(0x07)
	FLUSH      = CommandCode(0x08)
	GETQ       = CommandCode(0x09)
	NOOP       = CommandCode(0x0a)
	VERSION    = CommandCode(0x0b)
	GETK       = CommandCode(0x0c)
	GETKQ      = CommandCode(0x0d)
	APPEND     = CommandCode(0x0e)
	PREPEND    = CommandCode(0x0f)
	STAT       = CommandCode(0x10)
	SETQ       = CommandCode(0x11)
	ADDQ       = CommandCode(0x12)
	REPLACEQ   = CommandCode(0x13)
	DELETEQ    = CommandCode(0x14)
	INCREMENTQ = CommandCode(0x15)
	DECREMENTQ = CommandCode(0x16)
	QUITQ      = CommandCode(0x17)
	FLUSHQ     = CommandCode(0x18)
	APPENDQ    = CommandCode(0x19)
	PREPENDQ   = CommandCode(0x1a)
	HELO       = CommandCode(0x1f)
	RGET       = CommandCode(0x30)
	RSET       = CommandCode(0x31)
	RSETQ      = CommandCode(0x32)
	RAPPEND    = CommandCode(0x33)
	RAPPENDQ   = CommandCode(0x34)
	RPREPEND   = CommandCode(0x35)
	RPREPENDQ  = CommandCode(0x36)
	RDELETE    = CommandCode(0x37)
	RDELETEQ   = CommandCode(0x38)
	RINCR      = CommandCode(0x39)
	RINCRQ     = CommandCode(0x3a)
	RDECR      = CommandCode(0x3b)
	RDECRQ     = CommandCode(0x3c)

	SASL_LIST_MECHS = CommandCode(0x20)
	SASL_AUTH       = CommandCode(0x21)
	SASL_STEP       = CommandCode(0x22)

	TAP_CONNECT          = CommandCode(0x40) // Client-sent request to initiate Tap feed
	TAP_MUTATION         = CommandCode(0x41) // Notification of a SET/ADD/REPLACE/etc. on the server
	TAP_DELETE           = CommandCode(0x42) // Notification of a DELETE on the server
	TAP_FLUSH            = CommandCode(0x43) // Replicates a flush_all command
	TAP_OPAQUE           = CommandCode(0x44) // Opaque control data from the engine
	TAP_VBUCKET_SET      = CommandCode(0x45) // Sets state of vbucket in receiver (used in takeover)
	TAP_CHECKPOINT_START = CommandCode(0x46) // Notifies start of new checkpoint
	TAP_CHECKPOINT_END   = CommandCode(0x47) // Notifies end of checkpoint
	DCP_GET_SEQNO        = CommandCode(0x48) // Get sequence number for all vbuckets.

	DCP_OPEN           = CommandCode(0x50) // Open a DCP connection with a name
	DCP_ADDSTREAM      = CommandCode(0x51) // Sent by ebucketMigrator to DCP Consumer
	DCP_CLOSESTREAM    = CommandCode(0x52) // Sent by eBucketMigrator to DCP Consumer
	DCP_FAILOVERLOG    = CommandCode(0x54) // Request failover logs
	DCP_STREAMREQ      = CommandCode(0x53) // Stream request from consumer to producer
	DCP_STREAMEND      = CommandCode(0x55) // Sent by producer when it has no more messages to stream
	DCP_SNAPSHOT       = CommandCode(0x56) // Start of a new snapshot
	DCP_MUTATION       = CommandCode(0x57) // Key mutation
	DCP_DELETION       = CommandCode(0x58) // Key deletion
	DCP_EXPIRATION     = CommandCode(0x59) // Key expiration
	DCP_FLUSH          = CommandCode(0x5a) // Delete all the data for a vbucket
	DCP_NOOP           = CommandCode(0x5c) // DCP NOOP
	DCP_BUFFERACK      = CommandCode(0x5d) // DCP Buffer Acknowledgement
	DCP_CONTROL        = CommandCode(0x5e) // Set flow controlparams
	DCP_SYSTEM_EVENT   = CommandCode(0x5f) // DCP system events for collection lifecycle messages
	DCP_SEQNO_ADVANCED = CommandCode(0x64) // DCP event which indicates the change in seqno for a vbucket
	DCP_OSO_SNAPSHOT   = CommandCode(0x65) // DCP event which indicates the start/stop of OSO snapshot

	CREATE_RANGE_SCAN   = CommandCode(0xda) // Range scans
	CONTINUE_RANGE_SCAN = CommandCode(0xdb) // Range scans
	CANCEL_RANGE_SCAN   = CommandCode(0xdc) // Range scans

	SELECT_BUCKET = CommandCode(0x89) // Select bucket

	OBSERVE = CommandCode(0x92)
)

const FEATURE_COLLECTIONS byte = 0x12
const FEATURE_JSON byte = 0x0b
const FEATURE_XATTR byte = 0x06
const FEATURE_RANGE_SCAN_INCLUDE_XATTR byte = 0x22

type CollectionEvent uint32

const (
	COLLECTION_CREATE  = CollectionEvent(0x00) // Collection has been created
	COLLECTION_DROP    = CollectionEvent(0x01) // Collection has been dropped
	COLLECTION_FLUSH   = CollectionEvent(0x02) // Collection has been flushed
	SCOPE_CREATE       = CollectionEvent(0x03) // Scope has been created
	SCOPE_DROP         = CollectionEvent(0x04) // Scope has been dropped
	COLLECTION_CHANGED = CollectionEvent(0x05) // Collection has changed

	OSO_SNAPSHOT_START = CollectionEvent(0x06) // OSO snapshot start
	OSO_SNAPSHOT_END   = CollectionEvent(0x07) // OSO snapshot end
)

// Status field for memcached response.
type Status uint16

const (
	SUCCESS            = Status(0x00)
	KEY_ENOENT         = Status(0x01)
	KEY_EEXISTS        = Status(0x02)
	E2BIG              = Status(0x03)
	EINVAL             = Status(0x04)
	NOT_STORED         = Status(0x05)
	DELTA_BADVAL       = Status(0x06)
	NOT_MY_VBUCKET     = Status(0x07)
	ERANGE             = Status(0x22)
	ROLLBACK           = Status(0x23)
	UNKNOWN_COMMAND    = Status(0x81)
	ENOMEM             = Status(0x82)
	TMPFAIL            = Status(0x86)
	UNKNOWN_COLLECTION = Status(0x88)
	MANIFEST_AHEAD     = Status(0x8b)
	UNKNOWN_SCOPE      = Status(0x8c)

	RANGE_SCAN_MORE     = Status(0xa6)
	RANGE_SCAN_COMPLETE = Status(0xa7)
)

// MCItem is an internal representation of an item.
type MCItem struct {
	Cas               uint64
	Flags, Expiration uint32
	Data              []byte
}

// Number of bytes in a binary protocol header.
const HDR_LEN = 24

// Mapping of CommandCode -> name of command (not exhaustive)
var CommandNames map[CommandCode]string

// StatusNames human readable names for memcached response.
var StatusNames map[Status]string

// Human readable names for memcached collection events
var CollectionEventNames map[CollectionEvent]string

func init() {
	CommandNames = make(map[CommandCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"
	CommandNames[RGET] = "RGET"
	CommandNames[RSET] = "RSET"
	CommandNames[RSETQ] = "RSETQ"
	CommandNames[RAPPEND] = "RAPPEND"
	CommandNames[RAPPENDQ] = "RAPPENDQ"
	CommandNames[RPREPEND] = "RPREPEND"
	CommandNames[RPREPENDQ] = "RPREPENDQ"
	CommandNames[RDELETE] = "RDELETE"
	CommandNames[RDELETEQ] = "RDELETEQ"
	CommandNames[RINCR] = "RINCR"
	CommandNames[RINCRQ] = "RINCRQ"
	CommandNames[RDECR] = "RDECR"
	CommandNames[RDECRQ] = "RDECRQ"

	CommandNames[SASL_LIST_MECHS] = "SASL_LIST_MECHS"
	CommandNames[SASL_AUTH] = "SASL_AUTH"
	CommandNames[SASL_STEP] = "SASL_STEP"

	CommandNames[TAP_CONNECT] = "TAP_CONNECT"
	CommandNames[TAP_MUTATION] = "TAP_MUTATION"
	CommandNames[TAP_DELETE] = "TAP_DELETE"
	CommandNames[TAP_FLUSH] = "TAP_FLUSH"
	CommandNames[TAP_OPAQUE] = "TAP_OPAQUE"
	CommandNames[TAP_VBUCKET_SET] = "TAP_VBUCKET_SET"
	CommandNames[TAP_CHECKPOINT_START] = "TAP_CHECKPOINT_START"
	CommandNames[TAP_CHECKPOINT_END] = "TAP_CHECKPOINT_END"

	CommandNames[DCP_OPEN] = "DCP_OPEN"
	CommandNames[DCP_ADDSTREAM] = "DCP_ADDSTREAM"
	CommandNames[DCP_CLOSESTREAM] = "DCP_CLOSESTREAM"
	CommandNames[DCP_FAILOVERLOG] = "DCP_FAILOVERLOG"
	CommandNames[DCP_STREAMREQ] = "DCP_STREAMREQ"
	CommandNames[DCP_STREAMEND] = "DCP_STREAMEND"
	CommandNames[DCP_SNAPSHOT] = "DCP_SNAPSHOT"
	CommandNames[DCP_MUTATION] = "DCP_MUTATION"
	CommandNames[DCP_DELETION] = "DCP_DELETION"
	CommandNames[DCP_EXPIRATION] = "DCP_EXPIRATION"
	CommandNames[DCP_FLUSH] = "DCP_FLUSH"
	CommandNames[DCP_NOOP] = "DCP_NOOP"
	CommandNames[DCP_BUFFERACK] = "DCP_BUFFERACK"
	CommandNames[DCP_CONTROL] = "DCP_CONTROL"
	CommandNames[DCP_GET_SEQNO] = "DCP_GET_SEQNO"
	CommandNames[DCP_SYSTEM_EVENT] = "DCP_SYSTEM_EVENT"
	CommandNames[DCP_SEQNO_ADVANCED] = "DCP_SEQNO_ADVANCED"
	CommandNames[DCP_OSO_SNAPSHOT] = "DCP_OSO_SNAPSHOT"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "SUCCESS"
	StatusNames[KEY_ENOENT] = "KEY_ENOENT"
	StatusNames[KEY_EEXISTS] = "KEY_EEXISTS"
	StatusNames[E2BIG] = "E2BIG"
	StatusNames[EINVAL] = "EINVAL"
	StatusNames[NOT_STORED] = "NOT_STORED"
	StatusNames[DELTA_BADVAL] = "DELTA_BADVAL"
	StatusNames[NOT_MY_VBUCKET] = "NOT_MY_VBUCKET"
	StatusNames[UNKNOWN_COMMAND] = "UNKNOWN_COMMAND"
	StatusNames[ERANGE] = "ERANGE"
	StatusNames[ROLLBACK] = "ROLLBACK"
	StatusNames[ENOMEM] = "ENOMEM"
	StatusNames[TMPFAIL] = "TMPFAIL"
	StatusNames[UNKNOWN_COLLECTION] = "UNKNOWN_COLLECTION"
	StatusNames[MANIFEST_AHEAD] = "MANIFEST_AHEAD"
	StatusNames[UNKNOWN_SCOPE] = "UNKNOWN_SCOPE"

	CollectionEventNames = make(map[CollectionEvent]string)
	CollectionEventNames[COLLECTION_CREATE] = "COLLECTION_CREATE"
	CollectionEventNames[COLLECTION_DROP] = "COLLECTION_DROP"
	CollectionEventNames[COLLECTION_FLUSH] = "COLLECTION_FLUSH"
	CollectionEventNames[SCOPE_CREATE] = "SCOPE_CREATE"
	CollectionEventNames[SCOPE_DROP] = "SCOPE_DROP"
	CollectionEventNames[COLLECTION_CHANGED] = "COLLECTION_CHANGED"
	CollectionEventNames[OSO_SNAPSHOT_START] = "OSO_SNAPSHOT_START"
	CollectionEventNames[OSO_SNAPSHOT_END] = "OSO_SNAPSHOT_END"
}

// String an op code.
func (o CommandCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String an op code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}

// IsQuiet will return true if a command is a "quiet" command.
func (o CommandCode) IsQuiet() bool {
	switch o {
	case GETQ,
		GETKQ,
		SETQ,
		ADDQ,
		REPLACEQ,
		DELETEQ,
		INCREMENTQ,
		DECREMENTQ,
		QUITQ,
		FLUSHQ,
		APPENDQ,
		PREPENDQ,
		RSETQ,
		RAPPENDQ,
		RPREPENDQ,
		RDELETEQ,
		RINCRQ,
		RDECRQ:
		return true
	}
	return false
}
