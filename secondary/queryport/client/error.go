package client

import "errors"
import "fmt"

// ErrorProtocol
var ErrorProtocol = errors.New("queryport.client.protocol")

// ErrorNoHost
var ErrorNoHost = errors.New("queryport.client.noHost")

// ErrorIndexNotFound
var ErrorIndexNotFound = errors.New("queryport.indexNotFound")

// ErrorInstanceNotFound
var ErrorInstanceNotFound = errors.New("queryport.instanceNotFound")

// ErrorClientUninitialized
var ErrorClientUninitialized = errors.New("queryport.clientUninitialized")

// ErrorNotImplemented
var ErrorNotImplemented = errors.New("queryport.notImplemented")

// ErrorInvalidConsistency
var ErrorInvalidConsistency = errors.New("queryport.invalidConsistency")

// ErrorExpectedTimestamp
var ErrorExpectedTimestamp = errors.New("queryport.expectedTimestamp")

// These error strings need to be in sync with common.ErrIndexNotFound
// and common.ErrIndexNotReady.
var ErrIndexNotFound = fmt.Errorf("Index not found")
var ErrIndexNotReady = fmt.Errorf("Index not ready for serving queries")

var errorDescriptions = map[string]string{
	ErrorProtocol.Error():            "fatal protocol error with server",
	ErrorNoHost.Error():              "All indexer replica is down or unavailable or unable to process request",
	ErrorIndexNotFound.Error():       "index deleted or node hosting the index is down",
	ErrorInstanceNotFound.Error():    "no instance available for the index",
	ErrorClientUninitialized.Error(): "gsi client is not initialized",
	ErrorNotImplemented.Error():      "client API not implemented",
	ErrorInvalidConsistency.Error():  "supplied consistency is invalid",
	ErrorExpectedTimestamp.Error():   "consistency timestamp is expected",
	ErrIndexNotFound.Error():         "index is deleted or node hosting index is down",
	ErrIndexNotReady.Error():         ErrIndexNotReady.Error(),
}
