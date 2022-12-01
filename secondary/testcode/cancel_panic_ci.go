//go:build 2ici_test
// +build 2ici_test

package testcode

import "github.com/couchbase/indexing/secondary/common"
import "strings"
import "fmt"
import "github.com/couchbase/indexing/secondary/logging"

func CancelOrPanicAtTag(cfg common.Config, tag string, cancel chan struct{}) {
	configTag := cfg["shardRebalance.cancelOrPanicTag"].String()
	clusterAddr := cfg["clusterAddr"].String()
	cancelOrPanicNode := cfg["shardRebalance.cancelOrPanicNode"].String()
	cancelOrPanic := cfg["shardRebalance.cancelOrPanic"].String()

	if cancelOrPanic == "" {
		return
	}

	// Process clusterAddr for tags with no "master" string in them as
	// there will only be one master during rebalance
	if !strings.Contains(strings.ToLower(tag), "master") {
		if cancelOrPanicNode != clusterAddr {
			return
		}
	}

	logging.Infof("TestCode::CancelOrPanicAtTag: configTag: %v, expectedTag: %v, clusterAddr: %v, cancelOrPanicNode: %v, cancelOrPanic: %v",
		configTag, tag, clusterAddr, cancelOrPanicNode, cancelOrPanic)

	if strings.ToLower(tag) == strings.ToLower(configTag) {
		if cancelOrPanic == "cancel" {
			close(cancel)
		} else if cancelOrPanic == "panic" {
			panic(fmt.Errorf("CancelOrPanic - Erring out as tranfser token is at state: %v", tag))
		}
	}
	// No-op for other states
	return
}
