package cbauthutil

import (
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

func RegisterConfigRefreshCallback() error {
	// Register config refresh callback
	err := cbauth.RegisterConfigRefreshCallback(ConfigRefreshCallback)
	if err != nil {
		return err
	}
	return nil
}

func ConfigRefreshCallback(code uint64) error {
	logging.Infof("ConfigRefreshCallback: Received notificaiton with code: %v", code)

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 ||
		code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		return security.SecurityConfigRefresh(code)
	}

	return nil
}
