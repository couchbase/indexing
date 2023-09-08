//go:build community
// +build community

package indexer

import c "github.com/couchbase/indexing/secondary/common"

func setCopierTestConfigIfEnabled(config c.Config) (string, bool) {
	return "", false
}

func unsetCopierTestConfigIfEnabled(config c.Config) (string, bool) {
	return "", false
}

func MakeFileCopier(location, region, endpoint, staging string, ratelimit int, cfg c.Config) (Copier, error) {
	return nil, nil
}

func MakeFileCopierForPauseResume(task *taskObj, cfg c.Config) (Copier, error) {
	return nil, nil
}
