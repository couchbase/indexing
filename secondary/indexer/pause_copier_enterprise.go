//go:build !community
// +build !community

package indexer

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/plasma"
)

func generatePlasmaCopierConfigForPauseResume(task *taskObj, config c.Config) *plasma.Config {
	cfg := plasma.DefaultConfig()
	cfg.CopyConfig.KeyEncoding = true
	cfg.CopyConfig.KeyPrefix = task.archivePath
	cfg.CopyConfig.Region = task.region
	cfg.CopyConfig.EndPoint = config["pause_resume.blob_storage_endpoint"].String()
	return &cfg
}

func generatePlasmaCopierConfig(prefix, region, endpoint string) *plasma.Config {
	cfg := plasma.DefaultConfig()
	cfg.CopyConfig.KeyEncoding = true
	cfg.CopyConfig.KeyPrefix = prefix
	cfg.CopyConfig.Region = region
	cfg.CopyConfig.EndPoint = endpoint

	return &cfg
}

func setCopierTestConfigIfEnabled(config c.Config) (string, bool) {
	if endpoint, ok := config["pause_resume.blob_storage_endpoint"]; ok {
		cfg := plasma.DefaultConfig()
		cfg.EndPoint = endpoint.String()
		plasma.UpdateShardCopyConfig(&cfg)

		return cfg.EndPoint, true
	}

	return "", false
}

func unsetCopierTestConfigIfEnabled(config c.Config) (string, bool) {
	if endpoint, ok := config["pause_resume.blob_storage_endpoint"]; ok {
		cfg := plasma.DefaultConfig()
		plasma.UpdateShardCopyConfig(&cfg)

		return endpoint.String(), true
	}

	return "", false
}

func MakeFileCopier(location, region, endpoint, staging string, ratelimit int, cfg c.Config) (Copier, error) {
	copyCfg := generatePlasmaCopierConfig(location, region, endpoint)
	return plasma.MakeFileCopier(location, staging, nil, plasma.GetOpRateLimiter("pause"), copyCfg.CopyConfig)
}

func MakeFileCopierForPauseResume(task *taskObj, cfg c.Config) (Copier, error) {
	copyCfg := generatePlasmaCopierConfigForPauseResume(task, cfg)
	return plasma.MakeFileCopier(task.archivePath, "", nil, plasma.GetOpRateLimiter("pause"), copyCfg.CopyConfig)
}
