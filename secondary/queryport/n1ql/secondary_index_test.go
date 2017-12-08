package n1ql

import (
	"testing"
)

func TestIndexConfig(t *testing.T) {

	conf, _ := GetIndexConfig()

	preconf := make(map[string]interface{})
	preconf[gConfigKeyTmpSpaceDir] = "/home"
	preconf[gConfigKeyTmpSpaceLimit] = int64(10000)

	conf.SetConfig(preconf)
	postconf := conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)

	preconf[gConfigKeyTmpSpaceLimit] = int64(100)
	conf.SetConfig(preconf)
	postconf = conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)

	preconf[gConfigKeyTmpSpaceLimit] = int64(500)
	conf.SetParam(gConfigKeyTmpSpaceLimit, int64(500))
	postconf = conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)

	preconf[gConfigKeyTmpSpaceDir] = "."
	conf.SetConfig(preconf)
	postconf = conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)

	preconf[gConfigKeyTmpSpaceDir] = "/tmp"
	conf.SetParam(gConfigKeyTmpSpaceDir, "/tmp")
	postconf = conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)

	preconf = nil
	conf.SetConfig(preconf)
	postconf = conf.(*indexConfig).getConfig()

	validateConf(t, preconf, postconf)
}

func validateConf(t *testing.T, preconf, postconf map[string]interface{}) {

	if len(preconf) != len(postconf) {
		t.Errorf("config mismatch %v %v", preconf, postconf)
	}

	if preconf == nil && postconf == nil {
		return
	}

	if preconf[gConfigKeyTmpSpaceDir] != postconf[gConfigKeyTmpSpaceDir] {
		t.Errorf("config mismatch %v %v", preconf, postconf)
	}

	if preconf[gConfigKeyTmpSpaceLimit] != postconf[gConfigKeyTmpSpaceLimit] {
		t.Errorf("config mismatch %v %v", preconf, postconf)
	}
}
