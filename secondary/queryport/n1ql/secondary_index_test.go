package n1ql

import (
	"testing"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
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

func TestClusterVersionGECheck(t *testing.T) {
	si := &secondaryIndex6{}
	if si.ClusterVersionGECheck(7, 0) {
		t.Fatalf("check passed before the cluster compat version was observed")
	}

	qclient.UpdateClusterCompat([]couchbase.Node{{ClusterCompatibility: 8*65536 + 5}})
	tests := []struct {
		major, minor uint32
		want         bool
	}{
		{7, 6, true},
		{8, 0, true},
		{8, 5, true},
		{8, 6, false},
		{9, 0, false},
	}
	for _, tt := range tests {
		if got := si.ClusterVersionGECheck(tt.major, tt.minor); got != tt.want {
			t.Errorf("ClusterVersionGECheck(%d, %d) on compat 8.5 = %v, want %v", tt.major, tt.minor, got, tt.want)
		}
	}
}
