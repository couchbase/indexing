//go:build 2ici_test
// +build 2ici_test

package testcode

import "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/logging"
import "fmt"
import "time"
import "strings"
import "github.com/couchbase/indexing/secondary/security"

func TestActionAtTag(cfg common.Config, tag TestActionTag) {
	execTestAction := cfg["shardRebalance.execTestAction"].Bool()
	if !execTestAction {
		return
	}

	var option TestOptions
	// Read option from metaKV - The test is expected to persist the options
	// to metaKV before the start of the test
	_, err := common.MetakvGet(common.IndexingMetaDir+METAKV_TEST_PATH, &option)
	if err != nil {
		logging.Errorf("TestCode::TestActionAtTag: Error while retrieving options from metaKV, err: %v", err)
		return
	}

	clusterAddr := cfg["clusterAddr"].String()
	// For non-master nodes, take action only for the node specified in the action
	if !isMasterTag(option.ActionAtTag) {
		if option.ActionOnNode != clusterAddr {
			return
		}
	}

	logging.Infof("TestCode::TestActionAtTag: currTag: %v, actionAtTag: %v, actionOnNode: %v, currNode: %v, action: %v",
		tag, option.ActionAtTag, option.ActionOnNode, clusterAddr, option.Action)

	if tag == option.ActionAtTag {
		switch option.Action {
		case INDEXER_PANIC:
			panic(fmt.Errorf("TestCode::TestActionAtTag - Inducing artificial panic at tag: %v as wished", tag))
		case REBALANCE_CANCEL:
			resp, err := security.PostWithAuth(clusterAddr+"/controller/stopRebalance", "application/json", strings.NewReader(""), nil)
			if err != nil {
				logging.Errorf("TestCode::TestActionAtTag - Error observed while posting cancel message, err: %v", err)
			}
			if resp != nil {
				defer resp.Body.Close()
			}
			time.Sleep(5 * time.Second)
		case SLEEP:
			logging.Infof("TestCode::TestActionAtTag: Sleeping for %v milliseconds as wished", option.SleepTime)
			time.Sleep(time.Duration(option.SleepTime) * time.Millisecond)
			logging.Infof("TestCode::TestActionAtTag: Woke-up from sleep")

		case EXEC_N1QL_STATEMENT:
			// TODO: Add support for n1ql statement execution
		}
	}

	// No-op for other states
	return
}
