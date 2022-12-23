package testcode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/couchbase/indexing/secondary/common"
)

const METAKV_TEST_PATH = "testcode_2ici_test"

type TestOptions struct {
	ActionOnNode string        `json:"actionOnNode,omitempty"`
	ActionAtTag  TestActionTag `json:"actionAtTag,omitempty"`
	Action       TestAction    `json:"testAction,omitempty"`

	// N1QL statement to be executed when the test hits the tag
	N1QLStatement string `json:"n1qlStatement,omitempty"`

	// Time (in milliseconds) to sleep when the test hits the tag
	SleepTime int `json:"sleepTime,omitempty"`
}

func MarshalTestOptions(clusterAddr string,
	actionAtTag TestActionTag, action TestAction,
	n1qlStatement string, sleepTime int) ([]byte, error) {

	req := &TestOptions{
		ActionOnNode:  clusterAddr,
		ActionAtTag:   actionAtTag,
		Action:        action,
		N1QLStatement: n1qlStatement,
		SleepTime:     sleepTime,
	}

	body, err := json.Marshal(req)
	if err != nil {
		log.Printf("MarshalTestOptions, error while marshaling request(%v, %v, %v, %v, %v), err: %v",
			clusterAddr, actionAtTag, action, n1qlStatement, sleepTime, err)
		return nil, err
	}
	return body, nil
}

func UnmarshalTestOptions(data []byte) (*TestOptions, error) {

	var opt TestOptions
	if err := json.Unmarshal(data, &opt); err != nil {
		log.Printf("MarshalTestOptions, error observed while unmarshaling data(%s), err: %v",
			data, err)
		return nil, err
	}

	return &opt, nil
}

func PostOptionsRequestToMetaKV(actionAtNode, username, password string,
	actionAtTag TestActionTag, action TestAction,
	n1qlStatement string, sleepTime int) error {

	option := &TestOptions{
		ActionOnNode:  actionAtNode,
		ActionAtTag:   actionAtTag,
		N1QLStatement: n1qlStatement,
		SleepTime:     sleepTime,
		Action:        action,
	}

	if err := common.MetakvSet(common.IndexingMetaDir+METAKV_TEST_PATH, option); err != nil {
		return errors.New(fmt.Sprintf("Fail to post options. Error = %v", err))
	}

	return nil
}

func ResetMetaKV() error {
	if err := common.MetakvDel(common.IndexingMetaDir + METAKV_TEST_PATH); err != nil {
		return errors.New(fmt.Sprintf("Fail to reset metaKV. Error = %v", err))
	}
	return nil
}
