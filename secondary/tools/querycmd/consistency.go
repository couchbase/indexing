package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

var docid = "docCons"
var constDocValue1 = map[string]interface{}{"city": "bangalore"}
var constDocValue2 = map[string]interface{}{"city": "bombay"}
var constDocValue3 = map[string]interface{}{"city": "delhi"}
var constDocValue4 = map[string]interface{}{"city": "kolkata"}
var constDocValue5 = map[string]interface{}{"city": "madras"}

var constEqualLookup1 = []byte(`["bangalore"]`)
var constEqualLookup2 = []byte(`["bombay"]`)
var constEqualLookup3 = []byte(`["delhi"]`)
var constEqualLookup4 = []byte(`["kolkata"]`)
var constEqualLookup5 = []byte(`["madras"]`)

var scanParams = map[string]interface{}{"skipReadMetering": false, "user": ""}

func doConsistency(
	cluster string, maxvb int, client *qclient.GsiClient) (err error) {

	b, err := common.ConnectBucket(cluster, "default", "beer-sample")
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	vbnos := make([]uint16, maxvb)
	for i := range vbnos {
		vbnos[i] = uint16(i)
	}

	// Drop index
	args := []string{
		"-type", "drop", "-bucket", "beer-sample", "-index", "index-city",
	}
	cmd, _, _, _ := querycmd.ParseArgs(args)
	querycmd.HandleCommand(client, cmd, true, os.Stdout)

	// Create index
	args = []string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-city",
		"-fields", "city",
	}
	cmd, _, _, err = querycmd.ParseArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	querycmd.HandleCommand(client, cmd, true, os.Stdout)

	// Wait for index to come active.
	index, ok := querycmd.GetIndex(client, "beer-sample", "index-city")
	if !ok {
		log.Fatalf("cannot get definition ID")
	}
	defnID := uint64(index.Definition.DefnId)
	_, err = querycmd.WaitUntilIndexState(
		client, []uint64{defnID}, common.INDEX_STATE_ACTIVE,
		100 /*period*/, 20000 /*timeout*/)

	synch := make(chan bool, 1)
	// Get the latest seqnos,vbuuid and vbucket that contains `docid`.
	seqnos, vbuuids, vbno, vbuuid, seqno := setValueConst(b, maxvb, constDocValue1)
	equal := common.SecondaryKey(querycmd.Arg2Key(constEqualLookup1))
	equals := []common.SecondaryKey{equal}

	anyConsistency(client, defnID, equals)

	// query-consistency without any new mutations.
	ts := qclient.NewTsConsistency(vbnos, seqnos, vbuuids)
	queryConsistency(client, defnID, ts, equals, synch)
	<-synch

	// query-consistency with a new mutation.
	equal = common.SecondaryKey(querycmd.Arg2Key(constEqualLookup2))
	equals = []common.SecondaryKey{equal}
	seqno++
	ts = ts.Override(vbno, seqno, vbuuid)
	queryConsistency(client, defnID, ts, equals, synch)
	time.Sleep(2 * time.Second)
	setValueConst(b, maxvb, constDocValue2)
	<-synch

	// query-consistency with a new mutation.
	equal = common.SecondaryKey(querycmd.Arg2Key(constEqualLookup3))
	equals = []common.SecondaryKey{equal}
	seqno++
	ts = qclient.NewTsConsistency([]uint16{vbno}, []uint64{seqno}, []uint64{vbuuid})
	queryConsistency(client, defnID, ts, equals, synch)
	time.Sleep(2 * time.Second)
	setValueConst(b, maxvb, constDocValue3)
	<-synch

	// session-consistency without any new mutations.
	sessionConsistency(client, defnID, equals, synch)
	<-synch

	// session-consistency with a new mutation.
	setValueConst(b, maxvb, constDocValue4)
	equal = common.SecondaryKey(querycmd.Arg2Key(constEqualLookup4))
	equals = []common.SecondaryKey{equal}
	sessionConsistency(client, defnID, equals, synch)
	<-synch

	// session-consistency with a new mutation.
	equal = common.SecondaryKey(querycmd.Arg2Key(constEqualLookup5))
	equals = []common.SecondaryKey{equal}
	setValueConst(b, maxvb, constDocValue5)
	sessionConsistency(client, defnID, equals, synch)
	<-synch
	return nil
}

func queryConsistency(
	client *qclient.GsiClient,
	defnID uint64,
	ts *qclient.TsConsistency,
	equals []common.SecondaryKey, synch chan bool) {

	fmt.Println("Scan: QueryConsistency ...")
	go func() {
		client.Lookup(
			uint64(defnID), "requestId", equals, false, 10, common.QueryConsistency, ts,
			func(res qclient.ResponseReader) bool {
				if res.Error() != nil {
					log.Fatalf("Error: %v", res)
				} else if skeys, pkeys, err := res.GetEntries(); err != nil {
					log.Fatalf("Error: %v", err)
				} else {
					for i, pkey := range pkeys {
						fmt.Printf("    %v ... %v\n", skeys[i], string(pkey))
					}
				}
				return true
			}, scanParams)
		fmt.Println("Scan: QueryConsistency ... ok\n")
		synch <- true
	}()
}

func sessionConsistency(
	client *qclient.GsiClient,
	defnID uint64,
	equals []common.SecondaryKey, synch chan bool) {

	go func() {
		fmt.Println("Scan: SessionConsistency ...")
		client.Lookup(
			uint64(defnID), "requestId", equals, false, 10, common.SessionConsistency, nil,
			func(res qclient.ResponseReader) bool {
				if res.Error() != nil {
					log.Fatalf("Error: %v", res)
				} else if skeys, pkeys, err := res.GetEntries(); err != nil {
					log.Fatalf("Error: %v", err)
				} else {
					for i, pkey := range pkeys {
						fmt.Printf("    %v ... %v\n", skeys[i], string(pkey))
					}
				}
				return true
			}, scanParams)
		fmt.Println("Scan: SessionConsistency ... ok\n")
		synch <- true
	}()
}

func anyConsistency(
	client *qclient.GsiClient, defnID uint64, equals []common.SecondaryKey) {

	// Scan with AnyConsistency
	fmt.Println("Scan: AnyConsistency ...")
	client.Lookup(
		uint64(defnID), "requestId", equals, false, 10, common.AnyConsistency, nil,
		func(res qclient.ResponseReader) bool {
			if res.Error() != nil {
				log.Fatalf("Error: %v", res)
			} else if skeys, pkeys, err := res.GetEntries(); err != nil {
				log.Fatalf("Error: %v", err)
			} else {
				for i, pkey := range pkeys {
					fmt.Printf("    %v ... %v\n", skeys[i], string(pkey))
				}
			}
			return true
		}, scanParams)
	fmt.Println("Scan: AnyConsistency ... ok\n")
}

func diffSeqno(seqnos1, seqnos2, vbuuids2 []uint64) (uint16, uint64, uint64) {
	for i, seqno1 := range seqnos1 {
		if seqnos2[i] == (seqno1 + 1) {
			return uint16(i), vbuuids2[i], seqnos2[i]
		}
	}
	return 0, 0, 0
}

func setValueConst(
	b *couchbase.Bucket,
	maxvb int, value map[string]interface{}) ([]uint64, []uint64, uint16, uint64, uint64) {

	seqnos1, _, err := common.BucketTs(b, maxvb)
	if err != nil {
		log.Fatal(err)
	}
	// identify vbucket for `docid`
	err = b.Set(docid, 0, value)
	if err != nil {
		log.Fatal(err)
	}
	seqnos2, vbuuids2, err := common.BucketTs(b, maxvb)
	if err != nil {
		log.Fatal(err)
	}
	vbno, vbuuid, seqno := diffSeqno(seqnos1, seqnos2, vbuuids2)
	fmt.Printf(
		"Updated %v on vbucket: %v(%v) seqno: %v\n",
		docid, vbno, vbuuid, seqno)
	return seqnos2, vbuuids2, vbno, vbuuid, seqno
}
