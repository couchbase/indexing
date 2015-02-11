package main

import "fmt"
import "log"
import "os"
import "time"
import "strconv"

import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/querycmd"
import "github.com/couchbase/indexing/secondary/common"

var docid = "docCons"
var jsonVal1 = map[string]interface{}{"city": "bangalore"}
var jsonVal2 = `{"city": "delhi"}`
var jsonVal3 = `{"city": "mumbai"}`

func doConsistency(cluster string, client *qclient.GsiClient) (err error) {
	b, err := common.ConnectBucket(cluster, "default", "default")
	if err != nil {
		log.Fatal(err)
	}

	seqnos1, vbuuids1 := getSeqnos(b.GetStats("vbucket-seqno"))
	// identify vbucket for `docid`
	err = b.Set(docid, 0, jsonVal1)
	if err != nil {
		log.Fatal(err)
	}
	seqnos2, vbuuids2 := getSeqnos(b.GetStats("vbucket-seqno"))
	vbno, vbuuid, seqno := diffSeqno(seqnos1, seqnos2, vbuuids2)
	fmt.Printf(
		"Updated %v on vbucket: %v(%v) seqno: %v\n",
		docid, vbno, vbuuid, seqno)

	// Drop index
	args := []string{"-type", "drop", "-indexes", "default:index-city"}
	cmd, _, _, _ := querycmd.ParseArgs(args)
	querycmd.HandleCommand(client, cmd, true, os.Stdout)
	// Create index
	args = []string{
		"-type", "create", "-bucket", "default", "-index", "index-city",
		"-fields", "city",
	}
	cmd, _, _, err = querycmd.ParseArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	err = querycmd.HandleCommand(client, cmd, true, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	// Wait for index to come active.
	defnID, ok := querycmd.GetDefnID(client, "default", "index-city")
	if !ok {
		log.Fatalf("cannot get definition ID")
	}
	_, err = querycmd.WaitUntilIndexState(
		client, []uint64{defnID}, common.INDEX_STATE_ACTIVE,
		100 /*period*/, 20000 /*timeout*/)
	// Scan with AnyConsistency
	equal := common.SecondaryKey(querycmd.Arg2Key([]byte(`["bangalore"]`)))
	equals := []common.SecondaryKey{equal}
	fmt.Println("Scan: AnyConsistency ...")
	client.Lookup(
		uint64(defnID), equals, false, 10, common.AnyConsistency, nil,
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
		})
	fmt.Println("Scan: AnyConsistency ... ok\n")

	// Scan with QueryConsistency, with full timestamp, no mutations.
	ts := newTimestamp(seqnos1, vbuuids1)
	ts = ts.Override(vbno, seqno, vbuuid)
	fmt.Println("Scan: QueryConsistency (full timestamp, no mutations) ...")
	client.Lookup(
		uint64(defnID), equals, false, 10, common.QueryConsistency, ts,
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
		})
	fmt.Println("Scan: QueryConsistency (full timestamp, no mutations) ... ok\n")
	// Scan with QueryConsistency, with full timestamp, wait for mutations.
	ts = newTimestamp(seqnos2, vbuuids2)
	ts = ts.Override(vbno, seqno+1, vbuuid)
	synch := make(chan bool, 1)
	fmt.Printf(
		"Scan: QueryConsistency (wait on %v %v %v) ...\n",
		vbno, seqno+1, vbuuid)
	go func() {
		client.Lookup(
			uint64(defnID), equals, false, 10, common.QueryConsistency, ts,
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
			})
		fmt.Printf(
			"Scan: QueryConsistency (wait on %v %v %v) ... ok\n\n",
			vbno, seqno+1, vbuuid)
		synch <- true
	}()
	fmt.Println("Sleeping for 2 seconds...")
	time.Sleep(2 * time.Second)
	err = b.Set(docid, 0, jsonVal1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Mutation posted to {%v %v %v}\n", vbno, seqno+1, vbuuid)
	seqnos3, vbuuids3 := getSeqnos(b.GetStats("vbucket-seqno"))
	<-synch

	// Scan with QueryConsistency, with partial timestamp, wait for mutations.
	ts = newTimestamp(seqnos3, vbuuids3)
	ts = ts.Override(vbno, seqno+2, vbuuid)
	fmt.Printf(
		"Scan: QueryConsistency (wait on %v %v %v) ...\n",
		vbno, seqno+2, vbuuid)
	go func() {
		client.Lookup(
			uint64(defnID), equals, false, 10, common.QueryConsistency, ts,
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
			})
		fmt.Printf(
			"Scan: QueryConsistency (wait on %v %v %v) ... ok\n\n",
			vbno, seqno+2, vbuuid)
		synch <- true
	}()
	fmt.Println("Sleeping for 2 seconds...")
	time.Sleep(2 * time.Second)
	err = b.Set(docid, 0, jsonVal1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Mutation posted to {%v %v %v}\n", vbno, seqno+2, vbuuid)
	<-synch

	return nil
}

func getSeqnos(stats map[string]map[string]string) ([]uint64, []uint64) {
	seqnos := make([]uint64, 1024)
	vbuuids := make([]uint64, 1024)
	//for all nodes in cluster
	for _, nodestat := range stats {
		//for all vbuckets
		for i := 0; i < 1024; i++ {
			vbkey := "vb_" + strconv.Itoa(i) + ":high_seqno"
			if highseqno, ok := nodestat[vbkey]; ok {
				if s, err := strconv.Atoi(highseqno); err == nil {
					seqnos[i] = uint64(s)
				}
			}
			vbkey = "vb_" + strconv.Itoa(i) + ":uuid"
			if vbuuid, ok := nodestat[vbkey]; ok {
				if uuid, err := strconv.Atoi(vbuuid); err == nil {
					vbuuids[i] = uint64(uuid)
				}
			}
		}
	}
	return seqnos, vbuuids
}

func diffSeqno(seqnos1, seqnos2, vbuuids2 []uint64) (uint16, uint64, uint64) {

	for i, seqno1 := range seqnos1 {
		if seqnos2[i] == (seqno1 + 1) {
			return uint16(i), vbuuids2[i], seqnos2[i]
		}
	}
	return 0, 0, 0
}

func newTimestamp(seqnos, vbuuids []uint64) *qclient.TsConsistency {
	vbnos := make([]uint16, 0)
	for i, vbuuid := range vbuuids {
		if vbuuid == 0 {
			break
		}
		vbnos = append(vbnos, uint16(i))
	}
	n := len(vbnos)
	return qclient.NewTsConsistency(vbnos, seqnos[:n], vbuuids[:n])
}
