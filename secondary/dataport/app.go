// example application to demonstrate use case of dataport library.

package dataport

import (
	"fmt"
	"sort"
	"strings"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

var commandNames = map[byte]string{
	c.Upsert:         "Upsert",
	c.Deletion:       "Deletion",
	c.UpsertDeletion: "UpsertDeletion",
	c.Sync:           "Sync",
	c.DropData:       "DropData",
	c.StreamBegin:    "StreamBegin",
	c.StreamEnd:      "StreamEnd",
	c.Snapshot:       "Snapshot",
}

func Application(
	addr string, // data port address to listen for connections
	stats int, // timeout to periodically display statistics
	timeout int, // timeout to break out of this function
	callb func(string, interface{}) bool, // callback for mutations, messages
) {

	logPrefix := fmt.Sprintf("[Application:%v]", addr)

	doCallb := func(arg interface{}) bool {
		if callb != nil {
			return callb(addr, arg)
		}
		return true
	}

	mutch := make(chan []*protobuf.VbKeyVersions, c.MutationChannelSize)
	sbch := make(chan interface{}, 100)
	_, err := NewServer(addr, mutch, sbch)
	if err != nil && doCallb(err) == false {
		return
	}

	// bucket -> Command -> #int
	bucketWise := make(map[string]map[byte]int)
	keys := make(map[uint64]map[string]int)
	mutations, messages := 0, 0

	var tm, printTm <-chan time.Time
	if timeout > 0 {
		tm = time.Tick(time.Duration(timeout) * time.Millisecond)
	}
	if stats > 0 {
		printTm = time.Tick(time.Duration(stats) * time.Millisecond)
	}

loop:
	for {
		select {
		case vbs, ok := <-mutch:
			if ok {
				mutations += processMutations(vbs, bucketWise, keys)
				if doCallb(vbs) == false {
					break loop
				}
			} else {
				doCallb(nil)
				break loop
			}

		case sb, ok := <-sbch:
			if ok {
				messages++
				if doCallb(sb) == false {
					break loop
				}
			} else {
				doCallb(nil)
				break loop
			}

		case <-tm:
			break loop

		case <-printTm:
			c.Infof("%v received %v mutations and %v msgs\n",
				logPrefix, mutations, messages)
			for _, bucket := range sortedBuckets(bucketWise) {
				commandWise := bucketWise[bucket]
				c.Infof("%v %v, %v\n",
					logPrefix, bucket, sprintCommandCount(commandWise))
			}
			for id := uint64(0); id < 100; id++ {
				if ks, ok := keys[id]; ok {
					c.Infof("%v %v\n", logPrefix, sprintKeyCount(id, ks))
				}
			}
			c.Infof("\n")
		}
	}
}

func processMutations(
	vbs []*protobuf.VbKeyVersions,
	bucketWise map[string]map[byte]int,
	keys map[uint64]map[string]int) int {

	mutations := 0
	for _, vb := range vbs {
		bucket, kvs := vb.GetBucketname(), vb.GetKvs()
		commandWise, ok := bucketWise[bucket]
		if !ok {
			commandWise = make(map[byte]int)
		}

		for _, kv := range kvs {
			mutations++
			uuids, seckeys := kv.GetUuids(), kv.GetKeys()
			for i, command := range kv.GetCommands() {
				cmd, uuid, key := byte(command), uuids[i], string(seckeys[i])
				if _, ok := commandWise[cmd]; !ok {
					commandWise[cmd] = 0
				}
				commandWise[cmd]++

				if command == 0 || uuid == 0 {
					continue
				}

				m, ok := keys[uuid]
				if !ok {
					m = make(map[string]int)
				}
				if _, ok := m[key]; !ok {
					m[key] = 0
				}
				m[key]++
				keys[uuid] = m
			}
		}
		bucketWise[bucket] = commandWise
	}
	return mutations
}

func sprintKeyCount(id uint64, keys map[string]int) string {
	countKs, countDs := 0, 0
	for _, n := range keys {
		countKs++
		countDs += n
	}
	l := fmt.Sprintf("ii %v, %v unique keys in %v docs", id, countKs, countDs)
	return l
}

func sprintCommandCount(commandWise map[byte]int) string {
	line := ""
	for cmd := byte(0); cmd < 100; cmd++ {
		if n, ok := commandWise[cmd]; ok {
			line += fmt.Sprintf("%v: %v ", commandNames[cmd], n)
		}
	}
	return strings.TrimRight(line, " ")
}

func sortedBuckets(bucketWise map[string]map[byte]int) []string {
	buckets := make([]string, 0, len(bucketWise))
	for bucket, _ := range bucketWise {
		buckets = append(buckets, bucket)
	}
	sort.Sort(sort.StringSlice(buckets))
	return buckets
}
