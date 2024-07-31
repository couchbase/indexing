// example application to demonstrate use case of dataport library.

package dataport

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/logging"

	c "github.com/couchbase/indexing/secondary/common"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
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

	c.CollectionCreate:  "CollectionCreate",
	c.CollectionDrop:    "CollectionDrop",
	c.CollectionFlush:   "CollectionFlush",
	c.ScopeCreate:       "ScopeCreate",
	c.ScopeDrop:         "ScopeDrop",
	c.CollectionChanged: "CollectionChanged",

	c.UpdateSeqno:   "UpdateSeqno",
	c.SeqnoAdvanced: "UpdateSeqnoAdvanced",
}

// Application starts a new dataport application to receive mutations from the
// other end. Optionally it can print statistics and do callback for every
// mutation received.
func Application(
	addr string, // data port address to listen for connections
	stats int, // timeout to periodically display statistics
	timeout int, // timeout to break out of this function
	config c.Config,
	callb func(string, interface{}) bool, // callback for mutations, messages
) {

	logPrefix := fmt.Sprintf("[Application:%v]", addr)

	doCallb := func(arg interface{}) bool {
		if callb != nil {
			return callb(addr, arg)
		}
		return true
	}

	enableAuth := uint32(1)

	appch := make(chan interface{}, 10000)
	_, err := NewServer(addr, config, appch, &enableAuth, nil)
	if err != nil && doCallb(err) == false {
		return
	}

	// keyspaceId -> Command -> #count(int)
	keyspaceIdWise := make(map[string]map[byte]int)
	// instance-uuid -> key -> #count(int)
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
		case msg, ok := <-appch:
			if !ok {
				doCallb(nil)
				break loop
			}

			vbs, ok := msg.([]*protobuf.VbKeyVersions)
			if ok {
				mutations += processMutations(vbs, keyspaceIdWise, keys)
			} else {
				messages++
			}
			if doCallb(msg) == false {
				break loop
			}

		case <-tm:
			break loop

		case <-printTm:
			for _, keyspaceId := range sortedKeyspaceIds(keyspaceIdWise) {
				commandWise := keyspaceIdWise[keyspaceId]
				logging.Infof("%v %v, %v\n",
					logPrefix, keyspaceId, sprintCommandCount(commandWise))
			}
			for id := uint64(0); id < 100; id++ {
				if ks, ok := keys[id]; ok {
					logging.Infof("%v %v\n", logPrefix, sprintKeyCount(id, ks))
				}
			}
			logging.Infof("\n")
		}
	}
}

func processMutations(
	vbs []*protobuf.VbKeyVersions,
	keyspaceIdWise map[string]map[byte]int,
	keys map[uint64]map[string]int) int {

	mutations := 0
	for _, vb := range vbs {
		keyspaceId, kvs := vb.GetKeyspaceId(), vb.GetKvs()
		commandWise, ok := keyspaceIdWise[keyspaceId]
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

				if cmd == 0 || cmd == c.Snapshot || uuid == 0 || key == "" {
					continue
				}

				//m, ok := keys[uuid]
				//if !ok {
				//	m = make(map[string]int)
				//}
				//key = string(secJSON)
				//if _, ok := m[key]; !ok {
				//	m[key] = 0
				//}
				//m[key]++
				//keys[uuid] = m
			}
		}
		keyspaceIdWise[keyspaceId] = commandWise
	}
	return mutations
}

func sprintKeyCount(id uint64, keys map[string]int) string {
	countKs, countDs := 0, 0
	for _, n := range keys {
		countKs++
		countDs += n
	}
	l := fmt.Sprintf("instance %v, %v unique keys in %v docs", id, countKs, countDs)
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

func sortedKeyspaceIds(keyspaceIdWise map[string]map[byte]int) []string {
	keyspaceIds := make([]string, 0, len(keyspaceIdWise))
	for keyspaceId := range keyspaceIdWise {
		keyspaceIds = append(keyspaceIds, keyspaceId)
	}
	sort.Sort(sort.StringSlice(keyspaceIds))
	return keyspaceIds
}
