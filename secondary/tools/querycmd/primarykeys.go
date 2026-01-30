//go:build nolint

package main

import (
	"fmt"
	"log"

	c "github.com/couchbase/indexing/secondary/common"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

// expects `beer-sample` bucket to be created with primary index created.
func doCurePrimary(cluster string, client *qclient.GsiClient) error {
	doCuredRange(client, uint64(4110428477401750188),
		nil, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{true}, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{false}, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{10}, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{"0"}, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{[]interface{}{10, 20}}, nil)
	doCuredRange(client, uint64(4110428477401750188),
		c.SecondaryKey{map[string]interface{}{"10": 20}}, nil)

	fmt.Println("----")

	doCuredRange(client, uint64(4110428477401750188),
		nil, nil)
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{true})
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{false})
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{10})
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{"0"})
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{[]interface{}{10, 20}})
	doCuredRange(client, uint64(4110428477401750188),
		nil, c.SecondaryKey{map[string]interface{}{"10": 20}})
	return nil
}

func doCuredRange(
	client *qclient.GsiClient, defnID uint64, low, high c.SecondaryKey) {

	fmt.Printf("for %v low : {%T,%v}, high: {%T,%v}\n", low, low, high, high)
	var l, h []byte
	var count int
	count, l, h = 0, nil, nil
	var scanParams = map[string]interface{}{"skipReadMetering": true, "user": ""}
	err := client.Range(
		defnID, "1", low, high, 3, false, 10000000,
		c.AnyConsistency, nil,
		func(r qclient.ResponseReader) bool {
			_, pkeys, err := r.GetEntries()
			if err != nil {
				log.Fatal(err)
			}
			ln := len(pkeys)
			count += ln
			if ln > 0 {
				if l == nil {
					l = pkeys[0]
				}
				h = pkeys[ln-1]
			}
			return true
		}, scanParams)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  count: %v low : %q, high: %q\n", count, string(l), string(h))
}
