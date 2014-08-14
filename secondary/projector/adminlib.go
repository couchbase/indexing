package projector

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"log"
	"strconv"
	"strings"

	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/go-couchbase"
)

func GetVbmap(
	client ap.Client,
	pooln, bucketn string,
	kvaddrs []string) (*protobuf.VbmapResponse, error) {

	req := &protobuf.VbmapRequest{
		Pool:    proto.String(pooln),
		Bucket:  proto.String(bucketn),
		Kvaddrs: kvaddrs,
	}
	res := &protobuf.VbmapResponse{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	}
	return res, nil
}

func GetFailoverLogs(
	client ap.Client, pooln, bucketn string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	req := &protobuf.FailoverLogRequest{
		Pool:   proto.String(pooln),
		Bucket: proto.String(bucketn),
		Vbnos:  vbnos,
	}
	res := &protobuf.FailoverLogResponse{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	}
	return res, nil
}

func InitialMutationStream(
	client ap.Client,
	topic, pooln string,
	buckets, kvaddrs []string,
	tss map[string]*c.Timestamp,
	instances []*protobuf.IndexInst) (*protobuf.MutationStreamResponse, error) {

	var ts *c.Timestamp

	req := &protobuf.MutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.BranchTimestamp{},
		Instances:         instances,
	}
	for _, bucketn := range buckets {
		vbmap, err := GetVbmap(client, pooln, bucketn, kvaddrs)
		if err != nil {
			return nil, err
		}
		vbnos := vbmap.Vbuckets32()

		flogs, err := GetFailoverLogs(client, pooln, bucketn, vbnos)
		if err != nil {
			return nil, err
		}

		if tss == nil {
			ts = c.NewTimestamp(bucketn, c.MaxVbuckets)
			for _, vbno := range vbnos {
				ts.Append(uint16(vbno), 0, 0, 0, 0)
			}
		} else {
			ts = tss[bucketn]
		}

		ts = computeRestartTs(flogs.FailoverLogs(ts.Vbnos), ts)
		bTs := protobuf.ToBranchTimestamp(ts)

		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucketn)
		req.RestartTimestamps = append(req.RestartTimestamps, bTs)
	}
	req.SetStartFlag()
	res := &protobuf.MutationStreamResponse{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	} else if err := res.GetErr(); err != nil {
		return nil, fmt.Errorf(err.GetError())
	}
	return res, nil
}

func RestartMutationStream(
	client ap.Client,
	topic, pooln string, tss map[string]*c.Timestamp,
	instances []*protobuf.IndexInst,
	callb func(*protobuf.MutationStreamResponse, error) bool) {

	req := &protobuf.UpdateMutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.BranchTimestamp{},
		Instances:         instances,
	}
	for bucketn, ts := range tss {
		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucketn)
		bTs := protobuf.ToBranchTimestamp(ts)
		req.RestartTimestamps = append(req.RestartTimestamps, bTs)
	}
	req.SetRestartFlag()
	res := &protobuf.MutationStreamResponse{}
	if err := client.Request(req, res); err != nil {
		callb(nil, err)
	} else {
		callb(res, nil)
	}
}

func RepairEndpoints(
	client ap.Client,
	topic string, endpoints []string) (*protobuf.Error, error) {

	req := &protobuf.RepairDownstreamEndpoints{
		Topic: proto.String(topic), Endpoints: endpoints}
	res := &protobuf.Error{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	} else if err := res.GetError(); err != "" {
		return nil, fmt.Errorf(err)
	}
	return res, nil
}

func ShutdownStream(client ap.Client, topic string) (*protobuf.Error, error) {
	req := &protobuf.ShutdownStreamRequest{Topic: proto.String(topic)}
	res := &protobuf.Error{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	} else if err := res.GetError(); err != "" {
		return nil, fmt.Errorf(err)
	}
	return res, nil
}

func SpawnProjectors(
	cluster, pooln string, buckets []string,
	projectors map[string]ap.Client) map[string]ap.Client {

	var b *couchbase.Bucket
	var err error

	for _, bucketn := range buckets {
		if b, err = c.ConnectBucket(cluster, pooln, bucketn); err != nil {
			log.Fatal(err)
		} else {
			break
		}
	}

	b.Refresh()
	m, err := b.GetVBmap(nil)
	if err != nil {
		log.Fatal(err)
	}

	newkvaddrs := make(map[string]ap.Client)
	for kvaddr := range m { // create a projector instance for each kvnode
		if _, ok := projectors[kvaddr]; ok {
			continue
		}
		ss := strings.Split(kvaddr, ":")
		kport, err := strconv.Atoi(ss[1])
		if err != nil {
			log.Fatal(err)
		}
		adminport := ss[0] + ":" + strconv.Itoa(kport+500)
		NewProjector(cluster, []string{kvaddr}, adminport)
		c := ap.NewHTTPClient("http://"+adminport, c.AdminportURLPrefix)
		projectors[kvaddr] = c
		newkvaddrs[kvaddr] = c
	}
	return newkvaddrs
}

func ShutdownProjectors(
	cluster, pooln string, buckets []string,
	projectors map[string]ap.Client) map[string]ap.Client {

	var b *couchbase.Bucket
	var err error

	for _, bucketn := range buckets {
		if b, err = c.ConnectBucket(cluster, pooln, bucketn); err != nil {
			log.Fatal(err)
		} else {
			break
		}
	}

	b.Refresh()
	m, err := b.GetVBmap(nil)
	if err != nil {
		log.Fatal(err)
	}

	newProjectors := make(map[string]ap.Client)
	for kvaddr, c := range projectors {
		if vbnos, ok := m[kvaddr]; !ok || (vbnos != nil && len(vbnos) == 0) {
			ShutdownStream(c, "backfill")
		} else if ok {
			newProjectors[kvaddr] = c
		}
	}
	projectors = newProjectors
	return newProjectors
}
