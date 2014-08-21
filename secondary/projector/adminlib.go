package projector

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
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

// Based on the latest vbmap gather the list of kvnode-address and return the
// same.
func GetKVAddrs(cluster, pooln, bucketn string) ([]string, error) {
	b, err := c.ConnectBucket(cluster, pooln, bucketn)
	if err != nil {
		return nil, err
	}
	b.Close()

	b.Refresh()
	m, err := b.GetVBmap(nil)
	if err != nil {
		return nil, err
	}

	kvaddrs := make([]string, 0, len(m))
	for kvaddr := range m {
		kvaddrs = append(kvaddrs, kvaddr)
	}
	return kvaddrs, nil
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

// Start Initial stream for a set of buckets hosted by `kvaddr`.
func InitialMutationStream(
	client ap.Client,
	topic, pooln, kvaddr string, buckets []string,
	instances []*protobuf.IndexInst) (*protobuf.MutationStreamResponse, error) {

	req := &protobuf.MutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.TsVbuuid{},
		Instances:         instances,
	}
	for _, bucketn := range buckets {
		vbnos, flogs, err := flogsFromKV(client, pooln, bucketn, kvaddr)
		if err != nil {
			return nil, err
		}
		ts := protobuf.NewTsVbuuid(bucketn, c.MaxVbuckets)
		ts = ts.InitialRestartTs(vbnos).ComputeRestartTs(flogs)

		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucketn)
		req.RestartTimestamps = append(req.RestartTimestamps, ts)
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
	topic, pooln string, tss map[string]*protobuf.TsVbuuid,
	instances []*protobuf.IndexInst,
	callb func(*protobuf.MutationStreamResponse, error) bool) {

	req := &protobuf.UpdateMutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.TsVbuuid{},
		Instances:         instances,
	}
	for bucketn, ts := range tss {
		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucketn)
		req.RestartTimestamps = append(req.RestartTimestamps, ts)
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

// SpawnProjectors for each kvaddrs, provided they are not already spawned and
// remembered in `projectors` parameter. return the new set of projectors that
// were spawned.
// adminport of the projector will be 500+kvport.
func SpawnProjectors(
	cluster string, kvaddrs []string,
	projectors map[string]ap.Client) (map[string]ap.Client, error) {

	newprojectors := make(map[string]ap.Client)
	// create a projector instance for each kvnode
	for _, kvaddr := range kvaddrs {
		if _, ok := projectors[kvaddr]; ok {
			continue
		}
		ss := strings.Split(kvaddr, ":")
		kport, err := strconv.Atoi(ss[1])
		if err != nil {
			return nil, err
		}
		adminport := ss[0] + ":" + strconv.Itoa(kport+500)
		NewProjector(cluster, []string{kvaddr}, adminport)
		c := ap.NewHTTPClient("http://"+adminport, c.AdminportURLPrefix)
		projectors[kvaddr] = c
		newprojectors[kvaddr] = c
	}
	return newprojectors, nil
}

func ShutdownProjectors(
	cluster, pooln string, buckets []string,
	projectors map[string]ap.Client) (map[string]ap.Client, error) {

	var b *couchbase.Bucket
	var err error

	for _, bucketn := range buckets {
		if b, err = c.ConnectBucket(cluster, pooln, bucketn); err != nil {
			return nil, err
		} else {
			break
		}
	}

	defer b.Close()

	b.Refresh()
	m, err := b.GetVBmap(nil)
	if err != nil {
		return nil, err
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
	return newProjectors, nil
}

func AddBuckets(
	client ap.Client,
	topic, kvaddr, pooln string, buckets []string,
	tss map[string]*protobuf.TsVbuuid,
	instances []*protobuf.IndexInst) error {

	req := &protobuf.UpdateMutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.TsVbuuid{},
		Instances:         instances,
	}
	for _, bucketn := range buckets {
		var ts *protobuf.TsVbuuid
		if tss == nil || tss[bucketn] == nil {
			vbnos, flogs, err := flogsFromKV(client, pooln, bucketn, kvaddr)
			if err != nil {
				return err
			}
			ts = protobuf.NewTsVbuuid(bucketn, c.MaxVbuckets)
			ts = ts.InitialRestartTs(vbnos).ComputeRestartTs(flogs)
		} else {
			ts = tss[bucketn]
		}
		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucketn)
		req.RestartTimestamps = append(req.RestartTimestamps, ts)
	}
	req.SetRestartFlag()
	req.SetAddBucketFlag()
	res := &protobuf.MutationStreamResponse{}
	return client.Request(req, res)
}

func DelBuckets(client ap.Client, topic, pooln string, buckets []string) error {
	req := &protobuf.UpdateMutationStreamRequest{
		Topic:   proto.String(topic),
		Pools:   []string{},
		Buckets: []string{},
	}
	for _, bucket := range buckets {
		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucket)
	}
	req.SetRestartFlag()
	req.SetDelBucketFlag()
	res := &protobuf.MutationStreamResponse{}
	return client.Request(req, res)
}

func flogsFromKV(
	client ap.Client,
	pooln, bucketn, kvaddr string) ([]uint16, couchbase.FailoverLog, error) {

	// get subset of vbuckets hosted by `kvaddr`
	vbmap, err := GetVbmap(client, pooln, bucketn, []string{kvaddr})
	if err != nil {
		return nil, nil, err
	}
	// get failover logs
	flogs, err := GetFailoverLogs(client, pooln, bucketn, vbmap.Vbuckets32())
	if err != nil {
		return nil, nil, err
	}
	return vbmap.Vbuckets16(), flogs.ToFailoverLog(vbmap.Vbuckets16()), nil
}
