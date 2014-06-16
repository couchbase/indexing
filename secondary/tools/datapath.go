package main

import (
	"code.google.com/p/goprotobuf/proto"
	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/couchbase/indexing/secondary/projector"
	"github.com/couchbase/indexing/secondary/protobuf"
	"log"
)

var kvaddrs = []string{"localhost:9000"}
var adminport = "localhost:9999"
var endpoint = "localhost:9998"
var coordEndpoint = "localhost:9997"
var vbnos = []uint16{0, 1, 2, 3, 4, 5, 6, 7}

func main() {
	c.SetLogLevel(c.LogLevelTrace)
	projector.NewProjector(kvaddrs, adminport)
	aport := ap.NewHTTPClient("http://"+adminport, "/adminport/")
	fReq := protobuf.FailoverLogRequest{
		Pool:   proto.String("default"),
		Bucket: proto.String("default"),
		Vbnos:  c.Vbno16to32(vbnos),
	}
	fRes := protobuf.FailoverLogResponse{}
	if err := aport.Request(&fReq, &fRes); err != nil {
		log.Fatal(err)
	}
	vbuuids := make([]uint64, 0)
	for _, flog := range fRes.GetLogs() {
		vbuuids = append(vbuuids, flog.Vbuuids[len(flog.Vbuuids)-1])
	}
	log.Println(vbuuids)

	mReq := makeStartRequest(vbuuids)
	mRes := protobuf.MutationStreamResponse{}
	if err := aport.Request(mReq, &mRes); err != nil {
		log.Fatal(err)
	}
}

func makeStartRequest(vbuuids []uint64) *protobuf.MutationStreamRequest {
	bTs := &protobuf.BranchTimestamp{
		Bucket:  proto.String("default"),
		Vbnos:   []uint32{0, 1, 2, 3, 4, 5, 6, 7},
		Seqnos:  []uint64{0, 0, 0, 0, 0, 0, 0, 0},
		Vbuuids: vbuuids,
	}
	instance := makeIndexInstance()
	req := protobuf.MutationStreamRequest{
		Topic:             proto.String("maintanence"),
		Pools:             []string{"default"},
		Buckets:           []string{"default"},
		RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
		Instances:         []*protobuf.IndexInst{instance},
	}
	req.SetStartFlag()
	return &req
}

func makeIndexInstance() *protobuf.IndexInst {
	sExprs := []string{`{"type":"property","path":"name"}`,
		`{"type":"property","path":"abv"}`}
	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(0x1234567812345678),
		Bucket:          proto.String("default"),
		IsPrimary:       proto.Bool(false),
		Name:            proto.String("example-index"),
		Using:           protobuf.StorageType_View.Enum(),
		ExprType:        protobuf.ExprType_Simple.Enum(),
		SecExpressions:  sExprs,
		PartitionScheme: protobuf.PartitionScheme_TestPartitionScheme.Enum(),
		PartnExpression: proto.String(`{"type":"property","path":"type"}`),
	}
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(0x3),
		State:      protobuf.IndexState_IndexInitial.Enum(),
		Definition: defn,
		Tp: &protobuf.TestPartition{
			CoordEndpoint: proto.String(coordEndpoint),
			Endpoints:     []string{endpoint},
		},
	}
	return instance
}

func endpointServer(addr string) {
	mutChanSize := 100
	mutch := make(chan []*protobuf.VbKeyVersions, mutChanSize)
	sbch := make(chan interface{}, 100)
	_, err := indexer.NewMutationStream(addr, mutch, sbch)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case <-mutch:
		case <-sbch:
		}
	}
}
