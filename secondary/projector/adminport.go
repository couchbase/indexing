// projector's adminport.

package projector

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"log"
)

// error codes

// ErrorFeedAlreadyActive
var ErrorFeedAlreadyActive = errors.New("projector.adminport.FeedAlreadyActive")

// ErrorInvalidTopic
var ErrorInvalidTopic = errors.New("projector.adminport.InvalidTopic")

// list of requests handled by this adminport
var reqFailoverLog = &protobuf.FailoverLogRequest{}
var reqMutationFeed = &protobuf.MutationStreamRequest{}
var reqUpdateFeed = &protobuf.UpdateMutationStreamRequest{}
var reqSubscribeFeed = &protobuf.SubscribeStreamRequest{}
var reqRepairEndpoints = &protobuf.RepairDownstreamEndpoints{}
var reqShutdownFeed = &protobuf.ShutdownStreamRequest{}

// admin-port entry point
func mainAdminPort(laddr string, p *Projector) {
	var err error

	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("projector", laddr, c.AdminportURLPrefix, reqch)
	server.Register(reqFailoverLog)
	server.Register(reqMutationFeed)
	server.Register(reqUpdateFeed)
	server.Register(reqSubscribeFeed)
	server.Register(reqRepairEndpoints)
	server.Register(reqShutdownFeed)

	server.Start()

loop:
	for {
		select {
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				break loop
			}
			msg := req.GetMessage()
			if response, err := p.handleRequest(msg); err == nil {
				req.Send(response)
			} else {
				req.SendError(err)
			}
		}
	}
	if err != nil {
		log.Printf("error: projector adminport %q exiting with %v\n", laddr, err)
	} else {
		log.Printf("projector adminport %q exiting\n", laddr)
	}
	server.Stop()
}

func (p *Projector) handleRequest(msg ap.MessageMarshaller) (response ap.MessageMarshaller, err error) {
	switch request := msg.(type) {
	case *protobuf.FailoverLogRequest:
		response = p.doFailoverLog(request)
		p.statRequest(response, "failoverLogRequests")
	case *protobuf.MutationStreamRequest:
		response = p.doMutationFeed(request)
		p.statRequest(response, "mutationStreamRequests")
	case *protobuf.UpdateMutationStreamRequest:
		response = p.doUpdateFeed(request)
		p.statRequest(response, "updateMutationStreamRequests")
	case *protobuf.SubscribeStreamRequest:
		response = p.doSubscribeFeed(request)
		p.statRequest(response, "subscribeStreamRequests")
	case *protobuf.RepairDownstreamEndpoints:
		response = p.doRepairEndpoints(request)
		p.statRequest(response, "repairDownstreamEndpoints")
	case *protobuf.ShutdownStreamRequest:
		response = p.doShutdownFeed(request)
		p.statRequest(response, "shutdownStreamRequests")
	default:
		err = c.ErrorInvalidRequest
		p.stats.Incr("invalidRequests", 1)
	}
	return response, err
}

// handler neither use upstream connections nor disturbs upstream data path.
func (p *Projector) doFailoverLog(request *protobuf.FailoverLogRequest) ap.MessageMarshaller {
	var bucket BucketAccess
	var err error

	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	if bucket, err = p.getBucket(p.kvaddrs[0], pooln, bucketn); err != nil {
		response.Err = protobuf.NewError(err)
		return response
	}

	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	for _, vbno := range vbuckets {
		if flog, err := bucket.GetFailoverLog(uint16(vbno)); err == nil {
			vbuuids := make([]uint64, 0, len(flog))
			seqnos := make([]uint64, 0, len(flog))
			for _, x := range flog {
				vbuuids = append(vbuuids, x[0])
				seqnos = append(seqnos, x[1])
			}
			protoFlog := &protobuf.FailoverLog{
				Vbno:    proto.Uint32(vbno),
				Vbuuids: vbuuids,
				Seqnos:  seqnos,
			}
			protoFlogs = append(protoFlogs, protoFlog)
		} else {
			response.Err = protobuf.NewError(err)
			return response
		}
	}
	response.Logs = protoFlogs
	return response
}

// start a new mutation feed, on error the feed is shutdown.
func (p *Projector) doMutationFeed(request *protobuf.MutationStreamRequest) ap.MessageMarshaller {
	var err error

	response := protobuf.NewMutationStreamResponse(request)

	topic := request.GetTopic()
	buckets := request.GetBuckets()

	feed, err := p.GetFeed(topic)
	if err == nil { // only fresh feed to be started
		response.UpdateErr(ErrorFeedAlreadyActive)
		return response
	}

	feed, err = NewFeed(p, topic, request) // fresh feed
	if err != nil {
		response.UpdateErr(err)
		return response
	}

	if err = feed.RequestFeed(request); err == nil {
		// we expect failoverTimestamps and kvTimestamps to be populated.
		failTss := make([]*protobuf.BranchTimestamp, 0, len(buckets))
		kvTss := make([]*protobuf.BranchTimestamp, 0, len(buckets))
		for _, bucket := range buckets {
			failTs := protobuf.ToBranchTimestamp(feed.failoverTimestamps[bucket])
			kvTs := protobuf.ToBranchTimestamp(feed.kvTimestamps[bucket])
			failTss = append(failTss, failTs)
			kvTss = append(kvTss, kvTs)
		}
		response.UpdateTimestamps(failTss, kvTss)
	} else {
		feed.CloseFeed() // on error close the feed
		response.UpdateErr(err)
	}
	p.AddFeed(topic, feed)
	return response
}

// update an already existing feed.
// - start / restart / shutdown one or more vbucket streams
// - update partition tables that will affect routing.
// - update topology that will add or remove endpoints from existing list.
// upon error, it is left to the caller to shutdown the feed.
func (p *Projector) doUpdateFeed(request *protobuf.UpdateMutationStreamRequest) ap.MessageMarshaller {
	var err error

	response := protobuf.NewMutationStreamResponse(request)

	topic := request.GetTopic()
	buckets := request.GetBuckets()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		response.UpdateErr(err)
		return response
	}

	if err = feed.UpdateFeed(request); err == nil {
		// we expect failoverTimestamps and kvTimestamps to be re-populated.
		failTss := make([]*protobuf.BranchTimestamp, 0, len(buckets))
		kvTss := make([]*protobuf.BranchTimestamp, 0, len(buckets))
		for _, bucket := range buckets {
			failTs := protobuf.ToBranchTimestamp(feed.failoverTimestamps[bucket])
			kvTs := protobuf.ToBranchTimestamp(feed.kvTimestamps[bucket])
			failTss = append(failTss, failTs)
			kvTss = append(kvTss, kvTs)
		}
		response.UpdateTimestamps(failTss, kvTss)
	} else {
		response.UpdateErr(err)
	}
	return response
}

// add or remove endpoints.
func (p *Projector) doSubscribeFeed(request *protobuf.SubscribeStreamRequest) ap.MessageMarshaller {
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		return protobuf.NewError(err)
	}

	if request.IsUpdateSubscription() {
		err = feed.UpdateEngines(request)
	} else if request.IsDeleteSubscription() {
		err = feed.DeleteEngines(request)
	} else {
		err = c.ErrorInvalidRequest
	}
	return protobuf.NewError(err)
}

// restart connection with specified list of endpoints.
func (p *Projector) doRepairEndpoints(request *protobuf.RepairDownstreamEndpoints) ap.MessageMarshaller {
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		return protobuf.NewError(err)
	}
	return protobuf.NewError(feed.RepairEndpoints())
}

// shutdown feed, all upstream vbuckets and downstream endpoints.
func (p *Projector) doShutdownFeed(request *protobuf.ShutdownStreamRequest) ap.MessageMarshaller {
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		return protobuf.NewError(err)
	}
	response := protobuf.NewError(feed.CloseFeed())
	p.DelFeed(topic)
	return response
}

func (p *Projector) statRequest(response interface{}, key string) {
	var err string

	requestStat, errStat := 1, 0
	switch resp := response.(type) {
	case *protobuf.FailoverLogResponse:
		err = resp.Err.GetError()
	case *protobuf.MutationStreamResponse:
		err = resp.Err.GetError()
	case *protobuf.Error:
		err = resp.GetError()
	}
	if err != "" {
		errStat = 1
	}
	p.stats.Incrs(key, requestStat, errStat)
}
