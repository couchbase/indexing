package projector

import "expvar"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/couchbase/indexing/secondary/logging"

// list of requests handled by this adminport
var reqVbmap = &protobuf.VbmapRequest{}
var reqFailoverLog = &protobuf.FailoverLogRequest{}
var reqMutationFeed = &protobuf.MutationTopicRequest{}
var reqRestartVbuckets = &protobuf.RestartVbucketsRequest{}
var reqShutdownVbuckets = &protobuf.ShutdownVbucketsRequest{}
var reqAddBuckets = &protobuf.AddBucketsRequest{}
var reqDelBuckets = &protobuf.DelBucketsRequest{}
var reqAddInstances = &protobuf.AddInstancesRequest{}
var reqDelInstances = &protobuf.DelInstancesRequest{}
var reqRepairEndpoints = &protobuf.RepairEndpointsRequest{}
var reqShutdownFeed = &protobuf.ShutdownTopicRequest{}
var reqStats = c.Statistics{}

var angioToken = uint16(1)

// admin-port entry point, once started never shutsdown.
func (p *Projector) mainAdminPort(reqch chan ap.Request) {
	p.admind.Register(reqVbmap)
	p.admind.Register(reqFailoverLog)
	p.admind.Register(reqMutationFeed)
	p.admind.Register(reqRestartVbuckets)
	p.admind.Register(reqShutdownVbuckets)
	p.admind.Register(reqAddBuckets)
	p.admind.Register(reqDelBuckets)
	p.admind.Register(reqAddInstances)
	p.admind.Register(reqDelInstances)
	p.admind.Register(reqRepairEndpoints)
	p.admind.Register(reqShutdownFeed)
	p.admind.Register(reqStats)
	p.admind.RegisterHTTPHandler("/stats", p.handleStats)
	p.admind.RegisterHTTPHandler("/settings", p.handleSettings)

	expvar.Publish("projector", expvar.Func(p.doStatistics))

	p.admind.Start()

loop:
	for {
		select {
		case req, ok := <-reqch:
			if ok == false {
				break loop
			}
			// a go-routine is spawned so that requests to
			// different feeds can be simultaneously executed.
			go p.handleRequest(req, angioToken)
			angioToken++
			if angioToken >= 0xFFFE {
				angioToken = 1
			}
		}
	}

	p.admind.Stop()
	logging.Infof("%v ... adminport stopped\n", p.logPrefix)
}

// re-entrant / concurrent request handler.
func (p *Projector) handleRequest(req ap.Request, opaque uint16) {
	var response ap.MessageMarshaller
	var err error

	msg := req.GetMessage()
	switch request := msg.(type) {
	case *protobuf.VbmapRequest:
		response = p.doVbmapRequest(request, opaque)
	case *protobuf.FailoverLogRequest:
		response = p.doFailoverLog(request, opaque)
	case *protobuf.MutationTopicRequest:
		response = p.doMutationTopic(request, opaque)
	case *protobuf.RestartVbucketsRequest:
		response = p.doRestartVbuckets(request, opaque)
	case *protobuf.ShutdownVbucketsRequest:
		response = p.doShutdownVbuckets(request, opaque)
	case *protobuf.AddBucketsRequest:
		response = p.doAddBuckets(request, opaque)
	case *protobuf.DelBucketsRequest:
		response = p.doDelBuckets(request, opaque)
	case *protobuf.AddInstancesRequest:
		response = p.doAddInstances(request, opaque)
	case *protobuf.DelInstancesRequest:
		response = p.doDelInstances(request, opaque)
	case *protobuf.RepairEndpointsRequest:
		response = p.doRepairEndpoints(request, opaque)
	case *protobuf.ShutdownTopicRequest:
		response = p.doShutdownTopic(request, opaque)
	default:
		err = c.ErrorInvalidRequest
	}

	if err == nil {
		req.Send(response)
	} else {
		logging.Errorf("%v %v\n", p.logPrefix, err)
		req.SendError(err)
	}
}
