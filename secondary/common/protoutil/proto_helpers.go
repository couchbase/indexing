// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package protoutil

import (
	"net"
	"strings"

	c "github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/golang/protobuf/proto"
)

// convert IndexInst to protobuf format
func ConvertIndexListToProto(cfg c.Config, cip c.ClusterInfoProvider, indexList []c.IndexInst,
	streamId c.StreamId) []*protobuf.Instance {

	protoList := make([]*protobuf.Instance, 0)
	for _, index := range indexList {
		protoInst := ConvertIndexInstToProtoInst(cfg, cip, index, streamId)
		protoList = append(protoList, protoInst)
	}

	for _, index := range indexList {
		if c.IsPartitioned(index.Defn.PartitionScheme) && index.RealInstId != 0 {
			for _, protoInst := range protoList {
				if protoInst.IndexInstance.GetInstId() == uint64(index.RealInstId) {
					addPartnInfoToProtoInst(cfg, cip, index, streamId, protoInst.IndexInstance)
				}
			}
		}
	}

	return protoList

}

// convert IndexInst to protobuf format
func ConvertIndexInstToProtoInst(cfg c.Config, cip c.ClusterInfoProvider,
	indexInst c.IndexInst, streamId c.StreamId) *protobuf.Instance {

	protoDefn := ConvertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := ConvertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(cfg, cip, indexInst, streamId, protoInst)

	return &protobuf.Instance{IndexInstance: protoInst}
}

func ConvertIndexDefnToProtobuf(indexDefn c.IndexDefn) *protobuf.IndexDefn {

	using := protobuf.StorageType(
		protobuf.StorageType_value[strings.ToLower(string(indexDefn.Using))]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[strings.ToUpper(string(indexDefn.ExprType))]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(c.SINGLE)]).Enum()
	if c.IsPartitioned(indexDefn.PartitionScheme) {
		partnScheme = protobuf.PartitionScheme(
			protobuf.PartitionScheme_value[string(c.KEY)]).Enum()
	}

	secExprs, _, _, _ := c.GetUnexplodedExprs(indexDefn.SecExprs, nil, indexDefn.HasVectorAttr)
	defn := &protobuf.IndexDefn{
		DefnID:                 proto.Uint64(uint64(indexDefn.DefnId)),
		Bucket:                 proto.String(indexDefn.Bucket),
		IsPrimary:              proto.Bool(indexDefn.IsPrimary),
		Name:                   proto.String(indexDefn.Name),
		Using:                  using,
		ExprType:               exprType,
		SecExpressions:         secExprs,
		PartitionScheme:        partnScheme,
		PartnExpressions:       indexDefn.PartitionKeys,
		HashScheme:             protobuf.HashScheme(indexDefn.HashScheme).Enum(),
		WhereExpression:        proto.String(indexDefn.WhereExpr),
		RetainDeletedXATTR:     proto.Bool(indexDefn.RetainDeletedXATTR),
		Scope:                  proto.String(indexDefn.Scope),
		ScopeID:                proto.String(indexDefn.ScopeId),
		Collection:             proto.String(indexDefn.Collection),
		CollectionID:           proto.String(indexDefn.CollectionId),
		IndexMissingLeadingKey: proto.Bool(indexDefn.IndexMissingLeadingKey),
		HasVectorAttr:          indexDefn.HasVectorAttr,
	}

	if indexDefn.IsVectorIndex && indexDefn.VectorMeta != nil {
		defn.Dimension = proto.Uint64(uint64(indexDefn.VectorMeta.Dimension))
		defn.IsCosine = proto.Bool(indexDefn.VectorMeta.Similarity == c.COSINE)
		if indexDefn.VectorMeta.IsBhive {
			// [VECTOR_TODO]: Decouple include columns from vector index check
			defn.Include = indexDefn.Include
		}
	}

	return defn
}

func ConvertIndexInstToProtobuf(indexInst c.IndexInst,
	protoDefn *protobuf.IndexDefn) *protobuf.IndexInst {

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: protoDefn,
	}
	return instance
}

func addPartnInfoToProtoInst(cfg c.Config, cip c.ClusterInfoProvider,
	indexInst c.IndexInst, streamId c.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *c.KeyPartitionContainer:

		//Right now the fill the SinglePartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		//TODO move this to indexer init. These addresses cannot change.
		//Better to get these once and store.
		ninfo, err := cip.GetNodesInfoProvider()
		c.CrashOnError(err)

		ninfo.RLock()
		defer ninfo.RUnlock()

		host, err := ninfo.GetLocalHostname()
		c.CrashOnError(err)

		streamMaintAddr := net.JoinHostPort(host, cfg["streamMaintPort"].String())
		streamInitAddr := net.JoinHostPort(host, cfg["streamInitPort"].String())
		streamCatchupAddr := net.JoinHostPort(host, cfg["streamCatchupPort"].String())

		endpointsMap := make(map[c.Endpoint]bool)
		for _, p := range partnDefn {
			for _, e := range p.Endpoints() {
				//Set the right endpoint based on streamId
				switch streamId {
				case c.MAINT_STREAM:
					e = c.Endpoint(streamMaintAddr)
				case c.CATCHUP_STREAM:
					e = c.Endpoint(streamCatchupAddr)
				case c.INIT_STREAM:
					e = c.Endpoint(streamInitAddr)
				}
				endpointsMap[e] = true
			}
		}

		endpoints := make([]string, 0, len(endpointsMap))
		for e, _ := range endpointsMap {
			endpoints = append(endpoints, string(e))
		}

		if !c.IsPartitioned(indexInst.Defn.PartitionScheme) {
			protoInst.SinglePartn = &protobuf.SinglePartition{
				Endpoints: endpoints,
			}
		} else {
			partIds := make([]uint64, len(partnDefn))
			for i, p := range partnDefn {
				partIds[i] = uint64(p.GetPartitionId())
			}

			if protoInst.KeyPartn == nil || len(protoInst.KeyPartn.Partitions) == 0 {
				protoInst.KeyPartn = protobuf.NewKeyPartition(uint64(indexInst.Pc.GetNumPartitions()), endpoints, partIds)
			} else {
				protoInst.KeyPartn.AddPartitions(partIds)
			}
		}
	}
}
