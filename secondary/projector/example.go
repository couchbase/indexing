package projector

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/couchbase/indexing/secondary/protobuf"
)

var defn1 = &protobuf.IndexDefn{
	DefnID:    proto.Uint64(10),
	Bucket:    proto.String("users"),
	IsPrimary: proto.Bool(false),
	Name:      proto.String("index1"),
	Using:     protobuf.StorageType_View.Enum(),
	ExprType:  protobuf.ExprType_N1QL.Enum(),
	SecExpressions: []string{
		`{"type":"property","path":"age"}`,
		`{"type":"property","path":"firstname"}`},
	PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
	PartnExpression: proto.String(`{"type":"property","path":"city"}`),
}

var defn2 = &protobuf.IndexDefn{
	DefnID:          proto.Uint64(11),
	Bucket:          proto.String("users"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index2"),
	Using:           protobuf.StorageType_View.Enum(),
	ExprType:        protobuf.ExprType_N1QL.Enum(),
	SecExpressions:  []string{`{"type":"property","path":"city"}`},
	PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
	PartnExpression: proto.String(`{"type":"property","path":"gender"}`),
}

var defn3 = &protobuf.IndexDefn{
	DefnID:          proto.Uint64(12),
	Bucket:          proto.String("projects"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index3"),
	Using:           protobuf.StorageType_View.Enum(),
	ExprType:        protobuf.ExprType_N1QL.Enum(),
	SecExpressions:  []string{`{"type":"property","path":"name"}`},
	PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
	PartnExpression: proto.String(`{"type":"property","path":"language"}`),
}

var defn4 = &protobuf.IndexDefn{
	DefnID:          proto.Uint64(13),
	Bucket:          proto.String("beer-sample"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index4"),
	Using:           protobuf.StorageType_View.Enum(),
	ExprType:        protobuf.ExprType_N1QL.Enum(),
	SecExpressions:  []string{`{"type":"property","path":"name"}`},
	PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
	PartnExpression: proto.String(`{"type":"property","path":"type"}`),
}

// ExampleIndexInstances on buckets and documents created by tools/loadgen.
func ExampleIndexInstances(
	buckets, endpoints []string, coordEndpoint string) []*protobuf.IndexInst {

	makeInstance := func(id uint64, defn *protobuf.IndexDefn, bucket string) *protobuf.IndexInst {
		defn.Bucket = proto.String(bucket)
		return &protobuf.IndexInst{
			InstId:     proto.Uint64(id),
			State:      protobuf.IndexState_IndexInitial.Enum(),
			Definition: defn,
			Tp: &protobuf.TestPartition{
				CoordEndpoint: proto.String(coordEndpoint),
				Endpoints:     endpoints,
			},
		}
	}

	rs := make([]*protobuf.IndexInst, 0)
	for _, bucket := range buckets {
		switch bucket {
		case "default": // is alias of users
			i1 := makeInstance(0x1, defn1, "default")
			i2 := makeInstance(0x2, defn2, "default")
			rs = append(rs, i1, i2)
		case "users":
			i1 := makeInstance(0x1, defn1, "users")
			i2 := makeInstance(0x2, defn2, "users")
			rs = append(rs, i1, i2)
		case "projects":
			i3 := makeInstance(0x3, defn3, "projects")
			rs = append(rs, i3)
		case "beer-sample":
			i4 := makeInstance(0x4, defn4, "beer-sample")
			rs = append(rs, i4)
		}
	}
	return rs
}
