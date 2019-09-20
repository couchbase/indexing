package protoProjector

import "github.com/golang/protobuf/proto"

var defn1 = &IndexDefn{
	DefnID:          proto.Uint64(10),
	Bucket:          proto.String("default"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index1"),
	Using:           StorageType_memdb.Enum(),
	ExprType:        ExprType_N1QL.Enum(),
	SecExpressions:  []string{`name`},
	PartitionScheme: PartitionScheme_SINGLE.Enum(),
}

var defn2 = &IndexDefn{
	DefnID:          proto.Uint64(11),
	Bucket:          proto.String("default"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index2"),
	Using:           StorageType_memdb.Enum(),
	ExprType:        ExprType_N1QL.Enum(),
	SecExpressions:  []string{`members`},
	PartitionScheme: PartitionScheme_SINGLE.Enum(),
}

var defn3 = &IndexDefn{
	DefnID:          proto.Uint64(12),
	Bucket:          proto.String("default"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index3"),
	Using:           StorageType_memdb.Enum(),
	ExprType:        ExprType_N1QL.Enum(),
	SecExpressions:  []string{`language`},
	PartitionScheme: PartitionScheme_SINGLE.Enum(),
}

var defn4 = &IndexDefn{
	DefnID:          proto.Uint64(13),
	Bucket:          proto.String("default"),
	IsPrimary:       proto.Bool(false),
	Name:            proto.String("index4"),
	Using:           StorageType_memdb.Enum(),
	ExprType:        ExprType_N1QL.Enum(),
	SecExpressions:  []string{`type`},
	PartitionScheme: PartitionScheme_SINGLE.Enum(),
}

// ScaleDefault4i on buckets and documents created by tools/loadgen.
func ScaleDefault4i(
	buckets, endpoints []string, coordEndpoint string) []*Instance {

	partn := NewSinglePartition(endpoints).SetCoordinatorEndpoint(coordEndpoint)
	makeInstance := func(id uint64, defn *IndexDefn, bucket string) *Instance {
		defn.Bucket = proto.String(bucket)
		ii := &IndexInst{
			InstId:      proto.Uint64(id),
			State:       IndexState_IndexInitial.Enum(),
			Definition:  defn,
			SinglePartn: partn,
		}
		return &Instance{IndexInstance: ii}
	}

	rs := make([]*Instance, 0)
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
