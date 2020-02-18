package protoProjector

import (
	"github.com/golang/protobuf/proto"
	"math/rand"
	"time"
)

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

func MakeInstance(id uint64, defn *IndexDefn, bucket, coordEndpoint string, endpoints []string) *Instance {
	partn := NewSinglePartition(endpoints).SetCoordinatorEndpoint(coordEndpoint)
	defn.Bucket = proto.String(bucket)
	ii := &IndexInst{
		InstId:      proto.Uint64(id),
		State:       IndexState_IndexInitial.Enum(),
		Definition:  defn,
		SinglePartn: partn,
	}
	return &Instance{IndexInstance: ii}
}

// ScaleDefault4i on buckets and documents created by tools/loadgen.
func ScaleDefault4i(
	buckets, endpoints []string, coordEndpoint string) []*Instance {

	rs := make([]*Instance, 0)
	for _, bucket := range buckets {
		switch bucket {
		case "default": // is alias of users
			i1 := MakeInstance(0x1, defn1, "default", coordEndpoint, endpoints)
			i2 := MakeInstance(0x2, defn2, "default", coordEndpoint, endpoints)
			rs = append(rs, i1, i2)
		case "users":
			i1 := MakeInstance(0x1, defn1, "users", coordEndpoint, endpoints)
			i2 := MakeInstance(0x2, defn2, "users", coordEndpoint, endpoints)
			rs = append(rs, i1, i2)
		case "projects":
			i3 := MakeInstance(0x3, defn3, "projects", coordEndpoint, endpoints)
			rs = append(rs, i3)
		case "beer-sample":
			i4 := MakeInstance(0x4, defn4, "beer-sample", coordEndpoint, endpoints)
			rs = append(rs, i4)
		}
	}
	return rs
}

func GenDefn(bucket, name string, isPrimary bool, secExpressions []string, exprType ExprType, partitionScheme PartitionScheme,
	using StorageType, scopeName, scopeID, collectionName, collectionID string) *IndexDefn {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	return &IndexDefn{
		DefnID:          proto.Uint64(r1.Uint64()), // Random definition ID
		Bucket:          proto.String(bucket),
		Name:            proto.String(name),
		IsPrimary:       proto.Bool(isPrimary),
		SecExpressions:  secExpressions,
		ExprType:        &exprType,
		PartitionScheme: &partitionScheme,
		Using:           &using,
	}
}

func GenCollectionAwareDefn(bucket, name string, isPrimary bool, secExpressions []string, exprType ExprType, partitionScheme PartitionScheme,
	using StorageType, scopeName, scopeID, collectionName, collectionID string) *IndexDefn {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	return &IndexDefn{
		DefnID:          proto.Uint64(r1.Uint64()), // Random definition ID
		Bucket:          proto.String(bucket),
		Name:            proto.String(name),
		IsPrimary:       proto.Bool(isPrimary),
		SecExpressions:  secExpressions,
		ExprType:        &exprType,
		PartitionScheme: &partitionScheme,
		Using:           &using,
		ScopeID:         proto.String(scopeID),
		Scope:           proto.String(scopeName),
		CollectionID:    proto.String(collectionID),
		Collection:      proto.String(collectionName),
	}
}

func GenInstances(defns []*IndexDefn, coordEndpoint string, endpoints []string) []*Instance {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	instances := make([]*Instance, 0)
	for _, defn := range defns {
		instance := MakeInstance(r1.Uint64(), defn, defn.GetBucket(), coordEndpoint, endpoints)
		instances = append(instances, instance)
	}
	return instances
}
