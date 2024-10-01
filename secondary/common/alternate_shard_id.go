package common

import (
	crypt "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

type AlternateShard_SlotId = uint64
type AlternateShard_ReplicaId = uint8
type AlternateShard_GroupId = uint8

type AlternateShardId struct {
	SlotId    AlternateShard_SlotId
	ReplicaId AlternateShard_ReplicaId
	GroupId   AlternateShard_GroupId
}
type PartnAlternateShardIdMap map[PartitionId][]string

func NewAlternateId() (*AlternateShardId, error) {
	uuid := make([]byte, 8)
	n, err := io.ReadFull(crypt.Reader, uuid)
	if err != nil {
		return nil, err
	}
	if n != len(uuid) {
		return nil, fmt.Errorf("unable to generate 64 bits uuid")
	}

	return &AlternateShardId{SlotId: binary.LittleEndian.Uint64(uuid)}, nil
}

func ParseAlternateId(str string) (*AlternateShardId, error) {
	var slotId uint64
	var replicaId uint8
	var groupId uint8

	if len(strings.TrimSpace(str)) == 0 {
		return nil, fmt.Errorf("alternateId string is empty")
	}

	n, err := fmt.Sscanf(str, "%d-%d-%d", &slotId, &replicaId, &groupId)
	if err != nil {
		return nil, err
	}
	if n != 3 {
		return nil, fmt.Errorf("invalid alternateId %v", str)
	}

	return &AlternateShardId{
		SlotId:    slotId,
		ReplicaId: replicaId,
		GroupId:   groupId,
	}, nil
}

func (s *AlternateShardId) String() string {
	return fmt.Sprintf("%d-%d-%d", s.SlotId, s.ReplicaId, s.GroupId)
}

func (s *AlternateShardId) IsNil() bool {
	return s == nil || (s.SlotId == 0 && s.ReplicaId == 0 && s.GroupId == 0)
}

func (s *AlternateShardId) IsSame(other *AlternateShardId) bool {
	return other != nil && s.SlotId == other.SlotId && s.ReplicaId == other.ReplicaId && s.GroupId == other.GroupId
}

func (s *AlternateShardId) IsReplica(other *AlternateShardId) bool {
	return other != nil && s.SlotId == other.SlotId && s.GroupId == other.GroupId && s.ReplicaId != other.ReplicaId
}

func (s *AlternateShardId) IsPair(other *AlternateShardId) bool {
	return other != nil && s.SlotId == other.SlotId && s.ReplicaId == other.ReplicaId && s.GroupId != other.GroupId
}

// DEPRECATED: use SetReplicaId
func (asi *AlternateShardId) SetReplicaGroup(rg uint8) {
	asi.ReplicaId = rg
}

// DEPRECATED: use SetGroupId
func (asi *AlternateShardId) SetInstaceGroup(ig uint8) {
	asi.GroupId = ig
}

func (asi *AlternateShardId) GetSlotId() AlternateShard_SlotId {
	return AlternateShard_SlotId(asi.SlotId)
}

func (asi *AlternateShardId) SetSlotId(slotId AlternateShard_SlotId) {
	asi.SlotId = uint64(slotId)
}

func (asi *AlternateShardId) GetReplicaId() AlternateShard_ReplicaId {
	return AlternateShard_ReplicaId(asi.ReplicaId)
}

func (asi *AlternateShardId) SetReplicaId(rId AlternateShard_ReplicaId) {
	asi.ReplicaId = uint8(rId)
}

func (asi *AlternateShardId) GetGroupId() AlternateShard_GroupId {
	return AlternateShard_GroupId(asi.GroupId)
}

func (asi *AlternateShardId) SetGroupId(gId AlternateShard_GroupId) {
	asi.GroupId = uint8(gId)
}

func (asi *AlternateShardId) Clone() *AlternateShardId {
	return &AlternateShardId{
		SlotId:    asi.SlotId,
		ReplicaId: asi.ReplicaId,
		GroupId:   asi.GroupId,
	}
}
