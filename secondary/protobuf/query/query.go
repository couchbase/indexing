package protobuf

import "errors"
import json "github.com/couchbase/indexing/secondary/common/json"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/golang/protobuf/proto"

// GetEntries implements queryport.client.ResponseReader{} method.
func (r *ResponseStream) GetEntries() ([]c.SecondaryKey, [][]byte, error) {
	entries := r.GetIndexEntries()
	skeys := make([]c.SecondaryKey, 0, len(entries))
	pkeys := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		secKeyData := entry.GetEntryKey()
		if len(secKeyData) > 0 {
			skey := make(c.SecondaryKey, 0)
			if err := json.Unmarshal(entry.GetEntryKey(), &skey); err != nil {
				return nil, nil, err
			}
			skeys = append(skeys, skey)
		} else {
			skeys = append(skeys, nil)
		}
		pkeys = append(pkeys, entry.GetPrimaryKey())
	}
	return skeys, pkeys, nil
}

// Error implements queryport.client.ResponseReader{} method.
func (r *ResponseStream) Error() error {
	if e := r.GetErr(); e != nil {
		if ee := e.GetError(); ee != "" {
			return errors.New(ee)
		}
	}
	return nil
}

// GetEntries implements queryport.client.ResponseReader{} method.
func (r *StreamEndResponse) GetEntries() ([]c.SecondaryKey, [][]byte, error) {
	return nil, nil, nil
}

// Error implements queryport.client.ResponseReader{} method.
func (r *StreamEndResponse) Error() error {
	if e := r.GetErr(); e != nil {
		if ee := e.GetError(); ee != "" {
			return errors.New(ee)
		}
	}
	return nil
}

// Count implements common.IndexStatistics{} method.
func (s *IndexStatistics) Count() (int64, error) {
	return int64(s.GetKeysCount()), nil
}

// Min implements common.IndexStatistics{} method.
func (s *IndexStatistics) MinKey() (c.SecondaryKey, error) {
	skey := make(c.SecondaryKey, 0)
	if err := json.Unmarshal(s.GetKeyMin(), &skey); err != nil {
		return nil, err
	}
	return skey, nil
}

// Max implements common.IndexStatistics{} method.
func (s *IndexStatistics) MaxKey() (c.SecondaryKey, error) {
	skey := make(c.SecondaryKey, 0)
	if err := json.Unmarshal(s.GetKeyMax(), &skey); err != nil {
		return nil, err
	}
	return skey, nil
}

// DistinctCount implements common.IndexStatistics{} method.
func (s *IndexStatistics) DistinctCount() (int64, error) {
	return int64(s.GetUniqueKeysCount()), nil
}

// Bins implements common.IndexStatistics{} method.
func (s *IndexStatistics) Bins() ([]c.IndexStatistics, error) {
	return nil, nil
}

func NewTsConsistency(
	vbnos []uint16, seqnos []uint64, vbuuids []uint64,
	crc64 uint64) *TsConsistency {

	vbnos32 := make([]uint32, len(vbnos))
	for i, vbno := range vbnos {
		vbnos32[i] = uint32(vbno)
	}
	return &TsConsistency{
		Vbnos: vbnos32, Seqnos: seqnos, Vbuuids: vbuuids,
		Crc64: proto.Uint64(crc64),
	}
}
