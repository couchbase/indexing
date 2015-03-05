package forestdb

import (
	"fmt"
	"os"
	"testing"
)

func TestSnapshotMarkersSingleKvs(t *testing.T) {

	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore.Close()

	//insert first batch
	for i := 0; i < 2; i++ {
		doc, err := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Error(err)
		}
		err = kvstore.Set(doc)
		if err != nil {
			t.Error(err)
		}
		doc.Close()
	}
	// commit changes
	err = dbfile.Commit(COMMIT_NORMAL)
	if err != nil {
		t.Error(err)
	}

	//insert second batch
	for i := 2; i < 4; i++ {
		doc, err := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Error(err)
		}
		err = kvstore.Set(doc)
		if err != nil {
			t.Error(err)
		}
		doc.Close()
	}
	// commit changes
	err = dbfile.Commit(COMMIT_NORMAL)
	if err != nil {
		t.Error(err)
	}

	snap, err := dbfile.GetAllSnapMarkers()
	if err != nil {
		t.Error(err)
	}
	defer snap.FreeSnapMarkers()

	snapList := snap.SnapInfoList()
	if len(snapList) != 2 {
		t.Errorf("expected num markers 2, got %v", len(snapList))
	}

	for i, s := range snapList {
		snapMarker := s.GetSnapMarker()
		if int(snapMarker.marker) == 0 {
			t.Errorf("invalid snapshot marker %v", snapMarker.marker)
		}
		if s.GetNumKvsMarkers() != 1 {
			t.Errorf("expected num_kvs_markers 1, got %v", s.GetNumKvsMarkers())
		}
		cm := s.GetKvsCommitMarkers()
		for _, c := range cm {
			if c.GetSeqNum() != SeqNum(4-(i*2)) {
				t.Errorf("expected commit marker seqnum %v, got %v", 4-(i*2), c.GetSeqNum())
			}
		}
	}

}

func TestSnapshotMarkersMultiKvs(t *testing.T) {

	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvstore1, err := dbfile.OpenKVStore("test1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore1.Close()

	kvstore2, err := dbfile.OpenKVStore("test2", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore2.Close()

	//insert first batch
	for i := 0; i < 2; i++ {
		doc, err := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Error(err)
		}
		err = kvstore1.Set(doc)
		if err != nil {
			t.Error(err)
		}
		doc.Close()
	}

	//insert second kv store
	for i := 0; i < 3; i++ {
		doc, err := NewDoc([]byte(fmt.Sprintf("key-%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Error(err)
		}
		err = kvstore2.Set(doc)
		if err != nil {
			t.Error(err)
		}
		doc.Close()
	}
	// commit changes
	err = dbfile.Commit(COMMIT_MANUAL_WAL_FLUSH)
	if err != nil {
		t.Error(err)
	}

	snap, err := dbfile.GetAllSnapMarkers()
	if err != nil {
		t.Error(err)
	}
	defer snap.FreeSnapMarkers()

	snapList := snap.SnapInfoList()
	if len(snapList) != 3 {
		t.Errorf("expected num markers 3, got %v", len(snapList))
	}

	//first snap marker is the latest one
	s0 := snapList[0]
	snapMarker := s0.GetSnapMarker()
	if int(snapMarker.marker) == 0 {
		t.Errorf("invalid snapshot marker %v", snapMarker.marker)
	}
	if s0.GetNumKvsMarkers() != 2 {
		t.Errorf("expected num_kvs_markers 2, got %v", s0.GetNumKvsMarkers())
	}
	cm := s0.GetKvsCommitMarkers()

	if len(cm) != 2 {
		t.Errorf("expected commit markers 2, got %v", len(cm))
	}

	for _, c := range cm {
		if c.GetKvStoreName() == "test1" && c.GetSeqNum() != 2 {
			t.Errorf("expected commit marker seqnum 2, got %v", c.GetSeqNum())

		} else if c.GetKvStoreName() == "test2" && c.GetSeqNum() != 3 {
			t.Errorf("expected commit marker seqnum 2, got %v", c.GetSeqNum())
		}
	}

}
