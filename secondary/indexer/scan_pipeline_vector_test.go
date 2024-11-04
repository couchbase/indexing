package indexer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	log "github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	n1ql "github.com/couchbase/query/value"
	"github.com/golang/protobuf/proto"
)

// ----
// name - (a to e)
// age - (1 - 9)
// color - (Red AnotherRed Blue AnotherBlue Green AnotherGreen)
// color vectors - Red -> [(1.0, 0.0, 0.0) (0.8, 0.0, 0.0), (0.0, 1.0, 0.0), (0.0, 0.8, 0.0), (0.0, 0.0, 1.0), (0.0, 0.0, 0.8)]
// -----
var gCount uint64

func encodeVector(vec []float32, code []byte) error {
	if len(code) < len(vec)*4 {
		return errors.New("code slice is too small to hold the encoded data")
	}

	for i, v := range vec {
		start := i * 4
		end := start + 4
		binary.LittleEndian.PutUint32(code[start:end], math.Float32bits(v))
	}

	return ErrSecKeyNil
}

func getVectorDataFeeder(feedError bool, errDocId int, inputErr error,
	injectDelay bool, delayDocId int, delay time.Duration, addHex bool) snapshotFeeder {

	return func(datach chan Row, errch chan error) {
		var colors = []struct {
			id  string
			v   []float32
			vv  []int
			hex string
		}{
			{"1", []float32{0.4, 1.0, 0.4}, []int{102, 255, 102}, "#66ff66"},
			{"1", []float32{0.5, 1.0, 0.4}, []int{127, 255, 102}, "#7fff66"},
			{"1", []float32{0.5, 1.0, 0.5}, []int{127, 255, 127}, "#7fff7f"},
			{"1", []float32{0.4, 1.0, 0.5}, []int{102, 255, 127}, "#66ff7f"},
			{"2", []float32{0.4, 0.5, 1.0}, []int{102, 127, 255}, "#667fff"},
			{"2", []float32{0.5, 0.5, 1.0}, []int{127, 127, 255}, "#7f7fff"},
			{"2", []float32{0.5, 0.4, 1.0}, []int{127, 102, 255}, "#7f66ff"},
			{"2", []float32{0.4, 0.4, 1.0}, []int{102, 102, 255}, "#6666ff"},
			{"3", []float32{1.0, 0.4, 0.4}, []int{255, 102, 102}, "#ff6666"},
			{"3", []float32{1.0, 0.5, 0.4}, []int{255, 127, 102}, "#ff7f66"},
			{"3", []float32{1.0, 0.5, 0.5}, []int{255, 127, 127}, "#ff7f7f"},
			{"3", []float32{1.0, 0.4, 0.5}, []int{255, 102, 127}, "#ff667f"},
			{"4", []float32{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
			{"4", []float32{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
			{"4", []float32{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
		}

		docid := 0
	toploop:
		for _, name := range []string{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e"} {
			for age := 30; age < 40; age++ {
				for _, c := range colors {
					codec := collatejson.NewCodec(16)

					keyArray := make([]interface{}, 3)
					keyArray[0] = name
					keyArray[1] = age
					keyArray[2] = c.id
					if addHex {
						keyArray = append(keyArray, c.hex)
					}

					keyn1ql := n1ql.NewValue(keyArray)
					buf := make([]byte, 0, 1000)
					keyEntry, err := codec.EncodeN1QLValue(keyn1ql, buf)
					if err != nil {
						errch <- err
						break toploop
					}

					valEntry := make([]byte, len(c.v)*4)
					encodeVector(c.v, valEntry)

					cfg := common.SystemConfig.SectionConfig("indexer.", true)
					szCfg := getKeySizeConfig(cfg)
					g := atomic.LoadUint64(&gCount)
					docidByte := []byte(fmt.Sprintf("docid-%d", g))
					buf = make([]byte, 0, 1000)
					secIdxEntry, err := NewSecondaryIndexEntry2(keyEntry, docidByte, false, 0, nil, buf, false, nil, szCfg)
					if err != nil {
						errch <- err
						break toploop
					}

					datach <- Row{
						key:   secIdxEntry,
						value: valEntry,
					}
					atomic.AddUint64(&gCount, 1)

					docid++
					if feedError && docid == errDocId {
						errch <- inputErr
						break toploop
					}
					if injectDelay && docid == delayDocId {
						time.Sleep(delay)
					}
				}
			}
		}
		close(datach)
	}
}

func getScanRequest1(dim, vectorKeyPos int, queryVector []float32) *ScanRequest {
	sr := &ScanRequest{
		isPrimary:   false,
		vectorPos:   vectorKeyPos,
		queryVector: queryVector,
		IndexInst: c.IndexInst{
			Defn: c.IndexDefn{
				VectorMeta: &c.VectorMetadata{
					Dimension:  dim,
					Similarity: common.L2_SQUARED,
				},
				SecExprs: []string{"name", "age", "color"},
			},
		},
		keySzCfg: keySizeConfig{
			allowLargeKeys: true,
			maxSecKeyLen:   4608,
		},
	}
	return sr
}

func getSliceSnapshot1(feeder snapshotFeeder) SliceSnapshot {
	sr := &mockSnapshot{feeder: feeder}
	ss := &mockSliceSnapshot{snap: sr}
	return ss
}

func getProtoScans(centroidId string) []*protobuf.Scan {
	protoScans := make([]*protobuf.Scan, 0)
	f := make([]*protobuf.CompositeElementFilter, 3)
	ll, _ := json.Marshal("a")
	hh, _ := json.Marshal("c")
	f[0] = &protobuf.CompositeElementFilter{
		Low:       ll,
		High:      hh,
		Inclusion: proto.Uint32(uint32(3)),
	}
	ll, _ = json.Marshal(34)
	hh, _ = json.Marshal(39)
	f[1] = &protobuf.CompositeElementFilter{
		Low:       ll,
		High:      hh,
		Inclusion: proto.Uint32(uint32(3)),
	}
	ll, _ = json.Marshal(centroidId)
	hh, _ = json.Marshal(centroidId)
	f[2] = &protobuf.CompositeElementFilter{
		Low:       ll,
		High:      hh,
		Inclusion: proto.Uint32(uint32(3)),
	}
	s := &protobuf.Scan{Filters: f}
	protoScans = append(protoScans, s)
	return protoScans
}

func TestVectorPipelineScanWorker(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	var ssnap SliceSnapshot
	var senderChSize = 20
	var senderBatchSize = 1
	var compDistDelay time.Duration

	testFunc := func(testErr error, stop bool, injectCompDistErr error, injectCompDistErrOnCout int) {
		vectorDim := 3

		r := getScanRequest1(vectorDim, 2, []float32{0.4, 1.0, 0.4})

		protoScans := getProtoScans("1")
		scans, err := r.makeScans(protoScans)
		if err != nil {
			t.Fatal(err)
		}

		mcb := codebook.NewMockCodebook(r.IndexInst.Defn.VectorMeta)
		if injectCompDistErr != nil {
			mcbImpl := mcb.(*codebook.MockCodebook)
			mcbImpl.InjectedErr = injectCompDistErr
			mcbImpl.CompDistErrOnCount = injectCompDistErrOnCout
			mcbImpl.CompDistDelay = compDistDelay
		}

		workCh := make(chan *ScanJob)
		recvCh := make(chan *Row, 10)
		stopCh := make(chan struct{})
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		NewScanWorker(1, r, workCh, recvCh, stopCh, errCh, nil, senderChSize, senderBatchSize)

		var j = ScanJob{
			pid:      c.PartitionId(0),
			cid:      0,
			scan:     scans[0],
			snap:     ssnap,
			codebook: mcb,
			ctx:      nil,
			doneCh:   doneCh,
		}

		logging.Infof("Scan: %+v", j.scan)

		workCh <- &j

		go func() {
			defer close(recvCh)
			select {
			case err := <-errCh:
				if err != testErr {
					t.Fatalf("Expected: %v Got %v", testErr, err)
				}
				logging.Infof("Error received %v", err)
				return
			case <-doneCh:
				logging.Infof("Good Job Done..")
				return
			}
		}()

		if stop {
			close(stopCh)
		}

		receivedCount := 0
		for row := range recvCh {
			logging.Tracef("Row: %+v", row)
			receivedCount++
		}
		log.Infof("Receive %v elements in output of test %v", receivedCount, t.Name())
	}

	testCases := func(t *testing.T) {
		t.Run("general", func(t *testing.T) {
			gCount = 0
			ssnap = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
				false, 0, 0, false))
			testFunc(nil, false, nil, 0)
			logging.Infof("gCount: %v", gCount)
		})

		t.Run("storageerror", func(t *testing.T) {
			gCount = 0
			testErr := fmt.Errorf("test injected storage error")
			ssnap = getSliceSnapshot1(getVectorDataFeeder(true, 70, testErr,
				false, 0, 0, false))
			testFunc(testErr, false, nil, 0)
			logging.Infof("gCount: %v", gCount)
		})

		t.Run("codebookerror", func(t *testing.T) {
			gCount = 0
			testErr := fmt.Errorf("test injected codebook error")
			ssnap = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
				false, 0, 0, false))
			testFunc(testErr, false, testErr, 10)
			logging.Infof("gCount: %v", gCount)
		})

		t.Run("stop", func(t *testing.T) {
			gCount = 0
			ssnap = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
				true, 00, 2*time.Second, false))
			testFunc(nil, true, nil, 0)
			logging.Infof("gCount: %v", gCount)
		})

		t.Run("cberrBlockedSenderCh", func(t *testing.T) {
			gCount = 0
			oldSenderChSize := senderChSize
			senderChSize = 0
			compDistDelay = 1 * time.Second
			testErr := fmt.Errorf("test injected codebook error")
			ssnap = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
				false, 0, 0, false))
			testFunc(testErr, false, testErr, 1)
			logging.Infof("gCount: %v", gCount)
			senderChSize = oldSenderChSize
		})
	}

	batchSizes := []int{1, 2, 3, 5, 10, 20, 50}
	for _, senderBatchSize = range batchSizes {
		name := fmt.Sprintf("batchSize_%v", senderBatchSize)
		t.Run(name, func(t *testing.T) {
			testCases(t)
		})
	}
}

func TestVectorPipelineWorkerPool(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	var ssnap1, ssnap2 SliceSnapshot

	testFunc := func(testErr error, stopPostWait, stopPreWait, restart bool,
		injectCompDistErr error, injectCompDistErrOnCout int, injectMCB2Error error,
		injectMCB2CDErrCount int) {
		vectorDim := 3

		r := getScanRequest1(vectorDim, 2, []float32{0.8, 0.0, 0.0})
		mcb1 := codebook.NewMockCodebook(r.IndexInst.Defn.VectorMeta)
		if injectCompDistErr != nil {
			mcbImpl := mcb1.(*codebook.MockCodebook)
			mcbImpl.InjectedErr = injectCompDistErr
			mcbImpl.CompDistErrOnCount = injectCompDistErrOnCout
		}

		mcb2 := codebook.NewMockCodebook(r.IndexInst.Defn.VectorMeta)
		if injectMCB2Error != nil {
			mcbImpl := mcb1.(*codebook.MockCodebook)
			mcbImpl.InjectedErr = injectMCB2Error
			mcbImpl.CompDistErrOnCount = injectMCB2CDErrCount
		}

		protoScans := getProtoScans("1")
		scans1, err := r.makeScans(protoScans)
		if err != nil {
			t.Fatal(err)
		}

		protoScans = getProtoScans("2")
		scans2, err := r.makeScans(protoScans)
		if err != nil {
			t.Fatal(err)
		}

		wp := NewWorkerPool(2)
		wp.Init(r, 20, 1)
		recvCh := wp.GetOutCh()

		var j1 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      0,
			scan:     scans1[0],
			snap:     ssnap1,
			codebook: mcb1,
			ctx:      nil,
		}
		logging.Infof("J1 Scan: %+v", j1.scan)

		var j2 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      0,
			scan:     scans2[0],
			snap:     ssnap2,
			codebook: mcb2,
			ctx:      nil,
		}
		logging.Infof("J2 Scan: %+v", j1.scan)

		wp.Submit(&j1)
		wp.Submit(&j2)

		lastCh := make(chan struct{})
		go func() {
			defer close(lastCh)
			receivedCount := 0
			for row := range recvCh {
				logging.Tracef("Row: key:%s value:%s dist: %v len:%v",
					row.key, row.value, row.dist, row.len)
				receivedCount++
			}
			logging.Infof("Receive channel closed after getting %v items", receivedCount)
		}()

		if stopPreWait {
			wp.Stop()
		}

		wpErr := wp.Wait()
		if wpErr != testErr {
			wp.StopOutCh() // On Err from wait close down stream
			t.Fatal(wpErr)
			return
		}

		if stopPostWait {
			wp.Stop()
		}

		if stopPostWait || testErr != nil {
			wp.StopOutCh()
			logging.Infof("Waiting for recv channel to get closed 1")
			<-lastCh
		}

		if restart {
			logging.Infof("Submitting jobs again")

			wp.Submit(&j1)
			wp.Submit(&j2)

			wpErr := wp.Wait()
			if wpErr != testErr {
				t.Fatal(wpErr)
			}

			wp.Stop()
			wp.StopOutCh()

			logging.Infof("Waiting for recv channel to get closed 2")
			<-lastCh
		}
	}

	t.Run("general", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		testFunc(nil, false, false, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("storageerror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected storage error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(true, 70, testErr,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		testFunc(testErr, false, false, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("dualstorageerror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected storage error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(true, 70, testErr,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(true, 70, testErr,
			false, 0, 0, false))
		testFunc(testErr, false, false, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("codebookerror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected codebook error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		testFunc(testErr, false, false, false, testErr, 1, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("dualcodebookerror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected codebook error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, false))
		testFunc(testErr, false, false, false, testErr, 1, testErr, 1)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("stoppostwait", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 70, 2*time.Second, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 90, 3*time.Second, false))
		testFunc(nil, true, false, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("stoprewait", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 70, 2*time.Second, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 90, 3*time.Second, false))
		testFunc(nil, false, true, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("stop", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 70, 2*time.Second, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			true, 90, 5*time.Second, false))
		testFunc(nil, true, true, false, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("restart", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 70, 2*time.Second, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 90, 9*time.Second, false))
		testFunc(nil, false, false, true, nil, 0, nil, 0)
		logging.Infof("gCount: %v", gCount)
	})
}

func projToProtoProj(projection *client.IndexProjection) *protobuf.IndexProjection {
	return &protobuf.IndexProjection{
		EntryKeys:  projection.EntryKeys,
		PrimaryKey: proto.Bool(projection.PrimaryKey),
	}
}

func TestVectorPipelineMergeOperator(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	var ssnap1 SliceSnapshot
	var ssnap2 SliceSnapshot

	getWriteItem := func(injectError bool) WriteItem {
		return func(data ...[]byte) error {
			if injectError {
				return fmt.Errorf("test error injection in writeItem")
			}
			logging.Verbosef("Data: %s", data)
			return nil
		}
	}

	testFunc := func(testErr error, stopPostWait, stopPreWait, restart bool, injectWriteItemError bool) {

		var err error
		vectorDim := 3
		r := getScanRequest1(vectorDim, 2, []float32{0.8, 0.6, 0.3})
		r.IndexInst.Defn.SecExprs = append(r.IndexInst.Defn.SecExprs, "hex")
		r.Limit = 10
		cklen := len(r.IndexInst.Defn.SecExprs)
		proj := &client.IndexProjection{
			EntryKeys:  []int64{0, 2},
			PrimaryKey: true,
		}
		r.Indexprojection, err = validateIndexProjection(projToProtoProj(proj), cklen, 0)
		r.setExplodePositions()

		mcb := codebook.NewMockCodebook(r.IndexInst.Defn.VectorMeta)

		protoScans := getProtoScans("1")
		scans1, err := r.makeScans(protoScans)
		if err != nil {
			t.Fatal(err)
		}

		protoScans = getProtoScans("2")
		scans2, err := r.makeScans(protoScans)
		if err != nil {
			t.Fatal(err)
		}

		wp := NewWorkerPool(2)
		wp.Init(r, 100, 50)
		recvCh := wp.GetOutCh()

		var j1 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      0,
			scan:     scans1[0],
			snap:     ssnap1,
			codebook: mcb,
			ctx:      nil,
		}
		logging.Infof("J1 Scan: %+v", j1.scan)

		var j2 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      0,
			scan:     scans2[0],
			snap:     ssnap2,
			codebook: mcb,
			ctx:      nil,
		}
		logging.Infof("J2 Scan: %+v", j1.scan)

		fioDone := make(chan struct{})
		fio, err := NewMergeOperator(recvCh, r, getWriteItem(injectWriteItemError))
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			defer close(fioDone)
			err = fio.Wait()
			if err != nil {
				wp.Stop()
			}
		}()

		wp.Submit(&j1)
		wp.Submit(&j2)

		wpErr := wp.Wait()
		if wpErr != testErr {
			wp.StopOutCh()
			t.Fatal(wpErr)
			return
		}
		if wpErr != nil {
			<-fioDone
			return
		}

		wp.Submit(&j1)
		wp.Submit(&j2)

		wpErr = wp.Wait()
		if wpErr != testErr {
			wp.StopOutCh()
			t.Fatal(wpErr)
			return
		}
		if wpErr != nil {
			<-fioDone
			return
		}

		wp.Stop()
		wp.StopOutCh()
		logging.Infof("WorkerPool Stopped")
		<-fioDone
		logging.Infof("FIO Stopped")

	}

	t.Run("general", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, true))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, true))
		testFunc(nil, false, false, false, false)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("wperror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(true, 700, testErr,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, true))
		testFunc(testErr, false, false, false, false)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("wpdualerror", func(t *testing.T) {
		gCount = 0
		testErr := fmt.Errorf("test injected error")
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(true, 700, testErr,
			false, 0, 0, false))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(true, 700, testErr,
			false, 0, 0, true))
		testFunc(testErr, false, false, false, false)
		logging.Infof("gCount: %v", gCount)
	})

	t.Run("writeitemerror", func(t *testing.T) {
		gCount = 0
		ssnap1 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, true))
		ssnap2 = getSliceSnapshot1(getVectorDataFeeder(false, 0, nil,
			false, 0, 0, true))
		testFunc(nil, false, false, false, true)
		logging.Infof("gCount: %v", gCount)
	})
}
