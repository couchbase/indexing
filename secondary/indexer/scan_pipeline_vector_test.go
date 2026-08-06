package indexer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
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
					Quantizer:  &c.VectorQuantizer{Type: common.SQ},
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
		r.setExplodePositions()

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

		cfg := common.SystemConfig.SectionConfig("indexer.", true)
		cfg.SetValue("scan.vector.scanworker_batch_size", senderBatchSize)
		cfg.SetValue("scan.vector.scanworker_senderch_size", senderChSize)

		NewScanWorker(1, r, workCh, recvCh, stopCh, errCh, nil, cfg, false, nil)

		var j = ScanJob{
			pid:      c.PartitionId(0),
			cid:      -1,
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
		r.setExplodePositions()

		cfg := common.SystemConfig.SectionConfig("indexer.", true)
		cfg.SetValue("scan.vector.scanworker_batch_size", 1)
		cfg.SetValue("scan.vector.scanworker_senderch_size", 20)

		wp, _ := NewWorkerPool(r, 2, false, cfg)
		wp.Init()
		recvCh := wp.GetOutCh()

		var j1 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      -1,
			scan:     scans1[0],
			snap:     ssnap1,
			codebook: mcb1,
			ctx:      nil,
		}
		logging.Infof("J1 Scan: %+v", j1.scan)

		var j2 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      -1,
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
			wp.Stop("TestVectorPipelineWorkerPool.stopPreWait")
		}

		wpErr := wp.Wait()
		if wpErr != testErr {
			wp.StopOutCh() // On Err from wait close down stream
			t.Fatal(wpErr)
			return
		}

		if stopPostWait {
			wp.Stop("TestVectorPipelineWorkerPool.stopPostWait")
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

			wp.Stop("TestVectorPipelineWorkerPool.stopPostWait")
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

		cfg := common.SystemConfig.SectionConfig("indexer.", true)
		cfg.SetValue("scan.vector.scanworker_batch_size", 50)
		cfg.SetValue("scan.vector.scanworker_senderch_size", 100)

		wp, _ := NewWorkerPool(r, 2, false, cfg)
		wp.Init()
		recvCh := wp.GetOutCh()

		var j1 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      -1,
			scan:     scans1[0],
			snap:     ssnap1,
			codebook: mcb,
			ctx:      nil,
		}
		logging.Infof("J1 Scan: %+v", j1.scan)

		var j2 = ScanJob{
			pid:      c.PartitionId(0),
			cid:      -1,
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
				wp.Stop("TestVectorPipelineMergeOperator.Wait")
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

		wp.Stop("TestVectorPipelineMergeOperator.stopPostWait")
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

// ----------------------------------
// Shared top-K distance threshold
// ----------------------------------

func newTopKDist(v float32) *atomic.Uint32 {
	a := &atomic.Uint32{}
	a.Store(math.Float32bits(v))
	return a
}

func loadTopKDist(a *atomic.Uint32) float32 {
	return math.Float32frombits(a.Load())
}

func equalDists(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCasMinFloat32 verifies the shared top-K threshold lowers correctly,
// including for negative distances (negated inner products) where uint32
// bit-pattern ordering does not match float ordering.
func TestCasMinFloat32(t *testing.T) {
	a := newTopKDist(float32(math.Inf(1)))

	load := func() float32 { return loadTopKDist(a) }

	casMinFloat32(a, 5.0)
	if load() != 5.0 {
		t.Fatalf("expected 5.0 got %v", load())
	}
	casMinFloat32(a, 7.0) // larger, must not raise
	if load() != 5.0 {
		t.Fatalf("expected 5.0 got %v", load())
	}
	casMinFloat32(a, -3.5) // negative must lower below positive
	if load() != -3.5 {
		t.Fatalf("expected -3.5 got %v", load())
	}
	casMinFloat32(a, -1.0) // less negative, must not raise
	if load() != -3.5 {
		t.Fatalf("expected -3.5 got %v", load())
	}
	casMinFloat32(a, -8.25) // more negative must lower
	if load() != -8.25 {
		t.Fatalf("expected -8.25 got %v", load())
	}

	// A NaN must be dropped, not stored: a stored NaN compares false against
	// everything, so it would stop all pruning and let the next call install
	// any value, including a larger one.
	casMinFloat32(a, float32(math.NaN()))
	if load() != -8.25 {
		t.Fatalf("NaN must not be stored, expected -8.25 got %v", load())
	}
	casMinFloat32(a, -2.0) // still must not raise after the NaN attempt
	if load() != -8.25 {
		t.Fatalf("expected -8.25 got %v", load())
	}
	casMinFloat32(a, -9.0) // and must still lower
	if load() != -9.0 {
		t.Fatalf("expected -9.0 got %v", load())
	}
}

// TestScanWorkerPruneBatch covers the batch-level top-K prune. It must drop
// exactly the rows that cannot beat the published threshold - ties included,
// since the publisher already holds heapSize rows at least as good - keep the
// survivors in their original order, and keep dists in step with the rows it
// compacts. A threshold that was never published (the +Inf sentinel) and a
// scan with no shared threshold at all must both leave the batch untouched.
func TestScanWorkerPruneBatch(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	// Row.dist is set up front here only so the assertions can tell the rows
	// apart; processCurrentBatch assigns it after pruning, from w.dists.
	newWorker := func(topKDist *atomic.Uint32, dists []float32) *ScanWorker {
		w := &ScanWorker{logPrefix: "pruneBatchTest", globalTopKDist: topKDist}
		w.dists = append(w.dists, dists...)
		for _, d := range dists {
			w.currBatchRows = append(w.currBatchRows, &Row{dist: d})
		}
		return w
	}
	rowDists := func(w *ScanWorker) []float32 {
		out := make([]float32, 0, len(w.currBatchRows))
		for _, row := range w.currBatchRows {
			out = append(out, row.dist)
		}
		return out
	}

	tests := []struct {
		name     string
		topKDist *atomic.Uint32
		dists    []float32
		kept     []float32
	}{
		{
			name:  "NoSharedThreshold",
			dists: []float32{9.0, 1.0, 5.0},
			kept:  []float32{9.0, 1.0, 5.0},
		},
		{
			name:     "ThresholdNotYetPublished",
			topKDist: newTopKDist(float32(math.Inf(1))),
			dists:    []float32{9.0, 1.0, 5.0},
			kept:     []float32{9.0, 1.0, 5.0},
		},
		{
			name:     "DropsWorseAndTies",
			topKDist: newTopKDist(5.0),
			dists:    []float32{9.0, 1.0, 5.0, 4.0},
			kept:     []float32{1.0, 4.0},
		},
		{
			// negated inner products: the threshold and the rows are negative,
			// where uint32 bit-pattern ordering would disagree with float order
			name:     "NegativeDistances",
			topKDist: newTopKDist(-3.0),
			dists:    []float32{-1.0, -8.0, -3.0, -4.0},
			kept:     []float32{-8.0, -4.0},
		},
		{
			name:     "AllPruned",
			topKDist: newTopKDist(0.5),
			dists:    []float32{9.0, 1.0},
			kept:     []float32{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newWorker(test.topKDist, test.dists)

			if got := w.pruneBatch(len(test.dists)); got != len(test.kept) {
				t.Fatalf("pruneBatch returned %v, expected %v", got, len(test.kept))
			}
			if got := rowDists(w); !equalDists(got, test.kept) {
				t.Fatalf("surviving rows %v, expected %v", got, test.kept)
			}
			if !equalDists(w.dists, test.kept) {
				t.Fatalf("dists %v out of step with rows %v", w.dists, test.kept)
			}
			if want := uint64(len(test.dists) - len(test.kept)); w.rowsPruned != want {
				t.Fatalf("rowsPruned %v, expected %v", w.rowsPruned, want)
			}
		})
	}
}

// TestScanWorkerPublishTopKDist covers publishing a worker's k-th best
// distance to the shared threshold: only a full heap has a k-th best row to
// publish, the shared value only ever lowers, and a worker does not republish
// a root it has already published.
func TestScanWorkerPublishTopKDist(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	newWorker := func(topKDist *atomic.Uint32, heapSize int, dists ...float32) *ScanWorker {
		heap, err := NewTopKRowHeap(heapSize, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range dists {
			heap.Push(&Row{dist: d})
		}
		return &ScanWorker{
			logPrefix:         "publishTest",
			globalTopKDist:    topKDist,
			heap:              heap,
			heapSize:          heapSize,
			lastPublishedDist: float32(math.Inf(1)),
		}
	}

	// a heap that is not full yet has no k-th best row to publish
	shared := newTopKDist(float32(math.Inf(1)))
	w := newWorker(shared, 3, 1.0, 2.0)
	w.publishTopKDist()
	if got := loadTopKDist(shared); !math.IsInf(float64(got), 1) {
		t.Fatalf("a partial heap must not publish, got %v", got)
	}

	// once full, the root - the worst row it kept - is its k-th best
	w.heap.Push(&Row{dist: 7.0})
	w.publishTopKDist()
	if got := loadTopKDist(shared); got != 7.0 {
		t.Fatalf("expected 7.0 got %v", got)
	}

	// a better row lowers the root, and with it the shared threshold
	w.heap.Push(&Row{dist: 3.0})
	w.publishTopKDist()
	if got := loadTopKDist(shared); got != 3.0 {
		t.Fatalf("expected 3.0 got %v", got)
	}

	// a root already published is not published again. Raising the shared
	// value by hand is the only way to observe that from outside: a
	// republish would lower it back to 3.
	shared.Store(math.Float32bits(10.0))
	w.publishTopKDist()
	if got := loadTopKDist(shared); got != 10.0 {
		t.Fatalf("root 3.0 was already published, expected 10.0 got %v", got)
	}

	// a worker whose k-th best is worse must not raise the shared value
	shared.Store(math.Float32bits(3.0))
	newWorker(shared, 2, 20.0, 30.0).publishTopKDist()
	if got := loadTopKDist(shared); got != 3.0 {
		t.Fatalf("shared threshold must not rise, got %v", got)
	}

	// a scan with no shared threshold must be a no-op, not a panic
	newWorker(nil, 2, 1.0, 2.0).publishTopKDist()
}

// TestScanWorkerFlushPersistentHeapPrune covers the final flush of a worker's
// persistent heap. Rows strictly worse than the published threshold are
// dropped, but rows tying it are sent: the threshold is some worker's k-th
// best distance, so a tying row may be that row itself, and dropping every
// tie could leave the merge with fewer than heapSize candidates.
func TestScanWorkerFlushPersistentHeapPrune(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	heap, err := NewTopKRowHeap(3, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []float32{1.0, 5.0, 9.0} {
		heap.Push(&Row{dist: d})
	}

	outCh := make(chan *Row, 8)
	w := &ScanWorker{
		logPrefix:         "flushTest",
		usePersistentHeap: true,
		heap:              heap,
		heapSize:          3,
		globalTopKDist:    newTopKDist(5.0),
		stopCh:            make(chan struct{}),
		outCh:             outCh,
	}

	w.flushPersistentHeap()
	close(outCh)

	// heap order is unspecified, so compare the flushed set
	got := make([]float32, 0, 3)
	for row := range outCh {
		got = append(got, row.dist)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

	// 9.0 is strictly worse than the threshold and dropped; 5.0 ties it and
	// must still be sent
	if want := []float32{1.0, 5.0}; !equalDists(got, want) {
		t.Fatalf("flushed %v, expected %v", got, want)
	}
	if w.heap != nil {
		t.Fatal("heap must be released once its rows are handed downstream")
	}
}

// TestScanWorkerDedupBatch covers keeping a doc returned by two jobs of the
// same bhive scan out of the worker's persistent heap. Without it the two
// copies would take two of the heapSize slots the worker gets to fill, and the
// merge - which deduplicates only after every worker has made its cut - could
// not recover the distinct candidate they displaced.
func TestScanWorkerDedupBatch(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	// (storeId, recordId) identifies the doc; a repeat carries the same
	// distance, as both copies read the same stored entry
	type doc struct {
		storeId  uint64
		recordId uint64
		dist     float32
	}

	newWorker := func(seen map[rowDedupKey]struct{}, docs []doc) *ScanWorker {
		w := &ScanWorker{logPrefix: "dedupBatchTest", seen: seen}
		for _, d := range docs {
			w.currBatchRows = append(w.currBatchRows, &Row{
				storeId: d.storeId, recordId: d.recordId, dist: d.dist,
			})
			w.dists = append(w.dists, d.dist)
		}
		return w
	}

	tests := []struct {
		name string
		seen map[rowDedupKey]struct{}
		docs []doc
		kept []float32
	}{
		{
			// a worker with no dedup map - any non-bhive scan - is untouched
			name: "NoDedupMap",
			docs: []doc{{1, 1, 5.0}, {1, 1, 5.0}},
			kept: []float32{5.0, 5.0},
		},
		{
			name: "DistinctDocsAllKept",
			seen: map[rowDedupKey]struct{}{},
			docs: []doc{{1, 1, 5.0}, {1, 2, 4.0}, {2, 1, 3.0}},
			kept: []float32{5.0, 4.0, 3.0},
		},
		{
			// recordId numbering is per-kvstore, so storeId disambiguates it
			name: "SameRecordIdDifferentStore",
			seen: map[rowDedupKey]struct{}{},
			docs: []doc{{1, 7, 5.0}, {2, 7, 4.0}},
			kept: []float32{5.0, 4.0},
		},
		{
			name: "RepeatWithinOneBatch",
			seen: map[rowDedupKey]struct{}{},
			docs: []doc{{1, 1, 5.0}, {1, 2, 4.0}, {1, 1, 5.0}},
			kept: []float32{5.0, 4.0},
		},
		{
			// the sentinel job resurfacing a doc a per-cell job already offered
			name: "RepeatFromEarlierJob",
			seen: map[rowDedupKey]struct{}{{storeId: 1, recordId: 1}: {}},
			docs: []doc{{1, 1, 5.0}, {1, 2, 4.0}},
			kept: []float32{4.0},
		},
		{
			name: "EveryRowARepeat",
			seen: map[rowDedupKey]struct{}{{storeId: 1, recordId: 1}: {}},
			docs: []doc{{1, 1, 5.0}, {1, 1, 5.0}},
			kept: []float32{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newWorker(test.seen, test.docs)

			if got := w.dedupBatch(len(test.docs)); got != len(test.kept) {
				t.Fatalf("dedupBatch returned %v, expected %v", got, len(test.kept))
			}

			got := make([]float32, 0, len(w.currBatchRows))
			for _, row := range w.currBatchRows {
				got = append(got, row.dist)
			}
			if !equalDists(got, test.kept) {
				t.Fatalf("surviving rows %v, expected %v", got, test.kept)
			}
			if !equalDists(w.dists, test.kept) {
				t.Fatalf("dists %v out of step with rows %v", w.dists, test.kept)
			}
			if want := uint64(len(test.docs) - len(test.kept)); w.rowsDeduped != want {
				t.Fatalf("rowsDeduped %v, expected %v", w.rowsDeduped, want)
			}
		})
	}

	// every surviving doc must be recorded, so a later job's repeat is caught
	seen := map[rowDedupKey]struct{}{}
	w := newWorker(seen, []doc{{1, 1, 5.0}, {1, 2, 4.0}})
	w.dedupBatch(2)
	for _, want := range []rowDedupKey{{storeId: 1, recordId: 1}, {storeId: 1, recordId: 2}} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("doc %+v was offered to the heap but not recorded", want)
		}
	}
}

// TestScanWorkerMaterializeHeapRowsBhive covers the job-end materialization of
// a bhive scan's persistent heap. A surviving row must outlive the storage
// iterator that produced it - so its key and include column are copied out of
// iterator memory - and must still carry the record identity the merge stage
// reads to deduplicate docs across scan sources and to re-rank on the full
// vector.
func TestScanWorkerMaterializeHeapRowsBhive(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	heap, err := NewTopKRowHeap(2, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	// iterator-owned memory, reused once the current job's iterator closes
	itrKey := []byte("docid-1")
	itrInclude := []byte("include-1")
	cid := []byte("centroid-1") // request-owned, outlives every row of the scan

	pool := NewRowPool(1)
	row := pool.Get()
	row.key = itrKey
	row.includeColumn = itrInclude
	row.value = []byte("quantized-sparse-wire")
	row.len = len(itrKey)
	row.dist = -7.0
	row.distValid = true
	row.storeId = 11
	row.recordId = 22
	row.partnId = 3
	row.cid = cid
	heap.Push(row)

	w := &ScanWorker{
		logPrefix:         "materializeTest",
		id:                4,
		r:                 &ScanRequest{isBhiveScan: true},
		currJob:           &ScanJob{},
		usePersistentHeap: true,
		heap:              heap,
		heapSize:          2,
	}

	w.materializeHeapRows()

	// the worker moves on to its next job and the iterator memory is rewritten
	copy(itrKey, "docid-9")
	copy(itrInclude, "include-9")

	rows := heap.List()
	if len(rows) != 1 {
		t.Fatalf("heap holds %v rows, expected 1", len(rows))
	}
	got := rows[0]

	if got.rowBuf != nil {
		t.Fatal("materialized row must not be pool-bound; the storage-backed row it replaced went back to the pool")
	}
	if string(got.key) != "docid-1" {
		t.Fatalf("key %q, expected it copied out of iterator memory", got.key)
	}
	if string(got.includeColumn) != "include-1" {
		t.Fatalf("includeColumn %q, expected it copied out of iterator memory", got.includeColumn)
	}
	if got.value != nil {
		t.Fatal("value holds the vector payload, already consumed to compute dist, and must not be carried")
	}
	if got.len != len("docid-1") {
		t.Fatalf("len %v, expected the docid length the bhive iterator recorded", got.len)
	}
	if got.dist != -7.0 || !got.distValid {
		t.Fatalf("dist %v distValid %v, expected -7 and true", got.dist, got.distValid)
	}
	if got.storeId != 11 || got.recordId != 22 || got.partnId != 3 {
		t.Fatalf("record identity lost: storeId %v recordId %v partnId %v", got.storeId, got.recordId, got.partnId)
	}
	if string(got.cid) != "centroid-1" {
		t.Fatalf("cid %q, expected the request-owned centroid id", got.cid)
	}
	if got.workerId != 4 {
		t.Fatalf("workerId %v, expected the id of the worker that materialized it", got.workerId)
	}

	// the next job boundary must leave an already materialized row alone
	w.materializeHeapRows()
	if heap.List()[0] != got {
		t.Fatal("an already materialized row must not be copied again")
	}
}

// TestScanWorkerMaterializeHeapRowsComposite is the composite counterpart. Only
// bhiveIteratorCallback writes the record identity fields, so a composite row
// must not pick them up - copying them regardless would also alias cid on a
// row that outlives its iterator. Its len, unlike a bhive row's, is the length
// of the secondary index entry its key is.
func TestScanWorkerMaterializeHeapRowsComposite(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	entry, err := newSKEntry([]byte(`["a",1]`), []byte("docid-1"))
	if err != nil {
		t.Fatal(err)
	}

	heap, err := NewTopKRowHeap(2, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	pool := NewRowPool(1)
	row := pool.Get()
	row.key = entry
	row.len = len(entry)
	row.dist = 2.5
	// set to pin the gate, not because a composite scan produces these: a
	// pooled Row keeps its scalars until the scan overwrites them
	row.storeId = 11
	row.recordId = 22
	row.partnId = 3
	row.cid = []byte("centroid-1")
	heap.Push(row)

	w := &ScanWorker{
		logPrefix:         "materializeCompositeTest",
		id:                4,
		r:                 &ScanRequest{},
		currJob:           &ScanJob{},
		usePersistentHeap: true,
		heap:              heap,
		heapSize:          2,
	}

	w.materializeHeapRows()

	got := heap.List()[0]
	if got.rowBuf != nil {
		t.Fatal("materialized row must not be pool-bound")
	}
	if got.storeId != 0 || got.recordId != 0 || got.partnId != 0 || got.cid != nil {
		t.Fatalf("composite row carried bhive record identity: storeId %v recordId %v partnId %v cid %q",
			got.storeId, got.recordId, got.partnId, got.cid)
	}
	if want := entry.lenKey(); got.len != want {
		t.Fatalf("len %v, expected the secondary entry key length %v", got.len, want)
	}
	if string(got.key) != string(entry) {
		t.Fatalf("key %q, expected a copy of %q", got.key, entry)
	}
}
