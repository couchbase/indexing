package indexer

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
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

func getVectorDataFeeder(feedError bool, errDocId int, inputErr error,
	injectDelay bool, delayDocId int, delay time.Duration, addHex bool) snapshotFeeder {

	return func(datach chan Row, errch chan error) {
		var colors = []struct {
			id  string
			v   []float64
			vv  []int
			hex string
		}{
			{"1", []float64{0.4, 1.0, 0.4}, []int{102, 255, 102}, "#66ff66"},
			{"1", []float64{0.5, 1.0, 0.4}, []int{127, 255, 102}, "#7fff66"},
			{"1", []float64{0.5, 1.0, 0.5}, []int{127, 255, 127}, "#7fff7f"},
			{"1", []float64{0.4, 1.0, 0.5}, []int{102, 255, 127}, "#66ff7f"},
			{"2", []float64{0.4, 0.5, 1.0}, []int{102, 127, 255}, "#667fff"},
			{"2", []float64{0.5, 0.5, 1.0}, []int{127, 127, 255}, "#7f7fff"},
			{"2", []float64{0.5, 0.4, 1.0}, []int{127, 102, 255}, "#7f66ff"},
			{"2", []float64{0.4, 0.4, 1.0}, []int{102, 102, 255}, "#6666ff"},
			{"3", []float64{1.0, 0.4, 0.4}, []int{255, 102, 102}, "#ff6666"},
			{"3", []float64{1.0, 0.5, 0.4}, []int{255, 127, 102}, "#ff7f66"},
			{"3", []float64{1.0, 0.5, 0.5}, []int{255, 127, 127}, "#ff7f7f"},
			{"3", []float64{1.0, 0.4, 0.5}, []int{255, 102, 127}, "#ff667f"},
			{"4", []float64{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
			{"4", []float64{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
			{"4", []float64{0.02, 0.6, 0.76}, []int{5, 153, 193}, "#599c1"},
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

					valArray := make([]interface{}, len(c.v))
					for i, v := range c.v {
						valArray[i] = v
					}
					valn1ql := n1ql.NewValue(valArray)
					buf = make([]byte, 0, 1000)
					valEntry, err := codec.EncodeN1QLValue(valn1ql, buf)
					if err != nil {
						errch <- err
						break toploop
					}

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
		}

		workCh := make(chan *ScanJob)
		recvCh := make(chan *Row, 10)
		stopCh := make(chan struct{})
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		NewScanWorker(1, r, workCh, recvCh, stopCh, errCh, nil)

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

		for row := range recvCh {
			logging.Infof("Row: %+v", row)
		}
	}
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
}
