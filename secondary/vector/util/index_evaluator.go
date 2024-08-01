package vectorutil

import (
	protoutil "github.com/couchbase/indexing/secondary/common/protoutil"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	protoProj "github.com/couchbase/indexing/secondary/protobuf/projector"

	qexpr "github.com/couchbase/query/expression"
	qvalue "github.com/couchbase/query/value"

	c "github.com/couchbase/indexing/secondary/common"
)

const ENCODE_BUF_SIZE = 2 * 1024 //2KB

// FetchSampleVectorsForIndexes fetches a given sampleSize number of
// documents from KV via random sampling. Then it evaluates each document
// for the input list of index instances and extracts any vectors which
// qualify for the index definition. The list of extracted vectors per
// index is returned.
func FetchSampleVectorsForIndexes(cluster string,
	pooln string,
	bucketn string,
	scope string,
	collection string,
	cid string,
	idxInsts []*c.IndexInst,
	sampleSize int64) ([][]float32, error) {

	evaluators := make([]*protoProj.IndexEvaluator, len(idxInsts))

	var err error
	for i, idxInst := range idxInsts {
		idxProtoDefn := protoutil.ConvertIndexDefnToProtobuf(idxInst.Defn)
		idxProtoInst := protoutil.ConvertIndexInstToProtobuf(*idxInst, idxProtoDefn)
		evaluators[i], err = protoProj.NewIndexEvaluator(idxProtoInst, protoProj.FeedVersion_cheshireCat, idxInst.Defn.Bucket)
		if err != nil {
			return nil, err
		}
	}

	donech := make(chan bool)
	datach, errch, err := c.FetchRandomKVSample(cluster, pooln, bucketn, scope, collection, cid, sampleSize, donech)
	if err != nil {
		return nil, err
	}

	defer close(donech)

	encodeBuf := make([]byte, 0, ENCODE_BUF_SIZE)
	vectors := make([][]float32, len(idxInsts)) //slice of [instId][multiple unpacked vectors as float32 slice]

	var newBuf []byte

	sampleCnt := 0

sampling:
	for {

		select {
		case mut, ok := <-datach:
			if ok {
				newBuf, err = evaluateEvent(evaluators, mut, vectors, encodeBuf)
				if err != nil {
					//even if some document evals return error, continue with
					//evaluation as this method is only doing sampling.
					logging.Warnf("FetchSampleVectorsForIndexes::evaluateEvent ignore eval err %v ", err)
				}
				if cap(newBuf) > cap(encodeBuf) {
					encodeBuf = newBuf[:0]
				}
				sampleCnt++
			} else {
				//doc sampling has finished
				break sampling
			}

		case err = <-errch:
			//Even if some document sampling return error, continue
			//as this method is only doing sampling. Caller can decide
			//if the sampled docs are enough or retry if number of returned
			//vectors are not sufficient.
			logging.Warnf("FetchSampleVectorsForIndexes::evaluateEvent ignore sampling err %v ", err)

		}
	}
	logging.Infof("FetchSampleVectorsForIndexes Total sampled %v for requested "+
		"sampleSize %v", sampleCnt, sampleSize)
	return vectors, nil
}

func evaluateEvent(evaluators []*protoProj.IndexEvaluator,
	m *memcached.DcpEvent,
	outvecs [][]float32,
	encodeBuf []byte) ([]byte, error) {

	var nvalue qvalue.Value
	if m.IsJSON() {
		nvalue = qvalue.NewParsedValueWithOptions(m.Value, true, true)
	} else {
		nvalue = qvalue.NewBinaryValue(m.Value)
	}
	context := qexpr.NewIndexContext()
	docval := qvalue.NewAnnotatedValue(nvalue)

	var newBuf []byte
	var err error
	var vectors [][]float32

	for i, evaluator := range evaluators {
		_, _, _, _, newBuf, _, _, vectors, _, err = evaluator.ProcessEvent(m,
			encodeBuf, docval, context, nil)

		if err != nil {
			return nil, err
		}

		if len(vectors) != 0 {
			for _, vec := range vectors {
				outvecs[i] = append(outvecs[i], vec...)
			}
		}
	}
	return newBuf, nil
}
