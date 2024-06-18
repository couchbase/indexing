package protoProjector

import (
	"errors"
	math "math"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/logging"

	qexpr "github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	qparser "github.com/couchbase/query/expression/parser"

	qvalue "github.com/couchbase/query/value"
)

var ErrInvalidVectorType = errors.New("The value of a VECTOR expression is expected to be an array of floats. Found non-array type")
var ErrInvalidVectorDimension = errors.New("Length of VECTOR in incoming document is different from the expected dimension")
var ErrHeterogenousVectorData = errors.New("All entries of a vector are expected to be floating point numbers. Found other data types")
var ErrDataOutOfBounds = errors.New("Value of the vector exceeds float32 range")

// CompileN1QLExpression will take expressions defined in N1QL's DDL statement
// and compile them for evaluation.
func CompileN1QLExpression(expressions []string) ([]interface{}, error) {
	cExprs := make([]interface{}, 0, len(expressions))
	for _, expr := range expressions {
		cExpr, err := qparser.Parse(expr)
		if err != nil {
			arg1 := logging.TagUD(expr)
			logging.Errorf("CompileN1QLExpression() %v: %v\n", arg1, err)
			return nil, err
		}
		cExprs = append(cExprs, cExpr)
	}
	return cExprs, nil
}

var missing = qvalue.NewValue(string(collatejson.MissingLiteral))
var dummy_centroid = qvalue.NewValue(int(-1))

// N1QLTransform will use compiled list of expression from N1QL's DDL
// statement and evaluate a document using them to return a secondary
// key as JSON object.
func N1QLTransform(
	docid []byte, docval qvalue.AnnotatedValue, context qexpr.Context,
	cExprs []interface{}, numFlattenKeys int, encodeBuf []byte,
	stats *IndexEvaluatorStats, indexMissingLeadingKey bool) ([]byte,
	[]byte, error) {

	arrValue := make([]interface{}, 0, len(cExprs))
	isLeadingKey := !indexMissingLeadingKey
	for _, cExpr := range cExprs {
		expr := cExpr.(qexpr.Expression)
		start := time.Now()
		scalar, array, err := expr.EvaluateForIndex(docval, context)
		elapsed := time.Since(start)
		if stats != nil {
			stats.add(elapsed)
		}
		if err != nil {
			exprstr := qexpr.NewStringer().Visit(expr)
			fmsg := "EvaluateForIndex(%q) for docid %v, err: %v skip document"
			arg1 := logging.TagUD(exprstr)
			arg2 := logging.TagUD(string(docid))
			logging.Errorf(fmsg, arg1, arg2, err)
			return nil, nil, nil
		}
		isArray, _, isFlattened := expr.IsArrayIndexKey()
		if isArray == false {
			if scalar == nil { //nil is ERROR condition
				exprstr := qexpr.NewStringer().Visit(expr)
				fmsg := "EvaluateForIndex(%q) scalar=nil, skip document %v"
				arg1 := logging.TagUD(exprstr)
				arg2 := logging.TagUD(string(docid))
				logging.Errorf(fmsg, arg1, arg2)
				return nil, nil, nil
			}
			key := scalar
			if key.Type() == qvalue.MISSING && isLeadingKey {
				return nil, nil, nil
			} else if key.Type() == qvalue.MISSING {
				arrValue = append(arrValue, key)
				continue
			}
			isLeadingKey = false
			arrValue = append(arrValue, key)
		} else {
			if array == nil { //nil is ERROR condition
				exprstr := qexpr.NewStringer().Visit(expr)
				fmsg := "EvaluateForIndex(%q) array=nil, skip document %v"
				arg1 := logging.TagUD(exprstr)
				arg2 := logging.TagUD(string(docid))
				logging.Errorf(fmsg, arg1, arg2)
				return nil, nil, nil
			}
			if isLeadingKey {
				//if array is leading key and empty, skip indexing the entry
				if isArrayEmpty(array) {
					return nil, nil, nil
				}
				//if array is leading key and missing, skip indexing the entry
				if isArrayMissing(array) {
					return nil, nil, nil
				}

				if isFlattened {
					array = explodeArrayEntries(array, numFlattenKeys, isLeadingKey)
					if len(array) == 0 {
						return nil, nil, nil
					}
				}
			} else {
				if isFlattened {
					if isArrayEmpty(array) { // Populate "missing" for all keys
						array = populateValueForFlattenKeys(numFlattenKeys, missing)
					} else {
						array = explodeArrayEntries(array, numFlattenKeys, isLeadingKey)
					}
				} else {
					//if array is non-leading key and empty, treat it as missing
					if isArrayEmpty(array) {
						array = []qvalue.Value{missing}
					}
				}
			}

			isLeadingKey = false

			arrValue = append(arrValue, qvalue.NewValue([]qvalue.Value(array)))
		}
	}

	if len(cExprs) == 1 && len(arrValue) == 1 && docid == nil {
		// used for partition-key evaluation and where predicate.
		// Marshal partition-key and where as a basic JSON data-type.
		out, err := qvalue.NewValue(arrValue[0]).MarshalJSON()
		return out, nil, err

	} else if len(arrValue) > 0 {
		// The shape of the secondary key looks like,
		//     [expr1] - for simple key
		//     [expr1, expr2] - for composite key

		// in case we need to append docid to skeys, it is applicable
		// only when docid is not `nil`
		//if docid != nil {
		//    arrValue = append(arrValue, qvalue.NewValue(string(docid)))
		//}
		if encodeBuf != nil {
			out, newBuf, err := CollateJSONEncode(qvalue.NewValue(arrValue), encodeBuf)
			if err != nil {
				fmsg := "N1QLTransform[%v<-%v] CollateJSONEncode: index field for docid: %s (err: %v), instId: %v skip document"
				arg := logging.TagUD(docid)
				logging.Errorf(fmsg, stats.KeyspaceId, stats.Topic, arg, err, stats.InstId)
				return nil, newBuf, nil
			}
			return out, newBuf, err // return as collated JSON array
		}
		secKey := qvalue.NewValue(make([]interface{}, len(arrValue)))
		for i, key := range arrValue {
			secKey.SetIndex(i, key)
		}
		out, err := secKey.MarshalJSON()
		return out, nil, err // return as JSON array
	}
	return nil, nil, nil
}

func logError(fmsg string, expr qexpr.Expression, docid []byte, err error) {
	exprstr := qexpr.NewStringer().Visit(expr)
	arg1 := logging.TagUD(exprstr)
	arg2 := logging.TagUD(string(docid))
	logging.Errorf(fmsg, arg1, arg2, err)
}

// N1QLTransformForVectorIndex is same as N1QLTransform except that
// it does some additional checks for VECTOR indexes
func N1QLTransformForVectorIndex(
	docid []byte, docval qvalue.AnnotatedValue, context qexpr.Context,
	encodeBuf []byte, ie *IndexEvaluator) ([]byte, []byte, [][]float32, error) {

	cExprs, stats, numFlattenKeys := ie.skExprs, ie.stats, ie.numFlattenKeys

	arrValue := make([]interface{}, 0, len(cExprs))
	isLeadingKey := !ie.indexMissingLeadingKey || (ie.vectorPos == 0)

	keyPos := 0
	var vectors [][]float32

	for _, cExpr := range cExprs {
		expr := cExpr.(qexpr.Expression)
		start := time.Now()
		scalar, array, err := expr.EvaluateForIndex(docval, context)
		elapsed := time.Since(start)
		if stats != nil {
			stats.add(elapsed)
		}
		if err != nil {
			logError("EvaluateForIndex(%q) for docid %v, err: %v skip document", expr, docid, err)
			return nil, nil, nil, nil
		}
		isArray, _, isFlattened := expr.IsArrayIndexKey()
		if isArray == false {
			if scalar == nil { //nil is ERROR condition
				logError("EvaluateForIndex(%q) scalar=nil, skip document %v", expr, docid, nil)
				return nil, nil, nil, nil
			}
			key := scalar
			var vector []float32

			if key.Type() == qvalue.MISSING && isLeadingKey {
				return nil, nil, nil, nil
			} else if key.Type() == qvalue.MISSING {
				if keyPos == ie.vectorPos {
					// If vector is missing/null, then skip indexing the document
					return nil, nil, nil, nil
				}
				arrValue = append(arrValue, key)
			} else {

				// Process vector entries. vectorPos will be >= 0 only when
				// a vector attribute has been defined for an expression. For
				// an expression with VECTOR attribute, we follow the below rules:
				//
				// a. If VECTOR entry is MISSING/NULL and it is leading key, the
				//    document will not be indexed
				// b. If VECTOR entry is MISSING/NULL and it is non-leading key,
				//    we index MISSING/NULL for the VECTOR entry
				// c. If the VECTOR entry is an invalid and it is leading key,
				//    the document will not be indexed
				// d. If the VECTOR entry is invalid and it is non-leading key,
				//    the VECTOR entry will be indexed as NULL
				if keyPos == ie.vectorPos {
					vector, err = validateVector(key, ie.dimension)

					if err != nil { // invalid vector entry
						// [VECTOR_TODO] - Add stats when rejecting incoming documents

						// If vector is missing/null, then skip indexing the document
						// irrespective of expression with VECTOR attribute being leading
						// key or not
						return nil, nil, nil, nil
					} else {
						vectors = append(vectors, vector)
						arrValue = append(arrValue, dummy_centroid)
					}
				} else {
					arrValue = append(arrValue, key)
				}
			}
			isLeadingKey = false
		} else {
			// [VECTOR_TODO]: Add support for array expressions with VECTOR attribute
			if array == nil { //nil is ERROR condition
				logError("EvaluateForIndex(%q) array=nil, skip document %v", expr, docid, nil)
				return nil, nil, nil, nil
			}

			if isLeadingKey {
				//if array is leading key and empty, skip indexing the entry
				if isArrayEmpty(array) {
					return nil, nil, nil, nil
				}
				//if array is leading key and missing, skip indexing the entry
				if isArrayMissing(array) {
					return nil, nil, nil, nil
				}

				if isFlattened {
					array = explodeArrayEntries(array, numFlattenKeys, isLeadingKey)
					if len(array) == 0 {
						return nil, nil, nil, nil
					}
				}
			} else {
				if isFlattened {
					if isArrayEmpty(array) { // Populate "missing" for all keys
						array = populateValueForFlattenKeys(numFlattenKeys, missing)
					} else {
						array = explodeArrayEntries(array, numFlattenKeys, isLeadingKey)
					}
				} else {
					//if array is non-leading key and empty, treat it as missing
					if isArrayEmpty(array) {
						array = []qvalue.Value{missing}
					}
				}
			}

			isLeadingKey = false

			arrValue = append(arrValue, qvalue.NewValue([]qvalue.Value(array)))
		}

		if isArray && isFlattened {
			keyPos += ie.numFlattenKeys
		} else {
			keyPos++
		}
	}

	if len(cExprs) == 1 && len(arrValue) == 1 && docid == nil {
		// used for partition-key evaluation and where predicate.
		// Marshal partition-key and where as a basic JSON data-type.
		out, err := qvalue.NewValue(arrValue[0]).MarshalJSON()
		return out, nil, nil, err

	} else if len(arrValue) > 0 {
		// The shape of the secondary key looks like,
		//     [expr1] - for simple key
		//     [expr1, expr2] - for composite key

		// in case we need to append docid to skeys, it is applicable
		// only when docid is not `nil`
		//if docid != nil {
		//    arrValue = append(arrValue, qvalue.NewValue(string(docid)))
		//}
		if encodeBuf != nil {
			out, newBuf, err := CollateJSONEncode(qvalue.NewValue(arrValue), encodeBuf)
			if err != nil {
				fmsg := "N1QLTransformForVectorIndex[%v<-%v] CollateJSONEncode: index field for docid: %s (err: %v), instId: %v skip document"
				arg := logging.TagUD(docid)
				logging.Errorf(fmsg, stats.KeyspaceId, stats.Topic, arg, err, stats.InstId)
				return nil, newBuf, nil, nil
			}
			return out, newBuf, vectors, err // return as collated JSON array
		}
		secKey := qvalue.NewValue(make([]interface{}, len(arrValue)))
		for i, key := range arrValue {
			secKey.SetIndex(i, key)
		}
		out, err := secKey.MarshalJSON()
		return out, nil, vectors, err // return as JSON array
	}
	return nil, nil, nil, nil
}

// EvaluateForIndex will cache the evaluated values for a document. Therefore, it is
// not advisable to populate array directly. E.g., if array[i] is [null] and if we
// explode array[i] for numFlattenKeys = 2, then array[i] would become [[null, null]].
// When an index with numFlattenKeys = 3 has to be evalated, query can use the cached
// array and it returns [[null, null]] instead of [null]. This would lead to incorrect
// results as the check array[i].Type() == qvalue.NULL would fail. Hence, do not directly
// override array
func explodeArrayEntries(array qvalue.Values, numFlattenKeys int, isLeadingKey bool) qvalue.Values {
	var newValues []qvalue.Value

	for i := range array {
		if array[i].Type() == qvalue.NULL {
			newValues = append(newValues, populateValueForFlattenKeys(numFlattenKeys, qvalue.NULL_VALUE)[0])
		} else if array[i].Type() == qvalue.MISSING {
			if isLeadingKey { // Skip indexing missing leading key
				continue
			}
			newValues = append(newValues, populateValueForFlattenKeys(numFlattenKeys, qvalue.MISSING_VALUE)[0])
		} else {
			leadingVal, ok := array[i].Index(0)
			if isLeadingKey && ok && leadingVal.Type() == qvalue.MISSING { // Skip indexing missing leading key
				continue
			}
			newValues = append(newValues, array[i])
		}
	}

	return newValues
}

func populateValueForFlattenKeys(numFlattenKeys int, value qvalue.Value) []qvalue.Value {
	flattenKeyVals := make(qvalue.Values, numFlattenKeys)
	for i := 0; i < numFlattenKeys; i++ {
		flattenKeyVals[i] = value
	}
	array := []qvalue.Value{qvalue.NewValue(flattenKeyVals)}
	return array
}

// For flattened array index, if the array is the leading expression
// in the index, filter all the values where the first key in
// the expression is MISSING. E.g., for the expression
// "distinct array flatten_keys(v.name, v.age, v.email) for v in friends"
// [[test 10 abcd@abcd.com] [MISSING 11 efgh@abcd.com]], the second
// entry in the array i.e. [MISSING 11 efgh@abcd.com] will be filtered from
// the result and only [test 10 abcd@abcd.com] is returned to the user
func filterArrayForMissingEntries(array qvalue.Values) qvalue.Values {
	index := 0 // Index is a monotonically increasing counter
	end := len(array)
	for index < end {
		if array[index] == nil {
			break
		}
		leadingVal, ok := array[index].Index(0)
		if ok && leadingVal.Type() == qvalue.MISSING {
			array[index] = array[end-1] // Swap with last element of the slice and set last element to nil
			array[end-1] = nil
			end--
		} else {
			index++
		}
	}
	return array[0:end]
}

func CollateJSONEncode(val qvalue.Value, encodeBuf []byte) ([]byte, []byte, error) {
	codec := collatejson.NewCodec(16)
	encoded, err := codec.EncodeN1QLValue(val, encodeBuf[:0])

	if err != nil && err.Error() == collatejson.ErrorOutputLen.Error() {
		valBytes, e1 := val.MarshalJSON()
		if e1 != nil {
			return append([]byte(nil), encoded...), nil, err
		}
		newBuf := make([]byte, 0, len(valBytes)*3)
		enc, e2 := codec.EncodeN1QLValue(val, newBuf)
		return append([]byte(nil), enc...), newBuf, e2
	}
	return append([]byte(nil), encoded...), nil, err
}

func isArrayEmpty(array qvalue.Values) bool {
	return (len(array) == 0)
}

func isArrayMissing(array qvalue.Values) bool {
	return (len(array) == 1 && array[0].Type() == qvalue.MISSING)
}

func isArrayNull(array qvalue.Values) bool {
	return (len(array) == 1 && array[0].Type() == qvalue.NULL)
}

func validateVector(vector qvalue.Value, dimension int) ([]float32, error) {
	if vector.Type() != qvalue.ARRAY {
		return nil, ErrInvalidVectorType
	}

	// [VECTOR_TODO]: Instead of allocating []float32 everytime, n1QL evalate can use a sync.Pool
	// object or a local buffer of []float32 to prevent the re-allocation everytime
	res := make([]float32, dimension)

	for i := 0; ; i++ {
		v, ok := vector.Index(i)
		if !ok || i >= dimension {
			if !ok && i == dimension {
				return res, nil
			} else {
				return nil, ErrInvalidVectorDimension
			}
		} else {
			if v.Type() != value.NUMBER {
				return nil, ErrHeterogenousVectorData
			}
			vf := value.AsNumberValue(v).Float64()
			if vf < -math.MaxFloat32 || vf > math.MaxFloat32 {
				return nil, ErrDataOutOfBounds
			}
			res[i] = (float32)(vf)
		}
	}
}
