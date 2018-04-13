package protobuf

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/collatejson"
import qexpr "github.com/couchbase/query/expression"
import qparser "github.com/couchbase/query/expression/parser"
import qvalue "github.com/couchbase/query/value"

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

// N1QLTransform will use compiled list of expression from N1QL's DDL
// statement and evaluate a document using them to return a secondary
// key as JSON object.
func N1QLTransform(
	docid []byte, docval qvalue.AnnotatedValue, context qexpr.Context,
	cExprs []interface{},
	encodeBuf []byte) ([]byte, []byte, error) {

	arrValue := make([]interface{}, 0, len(cExprs))
	skip := true
	for _, cExpr := range cExprs {
		expr := cExpr.(qexpr.Expression)
		scalar, vector, err := expr.EvaluateForIndex(docval, context)
		if err != nil {
			exprstr := qexpr.NewStringer().Visit(expr)
			fmsg := "EvaluateForIndex(%q) for docid %v, err: %v skip document"
			arg1 := logging.TagUD(exprstr)
			arg2 := logging.TagUD(string(docid))
			logging.Errorf(fmsg, arg1, arg2, err)
			return nil, nil, nil
		}
		isArray, _ := expr.IsArrayIndexKey()
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
			if key.Type() == qvalue.MISSING && skip {
				return nil, nil, nil

			} else if key.Type() == qvalue.MISSING {
				arrValue = append(arrValue, key)
				continue
			}
			skip = false
			arrValue = append(arrValue, key)
		} else {
			if vector == nil { //nil is ERROR condition
				exprstr := qexpr.NewStringer().Visit(expr)
				fmsg := "EvaluateForIndex(%q) vector=nil, skip document %v"
				arg1 := logging.TagUD(exprstr)
				arg2 := logging.TagUD(string(docid))
				logging.Errorf(fmsg, arg1, arg2)
				return nil, nil, nil
			}

			if skip { //array is leading
				if len(vector) == 0 { //array is empty
					return nil, nil, nil
				}
				if len(vector) == 1 && vector[0].Type() == qvalue.MISSING { //array is missing
					return nil, nil, nil
				}
			} else if !skip {
				if len(vector) == 0 { //array is non-leading and is empty
					vector = []qvalue.Value{missing}
				}
			}
			skip = false

			arrValue = append(arrValue, qvalue.NewValue([]qvalue.Value(vector)))
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
				fmsg := "CollateJSONEncode: index field for docid: %s (err: %v) skip document"
				arg1 := logging.TagUD(docid)
				logging.Errorf(fmsg, arg1, err)
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
