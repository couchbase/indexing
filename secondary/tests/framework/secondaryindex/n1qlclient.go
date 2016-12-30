package secondaryindex

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	nclient "github.com/couchbase/indexing/secondary/queryport/n1ql"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
	"log"
	"strconv"
)

// Creates an index and waits for it to become active
func N1QLCreateSecondaryIndex(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, indexActiveTimeoutSeconds int64) error {

	log.Printf("N1QLCreateSecondaryIndex :: server = %v", server)
	nc, err := nclient.NewGSIIndexer(server, "default", bucketName)
	requestId := "12345"
	exprs := make(expression.Expressions, 0, len(indexFields))
	for _, exprS := range indexFields {
		expr, _ := parser.Parse(exprS)
		exprs = append(exprs, expr)
	}
	rangeKey := exprs

	_, err = nc.CreateIndex(requestId, indexName, nil, rangeKey, nil, nil)
	if err != nil {
		return err
	}
	return nil
}

func N1QLRange(indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	client, err := nclient.NewGSIIndexer(server, "default", bucketName)
	if err != nil {
		return nil, err
	}
	conn, err := datastore.NewSizedIndexConnection(100000, &testContext{})
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	go func() {
		l, h := skey2qkey(low), skey2qkey(high)
		rng := datastore.Range{Low: l, High: h, Inclusion: datastore.Inclusion(inclusion)}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	return getresultsfromchannel(conn.EntryChannel()), nil
}

func N1QLLookup(indexName, bucketName, server string, values []interface{},
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	client, err := nclient.NewGSIIndexer(server, "default", bucketName)
	if err != nil {
		return nil, err
	}
	conn, err := datastore.NewSizedIndexConnection(100000, &testContext{})
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	go func() {
		l, h := skey2qkey(values), skey2qkey(values)
		rng := datastore.Range{Low: l, High: h, Inclusion: datastore.BOTH}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	return getresultsfromchannel(conn.EntryChannel()), nil
}

func N1QLScanAll(indexName, bucketName, server string, limit int64,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	client, err := nclient.NewGSIIndexer(server, "default", bucketName)
	if err != nil {
		return nil, err
	}
	conn, err := datastore.NewSizedIndexConnection(100000, &testContext{})
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	go func() {
		rng := datastore.Range{Low: nil, High: nil, Inclusion: datastore.BOTH}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	return getresultsfromchannel(conn.EntryChannel()), nil
}

func N1QLScans(indexName, bucketName, server string, scans qc.Scans, reverse, distinct bool,
	offset, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponse, error) {

	client, err := nclient.NewGSIIndexer(server, "default", bucketName)
	if err != nil {
		return nil, err
	}
	conn, err := datastore.NewSizedIndexConnection(100000, &testContext{})
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}
	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	index2, useScan2 := index.(datastore.Index2)
	if err != nil {
		return nil, err
	}

	go func() {
		spans2 := make(datastore.Spans2, len(scans))
		for i, scan := range scans {
			spans2[i] = &datastore.Span2{}
			if len(scan.Seek) != 0 {
				spans2[i].Seek = skey2qkey(scan.Seek)
			}
			spans2[i].Ranges = filtertoranges2(scan.Filter)
		}

		cons := getConsistency(consistency)
		ordered := true
		if useScan2 {
			index2.Scan2(requestid, spans2, reverse, distinct, ordered, nil,
				offset, limit, cons, nil, conn)
		} else {
			log.Fatalf("Indexer does not suppor Scan2")
		}
	}()

	return getresultsfromchannel(conn.EntryChannel()), nil
}

func filtertoranges2(filters []*qc.CompositeElementFilter) datastore.Ranges2 {
	if filters == nil || len(filters) == 0 {
		return nil
	}
	ranges2 := make(datastore.Ranges2, len(filters))
	for i, cef := range filters {
		ranges2[i] = &datastore.Range2{}
		ranges2[i].Low = interfaceton1qlvalue(cef.Low)
		ranges2[i].High = interfaceton1qlvalue(cef.High)
		ranges2[i].Inclusion = datastore.Inclusion(cef.Inclusion)
	}

	return ranges2
}

func getrequestid() string {
	uuid, _ := c.NewUUID()
	return strconv.Itoa(int(uuid.Uint64()))
}

func getConsistency(consistency c.Consistency) datastore.ScanConsistency {
	var cons datastore.ScanConsistency
	if consistency == c.SessionConsistency {
		cons = datastore.SCAN_PLUS
	} else {
		cons = datastore.UNBOUNDED
	}
	return cons
}

func getresultsfromchannel(ch datastore.EntryChannel) tc.ScanResponse {
	scanResults := make(tc.ScanResponse)
	ok := true
	for ok {
		entry, ok := <-ch
		if ok {
			scanResults[entry.PrimaryKey] = values2SKey(entry.EntryKey)
		} else {
			break
		}
	}
	return scanResults
}

func interfaceton1qlvalue(key interface{}) value.Value {
	if s, ok := key.(string); ok && collatejson.MissingLiteral.Equal(s) {
		return value.NewMissingValue()
	} else {
		return value.NewValue(key)
	}
}

func skey2qkey(skey c.SecondaryKey) value.Values {
	qkey := make(value.Values, 0, len(skey))
	for _, x := range skey {
		qkey = append(qkey, value.NewValue(x))
	}
	return qkey
}

func values2SKey(vals value.Values) c.SecondaryKey {
	skey := make(c.SecondaryKey, 0, len(vals))
	for _, val := range []value.Value(vals) {
		skey = append(skey, val.Actual())
	}
	return skey
}

type testContext struct{}

func (ctxt *testContext) Error(err errors.Error) {
	fmt.Printf("Scan error: %v\n", err)
}

func (ctxt *testContext) Warning(wrn errors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *testContext) Fatal(fatal errors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}
