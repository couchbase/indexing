package secondaryindex

import (
	"bytes"
	"encoding/json"
	e "errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	nclient "github.com/couchbase/indexing/secondary/queryport/n1ql"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/tenant"
	"github.com/couchbase/query/value"
)

var n1qlClientMap map[string]datastore.Indexer
var clientMapLock sync.RWMutex

func init() {
	n1qlClientMap = make(map[string]datastore.Indexer)
	log.Default().SetOutput(os.Stdout)
	log.Default().SetFlags(log.LstdFlags)
}

func GetOrCreateN1QLClient(server, bucketName string) (datastore.Indexer, error) {

	return GetOrCreateN1QLClient2(server, bucketName, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION)

}

func GetOrCreateN1QLClient2(server, bucketName, scopeName, collectionName string) (datastore.Indexer, error) {
	var nc datastore.Indexer
	var err error
	var ok bool

	var keyspaceId string

	if scopeName == c.DEFAULT_SCOPE && collectionName == c.DEFAULT_COLLECTION {
		keyspaceId = bucketName
	} else {
		keyspaceId = strings.Join([]string{bucketName, scopeName, collectionName}, ":")
	}

	clientMapLock.RLock()
	nc, ok = n1qlClientMap[keyspaceId]
	clientMapLock.RUnlock()

	if !ok { // Client does not exist, so create new
		nc, err = nclient.NewGSIIndexer2(server, "default", bucketName, scopeName, collectionName, nil)
		if err != nil {
			return nil, err
		}
		clientMapLock.Lock()
		n1qlClientMap[keyspaceId] = nc
		clientMapLock.Unlock()
	}

	err = nc.Refresh()
	if err != nil {
		log.Printf("GetOrCreateN1QLClient2 error in Refresh(). err:%v", err)
	}
	return nc, err
}

func RemoveN1QLClientForBucket(server, bucketName string) {

	var nc datastore.Indexer
	var ok bool

	clientMapLock.RLock()
	nc, ok = n1qlClientMap[bucketName]
	clientMapLock.RUnlock()

	if !ok {
		return
	}

	nclient.CloseGsiKeyspace(nc)

	clientMapLock.Lock()
	delete(n1qlClientMap, bucketName)
	clientMapLock.Unlock()
}

// Creates an index and waits for it to become active
func N1QLCreateSecondaryIndex(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, indexActiveTimeoutSeconds int64) error {

	log.Printf("N1QLCreateSecondaryIndex :: server = %v", server)
	nc, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return err
	}

	logging.SetLogLevel(logging.Error)
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
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)
	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	var start time.Time
	go func() {
		l, h := skey2qkey(low), skey2qkey(high)
		rng := datastore.Range{Low: l, High: h, Inclusion: datastore.Inclusion(inclusion)}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		start = time.Now()
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	results, err2 := getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)
	elapsed := time.Since(start)
	tc.LogPerfStat("Range", elapsed)
	return results, err2
}

func N1QLLookup(indexName, bucketName, server string, values []interface{},
	distinct bool, limit int64, consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)
	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	var start time.Time
	go func() {
		l, h := skey2qkey(values), skey2qkey(values)
		rng := datastore.Range{Low: l, High: h, Inclusion: datastore.BOTH}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		start = time.Now()
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	results, err2 := getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)
	elapsed := time.Since(start)
	tc.LogPerfStat("Lookup", elapsed)
	return results, err2
}

func N1QLScanAll(indexName, bucketName, scopeName, collectionName, server string, limit int64,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient2(server, bucketName, scopeName, collectionName)
	if err != nil {
		return nil, err
	}

	if logLevel == logging.LogLevel(logging.Fatal) {
		logging.SetLogLevel(logging.Fatal)
	} else {
		logging.SetLogLevel(logging.Error)
	}

	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	var start time.Time
	go func() {
		rng := datastore.Range{Low: nil, High: nil, Inclusion: datastore.BOTH}
		span := &datastore.Span{Seek: nil, Range: rng}
		cons := getConsistency(consistency)
		start = time.Now()
		index.Scan(requestid, span, false, limit, cons, nil, conn)
	}()

	results, err2 := getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)
	elapsed := time.Since(start)
	tc.LogPerfStat("ScanAll", elapsed)
	return results, err2
}

func N1QLScans(indexName, bucketName, server string, scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)
	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}
	requestid := getrequestid()
	index, err := client.IndexByName(indexName)

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	index2, useScan2 := index.(datastore.Index2)
	if err != nil {
		return nil, err
	}

	var start time.Time
	go func() {
		spans2 := make(datastore.Spans2, len(scans))
		for i, scan := range scans {
			spans2[i] = &datastore.Span2{}
			if len(scan.Seek) != 0 {
				spans2[i].Seek = skey2qkey(scan.Seek)
			}
			spans2[i].Ranges = filtertoranges2(scan.Filter)
		}

		proj := projectionton1ql(projection)
		cons := getConsistency(consistency)
		ordered := true
		if useScan2 {
			start = time.Now()
			// TODO: pass the vector instead of nil.
			// Currently go tests do not pass timestamp vector
			index2.Scan2(requestid, spans2, reverse, distinct, ordered, proj,
				offset, limit, cons, nil, conn)
		} else {
			log.Fatalf("Indexer does not support Index2 API. Cannot call Scan2 method.")
		}
	}()

	results, err2 := getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)
	elapsed := time.Since(start)
	tc.LogPerfStat("MultiScan", elapsed)
	return results, err2
}

func N1QLMultiScanCount(indexName, bucketName, server string, scans qc.Scans, distinct bool,
	consistency c.Consistency, vector *qc.TsConsistency) (int64, error) {
	var count int64
	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return 0, err
	}

	logging.SetLogLevel(logging.Error)

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return 0, err1
	}

	index2, useScan2 := index.(datastore.CountIndex2)
	if err != nil {
		return 0, err
	}

	var start time.Time
	spans2 := make(datastore.Spans2, len(scans))
	for i, scan := range scans {
		spans2[i] = &datastore.Span2{}
		if len(scan.Seek) != 0 {
			spans2[i].Seek = skey2qkey(scan.Seek)
		}
		spans2[i].Ranges = filtertoranges2(scan.Filter)
	}

	cons := getConsistency(consistency)
	if useScan2 {
		start = time.Now()
		if distinct {
			// TODO: pass the vector instead of nil.
			// Currently go tests do not pass timestamp vector
			count, err = index2.CountDistinct(requestid, spans2, cons, nil)
		} else {
			count, err = index2.Count2(requestid, spans2, cons, nil)
		}
	} else {
		log.Fatalf("Indexer does not support CountIndex2 interface. Cannot call Count2 method.")
	}

	elapsed := time.Since(start)
	tc.LogPerfStat("MultiScanCount", elapsed)
	return count, err
}

func N1QLScan3(indexName, bucketName, server string, scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64, groupAggr *qc.GroupAggr,
	consistency c.Consistency, vector *qc.TsConsistency) (tc.ScanResponseActual, tc.GroupAggrScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, nil, err
	}

	logging.SetLogLevel(logging.Error)

	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}
	requestid := getrequestid()
	index, err := client.IndexByName(indexName)

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, nil, err1
	}

	index3, useScan3 := index.(datastore.Index3)
	if err != nil {
		return nil, nil, err
	}

	var start time.Time
	go func() {
		spans2 := make(datastore.Spans2, len(scans))
		for i, scan := range scans {
			spans2[i] = &datastore.Span2{}
			if len(scan.Seek) != 0 {
				spans2[i].Seek = skey2qkey(scan.Seek)
			}
			spans2[i].Ranges = filtertoranges2(scan.Filter)
		}

		proj := projectionton1ql(projection)
		groupAggregates := groupAggrton1ql(groupAggr)

		cons := getConsistency(consistency)

		if useScan3 {
			start = time.Now()
			// TODO: pass the vector instead of nil.
			// Currently go tests do not pass timestamp vector
			index3.Scan3(requestid, spans2, reverse, distinct, proj,
				offset, limit, groupAggregates, nil, cons, nil, conn)
			// TODO: Passing nil for IndexKeyOrders
		} else {
			log.Fatalf("Indexer does not support Index3 API. Cannot call Scan3 method.")
		}
	}()

	var results tc.ScanResponseActual
	var garesults tc.GroupAggrScanResponseActual
	var err2 error
	if groupAggr != nil {
		garesults = resultsforaggrgates(conn.Sender())
	} else {
		results, err2 = getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)
	}

	elapsed := time.Since(start)
	tc.LogPerfStat("MultiScan", elapsed)
	return results, garesults, err2
}

func N1QLScan6(indexName, bucketName, scope, coll, server string, scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64, groupAggr *qc.GroupAggr, consistency c.Consistency,
	vector *qc.TsConsistency, indexKeyNames []string, inlineFilter string, indexVector *datastore.IndexVector,
	indexPartionSets datastore.IndexPartitionSets) (tc.ScanResponseActual, error) {

	client, err := GetOrCreateN1QLClient2(server, bucketName, scope, coll)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)

	tctx := &testContext{}
	conn, err := datastore.NewSizedIndexConnection(100000, tctx)
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}
	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	index6, useScan6 := index.(datastore.Index6)
	if err != nil {
		return nil, err
	}

	var start time.Time
	go func() {
		spans2 := make(datastore.Spans2, len(scans))
		for i, scan := range scans {
			spans2[i] = &datastore.Span2{}
			if len(scan.Seek) != 0 {
				spans2[i].Seek = skey2qkey(scan.Seek)
			}
			spans2[i].Ranges = filtertoranges3(scan.Filter)
		}

		proj := projectionton1ql(projection)
		groupAggregates := groupAggrton1ql(groupAggr)

		cons := getConsistency(consistency)

		if useScan6 {
			start = time.Now()
			// Currently go tests do not pass timestamp vector
			index6.Scan6(requestid, spans2, reverse, distinct, proj,
				offset, limit, groupAggregates, nil, indexKeyNames, inlineFilter, indexVector,
				indexPartionSets, cons, nil, conn)
		} else {
			log.Fatalf("Indexer does not support Index6 API. Cannot call Scan6 method.")
		}
	}()

	var results tc.ScanResponseActual
	var err2 error
	results, err2 = getresultsfromsender(conn.Sender(), index.IsPrimary(), tctx)

	elapsed := time.Since(start)
	tc.LogPerfStat("Scan6", elapsed)
	return results, err2
}

func N1QLStorageStatistics(indexName, bucketName, server string) ([]map[string]interface{}, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	index4, useScan4 := index.(datastore.Index4)

	if !useScan4 {
		return nil, e.New("Index4 implementation unavailable")
	}

	stats, err2 := index4.StorageStatistics(requestid)
	statsArr := convertN1QLStats(stats)
	return statsArr, err2
}

func N1QLDefnStorageStatistics(indexName, bucketName, server string) (
	map[uint64][]map[string]interface{}, error) {

	client, err := GetOrCreateN1QLClient(server, bucketName)
	if err != nil {
		return nil, err
	}

	logging.SetLogLevel(logging.Error)

	requestid := getrequestid()
	index, err := client.IndexByName(indexName)
	if err != nil {
		return nil, err
	}

	var err1 error
	index, err1 = WaitForIndexOnline(client, indexName, index)
	if err1 != nil {
		return nil, err1
	}

	index6, useScan4 := index.(datastore.Index6)

	if !useScan4 {
		return nil, e.New("Index6 implementation unavailable")
	}

	stats, err2 := index6.DefnStorageStatistics(requestid)
	statsMap := convertN1QLStats2(stats)
	return statsMap, err2
}

func convertN1QLStats(stats []map[datastore.IndexStatType]value.Value) []map[string]interface{} {
	statsArr := make([]map[string]interface{}, 0)
	for _, stat := range stats {
		newMap := make(map[string]interface{})
		for key, val := range stat {
			newMap[string(key)] = val.ActualForIndex()
		}
		statsArr = append(statsArr, newMap)
	}
	return statsArr
}

func convertN1QLStats2(
	stats map[uint64][]map[datastore.IndexStatType]value.Value,
) map[uint64][]map[string]interface{} {

	var statsMap = make(map[uint64][]map[string]interface{})

	for instID, instStat := range stats {
		statsMap[instID] = convertN1QLStats(instStat)
	}
	return statsMap
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

func filtertoranges3(filters []*qc.CompositeElementFilter) datastore.Ranges2 {
	if filters == nil || len(filters) == 0 {
		return nil
	}
	ranges2 := make(datastore.Ranges2, len(filters))
	for i, cef := range filters {
		ranges2[i] = &datastore.Range2{}
		if cef.Low != nil {
			ranges2[i].Low = interfaceton1qlvalue(cef.Low)
		}
		if cef.High != nil {
			ranges2[i].High = interfaceton1qlvalue(cef.High)
		}
		ranges2[i].Inclusion = datastore.Inclusion(cef.Inclusion)
	}

	return ranges2
}

func groupAggrton1ql(groupAggs *qc.GroupAggr) *datastore.IndexGroupAggregates {
	if groupAggs == nil {
		return nil
	}

	//Group
	var groups datastore.IndexGroupKeys
	if groupAggs.Group != nil {
		groups = make(datastore.IndexGroupKeys, len(groupAggs.Group))
		for i, grp := range groupAggs.Group {
			expr, _ := parser.Parse(grp.Expr)
			g := &datastore.IndexGroupKey{
				EntryKeyId: int(grp.EntryKeyId),
				KeyPos:     int(grp.KeyPos),
				Expr:       expr,
			}
			groups[i] = g
		}
	}

	//Aggrs
	var aggregates datastore.IndexAggregates
	if groupAggs.Aggrs != nil {
		aggregates = make(datastore.IndexAggregates, len(groupAggs.Aggrs))
		for i, aggr := range groupAggs.Aggrs {
			expr, _ := parser.Parse(aggr.Expr)
			a := &datastore.IndexAggregate{
				Operation:  gsiaggrtypeton1ql(aggr.AggrFunc),
				EntryKeyId: int(aggr.EntryKeyId),
				KeyPos:     int(aggr.KeyPos),
				Expr:       expr,
				Distinct:   aggr.Distinct,
			}
			aggregates[i] = a
		}
	}

	var dependsOnIndexKeys []int
	if groupAggs.DependsOnIndexKeys != nil {
		dependsOnIndexKeys = make([]int, len(groupAggs.DependsOnIndexKeys))
		for i, ikey := range groupAggs.DependsOnIndexKeys {
			dependsOnIndexKeys[i] = int(ikey)
		}
	}

	ga := &datastore.IndexGroupAggregates{
		Name:               groupAggs.Name,
		Group:              groups,
		Aggregates:         aggregates,
		DependsOnIndexKeys: dependsOnIndexKeys,
		IndexKeyNames:      groupAggs.IndexKeyNames,
		AllowPartialAggr:   groupAggs.AllowPartialAggr,
	}

	return ga
}

func projectionton1ql(projection *qc.IndexProjection) *datastore.IndexProjection {
	if projection == nil {
		return nil
	}

	entrykeys := make([]int, 0, len(projection.EntryKeys))
	for _, ek := range projection.EntryKeys {
		entrykeys = append(entrykeys, int(ek))
	}

	n1qlProj := &datastore.IndexProjection{
		EntryKeys:  entrykeys,
		PrimaryKey: projection.PrimaryKey,
	}

	return n1qlProj
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

func getresultsfromsender(sender datastore.Sender, isprimary bool, tctx *testContext) (tc.ScanResponseActual, error) {

	scanResults := make(tc.ScanResponseActual)
	ok := true
	var err error
	for ok {
		entry, ok := sender.GetEntry()
		if ok {
			// GetEntry() returns:
			// (1) nil, true    - in case of all done
			// (2) nil, false   - in case of explicit stop
			if isprimary {
				if entry != nil {
					scanResults[entry.PrimaryKey] = make([]value.Value, 0)
				} else {
					break
				}
			} else {
				if entry != nil {
					scanResults[entry.PrimaryKey] = entry.EntryKey
				} else {
					break
				}
			}
		} else {
			break
		}
	}

	if tctx.err != nil {
		err = tctx.err
	}

	return scanResults, err
}

func resultsforaggrgates(sender datastore.Sender) tc.GroupAggrScanResponseActual {
	scanResults := make(tc.GroupAggrScanResponseActual, 0)
	ok := true
	for ok {
		entry, ok := sender.GetEntry()
		if ok {
			if entry == nil {
				break
			}
			log.Printf("Scanresult Row  %v : %v ", entry.EntryKey, entry.PrimaryKey)
			scanResults = append(scanResults, entry.EntryKey)
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
		if key == c.MinUnbounded || key == c.MaxUnbounded {
			return nil
		}
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
	if len(vals) == 0 {
		return nil
	}
	skey := make(c.SecondaryKey, 0, len(vals))
	for _, val := range []value.Value(vals) {
		skey = append(skey, val.ActualForIndex())
	}
	return skey
}

func gsiaggrtypeton1ql(gsiaggr c.AggrFuncType) datastore.AggregateType {
	switch gsiaggr {
	case c.AGG_MIN:
		return datastore.AGG_MIN
	case c.AGG_MAX:
		return datastore.AGG_MAX
	case c.AGG_SUM:
		return datastore.AGG_SUM
	case c.AGG_COUNT:
		return datastore.AGG_COUNT
	case c.AGG_COUNTN:
		return datastore.AGG_COUNTN
	}
	return datastore.AGG_COUNT
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// BEGIN testContext and its Query Context interface stubs
////////////////////////////////////////////////////////////////////////////////////////////////////

// testContext is a mock of query/datastore/context.go's Context interface and thus implements stubs
// of all methods in that interface.
type testContext struct {
	err error
}

func (ctxt *testContext) GetScanCap() int64 {
	return 512 // Default index scan request size
}

func (ctxt *testContext) Error(err errors.Error) {
	fmt.Printf("Scan error: %v\n", err)
	ctxt.err = err.GetICause()
}

func (ctxt *testContext) Warning(wrn errors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *testContext) Fatal(fatal errors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}

func (ctxt *testContext) MaxParallelism() int {
	return 1
}

func (ctxt *testContext) GetReqDeadline() time.Time {
	return time.Time{}
}

// RecordFtsRU added for Elixir
func (ctxt *testContext) RecordFtsRU(ru tenant.Unit) {
}

// RecordGsiRU added for Elixir
func (ctxt *testContext) RecordGsiRU(ru tenant.Unit) {
}

// RecordKvRU added for Elixir
func (ctxt *testContext) RecordKvRU(ru tenant.Unit) {
}

// RecordKvWU added for Elixir
func (ctxt *testContext) RecordKvWU(wu tenant.Unit) {
}

func (ctxt *testContext) Credentials() *auth.Credentials {
	return nil
}

func (ctxt *testContext) SkipKey(key string) bool {
	return false
}

func (ctxt *testContext) TenantCtx() tenant.Context {
	return nil
}

func (ctxt *testContext) SetFirstCreds(cred string) {
}

func (ctxt *testContext) FirstCreds() (string, bool) {
	return "", true
}

func (ctxt *testContext) GetErrors() []errors.Error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// END testContext and its Query Context interface stubs
////////////////////////////////////////////////////////////////////////////////////////////////////

func WaitForIndexOnline(n1qlclient datastore.Indexer, indexName string, index datastore.Index) (datastore.Index, error) {

	var err error
	for i := 0; i < 30; i++ {
		if s, _, _ := index.State(); s == datastore.ONLINE {
			return index, nil
		}

		time.Sleep(1 * time.Second)
		if err := n1qlclient.Refresh(); err != nil {
			return nil, err
		}

		index, err = n1qlclient.IndexByName(indexName)
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("index %v fails to come online after 30s", indexName)
}

func ChangeQuerySettings(configKey string, configValue interface{}, serverUserName, serverPassword,
	hostaddress string) error {

	host, port, err := net.SplitHostPort(hostaddress)
	if err != nil {
		return fmt.Errorf("Failed to split hostport `%s': %s", hostaddress, err)
	}
	reqUrl := fmt.Sprintf("http://%s:%s/%s", host, port, "settings/querySettings")

	if len(configKey) > 0 {
		log.Printf("Changing config key %v to value %v\n", configKey, configValue)
		pbody := url.Values{}
		pbody.Set(configKey, fmt.Sprintf("%v", configValue))

		preq, err := http.NewRequest("POST", reqUrl, bytes.NewBuffer([]byte(pbody.Encode())))
		if err != nil {
			return err
		}

		preq.SetBasicAuth(serverUserName, serverPassword)
		preq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		err = preq.ParseForm()
		if err != nil {
			return err
		}

		client := http.Client{}
		resp, err := client.Do(preq)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		respStr, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		newSetting := make(map[string]interface{})
		err = json.Unmarshal(respStr, &newSetting)
		if err != nil {
			return err
		}

		newValue, ok := newSetting[configKey]
		if !ok {
			return fmt.Errorf("did not get a response back for the change of key:%v", configKey)
		}

		// convert all the numeric values to uint64
		// keep other as it is
		switch typedNewValue := newValue.(type) {
		case int:
			newValue = uint64(typedNewValue)
		case int8:
			newValue = uint64(typedNewValue)
		case int16:
			newValue = uint64(typedNewValue)
		case int32:
			newValue = uint64(typedNewValue)
		case int64:
			newValue = uint64(typedNewValue)
		case uint:
			newValue = uint64(typedNewValue)
		case uint8:
			newValue = uint64(typedNewValue)
		case uint16:
			newValue = uint64(typedNewValue)
		case uint32:
			newValue = uint64(typedNewValue)
		case uint64:
			newValue = typedNewValue
		case float32:
			newValue = uint64(typedNewValue)
		case float64:
			newValue = uint64(typedNewValue)
		default:
		}

		if reflect.TypeOf(configValue).Kind() != reflect.TypeOf(newValue).Kind() || fmt.Sprintf("%v", newValue) != fmt.Sprintf("%v", configValue) {
			return fmt.Errorf("couldn't change %v to %v. curr value %v", configKey, configValue, newValue)
		}
	}

	return nil
}
