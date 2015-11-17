package main

import "fmt"
import "time"

import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/queryport/n1ql"
import c "github.com/couchbase/indexing/secondary/common"
import qparser "github.com/couchbase/query/expression/parser"
import qexpr "github.com/couchbase/query/expression"
import "github.com/couchbase/query/value"
import "github.com/couchbase/query/errors"
import "github.com/couchbase/query/datastore"

// expects `default` bucket to be created and available from the
// cluster, the document should contain the `company` field.
func doBufferedScan(cluster string, client *qclient.GsiClient) (err error) {
	cluster, err = c.ClusterAuthUrl(cluster)
	if err != nil {
		return err
	}
	gsi, err := n1ql.NewGSIIndexer(cluster, "default" /*namespace*/, "default")
	if err != nil {
		return err
	}
	rangeKey := []string{`company`}
	rangeExprs := make(qexpr.Expressions, 0)
	for _, key := range rangeKey {
		expr, err := qparser.Parse(key)
		if err != nil {
			err = fmt.Errorf("CompileN1QLExpression() %v: %v\n", key, err)
			return err
		}
		rangeExprs = append(rangeExprs, expr)
	}

	// create index if not created already.
	index, err := gsi.IndexByName("companyidx")
	if err != nil {
		index, err = gsi.CreateIndex(
			"bufferedscan", "companyidx",
			nil,        // seekKey
			rangeExprs, // rangeKey
			nil,        // where
			nil,        // with
		)
	}
	state := datastore.OFFLINE
	for state != datastore.ONLINE {
		state, _, err = index.State()
		time.Sleep(100 * time.Millisecond)
	}

	// query setup
	low := value.Values{value.NewValue("A")}
	high := value.Values{value.NewValue("z")}
	rng := datastore.Range{Low: low, High: high, Inclusion: datastore.BOTH}
	span := &datastore.Span{Seek: nil, Range: rng}
	doquery := func(limit int64, conn *datastore.IndexConnection) {
		index.Scan(
			"bufferedscan", /*requestId*/
			span,
			false, /*distinct*/
			limit,
			datastore.UNBOUNDED,
			nil,
			conn,
		)
	}

	// query with limit 1
	conn, err := datastore.NewSizedIndexConnection(1, &qcmdContext{})
	if err != nil {
		return err
	}
	count, ch := 0, conn.EntryChannel()
	now := time.Now()
	go doquery(int64(1), conn)
	for range ch {
		count++
	}
	fmsg := "limit=1,chsize=1; received %v items; took %v\n"
	fmt.Printf(fmsg, count, time.Since(now))

	// query with limit 1000000, channel buffer is 1000000
	conn, err = datastore.NewSizedIndexConnection(1000000, &qcmdContext{})
	if err != nil {
		return err
	}
	count, ch = 0, conn.EntryChannel()
	now = time.Now()
	go doquery(int64(1000000), conn)
	for range ch {
		count++
	}
	fmsg = "limit=1000000,chsize=1000000; received %v items; took %v\n"
	fmt.Printf(fmsg, count, time.Since(now))

	// query with limit 1000000, channel buffer is 256
	conn, err = datastore.NewSizedIndexConnection(256, &qcmdContext{})
	if err != nil {
		return err
	}
	count, ch = 0, conn.EntryChannel()
	now = time.Now()
	go doquery(int64(1000000), conn)
	for range ch {
		count++
	}
	fmsg = "limit=1000000,chsize=256; received %v items; took %v\n"
	fmt.Printf(fmsg, count, time.Since(now))
	return nil
}

type qcmdContext struct{}

func (ctxt *qcmdContext) Error(err errors.Error) {
	fmt.Printf("Scan error: %v\n", err)
}

func (ctxt *qcmdContext) Warning(wrn errors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *qcmdContext) Fatal(fatal errors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}
