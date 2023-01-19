package functionaltests

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/queryport/n1ql"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	qerrors "github.com/couchbase/query/errors"
	qexpr "github.com/couchbase/query/expression"
	qparser "github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/tenant"
	//"github.com/couchbase/query/value"
)

func TestBufferedScan_BackfillDisabled(t *testing.T) {
	log.Printf("In TestBufferedScan_BackfillDisabled()")

	var indexName = "addressidx"
	var bucketName = "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(50000, "users.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	// Disable backfill
	err := secondaryindex.ChangeIndexerSettings("queryport.client.settings.backfillLimit", float64(0), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	cluster, err := c.ClusterAuthUrl(indexManagementAddress)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed in getting ClusterAuthUrl", t)
	}

	n1qlclient, err := n1ql.NewGSIIndexer(cluster, "default" /*namespace*/, "default", nil)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed in creating n1ql client", t)
	}
	defer n1ql.CloseGsiKeyspace(n1qlclient)

	rangeKey := []string{`address`}
	rangeExprs := make(qexpr.Expressions, 0)
	for _, key := range rangeKey {
		expr, err := qparser.Parse(key)
		if err != nil {
			err = fmt.Errorf("CompileN1QLExpression() %v: %v\n", key, err)
			FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed", t)
		}
		rangeExprs = append(rangeExprs, expr)
	}

	// create index if not created already.
	index, err := n1qlclient.IndexByName(indexName)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed in getting IndexByName", t)
	}
	index, err = secondaryindex.WaitForIndexOnline(n1qlclient, indexName, index)
	FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed in waiting for index on line", t)

	// query setup
	//low := value.Values{value.NewValue("A")}
	//high := value.Values{value.NewValue("zzzz")}
	rng := datastore.Range{Low: nil, High: nil, Inclusion: datastore.BOTH}
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

	// ******* query with limit 1 and read slow
	cleanbackfillFiles()

	// No backfill files should show up here.
	printBackfillFilesInfo()

	ctxt := &qcmdContext{}
	conn, err := datastore.NewSizedIndexConnection(256, ctxt)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed", t)
	}
	count, sender := 0, conn.Sender()
	now := time.Now()
	go doquery(int64(1), conn)
	for {
		entry, ok := sender.GetEntry()
		if !ok {
			break
		}
		if entry == nil {
			break
		}

		time.Sleep(1 * time.Millisecond)
		count++
	}

	log.Printf("limit=1,chsize=256; received %v items; took %v\n",
		count, time.Since(now))
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Unexpected backfill file")
		FailTestIfError(e, "TestBufferedScan_BackfillDisabled failed", t)
	} else if ctxt.err != nil {
		FailTestIfError(ctxt.err, "TestBufferedScan_BackfillDisabled failed", t)
	}

	// ******* query with limit 1000, channel buffer is 256
	cleanbackfillFiles()
	ctxt = &qcmdContext{}
	conn, err = datastore.NewSizedIndexConnection(256, ctxt)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillDisabled failed", t)
	}
	count, sender = 0, conn.Sender()
	now = time.Now()
	go doquery(int64(1000), conn)
	for {
		entry, ok := sender.GetEntry()
		if !ok {
			break
		}
		if entry == nil {
			break
		}

		time.Sleep(1 * time.Millisecond)
		count++
	}
	log.Printf("limit=1000,chsize=256; received %v items; took %v\n",
		count, time.Since(now))
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Unexpected backfill file")
		FailTestIfError(e, "TestBufferedScan_BackfillDisabled failed", t)
	} else if ctxt.err != nil {
		FailTestIfError(ctxt.err, "TestBufferedScan_BackfillDisabled failed", t)
	}
}

func TestBufferedScan_BackfillEnabled(t *testing.T) {
	log.Printf("In TestBufferedScan_BackfillEnabled()")

	var indexName = "addressidx"

	err := secondaryindex.ChangeIndexerSettings("queryport.client.settings.backfillLimit", float64(1), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	defer func() {
		err = secondaryindex.ChangeIndexerSettings("queryport.client.settings.backfillLimit", float64(0), clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in ChangeIndexerSettings")
	}()

	cluster, err := c.ClusterAuthUrl(indexManagementAddress)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed in getting ClusterAuthUrl", t)
	}

	n1qlclient, err := n1ql.NewGSIIndexer(cluster, "default" /*namespace*/, "default", nil)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed in creating n1ql client", t)
	}
	defer n1ql.CloseGsiKeyspace(n1qlclient)

	rangeKey := []string{`address`}
	rangeExprs := make(qexpr.Expressions, 0)
	for _, key := range rangeKey {
		expr, err := qparser.Parse(key)
		if err != nil {
			err = fmt.Errorf("CompileN1QLExpression() %v: %v\n", key, err)
			FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed", t)
		}
		rangeExprs = append(rangeExprs, expr)
	}

	// create index if not created already.
	index, err := n1qlclient.IndexByName(indexName)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed in getting IndexByName", t)
	}
	index, err = secondaryindex.WaitForIndexOnline(n1qlclient, indexName, index)
	FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed in waiting for index on line", t)

	// query setup
	//low := value.Values{value.NewValue("A")}
	//high := value.Values{value.NewValue("zzzz")}
	rng := datastore.Range{Low: nil, High: nil, Inclusion: datastore.BOTH}
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

	// ******* Case 1
	// cap(ch) == 256 & limit 1, read slow
	// no file should be created and we get valid results
	cleanbackfillFiles()
	ctxt := &qcmdContext{}
	conn, err := datastore.NewSizedIndexConnection(256, ctxt)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed", t)
	}
	count, sender := 0, conn.Sender()
	now := time.Now()
	go doquery(int64(1), conn)
	for {
		entry, ok := sender.GetEntry()
		if !ok {
			break
		}
		if entry == nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
		count++
	}

	log.Printf("limit=1,chsize=256; received %v items; took %v\n",
		count, time.Since(now))
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Unexpected backfill file")
		FailTestIfError(e, "TestBufferedScan_BackfillEnabled failed", t)
	} else if ctxt.err != nil {
		FailTestIfError(ctxt.err, "TestBufferedScan_BackfillEnabled failed", t)
	}

	// ******* Case 2
	// cap(ch) == 256 & limit 1000, read fast
	// no file should be created and we get valid results
	cleanbackfillFiles()
	ctxt = &qcmdContext{}
	conn, err = datastore.NewSizedIndexConnection(256, ctxt)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed", t)
	}
	count, sender = 0, conn.Sender()
	now = time.Now()
	go doquery(int64(1000), conn)
	for {
		entry, ok := sender.GetEntry()
		if !ok {
			break
		}
		if entry == nil {
			break
		}

		count++
	}
	log.Printf("limit=1000,chsize=256; received %v items; took %v\n",
		count, time.Since(now))
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Unexpected backfill file")
		FailTestIfError(e, "TestBufferedScan_BackfillEnabled failed", t)
	} else if ctxt.err != nil {
		FailTestIfError(ctxt.err, "TestBufferedScan_BackfillEnabled failed", t)
	}

	// ******* Case 3
	// cap(ch) == 256 & limit 1000 & read slow,
	// file should be created and we get valid results and file is deleted.
	cleanbackfillFiles()
	ctxt = &qcmdContext{}
	conn, err = datastore.NewSizedIndexConnection(256, ctxt)
	if err != nil {
		FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed", t)
	}

	count, sender = 0, conn.Sender()
	go doquery(int64(1000), conn)
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) != 1 {
		printBackfillFilesInfo()
		e := errors.New("Expected one backfill file")
		FailTestIfError(e, "TestBufferedScan_BackfillEnabled failed", t)
	}
	now = time.Now()
	for {
		entry, ok := sender.GetEntry()
		if !ok {
			break
		}
		if entry == nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
		count++
	}
	log.Printf("limit=1000,chsize=256; received %v items; took %v\n",
		count, time.Since(now))
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Expected backfill file to be deleted")
		FailTestIfError(e, "TestBufferedScan_BackfillEnabled failed", t)
	} else if ctxt.err != nil {
		FailTestIfError(ctxt.err, "TestBufferedScan_BackfillEnabled failed", t)
	}

	// ******* Case 4
	// cap(ch) == 256 & limit 50000 & 2 concur request, read slow,
	// file should be created and error out, and file is deleted
	cleanbackfillFiles()
	concur := 2
	donech := make(chan *qcmdContext, concur)
	for i := 0; i < concur; i++ {
		go func(donech chan *qcmdContext) {
			ctxt := &qcmdContext{}
			conn, err := datastore.NewSizedIndexConnection(256, ctxt)
			if err != nil {
				FailTestIfError(err, "TestBufferedScan_BackfillEnabled failed", t)
			}

			count, sender := 0, conn.Sender()
			go doquery(int64(50000), conn)
			now := time.Now()
			for {
				entry, ok := sender.GetEntry()
				if !ok {
					break
				}
				if entry == nil {
					break
				}

				time.Sleep(20 * time.Millisecond)
				count++
			}
			log.Printf("limit=1000,chsize=256; received %v items; took %v\n",
				count, time.Since(now))
			donech <- ctxt
		}(donech)
	}
	// wait for it to complete
	for i := 0; i < concur; i++ {
		ctxt := <-donech
		if ctxt.err == nil {
			fmsg := "TestBufferedScan_BackfillEnabled expected error"
			FailTestIfError(errors.New("expected"), fmsg, t)
		}
	}
	time.Sleep(1 * time.Second)
	if len(getbackfillFiles(backfillDir())) > 0 {
		printBackfillFilesInfo()
		e := errors.New("Expected backfill file to be deleted")
		FailTestIfError(e, "TestBufferedScan_BackfillEnabled failed", t)
	}
}

type qcmdContext struct {
	err error
}

func (ctxt *qcmdContext) GetScanCap() int64 {
	return 512 // Default index scan request size
}

func (ctxt *qcmdContext) Error(err qerrors.Error) {
	ctxt.err = err
	fmt.Printf("Scan error: %v\n", err)
}

func (ctxt *qcmdContext) Warning(wrn qerrors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *qcmdContext) Fatal(fatal qerrors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}

func (ctxt *qcmdContext) MaxParallelism() int {
	return 1
}

func (ctxt *qcmdContext) GetReqDeadline() time.Time {
	return time.Time{}
}

// RecordFtsRU added for Elixir
func (ctxt *qcmdContext) RecordFtsRU(ru tenant.Unit) {
}

// RecordGsiRU added for Elixir
func (ctxt *qcmdContext) RecordGsiRU(ru tenant.Unit) {
}

// RecordKvRU added for Elixir
func (ctxt *qcmdContext) RecordKvRU(ru tenant.Unit) {
}

// RecordKvWU added for Elixir
func (ctxt *qcmdContext) RecordKvWU(wu tenant.Unit) {
}

func (ctxt *qcmdContext) Credentials() *auth.Credentials {
	return nil
}

func (ctxt *qcmdContext) SkipKey(key string) bool {
	return false
}

func (ctxt *qcmdContext) TenantCtx() tenant.Context {
	return nil
}

func (ctxt *qcmdContext) SetFirstCreds(creds string) {
}

func (ctxt *qcmdContext) FirstCreds() (string, bool) {
	return "", true
}

func cleanbackfillFiles() {
	dir := backfillDir()
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Printf("Error (%v) in getting listing for backfill directory (%s)\n", err, dir)
		return
	}

	for _, file := range files {
		fname := path.Join(dir, file.Name())

		// The backfill dir for the tests may include files created by other programs. Avoid removing them.
		if strings.Contains(fname, n1ql.BACKFILLPREFIX) {
			log.Printf("Removing backfill file (%s)\n", fname)
			if err = os.Remove(fname); err != nil {
				log.Printf("Error (%v) in removing backfill file (%s)\n", err, fname)
			}
		}
	}
}

func getbackfillFiles(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Printf("Error (%v) in getting listing for backfill directory (%s)\n", err, dir)
		return nil
	}
	rv := make([]string, 0)
	for _, file := range files {
		fname := path.Join(dir, file.Name())
		if strings.Contains(fname, n1ql.BACKFILLPREFIX) {
			rv = append(rv, fname)
		}
	}

	return rv
}

func backfillDir() string {
	file, err := ioutil.TempFile("" /*dir*/, "onci")
	if err != nil {
		tc.HandleError(err, "Error in getting backfill dir")
	}
	dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file.
	return dir
}

func process_response_delay(n int) {
	count := float64(0)
	for i := 0; i < n; i++ {
		count *= float64(i)
	}
}

func printBackfillFilesInfo() {
	dir := backfillDir()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Printf("Error (%v) in getting listing for backfill directory\n", err)
	}

	for _, file := range files {
		fname := path.Join(dir, file.Name())
		if strings.Contains(fname, n1ql.BACKFILLPREFIX) {
			finfo, err := os.Stat(fname)
			if err != nil {
				log.Printf("Backfill file found. Error (%v) in getting stat "+
					"info about file %v\n", err, fname)
			} else {
				log.Printf("Backfill file found %v. File size: %v, Last "+
					"modified time: %v\n", fname, finfo.Size(), finfo.ModTime())
			}
		} else {
			log.Printf("Non-backfill file found: %v\n", fname)
		}
	}
}
