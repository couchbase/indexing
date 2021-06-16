package querycmd

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"

	c "github.com/couchbase/indexing/secondary/common"

	mclient "github.com/couchbase/indexing/secondary/manager/client"

	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

var batchProcess bool

// Command object containing parsed result from command-line
// or program constructued list of args.
type Command struct {
	OpType string
	// basic options.
	Server    string
	IndexName string
	Bucket    string
	AdminPort string
	QueryPort string
	Auth      string
	// options for create-index.
	Using         string
	ExprType      string
	PartnStr      string
	WhereStr      string
	SecStrs       []string
	IsPrimary     bool
	With          string
	WithPlan      map[string]interface{}
	Scheme        c.PartitionScheme
	PartitionKeys []string
	// options for build index
	Bindexes []string
	// options for Range, Statistics, Count
	Low         c.SecondaryKey
	High        c.SecondaryKey
	Equal       c.SecondaryKey
	Inclusion   qclient.Inclusion
	Limit       int64
	Distinct    bool
	Consistency c.Consistency
	// Configuration
	ConfigKey string
	ConfigVal string
	Help      bool

	Scope      string
	Collection string

	// Refresh settings during startup
	RefreshSettings bool

	// Batch process cbindex commands
	BatchProcessFile string

	// Time to wait until client bootstraps
	WaitForClientBootstrap int64

	NumBuilds int64
}

// ParseArgs into Command object, return the list of arguments,
// flagset used for parseing and error if any.
func ParseArgs(arguments []string) (*Command, []string, *flag.FlagSet, error) {
	var fields, bindexes, partitionKeys string
	var inclusion uint
	var equal, low, high, scheme string
	var useSessionCons bool

	cmdOptions := &Command{Consistency: c.AnyConsistency}
	fset := flag.NewFlagSet("cmd", flag.ExitOnError)

	// basic options
	fset.StringVar(&cmdOptions.Server, "server", "127.0.0.1:8091", "Cluster server address")
	fset.StringVar(&cmdOptions.Auth, "auth", "", "Auth user and password")
	fset.StringVar(&cmdOptions.Bucket, "bucket", "", "Bucket name")
	fset.StringVar(&cmdOptions.OpType, "type", "", "Command: scan|stats|scanAll|count|nodes|create|build|move|drop|list|config|batch_process|batch_build")
	fset.StringVar(&cmdOptions.IndexName, "index", "", "Index name")
	// options for create-index
	fset.StringVar(&cmdOptions.WhereStr, "where", "", "where clause for create index")
	fset.StringVar(&fields, "fields", "", "Comma separated on-index fields") // secStrs
	fset.BoolVar(&cmdOptions.IsPrimary, "primary", false, "Is primary index")
	fset.StringVar(&cmdOptions.With, "with", "", "index specific properties")
	// options for build-indexes, move-indexes, drop-indexes
	fset.StringVar(&bindexes, "indexes", "", "csv list of bucket:index to build")
	// options for Range, Statistics, Count
	fset.StringVar(&low, "low", "[]", "Span.Range: [low]")
	fset.StringVar(&high, "high", "[]", "Span.Range: [high]")
	fset.StringVar(&equal, "equal", "", "Span.Lookup: [key]")
	fset.UintVar(&inclusion, "incl", 0, "Range: 0|1|2|3")
	fset.Int64Var(&cmdOptions.Limit, "limit", 10, "Row limit")
	fset.BoolVar(&cmdOptions.Distinct, "distinct", false, "Only distinct entries")
	fset.BoolVar(&cmdOptions.Help, "h", false, "print help")
	fset.BoolVar(&useSessionCons, "consistency", false, "Use session consistency")
	// options for setting configuration
	fset.StringVar(&cmdOptions.ConfigKey, "ckey", "", "Config key")
	fset.StringVar(&cmdOptions.ConfigVal, "cval", "", "Config value")
	fset.StringVar(&cmdOptions.Using, "using", "gsi", "storage type to use")

	//collection specific
	fset.StringVar(&cmdOptions.Scope, "scope", "", "Scope name")
	fset.StringVar(&cmdOptions.Collection, "collection", "", "Collection name")

	fset.BoolVar(&cmdOptions.RefreshSettings, "refresh_settings", false, "When true, will read settings from metakv when instantiating client")

	// Input file for batch processing
	fset.StringVar(&cmdOptions.BatchProcessFile, "input", "", "Path to the file containing batch processing commands")

	fset.Int64Var(&cmdOptions.WaitForClientBootstrap, "bootstrap_wait", 60, "Time (in seconds) cbindex will wait for client bootstrap")
	fset.Int64Var(&cmdOptions.NumBuilds, "num_builds", 10, "Number of builds that can happen simultaneously across multiple collections")

	fset.StringVar(&scheme, "scheme", string(c.SINGLE), "Partition scheme for partitioned index.")
	fset.StringVar(&partitionKeys, "partitionKeys", "", "Comma separated fields for partition key for partitioned index.")

	// not useful to expose in sherlock
	cmdOptions.ExprType = "N1QL"
	cmdOptions.PartnStr = ""

	if err := fset.Parse(arguments); err != nil {
		return nil, nil, fset, err
	}

	if useSessionCons {
		cmdOptions.Consistency = c.SessionConsistency
	}

	// validate combinations
	err := validate(cmdOptions, fset)
	if err != nil {
		return nil, nil, fset, err
	}
	// bindexes
	if len(bindexes) > 0 {
		cmdOptions.Bindexes = strings.Split(bindexes, ",")
	}

	// inclusion, secStrs, equal, low, high
	cmdOptions.Inclusion = qclient.Inclusion(inclusion)
	cmdOptions.SecStrs = make([]string, 0)
	if fields != "" {
		for _, field := range strings.Split(fields, ",") {
			expr, err := n1ql.ParseExpression(field)
			if err != nil {
				msgf := "Error occured: Invalid field (%v) %v\n"
				return nil, nil, fset, fmt.Errorf(msgf, field, err)
			}
			secStr := expression.NewStringer().Visit(expr)
			cmdOptions.SecStrs = append(cmdOptions.SecStrs, secStr)
		}
	}
	if equal != "" {
		cmdOptions.Equal = c.SecondaryKey(Arg2Key([]byte(equal)))
	}
	cmdOptions.Low = c.SecondaryKey(Arg2Key([]byte(low)))
	cmdOptions.High = c.SecondaryKey(Arg2Key([]byte(high)))

	// Populate partition keys and scheme
	cmdOptions.Scheme = c.PartitionScheme(scheme)
	switch cmdOptions.Scheme {

	case c.KEY:
		if partitionKeys == "" {
			logging.Errorf("Missing input partition keys")
			os.Exit(1)
		}

		cmdOptions.PartitionKeys = make([]string, 0)
		if partitionKeys != "" {
			for _, partnKey := range strings.Split(partitionKeys, ",") {
				key, err := n1ql.ParseExpression(partnKey)
				if err != nil {
					msgf := "Error in ParseExpression: Invalid partn key (%v) error:%v\n"
					return nil, nil, fset, fmt.Errorf(msgf, partnKey, err)
				}

				keyStr := expression.NewStringer().Visit(key)
				cmdOptions.PartitionKeys = append(cmdOptions.PartitionKeys, keyStr)
			}
		}

	case c.SINGLE:
		if partitionKeys != "" {
			logging.Errorf("Unexpected partition keys %v for partition scheme %v", partitionKeys, cmdOptions.Scheme)
			os.Exit(1)
		}

	default:
		logging.Errorf("Unsupported partition scheme %v", cmdOptions.Scheme)
		os.Exit(1)
	}

	// with
	if len(cmdOptions.With) > 0 {
		err := json.Unmarshal([]byte(cmdOptions.With), &cmdOptions.WithPlan)
		if err != nil {
			logging.Fatalf("%v\n", err)
			os.Exit(1)
		}
	}

	// setup cbauth
	if cmdOptions.Auth != "" {
		up := strings.Split(cmdOptions.Auth, ":")
		_, err := cbauth.InternalRetryDefaultInit(cmdOptions.Server, up[0], up[1])
		if err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s\n", err)
			os.Exit(1)
		}
	}

	return cmdOptions, fset.Args(), fset, err
}

// HandleCommand after parsing it with ParseArgs().
func HandleCommand(
	client *qclient.GsiClient,
	cmd *Command,
	verbose bool,
	w io.Writer) (err error) {

	iname, bucket, limit, distinct := cmd.IndexName, cmd.Bucket, cmd.Limit, cmd.Distinct
	low, high, equal, incl := cmd.Low, cmd.High, cmd.Equal, cmd.Inclusion
	cons := cmd.Consistency

	scope, collection := cmd.Scope, cmd.Collection
	if scope == "" {
		scope = c.DEFAULT_SCOPE
	}
	if collection == "" {
		collection = c.DEFAULT_COLLECTION
	}

	indexes, _, _, _, err := client.Refresh()

	dataEncFmt := client.GetDataEncodingFormat()

	var tmpbuf *[]byte
	var tmpbufPoolIdx uint32

	entries := 0
	callb := func(res qclient.ResponseReader) bool {
		if res.Error() != nil {
			fmt.Fprintln(w, "Error: ", res)
			return true
		}

		var skeys *c.ScanResultEntries
		var pkeys [][]byte
		var err error

		if skeys, pkeys, err = res.GetEntries(dataEncFmt); err != nil {
			fmt.Fprintln(w, "Error: ", err)
		} else {
			if verbose == false {
				for i, pkey := range pkeys {
					skey, err, retBuf := skeys.Getkth(tmpbuf, i)
					if err != nil {
						fmt.Fprint(w, "Error: %v", err)
						return false
					}
					if retBuf != nil {
						tmpbuf = retBuf
					}
					fmt.Fprintf(w, "%v ... %v\n", skey, string(pkey))
				}
			}
			entries += len(pkeys)
		}
		return true
	}

	processIndexName := func(indexName string) (string, string, string, string, error) {
		v := strings.Split(indexName, ":")
		if len(v) < 0 {
			return "", "", "", "", fmt.Errorf("invalid index specified : %v", indexName)
		}

		scope = ""
		collection = ""
		if len(v) == 4 {
			bucket, scope, collection, iname = v[0], v[1], v[2], v[3]
		} else if len(v) == 2 {
			bucket, iname = v[0], v[1]
		}
		if scope == "" {
			scope = c.DEFAULT_SCOPE
		}
		if collection == "" {
			collection = c.DEFAULT_COLLECTION
		}
		return bucket, scope, collection, iname, nil
	}

	validateBatchFile := func(cmd *Command) (*os.File, error) {
		if batchProcess == false {
			batchProcess = true
		} else {
			err := fmt.Errorf("Nested batch processing is not supported")
			logging.Errorf(err.Error())
			return nil, err
		}
		// Read input file from input option
		if cmd.BatchProcessFile == "" {
			fmsg := "Empty file name for batch processing\n"
			logging.Errorf(fmsg)
			return nil, err
		}
		fd, err := os.Open(cmd.BatchProcessFile)
		if err != nil {
			logging.Errorf("Unable to open commands file %q, err: %v\n", cmd.BatchProcessFile, err)
			return nil, err
		}
		if cmd.WaitForClientBootstrap > 0 {
			// TODO: This sleep is added only to allow perf QE team to get
			// unblocked. Should be removed after client side logic is modified
			// to handle watcher sync-up with index metadata
			fmt.Fprintf(w, "cbindex Sleeping for %v seconds to allow client to catch-up with index meta", cmd.WaitForClientBootstrap)
			time.Sleep(time.Duration(cmd.WaitForClientBootstrap) * time.Second)
			fmt.Fprintf(w, "cbindex starting to process batch file: %v", cmd.BatchProcessFile)
		}
		return fd, nil
	}

	switch cmd.OpType {
	case "nodes":
		fmt.Fprintln(w, "List of nodes:")
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		for _, n := range nodes {
			fmsg := "    {%v, %v, %q}\n"
			fmt.Fprintf(w, fmsg, n.Adminport, n.Queryport, n.Status)
		}

	case "list":
		time.Sleep(2 * time.Second)
		indexes, _, _, _, err = client.Refresh()
		if err != nil {
			return err
		}
		fmt.Fprintln(w, "List of indexes:")
		for _, index := range indexes {
			printIndexInfo(w, index)
		}

	case "create":
		var defnID uint64
		if len(cmd.SecStrs) == 0 && !cmd.IsPrimary || cmd.IndexName == "" {
			return fmt.Errorf("createIndex(): required fields missing")
		}
		defnID, err = client.CreateIndex4(
			iname, bucket, scope, collection, cmd.Using, cmd.ExprType,
			cmd.WhereStr, cmd.SecStrs, nil, cmd.IsPrimary, cmd.Scheme, cmd.PartitionKeys,
			[]byte(cmd.With))
		if err == nil {
			fmt.Fprintf(w, "Index created: name: %q, ID: %v, WITH clause used: %q\n",
				iname, defnID, cmd.With)
		}

	case "build":
		defnIDs := make([]uint64, 0, len(cmd.Bindexes))
		for _, bindex := range cmd.Bindexes {
			bucket, scope, collection, iname, err := processIndexName(bindex)
			if err != nil {
				break
			}

			index, ok := GetIndex(client, bucket, scope, collection, iname)
			if ok {
				defnIDs = append(defnIDs, uint64(index.Definition.DefnId))
			} else {
				err = fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
				break
			}
		}
		if err == nil {
			err = client.BuildIndexes(defnIDs)
			fmt.Fprintf(w, "Index building for: %v\n", defnIDs)
		}

	case "move":
		index, ok := GetIndex(client, cmd.Bucket, scope, collection, cmd.IndexName)
		if !ok {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", cmd.Bucket, scope, collection, cmd.IndexName)
		}

		if err == nil {
			fmt.Fprintf(w, "Moving Index for: %v %v\n", index.Definition.DefnId, cmd.With)
			err = client.MoveIndex(uint64(index.Definition.DefnId), cmd.WithPlan)
			if err == nil {
				fmt.Fprintf(w, "Move Index has started. Check Indexes UI for progress and Logs UI for any error\n")
			}
		}

	case "drop":
		index, ok := GetIndex(client, cmd.Bucket, scope, collection, cmd.IndexName)
		if !ok {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", cmd.Bucket, scope, collection, cmd.IndexName)
		}

		err = client.DropIndex(uint64(index.Definition.DefnId))
		if err == nil {
			fmt.Fprintf(w, "Index dropped %v/%v/%v/%v\n", bucket, scope, collection, iname)
		} else {
			err = fmt.Errorf("index %v/%v/%v/%v drop failed", bucket, scope, collection, iname)
			break
		}

	case "scan":
		var state c.IndexState

		tmpbuf, tmpbufPoolIdx = qclient.GetFromPools()
		defer func() {
			qclient.PutInPools(tmpbuf, tmpbufPoolIdx)
		}()

		index, ok := GetIndex(client, bucket, scope, collection, iname)
		if !ok {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
		}

		defnID := uint64(index.Definition.DefnId)
		fmt.Fprintln(w, "Scan index:")
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)

		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v}\n", state, err)
		} else if cmd.Equal != nil {
			equals := []c.SecondaryKey{cmd.Equal}
			client.Lookup(
				uint64(defnID), "", equals, distinct, limit,
				cons, nil, callb)
		} else {
			err = client.Range(
				uint64(defnID), "", low, high, incl, distinct, limit,
				cons, nil, callb)
		}
		if err == nil {
			fmt.Fprintln(w, "Total number of entries: ", entries)
		}

	case "scanAll":
		var state c.IndexState

		tmpbuf, tmpbufPoolIdx = qclient.GetFromPools()
		defer func() {
			qclient.PutInPools(tmpbuf, tmpbufPoolIdx)
		}()

		index, found := GetIndex(client, bucket, scope, collection, iname)
		if !found {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
		}

		defnID := uint64(index.Definition.DefnId)
		fmt.Fprintln(w, "ScanAll index:")
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else {
			err = client.ScanAll(
				uint64(defnID), "", limit, cons, nil, callb)
		}
		if err == nil {
			fmt.Fprintln(w, "Total number of entries: ", entries)
		}

	case "stats":
		var state c.IndexState
		var statsResp c.IndexStatistics

		index, ok := GetIndex(client, bucket, scope, collection, iname)
		if !ok {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
		}

		defnID := uint64(index.Definition.DefnId)
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else if cmd.Equal != nil {
			statsResp, err = client.LookupStatistics(uint64(defnID), "", equal)
		} else {
			statsResp, err = client.RangeStatistics(
				uint64(defnID), "", low, high, incl)
		}
		if err == nil {
			fmt.Fprintln(w, "Stats: ", statsResp)
		}

	case "count":
		var state c.IndexState
		var count int64

		index, ok := GetIndex(client, bucket, scope, collection, iname)
		if !ok {
			return fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
		}
		defnID := uint64(index.Definition.DefnId)
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else if cmd.Equal != nil {
			fmt.Fprintln(w, "CountLookup:")
			equals := []c.SecondaryKey{cmd.Equal}
			count, err := client.CountLookup(uint64(defnID), "", equals, cons, nil)
			if err == nil {
				fmt.Fprintf(w, "Index %q/%q/%q/%q has %v entries\n", bucket,
					scope, collection, iname, count)
			}

		} else {
			fmt.Fprintln(w, "CountRange:")
			count, err = client.CountRange(uint64(defnID), "", low, high, incl, cons, nil)
			if err == nil {
				fmt.Fprintf(w, "Index %q/%q/%q/%q has %v entries\n", bucket,
					scope, collection, iname, count)
			}
		}

	case "config":
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		var adminurl string
		for _, indexer := range nodes {
			adminurl = indexer.Adminport
			break
		}
		host, sport, _ := net.SplitHostPort(adminurl)
		iport, _ := strconv.Atoi(sport)

		//
		// hack, fix this
		//
		ihttp := iport + 2
		url := "http://" + host + ":" + strconv.Itoa(ihttp) + "/settings"

		surl, err := security.GetURL(url)
		if err != nil {
			return err
		}

		client, err := security.MakeClient(surl.String())
		if err != nil {
			return err
		}

		oreq, err := http.NewRequest("GET", surl.String(), nil)
		if cmd.Auth != "" {
			up := strings.Split(cmd.Auth, ":")
			oreq.SetBasicAuth(up[0], up[1])
		}

		oresp, err := client.Do(oreq)
		if err != nil {
			return err
		}
		defer oresp.Body.Close()
		obody, err := ioutil.ReadAll(oresp.Body)
		if err != nil {
			return err
		}

		pretty := strings.Replace(string(obody), ",\"", ",\n\"", -1)
		fmt.Printf("Current Settings:\n%s\n", string(pretty))

		var jbody map[string]interface{}
		err = json.Unmarshal(obody, &jbody)
		if err != nil {
			return err
		}

		if len(cmd.ConfigKey) > 0 {
			fmt.Printf("Changing config key '%s' to value '%s'\n", cmd.ConfigKey, cmd.ConfigVal)
			jbody[cmd.ConfigKey] = cmd.ConfigVal

			pbody, err := json.Marshal(jbody)
			if err != nil {
				return err
			}
			preq, err := http.NewRequest("POST", surl.String(), bytes.NewBuffer(pbody))
			if cmd.Auth != "" {
				up := strings.Split(cmd.Auth, ":")
				preq.SetBasicAuth(up[0], up[1])
			}
			presp, err := client.Do(preq)
			if err != nil {
				return err
			}
			defer presp.Body.Close()
			ioutil.ReadAll(presp.Body)

			nresp, err := client.Do(oreq)
			if err != nil {
				return err
			}
			defer nresp.Body.Close()
			nbody, err := ioutil.ReadAll(nresp.Body)
			if err != nil {
				return err
			}
			pretty = strings.Replace(string(nbody), ",\"", ",\n\"", -1)
			fmt.Printf("New Settings:\n%s\n", string(pretty))
		}

	case "batch_process", "batch_build":

		fd, err := validateBatchFile(cmd)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(fd)
		for scanner.Scan() {
			line := scanner.Text()
			// Commented lines
			if strings.HasPrefix(line, "#") {
				continue
			}

			args := strings.Fields(line)

			if strings.Contains(line, "-auth") == false {
				args = append([]string{"-auth", cmd.Auth}, args...)
			}

			inputCmd, _, _, err := ParseArgs(args)
			if err != nil {
				logging.Fatalf("Error while parsing command: %v", line)
				return err
			} else {
				fmt.Fprintf(w, "cbindex processing command: %v", line)
				if err = HandleCommand(client, inputCmd, false, os.Stdout); err != nil {
					logging.Errorf("Error occured while executing command %v, err: %v\n", line, err)
				}
			}
		}

	case "batch_build_schedule":
		buildCh := make(chan bool, cmd.NumBuilds)
		errCh := make(chan error, cmd.NumBuilds*10)
		stopCh := make(chan bool)

		fd, err := validateBatchFile(cmd)
		if err != nil {
			return err
		}

		go func() {
			for {
				select {
				case err := <-errCh:
					logging.Errorf("cbindex, error observed: %v", err)
				case <-stopCh:
					return
				}
			}
		}()

		var wg sync.WaitGroup

		// Check if batch file contains only build commands
		scanner := bufio.NewScanner(fd)
		for scanner.Scan() {
			line := scanner.Text()
			// Commented lines
			if strings.HasPrefix(line, "#") {
				continue
			}
			args := strings.Fields(line)

			if strings.Contains(line, "-auth") == false {
				args = append([]string{"-auth", cmd.Auth}, args...)
			}

			inputCmd, _, _, err := ParseArgs(args)
			if err != nil {
				logging.Fatalf("Error while parsing command: %v", line)
				return err
			} else {
				if inputCmd.OpType != "build" {
					err := fmt.Errorf("Commands other than buildTyte are not allowed with batch_build type, line: %v, inputCmd type: %v\n", line, inputCmd.OpType)
					logging.Fatalf(err.Error())
					return err
				}

				select {
				// Allow only 10 index builds to happen simultaneously
				// After one build is done, spawn a go-routine to queue another build
				case buildCh <- true:
					wg.Add(1)
					go func(cmd *Command, line string) {
						defer func() {
							wg.Done()
							<-buildCh
						}()

						fmt.Fprintf(w, "cbindex processing command: %v\n", line)
						if err = HandleCommand(client, cmd, false, os.Stdout); err != nil {
							errStr := fmt.Errorf("Error occured while executing command %v, err: %v\n", line, err)
							errCh <- errStr
							return
						}

						defnIDs := make([]uint64, 0)
						for _, bindex := range cmd.Bindexes {
							bucket, scope, collection, iname, err := processIndexName(bindex)
							index, ok := GetIndex(client, bucket, scope, collection, iname)
							if ok {
								defnIDs = append(defnIDs, uint64(index.Definition.DefnId))
							} else {
								err = fmt.Errorf("Index %v/%v/%v/%v unknown for command: %v", bucket, scope, collection, iname, line)
								errCh <- err
								break
							}
						}

						// Try every 1 sec and and wait for index build to finish in 1-hour
						_, err = WaitUntilIndexState(client, defnIDs, c.INDEX_STATE_ACTIVE, 1000, 3600000)
						if err != nil {
							errStr := fmt.Errorf("Error occurred while processing command: %v, err: %v", line, err)
							errCh <- errStr
						} else {
							fmt.Fprintf(w, "cbindex finished processing command: %v\n", line)
						}
					}(inputCmd, line)
				}
			}
		}
		// wait for the last set of builds to be done
		wg.Wait()
		close(stopCh)
		close(buildCh)
		close(errCh)
	}
	return err
}

func printIndexInfo(w io.Writer, index *mclient.IndexMetadata) {
	defn := index.Definition
	fmt.Fprintf(w, "Index:%s/%s/%s/%s, Id:%v, Using:%s, Exprs:%v, isPrimary:%v\n",
		defn.Bucket, defn.Scope, defn.Collection, defn.Name, defn.DefnId, defn.Using, defn.SecExprs,
		defn.IsPrimary)
	insts := index.Instances
	if len(insts) < 1 {
		fmt.Fprintf(w, "    Error: zero instances")
	} else {
		fmt.Fprintf(w, "    State:%s, Error:%v\n", insts[0].State, insts[0].Error)
	}
}

// GetIndex for bucket/indexName.
func GetIndex(
	client *qclient.GsiClient,
	bucket, scope, collection, indexName string) (*mclient.IndexMetadata, bool) {

	indexes, _, _, _, err := client.Refresh()
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Scope == scope &&
			defn.Collection == collection && defn.Name == indexName {
			return index, true
			//return uint64(index.Definition.DefnId), true
		}
	}
	return nil, false
}

// WaitUntilIndexState comes to desired `state`,
// retry for every `period` mS until `timeout` mS.
func WaitUntilIndexState(
	client *qclient.GsiClient, defnIDs []uint64,
	state c.IndexState, period, timeout time.Duration) ([]c.IndexState, error) {

	expired := time.After(timeout * time.Millisecond)
	states := make([]c.IndexState, len(defnIDs))
	pending := len(defnIDs)
	for {
		select {
		case <-expired:
			return nil, errors.New("timeout")

		default:
		}
		for i, defnID := range defnIDs {
			if states[i] != state {
				st, err := client.IndexState(defnID)
				if err != nil {
					return nil, err
				} else if st == state {
					states[i] = state
					pending--
					continue
				}
			}
		}
		if pending == 0 {
			return states, nil
		}
		time.Sleep(period * time.Millisecond)
	}
}

//----------------
// local functions
//----------------

// Arg2Key convert JSON string to golang-native.
func Arg2Key(arg []byte) []interface{} {
	var key []interface{}
	if err := json.Unmarshal(arg, &key); err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}
	return key
}

func first(key c.SecondaryKey) []byte {
	if key == nil || len(key) == 0 {
		return nil
	}
	return []byte(key[0].(string))
}

func validate(cmd *Command, fset *flag.FlagSet) error {
	var have []string
	var dont []string

	switch cmd.OpType {
	case "":
		have = []string{}
		dont = []string{"type", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval", "refresh_settings"}

	case "nodes":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "list":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "create":
		have = []string{"type", "server", "auth", "index", "bucket", "primary"}
		dont = []string{"h", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "build":
		have = []string{"type", "server", "auth", "indexes"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "move":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "indexes", "where", "fields", "primary", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "drop":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "scan":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "ckey", "cval"}

	case "scanAll":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "ckey", "cval"}

	case "stats":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "limit", "distinct", "ckey", "cval"}

	case "count":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "ckey", "cval"}

	case "config":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct"}

	case "batch_process":
		have = []string{"type", "auth", "input"}
		dont = []string{"index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	case "batch_build":
		have = []string{"type", "auth", "input"}
		dont = []string{"index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "distinct", "ckey", "cval"}

	default:
		return fmt.Errorf("Specified operation type '%s' has no validation rule. Please add one to use.", cmd.OpType)
	}

	err := mustHave(fset, have...)
	if err != nil {
		return err
	}

	err = mustNotHave(fset, dont...)
	if err != nil {
		return err
	}

	return nil
}

func mustHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if !found {
			flag := fset.Lookup(key)
			if flag == nil || flag.DefValue == "" {
				return fmt.Errorf("Invalid flags. Flag '%s' is required for this operation", key)
			}
		}
	}
	return nil
}

func mustNotHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if found {
			return fmt.Errorf("Invalid flags. Flag '%s' cannot appear for this operation", key)
		}
	}
	return nil
}
