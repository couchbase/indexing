// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"flag"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
)

func main() {
	platform.HideConsole(true)
	defer platform.HideConsole(false)
	common.SeedProcess()

	logging.Infof("Indexer started with command line: %v\n", os.Args)

	fset := flag.NewFlagSet("indexer", flag.ContinueOnError)

	logLevel := fset.String("loglevel", "Info", "Log Level - Silent, Fatal, Error, Info, Debug, Trace")
	numVbuckets := fset.Int("vbuckets", indexer.MAX_NUM_VBUCKETS, "Number of vbuckets configured in Couchbase")
	cluster := fset.String("cluster", indexer.DEFAULT_CLUSTER_ENDPOINT, "Couchbase cluster address")
	adminPort := fset.String("adminPort", "9100", "Index ddl and status port")
	scanPort := fset.String("scanPort", "9101", "Index scanner port")
	httpPort := fset.String("httpPort", "9102", "Index http mgmt port")
	streamInitPort := fset.String("streamInitPort", "9103", "Index initial stream port")
	streamCatchupPort := fset.String("streamCatchupPort", "9104", "Index catchup stream port")
	streamMaintPort := fset.String("streamMaintPort", "9105", "Index maintenance stream port")
	storageDir := fset.String("storageDir", "./", "Index file storage directory path")
	diagDir := fset.String("diagDir", "./", "Directory for writing index diagnostic information")
	logDir := fset.String("logDir", "", "Directory for log files")
	enableManager := fset.Bool("enable_manager", true, "Enable Index Manager")
	auth := fset.String("auth", "", "Auth user and password")
	nodeuuid := fset.String("nodeUUID", "", "UUID of the node")
	storageMode := fset.String("storageMode", "", "Storage mode of indexer (forestdb/memory_optimized)")
	httpsPort := fset.String("httpsPort", "", "Index https mgmt port")
	certFile := fset.String("certFile", "", "Index https X509 certificate file")
	keyFile := fset.String("keyFile", "", "Index https cert key file")
	isEnterprise := fset.Bool("isEnterprise", true, "Enterprise Edition")
	ipv4 := fset.String("ipv4", "", "Specify if ipv4 is required|optional|off")
	ipv6 := fset.String("ipv6", "", "Specify if ipv6 is required|optional|off")
	caFile := fset.String("caFile", "", "Multiple Root/Client CAs")

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i : i+1]); err != nil {
			if strings.Contains(err.Error(), "flag provided but not defined") {
				logging.Warnf("Ignoring the unspecified argument error: %v", err)
			} else {
				common.CrashOnError(err)
			}
		}
	}

	logging.SetLogLevel(logging.Level(*logLevel))
	forestdb.Log = &logging.SystemLogger

	// setup cbauth
	if *auth != "" {
		up := strings.Split(*auth, ":")
		logging.Tracef("Initializing cbauth with user %v for cluster %v\n", logging.TagUD(up[0]), *cluster)
		if _, err := cbauth.InternalRetryDefaultInit(*cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	var isIPv6 bool
	var err error

	isIPv6, err = common.GetIPv6FromParam(*ipv4, *ipv6)
	if err != nil {
		logging.Fatalf("IsIPv6FromParam returns error: err=%v ipv4=%v ipv6=%v", err, *ipv4, *ipv6)
		common.CrashOnError(err)
	}

	go common.ExitOnStdinClose()

	config := common.SystemConfig
	config.SetValue("indexer.clusterAddr", *cluster)
	config.SetValue("indexer.numVbuckets", *numVbuckets)
	config.SetValue("indexer.enableManager", *enableManager)
	config.SetValue("indexer.adminPort", *adminPort)
	config.SetValue("indexer.scanPort", *scanPort)
	config.SetValue("indexer.httpPort", *httpPort)
	config.SetValue("indexer.httpsPort", *httpsPort)
	config.SetValue("indexer.certFile", *certFile)
	config.SetValue("indexer.keyFile", *keyFile)
	config.SetValue("indexer.caFile", *caFile)
	config.SetValue("indexer.streamInitPort", *streamInitPort)
	config.SetValue("indexer.streamCatchupPort", *streamCatchupPort)
	config.SetValue("indexer.streamMaintPort", *streamMaintPort)
	config.SetValue("indexer.storage_dir", *storageDir)
	config.SetValue("indexer.diagnostics_dir", *diagDir)
	config.SetValue("indexer.log_dir", *logDir)
	config.SetValue("indexer.nodeuuid", *nodeuuid)
	config.SetValue("indexer.isEnterprise", *isEnterprise)
	config.SetValue("indexer.isIPv6", isIPv6)

	// Prior to watson (4.5 version) storage_dir parameter was converted
	// to lower case. Post watson, the plan is to keep the parameter
	// case-sensitive. Following is the logic:
	// - when indexer restarts with same storage_dir parameter it will be
	// case-sensitive, so check wither lowercase(storage_dir) exist
	// - if so, copy them to case-sensitive directory and remove
	//   case-insensitive directory.
	// - else, it is not an upgrade situation.
	storage_dir := config["indexer.storage_dir"].String()
	if common.IsPathExist(storage_dir) == false {
		if err := iowrap.Os_MkdirAll(storage_dir, 0755); err != nil {
			common.CrashOnError(err)
		}
	}
	lowcase_storage_dir := strings.ToLower(storage_dir)
	if common.IsPathExist(lowcase_storage_dir) {
		func() {
			casefile, err := iowrap.Os_Open(storage_dir)
			if err != nil {
				logging.Errorf("iowrap.Os_Open(storage_dir): %v", err)
				common.CrashOnError(err)
			}
			defer iowrap.File_Close(casefile)
			lowerfile, err := iowrap.Os_Open(lowcase_storage_dir)
			if err != nil {
				logging.Errorf("iowrap.Os_Open(lowcase_storage_dir): %v", err)
				common.CrashOnError(err)
			}
			defer iowrap.File_Close(lowerfile)

			caseinfo, err := iowrap.File_Stat(casefile)
			if err != nil {
				logging.Errorf("File_Stat(storage_dir): %v", err)
				common.CrashOnError(err)
			}
			lowerinfo, err := iowrap.File_Stat(lowerfile)
			if err != nil {
				logging.Errorf("File_Stat(lowcase_storage_dir): %v", err)
				common.CrashOnError(err)
			}
			if os.SameFile(caseinfo, lowerinfo) == false {
				err := iowrap.Os_Rename(lowcase_storage_dir, storage_dir)
				if err != nil {
					fmsg := "renaming from %v to %v: %v"
					logging.Fatalf(fmsg, lowcase_storage_dir, storage_dir, err)
					fmsg = "reverting to lower-case storage_dir %v"
					logging.Infof(fmsg, lowcase_storage_dir)
					config.SetValue("storage_dir", lowcase_storage_dir)
				}
			}
		}()
	}

	if err := iowrap.Os_MkdirAll(*diagDir, 0755); err != nil {
		common.CrashOnError(err)
	}

	if *storageMode != "" {
		/*
			if common.SetClusterStorageModeStr(*storageMode) {
				logging.Infof("Indexer::Cluster Storage Mode Set %v", common.GetClusterStorageMode())
			} else {
				logging.Infof("Indexer::Cluster Invalid Storage Mode %v", *storageMode)
			}
		*/
	}

	logging.Infof("Setting ipv6=%v", isIPv6)

	common.SetIpv6(isIPv6)

	_, msg := indexer.NewIndexer(config)

	if msg.GetMsgType() != indexer.MSG_SUCCESS {
		logging.Warnf("Indexer Failure to Init %v", msg)
	}

	logging.Infof("Indexer exiting normally\n")
}
