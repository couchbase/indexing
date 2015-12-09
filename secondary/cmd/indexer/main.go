// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package main

import (
	"flag"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
)

func main() {
	platform.HideConsole(true)
	defer platform.HideConsole(false)
	common.SeedProcess()

	logging.Infof("Indexer started with command line: %v\n", os.Args)

	fset := flag.NewFlagSet("indexer", flag.ContinueOnError)

	logLevel          := fset.String("loglevel", "Info", "Log Level - Silent, Fatal, Error, Info, Debug, Trace")
	numVbuckets       := fset.Int("vbuckets", indexer.MAX_NUM_VBUCKETS, "Number of vbuckets configured in Couchbase")
	cluster           := fset.String("cluster", indexer.DEFAULT_CLUSTER_ENDPOINT, "Couchbase cluster address")
	adminPort         := fset.String("adminPort", "9100", "Index ddl and status port")
	scanPort          := fset.String("scanPort", "9101", "Index scanner port")
	httpPort          := fset.String("httpPort", "9102", "Index http mgmt port")
	streamInitPort    := fset.String("streamInitPort", "9103", "Index initial stream port")
	streamCatchupPort := fset.String("streamCatchupPort", "9104", "Index catchup stream port")
	streamMaintPort   := fset.String("streamMaintPort", "9105", "Index maintenance stream port")
	storageDir        := fset.String("storageDir", "./", "Index file storage directory path")
	diagDir           := fset.String("diagDir", "./", "Directory for writing index diagnostic information")
	enableManager     := fset.Bool("enable_manager", true, "Enable Index Manager")
	auth              := fset.String("auth", "", "Auth user and password")

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i:i+1]); err != nil {
			if strings.Contains(err.Error(), "flag provided but not defined") {
				logging.Warnf("Ignoring the unspecified argument error: %v", err)
			} else {
				common.CrashOnError(err)
			}
		}
	}

	logging.SetLogLevel(logging.Level(*logLevel))
	forestdb.Log = &logging.SystemLogger

	// Setup Breakpad to catch forestDB fatal errors.
	forestdb.InitBreakpadForFDB(*diagDir)

	// setup cbauth
	if *auth != "" {
		up := strings.Split(*auth, ":")
		logging.Tracef("Initializing cbauth with user %v for cluster %v\n", up[0], *cluster)
		if _, err := cbauth.InternalRetryDefaultInit(*cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	go platform.DumpOnSignal()
	go common.ExitOnStdinClose()

	config := common.SystemConfig
	config.SetValue("indexer.clusterAddr", *cluster)
	config.SetValue("indexer.numVbuckets", *numVbuckets)
	config.SetValue("indexer.enableManager", *enableManager)
	config.SetValue("indexer.adminPort", *adminPort)
	config.SetValue("indexer.scanPort", *scanPort)
	config.SetValue("indexer.httpPort", *httpPort)
	config.SetValue("indexer.streamInitPort", *streamInitPort)
	config.SetValue("indexer.streamCatchupPort", *streamCatchupPort)
	config.SetValue("indexer.streamMaintPort", *streamMaintPort)
	config.SetValue("indexer.storage_dir", *storageDir)
	config.SetValue("indexer.diagnostics_dir", *diagDir)

	storage_dir := config["indexer.storage_dir"].String()
	if err := os.MkdirAll(storage_dir, 0755); err != nil {
		common.CrashOnError(err)
	}

	if err := os.MkdirAll(*diagDir, 0755); err != nil {
		common.CrashOnError(err)
	}

	_, msg := indexer.NewIndexer(config)

	if msg.GetMsgType() != indexer.MSG_SUCCESS {
		logging.Warnf("Indexer Failure to Init %v", msg)
	}

	logging.Infof("Indexer exiting normally\n")
}
