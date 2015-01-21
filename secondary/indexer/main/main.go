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
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
)

var (
	logLevel          = flag.Int("log", common.LogLevelInfo, "Log Level - 1(Info), 2(Debug), 3(Trace)")
	numVbuckets       = flag.Int("vbuckets", indexer.MAX_NUM_VBUCKETS, "Number of vbuckets configured in Couchbase")
	cluster           = flag.String("cluster", indexer.DEFAULT_CLUSTER_ENDPOINT, "Couchbase cluster address")
	adminPort         = flag.String("adminPort", "9100", "Index ddl and status port")
	scanPort          = flag.String("scanPort", "9101", "Index scanner port")
	streamInitPort    = flag.String("streamInitPort", "9102", "Index initial stream port")
	streamCatchupPort = flag.String("streamCatchupPort", "9103", "Index catchup stream port")
	streamMaintPort   = flag.String("streamMaintPort", "9104", "Index maintenance stream port")
	storageDir        = flag.String("storageDir", "./", "Index file storage directory path")
	enableManager     = flag.Bool("enable_manager", true, "Enable Index Manager")
	auth              = flag.String("auth", "", "Auth user and password")
)

func MaybeSetEnv(key, value string) {
	if os.Getenv(key) != "" {
		return
	}
	os.Setenv(key, value)
}

func main() {

	flag.Parse()

	// setup cbauth
	if *auth != "" {
		up := strings.Split(*auth, ":")
		authU, authP := up[0], up[1]
		authURL := fmt.Sprintf("http://%s/_cbauth", *cluster)
		rpcURL := fmt.Sprintf("http://%s/index", *cluster)
		common.MaybeSetEnv("NS_SERVER_CBAUTH_RPC_URL", rpcURL)
		common.MaybeSetEnv("NS_SERVER_CBAUTH_USER", authU)
		common.MaybeSetEnv("NS_SERVER_CBAUTH_PWD", authP)
		cbauth.Default = cbauth.NewDefaultAuthenticator(authURL, nil)
	}

	go dumpOnSignalForPlatform()
	go common.ExitOnStdinClose()

	common.SetLogLevel(*logLevel)
	config := common.SystemConfig.SectionConfig("indexer.", true)

	config = config.SetValue("clusterAddr", *cluster)
	config = config.SetValue("numVbuckets", *numVbuckets)
	config = config.SetValue("enableManager", *enableManager)
	config = config.SetValue("adminPort", *adminPort)
	config = config.SetValue("scanPort", *scanPort)
	config = config.SetValue("streamInitPort", *streamInitPort)
	config = config.SetValue("streamCatchupPort", *streamCatchupPort)
	config = config.SetValue("streamMaintPort", *streamMaintPort)
	config = config.SetValue("storage_dir", *storageDir)

	_, msg := indexer.NewIndexer(config)

	if msg.GetMsgType() != indexer.MSG_SUCCESS {
		log.Printf("Indexer Failure to Init %v", msg)
	}
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	}
}
