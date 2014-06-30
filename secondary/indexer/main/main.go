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
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
)

var logLevel = flag.Int("log", common.LogLevelInfo, "Log Level - 1(Info), 2(Debug), 3(Trace)")
var numVbuckets = flag.Int("vbuckets", indexer.MAX_NUM_VBUCKETS, "Number of vbuckets configured in Couchbase")
var projector = flag.String("projector", indexer.DEFAULT_PROJECTOR_ADMIN_PORT_ENDPOINT, "Projector Admin Port Address")

func main() {

	flag.Parse()

	go dumpOnSignalForPlatform()

	common.SetLogLevel(*logLevel)
	if *projector == "" {
		indexer.PROJECTOR_ADMIN_PORT_ENDPOINT = indexer.DEFAULT_PROJECTOR_ADMIN_PORT_ENDPOINT
	} else {
		indexer.PROJECTOR_ADMIN_PORT_ENDPOINT = *projector
	}

	_, msg := indexer.NewIndexer(uint16(*numVbuckets))

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
