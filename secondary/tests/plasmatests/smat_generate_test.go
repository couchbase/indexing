//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build gofuzz

package indexer

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/mschoch/smat"
)

func TestGenerateSmatCorpus(t *testing.T) {
	for i, actionSeq := range smatActionSeqs {
		byteSequence, err := actionSeq.ByteEncoding(&smatContext{},
			smat.ActionID('S'), smat.ActionID('T'), actionMap)
		if err != nil {
			t.Fatalf("error from ByteEncoding, err: %v", err)
		}
		os.MkdirAll("workdir/corpus", 0700)
		ioutil.WriteFile(fmt.Sprintf("workdir/corpus/%d", i), byteSequence, 0600)
	}
}

// Different testcases defined here
var smatActionSeqs = []smat.ActionSeq{
	{
		smat.ActionID('a'),
		smat.ActionID('b'),
		smat.ActionID('c'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('g'),
		smat.ActionID('e'),
		smat.ActionID('f'),
		smat.ActionID('g'),
		smat.ActionID('h'),
	},
	{
		smat.ActionID('a'),
		smat.ActionID('b'),
		smat.ActionID('b'),
		smat.ActionID('b'),
		smat.ActionID('c'),
		smat.ActionID('c'),
		smat.ActionID('d'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('f'),
		smat.ActionID('h'),
	},
	{
		smat.ActionID('a'),
		smat.ActionID('b'),
		smat.ActionID('c'),
		smat.ActionID('d'),
		smat.ActionID('e'),
		smat.ActionID('f'),
		smat.ActionID('g'),
		smat.ActionID('h'),
	},
	{
		smat.ActionID('a'),
		smat.ActionID('b'),
		smat.ActionID('e'),
		smat.ActionID('d'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('g'),
		smat.ActionID('e'),
		smat.ActionID('e'),
		smat.ActionID('h'),
	},
}
