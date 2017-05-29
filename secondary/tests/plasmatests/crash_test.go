package indexer

import (
	"log"
	"os"
	"testing"

	"github.com/mschoch/smat"
)

func TestCrasher(t *testing.T) {
	// paste in your crash here:
	crasher := []byte("\x00\x1f>]|\x9b\xba\xd9")
	// turn on logger
	smat.Logger = log.New(os.Stderr, "smat ", log.LstdFlags)
	// fuzz the crasher input
	smat.Fuzz(&smatContext{}, smat.ActionID('S'), smat.ActionID('T'),
		actionMap, crasher)
}
