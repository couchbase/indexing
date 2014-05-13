package adminport

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"testing"
)

var addr = "http://localhost:9999"

type testMessage struct {
	Bucket        string `json:"bucket"`
	IsPrimary     bool   `json:"isPrimary"`
	IName         string `json:"name"`
	Uuid          uint64 `json:"uuid"`
	Using         string `json:"using"`
	ExprType      string `json:"exprType"`
	PartitionType string `json:"partitionType"`
	Expression    string `json:"expression"`
}

func TestLoopBack(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	q := make(chan bool)

	doServer(addr, t, q)

	client := NewHTTPClient(addr)
	req := &testMessage{
		Bucket:        "default",
		IsPrimary:     false,
		IName:         "example-index",
		Uuid:          uint64(0x1234567812345678),
		Using:         "forrestdb",
		ExprType:      "n1ql",
		PartitionType: "simplekeypartition",
		Expression:    "x+1",
	}
	resp := &testMessage{}
	if err := client.Request(req, resp); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(req, resp) == false {
		t.Fatal(fmt.Errorf("unexpected response"))
	}
}

func BenchmarkClientRequest(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	client := NewHTTPClient(addr)
	req := &testMessage{
		Bucket:        "default",
		IsPrimary:     false,
		IName:         "example-index",
		Uuid:          uint64(0x1234567812345678),
		Using:         "forrestdb",
		ExprType:      "n1ql",
		PartitionType: "simplekeypartition",
		Expression:    "x+1",
	}
	resp := &testMessage{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Request(req, resp); err != nil {
			b.Fatal(err)
		}
	}
}

func doServer(addr string, tb testing.TB, quit chan bool) Server {
	reqch := make(chan Request, 10)
	server := NewHTTPServer("test", "localhost:9999", reqch)
	if err := server.Register(&testMessage{}); err != nil {
		tb.Fatal(err)
	}

	if err := server.Start(); err != nil {
		tb.Fatal(err)
	}

	go func() {
	loop:
		for {
			select {
			case req, ok := <-reqch:
				if ok {
					msg := req.GetMessage().(*testMessage)
					if err := req.Send(msg); err != nil {
						tb.Fatal(err)
					}
				} else {
					break loop
				}
			}
		}
		close(quit)
	}()

	return server
}

func (tm *testMessage) Name() string {
	return "testMessage"
}

func (tm *testMessage) Encode() (data []byte, err error) {
	data, err = json.Marshal(tm)
	return
}

func (tm *testMessage) Decode(data []byte) (err error) {
	err = json.Unmarshal(data, tm)
	return
}

func (tm *testMessage) ContentType() string {
	return "application/json"
}
