package adminport

import "encoding/json"
import "log"
import "reflect"
import "testing"

import "github.com/couchbase/indexing/secondary/common"

var addr = "http://localhost:9999"

type testMessage struct {
	DefnID          uint64 `json:"defnId"`
	Bucket          string `json:"bucket"`
	IsPrimary       bool   `json:"isPrimary"`
	IName           string `json:"name"`
	Using           string `json:"using"`
	ExprType        string `json:"exprType"`
	PartitionScheme string `json:"partitionType"`
	Expression      string `json:"expression"`
}

var q = make(chan bool)
var server Server

func init() {
	server = doServer(addr, q)
	//common.SetLogLevel(common.LogLevelDebug)
}

func TestLoopback(t *testing.T) {
	common.LogIgnore()

	client := NewHTTPClient(addr, common.AdminportURLPrefix)
	req := &testMessage{
		DefnID:          uint64(0x1234567812345678),
		Bucket:          "default",
		IsPrimary:       false,
		IName:           "example-index",
		Using:           "forrestdb",
		ExprType:        "n1ql",
		PartitionScheme: "simplekeypartition",
		Expression:      makeLargeString(),
	}
	resp := &testMessage{}
	if err := client.Request(req, resp); err != nil {
		t.Error(err)
	}
	if reflect.DeepEqual(req, resp) == false {
		t.Error("unexpected response")
	}
	stats := common.Statistics{}
	if err := client.Request(&stats, &stats); err != nil {
		t.Error(err)
	}
	refstats := interface{}([]interface{}{float64(1), float64(1), float64(0)})
	if reflect.DeepEqual(stats["/adminport/testMessage"], refstats) == false {
		t.Errorf("%v", stats["/adminport/testMessage"])
	}
}

func BenchmarkClientRequest(b *testing.B) {
	client := NewHTTPClient(addr, common.AdminportURLPrefix)
	req := &testMessage{
		DefnID:          uint64(0x1234567812345678),
		Bucket:          "default",
		IsPrimary:       false,
		IName:           "example-index",
		Using:           "forrestdb",
		ExprType:        "n1ql",
		PartitionScheme: "simplekeypartition",
		Expression:      "x+1",
	}
	resp := &testMessage{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Request(req, resp); err != nil {
			b.Error(err)
		}
	}
}

func doServer(addr string, quit chan bool) Server {
	urlPrefix, reqch := common.AdminportURLPrefix, make(chan Request, 10)
	server := NewHTTPServer("test", "localhost:9999", urlPrefix, reqch)
	if err := server.Register(&testMessage{}); err != nil {
		log.Fatal(err)
	}
	if err := server.Register(&common.Statistics{}); err != nil {
		log.Fatal(err)
	}

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	go func() {
	loop:
		for {
			select {
			case req, ok := <-reqch:
				if ok {
					switch msg := req.GetMessage().(type) {
					case *testMessage:
						if err := req.Send(msg); err != nil {
							log.Println(err)
						}
					case *common.Statistics:
						m := server.GetStatistics()
						if err := req.Send(m); err != nil {
							log.Println(err)
						}
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

func makeLargeString() string {
	s := "large string"
	for i := 0; i < 16; i++ {
		s += s
	}
	return s
}
