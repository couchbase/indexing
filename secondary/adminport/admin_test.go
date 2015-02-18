package adminport

import "encoding/json"
import "log"
import "reflect"
import "testing"

import "github.com/couchbase/indexing/secondary/common"

var addr = "localhost:9999"

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
	server = doServer("http://"+addr, q)
}

func TestLoopback(t *testing.T) {
	urlPrefix := common.SystemConfig["projector.adminport.urlPrefix"].String()
	client := NewHTTPClient(addr, urlPrefix)
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
	urlPrefix := common.SystemConfig["projector.adminport.urlPrefix"].String()
	client := NewHTTPClient(addr, urlPrefix)
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
	apConfig := common.SystemConfig.SectionConfig("projector.adminport.", true)
	apConfig.SetValue("name", "test-adminport")
	apConfig.SetValue("listenAddr", "localhost:9999")
	reqch := make(chan Request, 10)
	server := NewHTTPServer(apConfig, reqch)
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
