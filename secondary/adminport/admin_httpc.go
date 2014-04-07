// admin client to talk to the server
//
// clients can talk to server by doing,
//
//  client := NewHttpClient("http://localhost:8888")
//  req := &protobuf.RequestMessage{}
//  resp := &protobuf.ResponseMessage{}
//  client.Request(req, resp)

package adminport

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

// httpClient is a concrete type implementing Client interface.
type httpClient struct {
	serverAddr string
	httpc      *http.Client
}

// Create a new instance of Client over HTTP.
func NewHttpClient(serverAddr string) Client {
	return &httpClient{serverAddr: serverAddr, httpc: http.DefaultClient}
}

func (c *httpClient) Request(msg, resp MessageMarshaller) (err error) {
	return doResponse(func() (*http.Response, error) {
		var body []byte
		var req *http.Request

		if body, err = msg.Encode(); err != nil {
			return nil, err
		}

		bodybuf := bytes.NewBuffer(body)
		url := c.serverAddr + "/" + msg.Name()
		if req, err = http.NewRequest("POST", url, bodybuf); err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/protobuf")
		return c.httpc.Do(req)
	}, resp)
}

func doResponse(fn func() (*http.Response, error), resp MessageMarshaller) (err error) {
	var body []byte
	var htresp *http.Response

	if htresp, err = fn(); err != nil {
		return err
	}
	defer htresp.Body.Close()

	if body, err = ioutil.ReadAll(htresp.Body); err != nil {
		return err
	}
	return resp.Decode(body)
}
