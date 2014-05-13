// admin client to talk to the server
//
// clients can talk to server by doing,
//
// Example client {
//     client := NewHTTPClient("http://localhost:9999")
//     req := &protobuf.RequestMessage{}
//     resp := &protobuf.ResponseMessage{}
//     client.Request(req, resp)
// }

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

// NewHTTPClient returns a new instance of Client over HTTP.
func NewHTTPClient(serverAddr string) Client {
	return &httpClient{
		serverAddr: serverAddr,
		httpc:      http.DefaultClient,
	}
}

// Request is part of `Client` interface
func (c *httpClient) Request(msg, resp MessageMarshaller) (err error) {
	return doResponse(func() (*http.Response, error) {
		// marshall message
		body, err := msg.Encode()
		if err != nil {
			return nil, err
		}
		// create request
		bodybuf := bytes.NewBuffer(body)
		url := c.serverAddr + "/" + msg.Name()
		req, err := http.NewRequest("POST", url, bodybuf)
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", msg.ContentType())
		// POST request and return back the response
		return c.httpc.Do(req)
	}, resp)
}

func doResponse(postRequest func() (*http.Response, error), resp MessageMarshaller) error {
	htresp, err := postRequest() // get response back from server
	if err != nil {
		return err
	}
	defer htresp.Body.Close()

	body, err := ioutil.ReadAll(htresp.Body)
	if err != nil {
		return err
	}
	return resp.Decode(body) // unmarshal and return
}
