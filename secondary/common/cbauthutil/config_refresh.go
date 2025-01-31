package cbauthutil

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"net"
	"sync"

	"github.com/couchbase/cbauth"
	httpreq "github.com/couchbase/cbauth/httpreq"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

func RegisterConfigRefreshCallback() error {
	// Register config refresh callback
	err := cbauth.RegisterConfigRefreshCallback(ConfigRefreshCallback)
	if err != nil {
		return err
	}
	return nil
}

func ConfigRefreshCallback(code uint64) error {
	logging.Infof("ConfigRefreshCallback: Received notificaiton with code: %v", code)

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 ||
		code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		return security.SecurityConfigRefresh(code)
	}

	return nil
}

/*
HandleClientCertAndAuth does the following tasks -
- if not a TLS connection, use basic auth for access verification
- if TLS connection, (perform optional handshake) and use TLS connection state for verification
*/
func HandleClientCertAndAuth(
	conn net.Conn,
	username, password string,
) (
	creds cbauth.Creds, _ error, isTls bool,
) {
	var connState *tls.ConnectionState
	raddr := conn.RemoteAddr().String()
	if tlsConn, ok := conn.(*tls.Conn); ok {
		isTls = true
		if !tlsConn.ConnectionState().HandshakeComplete {
			err := tlsConn.Handshake()
			if err != nil {
				return nil, err, isTls
			}
		}

		var tlsConnState = tlsConn.ConnectionState()
		connState = &tlsConnState
	}
	wrapper := NewCbAuthHttpReqWrapper(connState, raddr)
	if len(username) > 0 && len(password) > 0 {
		wrapper.Set("Authorization", createBasicAuthValue(username, password))
	}
	creds, err := cbauth.AuthWebCredsGeneric(wrapper)

	return creds, err, isTls
}

type CbAuthHttpReqWrapper struct {
	httpreq.HttpRequest
	connState *tls.ConnectionState
	host      string
	headers   map[string]string
	mu        sync.Mutex
}

func (wrapper *CbAuthHttpReqWrapper) Get(header string) string {
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()
	return wrapper.headers[header]
}

func (wrapper *CbAuthHttpReqWrapper) Set(key string, value string) {
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()
	wrapper.headers[key] = value
}

func (wrapper *CbAuthHttpReqWrapper) GetTLS() *tls.ConnectionState {
	return wrapper.connState
}

func (wrapper *CbAuthHttpReqWrapper) GetHost() string {
	return wrapper.host
}

func NewCbAuthHttpReqWrapper(connState *tls.ConnectionState, host string) *CbAuthHttpReqWrapper {
	return &CbAuthHttpReqWrapper{
		connState: connState, host: host,
		headers: make(map[string]string),
	}
}

func createBasicAuthValue(username, password string) string {
	var cred bytes.Buffer
	cred.WriteString(username)
	cred.WriteByte(':')
	cred.WriteString(password)
	return "Basic " + base64.StdEncoding.EncodeToString(cred.Bytes())
}
