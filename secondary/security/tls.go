//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package security

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	// cannot import indexing/secondary/common: import cycle
	"github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////
// TLS Connection
/////////////////////////////////////////////

var userAgentPrefix = "Go-http-client/1.1-indexer-"

func getCertPool(setting *SecuritySetting) (*x509.CertPool, error) {

	certInBytes := setting.certInBytes
	caInBytes := setting.caInBytes

	if len(caInBytes) == 0 && len(certInBytes) == 0 {
		return nil, fmt.Errorf("No certificate has been provided.")
	}

	caCertPool := x509.NewCertPool()

	// Prioritise caFile over certFile for root and client CAs.

	if len(caInBytes) != 0 {
		ok := caCertPool.AppendCertsFromPEM(caInBytes)
		if !ok {
			err := fmt.Errorf("Cannot load certificates from caFile")
			logging.Errorf("%v", err)
			return nil, err
		}
	} else {
		ok := caCertPool.AppendCertsFromPEM(certInBytes)
		if !ok {
			err := fmt.Errorf("Cannot load certificates from certFile")
			logging.Errorf("%v", err)
			return nil, err
		}
	}

	return caCertPool, nil
}

//
// Setup client TLSConfig
//
func setupClientTLSConfig(host string) (*tls.Config, error) {

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting is nil")
	}

	if !setting.encryptionEnabled {
		return nil, nil
	}

	// Get certificate and cbauth TLS setting
	pref := setting.tlsPreference

	// Setup  TLSConfig
	tlsConfig := &tls.Config{}

	//  Set up cert pool for rootCAs
	caCertPool, err := getCertPool(setting)
	if err != nil {
		return nil, fmt.Errorf("%v Can't establish ssl connection to %v", err, host)
	}

	tlsConfig.RootCAs = caCertPool

	if IsLocal(host) {
		// skip server verify if it is localhost
		tlsConfig.InsecureSkipVerify = true
	} else {
		// setup server host name
		tlsConfig.ServerName = host
	}

	// setup prefer ciphers
	if pref != nil {
		tlsConfig.MinVersion = pref.MinVersion
		tlsConfig.CipherSuites = pref.CipherSuites
		tlsConfig.PreferServerCipherSuites = pref.PreferServerCipherSuites
	}

	return tlsConfig, nil
}

//
// Set up a TLS client connection.  This function does not close conn upon error.
//
func makeTLSConn(conn net.Conn, hostname, port string) (net.Conn, error) {

	// Setup TLS Config
	tlsConfig, err := setupClientTLSConfig(hostname)
	if err != nil {
		return nil, err
	}

	// Setup TLS connection
	if tlsConfig != nil {
		tlsConn := tls.Client(conn, tlsConfig)

		// Initiate TlS handshake.  This is optional since first Read() or Write() will
		// initiate handshake implicitly.  By performing handshake now, we can detect
		// setup issue early on.

		// Spawn new routine to enforce timeout
		errChannel := make(chan error, 2)

		go func() {
			timer := time.NewTimer(time.Duration(2 * time.Minute))
			defer timer.Stop()

			select {
			case errChannel <- tlsConn.Handshake():
			case <-timer.C:
				errChannel <- errors.New("Unable to finish TLS handshake with 2 minutes")
			}
		}()

		err = <-errChannel
		if err != nil {
			return nil, fmt.Errorf("TLS handshake failed when connecting to %v, err=%v\n", hostname, err)
		}

		logging.Infof("TLS connection created for %v", net.JoinHostPort(hostname, port))
		return tlsConn, nil
	}

	return conn, nil
}

//
// Setup a TCP client connection
//
func makeTCPConn(addr string) (net.Conn, error) {

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Secure a TCP connection.   This function will not convert conn to a SSL port.
// So if encryption is required, conn.RemoteAddr must already be using a SSL port.
func SecureConn(conn net.Conn, hostname, port string) (net.Conn, error) {

	if EncryptionRequired(hostname, port) {
		return makeTLSConn(conn, hostname, port)
	}

	return conn, nil
}

//
// Setup a TCP or TLS client connection depending whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeConn(addr string) (net.Conn, error) {

	addr, hostname, port, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	conn, err := makeTCPConn(addr)
	if err != nil {
		return nil, err
	}

	conn2, err2 := SecureConn(conn, hostname, port)
	if err2 != nil {
		conn.Close()
		return nil, err2
	}

	return conn2, nil
}

//
// Setup a TCP client connection depending whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeTCPConn(addr string) (*net.TCPConn, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	conn, err := makeTCPConn(addr)
	if err != nil {
		return nil, err
	}

	return conn.(*net.TCPConn), nil
}

/////////////////////////////////////////////
// TLS Listener
/////////////////////////////////////////////

//
// Setup server TLSConfig
//
func setupServerTLSConfig() (*tls.Config, error) {

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting is nil")
	}

	if !setting.encryptionEnabled {
		return nil, nil
	}

	return getTLSConfigFromSetting(setting)
}

func getTLSConfigFromSetting(setting *SecuritySetting) (*tls.Config, error) {

	// Get certifiicate and cbauth config
	cert := setting.certificate
	if cert == nil {
		err := fmt.Errorf("No certificate has been provided. Can't establish ssl connection")
		return nil, err
	}

	pref := setting.tlsPreference

	// set up TLS server config
	config := &tls.Config{}

	// set up certificate
	config.Certificates = []tls.Certificate{*cert}

	if pref != nil {
		// setup ciphers
		config.CipherSuites = pref.CipherSuites
		config.PreferServerCipherSuites = pref.PreferServerCipherSuites

		// set up other attributes
		config.MinVersion = pref.MinVersion
		config.ClientAuth = pref.ClientAuthType

		// set up client cert
		if pref.ClientAuthType != tls.NoClientCert {

			caCertPool, err := getCertPool(setting)
			if err != nil {
				return nil, fmt.Errorf("%v Can't establish ssl connection", err)
			}

			config.ClientCAs = caCertPool
		}
	}

	return config, nil
}

//
// Set up a TLS listener
//
func MakeTLSListener(tcpListener net.Listener) (net.Listener, error) {

	config, err := setupServerTLSConfig()
	if err != nil {
		return nil, err
	}

	if config != nil {
		listener := tls.NewListener(tcpListener, config)
		logging.Infof("TLS listener created for %v", listener.Addr().String())
		return listener, nil
	}

	return tcpListener, nil
}

//
// Make a new tcp listener for given address.
// Always make it secure, even if the security is not enabled.
//
func MakeAndSecureTCPListener(addr string) (net.Listener, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	tcpListener, err := MakeProtocolAwareTCPListener(addr)
	if err != nil {
		return nil, err
	}

	setting := GetSecuritySetting()
	if setting == nil {
		return nil, fmt.Errorf("Security setting required for TLS listener")
	}

	config, err := getTLSConfigFromSetting(setting)
	if err != nil {
		return nil, err
	}

	tlsListener := tls.NewListener(tcpListener, config)
	return tlsListener, nil
}

//
// Set up a TCP listener
//
func makeTCPListener(addr string) (net.Listener, error) {

	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return tcpListener, nil
}

//
// Set up a TCP listener.
// If the cluster setting dictates ipv6, then listener will bind only on
// ipv6 addresses. Similarly if the cluster setting dictates ipv4, then
// the listener will bind only on ipv4 addresses.
//
func MakeProtocolAwareTCPListener(addr string) (net.Listener, error) {

	protocol := "tcp4"
	if IsIpv6() {
		protocol = "tcp6"
	}

	tcpListener, err := net.Listen(protocol, addr)
	if err != nil {
		return nil, err
	}

	return tcpListener, nil
}

//
// Secure a TCP listener.  If encryption is requird, listener must already
// setup with SSL port.
//
func SecureListener(listener net.Listener) (net.Listener, error) {
	if EncryptionEnabled() {
		return MakeTLSListener(listener)
	}

	return listener, nil
}

//
// Set up a TLS or TCP listener, depending on whether encryption is used.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeListener(addr string) (net.Listener, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	listener, err := MakeProtocolAwareTCPListener(addr)
	if err != nil {
		return nil, err
	}

	listener2, err2 := SecureListener(listener)
	if err2 != nil {
		listener.Close()
		return nil, err
	}

	return listener2, nil
}

/////////////////////////////////////////////
// HTTP / HTTPS Client
/////////////////////////////////////////////

//
// Get URL.  This function will convert non-SSL port to SSL port when necessary.
//
func GetURL(u string) (*url.URL, error) {

	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "http://" + u
	}

	parsedUrl, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	parsedUrl.Host, _, _, err = EncryptPortFromAddr(parsedUrl.Host)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(parsedUrl.Host)
	if err != nil {
		return nil, err
	}

	if EncryptionRequired(host, port) {
		parsedUrl.Scheme = "https"
	} else {
		parsedUrl.Scheme = "http"
	}

	return parsedUrl, nil
}

//
// Setup TLSTransport
//
func getTLSTransport(host string) (*http.Transport, error) {

	tlsConfig, err := setupClientTLSConfig(host)
	if err != nil {
		return nil, err
	}

	// There is no clone method for http.DefaultTransport
	// (transport := *(http.DefaultTransport.(*http.Transport))) causes runtime issue
	// see https://github.com/golang/go/issues/26013
	// The following code is a simple solution, but can cause upgrade issue.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	transport.TLSClientConfig = tlsConfig

	return transport, nil
}

//
// Secure HTTP Client if necessary
//
func SecureClient(client *http.Client, u string) error {

	parsedUrl, err := GetURL(u)
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(parsedUrl.Host)
	if err != nil {
		return err
	}

	if EncryptionRequired(host, port) {
		t, err := getTLSTransport(host)
		if err != nil {
			return err
		}

		client.Transport = t
	}

	return nil
}

//
// Get HTTP client.  If encryption is enabled, client will be setup with TLS Transport.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeClient(u string) (*http.Client, error) {

	// create a new Client.  Do not use http.DefaultClient.
	client := &http.Client{}
	if err := SecureClient(client, u); err != nil {
		return nil, err
	}

	return client, nil
}

/////////////////////////////////////////////
// HTTP / HTTPS Request
/////////////////////////////////////////////

type RequestParams struct {
	Timeout   time.Duration
	UserAgent string
}

// GetWithAuthAndTimeout submits a REST call with the specified timeoutSecs and returns
// the response. To match Planner's historical usage this also converts any HTTP error
// to a user-friendly response.
func GetWithAuthAndTimeout(url string, timeoutSecs uint32) (*http.Response, error) {
	params := &RequestParams{Timeout: time.Duration(timeoutSecs) * time.Second}
	response, err := GetWithAuth(url, params)
	if err == nil && response.StatusCode != http.StatusOK {
		return response, convertHttpError(response)
	}
	return response, err
}

// convertHttpError checks for an error in an http response. If present it reads and closes the
// response body (which avoids leaking the TSL connection) and converts the response body
// to a user-friendly error message.
func convertHttpError(r *http.Response) error {
	if r.StatusCode != http.StatusOK {
		if r.Body != nil {
			defer r.Body.Close()

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r.Body); err == nil {
				return fmt.Errorf("response status:%v cause:%v", r.StatusCode, string(buf.Bytes()))
			}
		}
		return fmt.Errorf("response status:%v cause:Unknown", r.StatusCode)
	}
	return nil
}

// ConvertHttpResponse function unmarshals a successful HTTP response. Caller
// passes in a pointer to an object of the correct type to unmarshal to.
func ConvertHttpResponse(r *http.Response, resp interface{}) error {
	defer r.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), resp); err != nil {
		return err
	}

	return nil
}

//
// GetWithAuth performs an HTTP(S) GET request with optional URL parameters
// and Basic Authentication. If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL port to SSL port.
// params may be nil. eTag may be the empty string, in which case it is not transmitted.
//
func GetWithAuth(u string, params *RequestParams) (*http.Response, error) {
	return getWithAuthInternal(u, params, "", true)
}

//
// GetWithAuthAndETag performs an HTTP(S) GET with Basic Auth and optional ETag header field.
// If encryption is enabled, the request is made over HTTPS. This function will make use of
// encrypt port mapping to translate non-SSL port to SSL port. params may be nil. eTag may
// be the empty string, in which case it is not transmitted.
//
func GetWithAuthAndETag(u string, params *RequestParams, eTag string) (*http.Response, error) {
	return getWithAuthInternal(u, params, eTag, true)
}

//
// GetWithAuthNonTLS performs an HTTP GET with Basic Auth. This function will not convert HTTP URL
// to HTTPS using Encrypted Port Mapping.
//
func GetWithAuthNonTLS(u string, params *RequestParams) (*http.Response, error) {
	return getWithAuthInternal(u, params, "", false)
}

func getWithAuthInternal(u string, params *RequestParams, eTag string, allowTls bool) (*http.Response, error) {

	var url *url.URL
	var err error

	if allowTls {
		url, err = GetURL(u)
		if err != nil {
			return nil, err
		}
	} else {
		if strings.HasPrefix(u, "https://") {
			return nil, fmt.Errorf("URL String %s starts with https and allowTls is set to false", u)
		}

		if !strings.HasPrefix(u, "http://") {
			u = "http://" + u
		}

		url, err = url.Parse(u)
		if err != nil {
			return nil, err
		}
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("getWithAuthInternal: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	if eTag != "" {
		req.Header.Set("If-None-Match", eTag) // common.HTTP_KEY_ETAG_REQUEST
	}

	if params != nil && params.UserAgent != "" {
		req.Header.Add("User-agent", userAgentPrefix+params.UserAgent)
	}

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		return nil, err
	}

	var client *http.Client
	if allowTls {
		client, err = MakeClient(url.String())
		if err != nil {
			return nil, err
		}
	} else {
		client = &http.Client{}
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Post with Basic Auth.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func PostWithAuth(u string, bodyType string, body io.Reader, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("PostWithAuth: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("POST", url.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		return nil, err
	}

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Get.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func Get(u string, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("Get: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

//
// HTTP Post.  If encryption is enabled, the request is made over HTTPS.
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func Post(u string, bodyType string, body io.Reader, params *RequestParams) (*http.Response, error) {

	url, err := GetURL(u)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	defer func() {
		logging.Verbosef("Post: url %v elapsed %v", url.String(), time.Now().Sub(start))
	}()

	req, err := http.NewRequest("POST", url.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)

	client, err := MakeClient(url.String())
	if err != nil {
		return nil, err
	}

	if params != nil && params.Timeout >= time.Duration(0) {
		client.Timeout = params.Timeout
	}

	return client.Do(req)
}

/////////////////////////////////////////////
// HTTP / HTTPS Server
/////////////////////////////////////////////

//
// Make HTTPS Server
//
func MakeHTTPSServer(server *http.Server) error {

	// get server TLSConfig
	config, err := setupServerTLSConfig()
	if err != nil {
		return err
	}

	if config != nil {
		server.TLSConfig = config
		server.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)

		logging.Infof("HTTPS server created for %v", server.Addr)
	}

	return nil
}

//
// Secure the HTTP Server by setting TLS config
// Always secure the given HTTP server (even if the security is not enabled).
//
func SecureHTTPServer(server *http.Server) error {

	setting := GetSecuritySetting()
	if setting == nil {
		return fmt.Errorf("Security setting required for https server")
	}

	config, err := getTLSConfigFromSetting(setting)
	if err != nil {
		return err
	}

	if config != nil {
		server.TLSConfig = config
		server.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0)

		logging.Infof("HTTPS server created for %v", server.Addr)
	}

	return nil
}

//
// Make HTTP Server
//
func makeHTTPServer(addr string) (*http.Server, error) {

	srv := &http.Server{
		Addr: addr,
	}

	return srv, nil
}

//
// Secure HTTP server.
// It expects that server must already be setup with HTTPS port.
//
func SecureServer(server *http.Server) error {

	if EncryptionEnabled() {
		return MakeHTTPSServer(server)
	}

	return nil
}

//
// Make HTTP/HTTPS server
// This function will make use of encrypt port mapping to translate non-SSL
// port to SSL port.
//
func MakeHTTPServer(addr string) (*http.Server, error) {

	addr, _, _, err := EncryptPortFromAddr(addr)
	if err != nil {
		return nil, err
	}

	server, err := makeHTTPServer(addr)
	if err != nil {
		return nil, err
	}

	if err := SecureServer(server); err != nil {
		return nil, err
	}

	return server, nil
}

func GetLocalHost() string {
	if IsIpv6() {
		return "[::1]"
	} else {
		return "127.0.0.1"
	}
}

//
// Cluster wide ipv6 setting.
//

var _isIpv6 bool

func SetIpv6(isIpv6 bool) {
	_isIpv6 = isIpv6
}

func IsIpv6() bool {
	return _isIpv6
}
