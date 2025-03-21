//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package security

import (
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	cbtls "github.com/couchbase/goutils/tls"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
)

//////////////////////////////////////////////////////
// CBAUTH security/encryption setting
//////////////////////////////////////////////////////

type SecuritySetting struct {
	encryptionEnabled bool
	disableNonSSLPort bool

	// srvrCert: Is loaded from serverCertFile.
	//              This is used to set tls.Config.Certificates
	srvrCert *tls.Certificate

	// srvrCertInBytes: Represents contents on the serverCertFile.
	//              This is used to set tls.Config.RootCAs or tls.Config.ClientCAs
	//              These won't be useful if caInBytes is not empty
	srvrCertInBytes []byte

	// caInBytes: Represents contents on the caFile.
	//            This is used to set tls.Config.RootCAs or tls.Config.ClientCAs
	//            This take precedence over certInBytes
	caInBytes []byte

	// clientCert: Is loaded from clientCertFile.
	// 			This is used to set tls.Config.Certificates
	clientCert *tls.Certificate

	// clientCertInBytes: Represents contents on the clientCertFile.
	//              This is used to set tls.Config.RootCAs or tls.Config.ClientCAs
	//              These won't be useful if caInBytes is not empty
	clientCertInBytes []byte

	tlsPreference *cbauth.TLSConfig
}

var pSecuritySetting unsafe.Pointer = unsafe.Pointer(new(SecuritySetting))

func GetSecuritySetting() *SecuritySetting {
	return (*SecuritySetting)(atomic.LoadPointer(&pSecuritySetting))
}

func UpdateSecuritySetting(s *SecuritySetting) {
	atomic.StorePointer(&pSecuritySetting, unsafe.Pointer(s))
}

func EncryptionEnabled() bool {
	setting := GetSecuritySetting()
	if setting == nil {
		return false
	}
	return setting.encryptionEnabled
}

func DisableNonSSLPort() bool {
	setting := GetSecuritySetting()
	if setting == nil {
		return false
	}
	return setting.disableNonSSLPort
}

// ShouldUseClientCertAuth returns the TLS config indicating if clients should only use cert based
// auth or basic auth
func ShouldUseClientCertAuth() bool {
	var settings = GetSecuritySetting()
	if settings == nil {
		return false
	}
	return settings.encryptionEnabled && settings.tlsPreference.ShouldClientsUseClientCert
}

//////////////////////////////////////////////////////
// Security Context
//////////////////////////////////////////////////////

type ConsoleLogger func(error)

type SecurityContext struct {
	//initialization
	initializer   sync.Once
	initializedCh chan bool
	logger        ConsoleLogger
	isInitialized int32

	// certificate
	srvrCertFile   string
	srvrKeyFile    string
	caFile         string
	clientCertFile string
	clientKeyFile  string

	// encryption for localhost
	encryptLocalHost bool
	localhosts       map[string]bool

	// TLS port mapping
	encryptPortMap unsafe.Pointer
	encryptPorts   unsafe.Pointer
	encryptPortRev atomic.Int64

	// notifier
	mutex     sync.RWMutex
	notifiers map[string]SecurityChangeNotifier
}

var pSecurityContext *SecurityContext
var pContextInitializer sync.Once

func init() {
	pSecurityContext = &SecurityContext{
		initializedCh:  make(chan bool),
		notifiers:      make(map[string]SecurityChangeNotifier),
		localhosts:     make(map[string]bool),
		encryptPortRev: atomic.Int64{},
	}

	emptyMap1 := make(map[string]string, 0)
	atomic.StorePointer(&pSecurityContext.encryptPortMap, unsafe.Pointer(&emptyMap1))

	emptyMap2 := make(map[string]bool, 0)
	atomic.StorePointer(&pSecurityContext.encryptPorts, unsafe.Pointer(&emptyMap2))

	pSecurityContext.encryptPortRev.Store(0)
}

// InitSecurityContext initialises the global SecurityContext object with relevant cert files
// and encryption port maps
func InitSecurityContext(logger ConsoleLogger, localhost, srvrCertFile, srvrKeyFile, caFile,
	clientCertFile, clientKeyFile string, encryptLocalHost bool) (err error) {

	pContextInitializer.Do(func() {
		var ips map[string]bool
		ips, err = buildLocalAddr(localhost)
		if err != nil {
			return
		}

		pSecurityContext = &SecurityContext{
			logger:           logger,
			srvrCertFile:     srvrCertFile,
			srvrKeyFile:      srvrKeyFile,
			clientCertFile:   clientCertFile,
			clientKeyFile:    clientKeyFile,
			caFile:           caFile,
			initializedCh:    make(chan bool),
			notifiers:        make(map[string]SecurityChangeNotifier),
			encryptLocalHost: encryptLocalHost,
			localhosts:       ips,
		}

		emptyMap1 := make(map[string]string, 0)
		atomic.StorePointer(&pSecurityContext.encryptPortMap, unsafe.Pointer(&emptyMap1))

		emptyMap2 := make(map[string]bool, 0)
		atomic.StorePointer(&pSecurityContext.encryptPorts, unsafe.Pointer(&emptyMap2))

		pSecurityContext.encryptPortRev.Store(0)

	})

	return
}

func WaitForSecurityCtxInit() {
	<-pSecurityContext.initializedCh
	logging.Infof("tls_setting: security context - encryptLocalHost %v Local IP's: %v",
		pSecurityContext.encryptLocalHost, pSecurityContext.localhosts)
}

func InitSecurityContextForClient(logger ConsoleLogger, localhost string, certFile, keyFile, caFile string, encryptLocalHost bool) (err error) {

	pContextInitializer.Do(func() {
		var ips map[string]bool
		ips, err = buildLocalAddr(localhost)
		if err != nil {
			return
		}

		pSecurityContext.logger = logger
		pSecurityContext.srvrCertFile = certFile
		pSecurityContext.srvrKeyFile = keyFile
		pSecurityContext.caFile = caFile
		pSecurityContext.encryptLocalHost = encryptLocalHost
		pSecurityContext.localhosts = ips
	})

	return
}

func Refresh(tlsConfig cbauth.TLSConfig, encryptConfig cbauth.ClusterEncryptionConfig,
	srvrCertFile, srvrKeyFile, caFile, clientCertFile, clientKeyFile string) {

	logging.Infof("Receive security change notification. encryption=%v", encryptConfig.EncryptData)

	newSetting := &SecuritySetting{}

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
	}

	newSetting.tlsPreference = &tlsConfig
	newSetting.encryptionEnabled = encryptConfig.EncryptData
	newSetting.disableNonSSLPort = encryptConfig.DisableNonSSLPorts

	if err := pSecurityContext.refreshCert(
		srvrCertFile,
		srvrKeyFile,
		caFile,
		clientCertFile, //clientCertFile string
		clientKeyFile,  // clientKeyFile string
		newSetting,
	); err != nil {
		logging.Errorf("error in reading certifcate %v", err)
		return
	}

	if err := pSecurityContext.update(newSetting, true, true); err != nil {
		logging.Errorf("Fail to update security setting %v", err)
		return
	}
}

// Used by cbindex to add cacert
func SetTLSConfigAndCACert(tlsConfig *cbauth.TLSConfig,
	encryptConfig *cbauth.ClusterEncryptionConfig,
	certFile, caFile string) {

	newSetting := &SecuritySetting{}

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
	}

	newSetting.tlsPreference = tlsConfig
	newSetting.encryptionEnabled = encryptConfig.EncryptData
	newSetting.disableNonSSLPort = encryptConfig.DisableNonSSLPorts

	if certFile != "" {
		certInBytes, err := iowrap.Ioutil_ReadFile(certFile)
		if err != nil {
			logging.Errorf("Fail to load SSL certificate"+
				" from File: %v, err; %v", certFile, err)
		}
		newSetting.srvrCertInBytes = certInBytes
	}

	if caFile != "" {
		caCertInBytes, err := iowrap.Ioutil_ReadFile(caFile)
		if err != nil {
			logging.Errorf("Fail to load SSL certificate"+
				" from File: %v, err; %v", caFile, err)
		}
		newSetting.caInBytes = caCertInBytes
	}

	UpdateSecuritySetting(newSetting)
}

func buildLocalAddr(localhost string) (map[string]bool, error) {

	var hostname string
	var err error

	if localhost != "" {
		hostname, _, err = net.SplitHostPort(localhost)
		if err != nil {
			return nil, err
		}
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	ips := make(map[string]bool)
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		case *net.TCPAddr:
			ip = v.IP
		case *net.UDPAddr:
			ip = v.IP
		}

		if ip != nil {
			ips[ip.String()] = true
		}
	}

	if localhost != "" {
		ips[hostname] = true
	}

	return ips, nil
}

func EncryptionRequired(host string, port string) bool {
	if !pSecurityContext.initialized() {
		return false
	}

	if !EncryptionEnabled() {
		return false
	}

	if !encryptLocalHost() && IsLocal(host) {
		// If it is local IP, then do not encrypt
		return false
	}

	// encrypt only if port is a known TLS port or it has a TLS port mapping
	return isTLSPort(port) || hasTLSPort(port)
}

func isTLSPort(port string) bool {
	ports := GetEncryptPorts()
	_, ok := ports[port]
	return ok
}

func hasTLSPort(port string) bool {
	mapping := GetEncryptPortMapping()
	_, ok := mapping[port]
	return ok
}

func (p *SecurityContext) initialized() bool {
	return atomic.LoadInt32(&pSecurityContext.isInitialized) == 1
}

func (p *SecurityContext) setInitialized() {
	atomic.StoreInt32(&pSecurityContext.isInitialized, 1)
	logging.Infof("tls_setting: security context initialized")
}

// ////////////////////////////////////////////////////
// Handle Security Change
// ////////////////////////////////////////////////////
func SecurityConfigRefresh(code uint64) error {
	return pSecurityContext.refresh(code)
}

func (p *SecurityContext) refresh(code uint64) error {

	logging.Infof("tls_setting: receive security change notification. code %v", code)

	newSetting := &SecuritySetting{}

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
	}

	var certsRefreshed = code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 ||
		code&cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0

	if certsRefreshed {
		if err := p.refreshConfig(newSetting); err != nil {
			return err
		}

		if err := p.refreshCert(p.srvrCertFile, // serverCertFile string
			p.srvrKeyFile,    // serverKeyFile string
			p.caFile,         // CAFile string
			p.clientCertFile, // clientCertFile string
			p.clientKeyFile,  // clientKeyFile string
			newSetting,       // setting *SecuritySetting
		); err != nil {
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		if err := p.refreshEncryption(newSetting); err != nil {
			return err
		}
	}

	return p.update(newSetting, // newSetting *SecuritySetting
		code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0,        // refreshServerCert
		code&cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0, // refreshClientCert
	)
}

func hasCipherSuitesChanged(oldTLSPref *cbauth.TLSConfig, newTLSPref *cbauth.TLSConfig) bool {
	var oldTLSCipherSuite, newTLSCipherSuite []uint16
	var oldTLSPrefCipherSuite, newTLSPrefCipherSuite bool
	if oldTLSPref != nil {
		oldTLSCipherSuite = oldTLSPref.CipherSuites
		oldTLSPrefCipherSuite = oldTLSPref.PreferServerCipherSuites
	}
	if newTLSPref != nil {
		newTLSCipherSuite = newTLSPref.CipherSuites
		newTLSPrefCipherSuite = newTLSPref.PreferServerCipherSuites
	}
	return oldTLSPrefCipherSuite != newTLSPrefCipherSuite ||
		(!reflect.DeepEqual(oldTLSCipherSuite, newTLSCipherSuite))
}

func (p *SecurityContext) update(newSetting *SecuritySetting, refreshServerCert, refreshClientCert bool) error {

	hasEnabled := false
	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		hasEnabled = oldSetting.encryptionEnabled
	}
	refreshEncrypt := hasEnabled != newSetting.encryptionEnabled

	if oldSetting != nil {
		// if Cipher suites have changed, then we should consider that as refreshEncrypt
		// and restart the servers as in refreshServerCert we will only update the inmem configs
		refreshEncrypt = refreshEncrypt ||
			hasCipherSuitesChanged(oldSetting.tlsPreference, newSetting.tlsPreference)
	}

	UpdateSecuritySetting(newSetting)

	if !refreshEncrypt && !refreshServerCert {
		logging.Infof("tls_setting: encryption is not enabled or no certificate refresh. Do not notify security change")
		return nil
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for key, notifier := range p.notifiers {
		logging.Infof("tls_setting: notify security setting change for %v", key)
		if err := notifier(refreshServerCert, refreshClientCert, refreshEncrypt); err != nil {
			err1 := fmt.Errorf("Fail to refresh security setting for %v: %v", key, err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
		}
	}

	p.initializer.Do(func() {
		close(p.initializedCh)
	})

	logging.Infof("tls_setting: updated notifiers on security change")

	return nil
}

func (p *SecurityContext) refreshConfig(setting *SecuritySetting) error {

	newConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		err1 := fmt.Errorf("tls_setting: Fail to refresh TLSConfig due to error: %v", err)
		if p.logger != nil {
			p.logger(err1)
		}
		logging.Fatalf(err1.Error())
		return err
	}

	setting.tlsPreference = &newConfig

	logging.Infof("tls_setting: TLS config refreshed successfully")

	return nil
}

func (p *SecurityContext) refreshCert(srvrCertFile, srvrKeyFile, caFile, clientCertFile,
	clientKeyFile string, setting *SecuritySetting) error {

	if len(srvrCertFile) == 0 || len(srvrKeyFile) == 0 {
		logging.Warnf("tls_setting: certifcate location is missing.  Cannot refresh certifcate")
		return nil
	}

	if len(caFile) > 0 {
		caInBytes, err := iowrap.Ioutil_ReadFile(caFile)
		if err != nil {
			err1 := fmt.Errorf("Fail to load SSL certificates from cfile: %v", err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
			return err
		}

		setting.caInBytes = caInBytes
	}

	var privateKeyPassphrase []byte
	if setting.tlsPreference != nil {
		privateKeyPassphrase = setting.tlsPreference.PrivateKeyPassphrase
	}

	if len(srvrCertFile) > 0 && len(srvrKeyFile) > 0 {
		cert, err := cbtls.LoadX509KeyPair(srvrCertFile, srvrKeyFile, privateKeyPassphrase)
		if err != nil {
			err1 := fmt.Errorf("Fail to due generate SSL certificate: %v", err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
			return err
		}

		certInBytes, err := iowrap.Ioutil_ReadFile(srvrCertFile)
		if err != nil {
			err1 := fmt.Errorf("Fail to due load SSL certificate from file: %v", err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
			return err
		}

		setting.srvrCertInBytes = certInBytes
		setting.srvrCert = &cert
	}

	var clientPrivateKeyPassphrase []byte
	if setting.tlsPreference != nil {
		clientPrivateKeyPassphrase = setting.tlsPreference.ClientPrivateKeyPassphrase
	}

	if len(clientCertFile) > 0 && len(clientKeyFile) > 0 {
		cert, err := cbtls.LoadX509KeyPair(clientCertFile, clientKeyFile, clientPrivateKeyPassphrase)
		if err != nil {
			err1 := fmt.Errorf("Fail to due generate SSL certificate: %v", err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
			return err
		}

		certInBytes, err := iowrap.Ioutil_ReadFile(clientCertFile)
		if err != nil {
			err1 := fmt.Errorf("Fail to due load SSL certificate from file: %v", err)
			if p.logger != nil {
				p.logger(err1)
			}
			logging.Fatalf(err1.Error())
			return err
		}

		setting.clientCertInBytes = certInBytes
		setting.clientCert = &cert
	}

	logging.Infof("tls_setting: certificate refreshed successfully with serverCertFile %v, serverKeyFile %v, caFile %v, clientCertFile %v, clientKeyFile %v",
		srvrCertFile, srvrKeyFile, caFile, clientCertFile, clientKeyFile)

	return nil
}

func (p *SecurityContext) refreshEncryption(setting *SecuritySetting) error {

	cfg, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		err1 := fmt.Errorf("Fail to due load encryption config: %v", err)
		if p.logger != nil {
			p.logger(err1)
		}
		logging.Fatalf(err1.Error())
		return err
	}

	setting.encryptionEnabled = cfg.EncryptData
	setting.disableNonSSLPort = cfg.DisableNonSSLPorts

	logging.Infof("tls_setting: encryption config refresh successfully. encryption enabled=%v", setting.encryptionEnabled)

	return nil
}

//////////////////////////////////////////////////////
// Security Change Notifier
//////////////////////////////////////////////////////

type SecurityChangeNotifier func(refreshServerCert, refreshClientCert, refreshEncrypt bool) error

func RegisterCallback(key string, cb SecurityChangeNotifier) {
	pSecurityContext.mutex.Lock()
	defer pSecurityContext.mutex.Unlock()

	pSecurityContext.notifiers[key] = cb
}

//////////////////////////////////////////////////////
// Encrypt Port Mapping
// - provide mapping from non-SSL port to SSL port
//////////////////////////////////////////////////////

func UpdateEncryptPortMapping(mapping map[string]string, rev int64) {
	if rev <= pSecurityContext.encryptPortRev.Load() && rev > 0 {
		return
	}

	pSecurityContext.encryptPortRev.Store(rev)
	SetEncryptPortMapping(mapping)
}

func SetEncryptPortMapping(mapping map[string]string) {
	ports := make(map[string]bool)
	for _, encrypted := range mapping {
		ports[encrypted] = true
	}

	/*
		there are 2 cases when these maps can go out of sync with the readers -
		* TLS config refresh
		* cluster topology change

		in both cases, readers can be susceptible to having shadow reads but it is fine. this will
		lead to clients like projector/GSI client retrying the connection with the server by
		creating a new connection which should then get the updated value of the map.

		this problem only exists for setups which do not have default ports on indexer else in all
		default cases these maps won't be undergoing changes
	*/
	atomic.StorePointer(&pSecurityContext.encryptPortMap, unsafe.Pointer(&mapping))
	atomic.StorePointer(&pSecurityContext.encryptPorts, unsafe.Pointer(&ports))

	pSecurityContext.setInitialized()

	logging.Infof("security port mapping updated : %v", mapping)
}

func GetEncryptPortMapping() map[string]string {
	return *(*map[string]string)(atomic.LoadPointer(&pSecurityContext.encryptPortMap))
}

func GetEncryptPorts() map[string]bool {
	return *(*map[string]bool)(atomic.LoadPointer(&pSecurityContext.encryptPorts))
}

func EncryptPortFromAddr(addr string) (string, string, string, error) {

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, "", "", err
	}

	port = EncryptPort(host, port)
	return net.JoinHostPort(host, port), host, port, nil
}

func EncryptPortInAddr(addr string) (string, error) {

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, err
	}

	port = EncryptPort(host, port)
	return net.JoinHostPort(host, port), nil
}

func EncryptPort(host string, port string) string {

	if EncryptionRequired(host, port) {
		mapping := GetEncryptPortMapping()
		for port1, port2 := range mapping {
			if port == port1 {
				return port2
			}
		}
	}

	return port
}

//////////////////////////////////////////////////////
// Skip Encryption on Localhost
//////////////////////////////////////////////////////

func encryptLocalHost() bool {

	if pSecurityContext.encryptLocalHost {
		return true
	}

	return DisableNonSSLPort()
}

func IsLocal(host string) bool {

	// empty host is treated as unknown host, rather than localhost
	if len(host) == 0 {
		return false
	}

	localhosts := pSecurityContext.localhosts
	if match, ok := localhosts[host]; ok {
		return match
	}

	ips, err := net.LookupIP(host)
	if err == nil {
		for _, ip := range ips {
			if match, ok := localhosts[ip.String()]; ok {
				return match
			}
		}
	}

	return false
}
