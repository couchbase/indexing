package mockcbauth

import (
	"crypto/tls"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/httpreq"
)

type AuthImpl struct {
	grStatuses   cbauth.GuardrailStatuses
	grStatusErr  error
	cfgRefreshCB cbauth.ConfigRefreshCallback
}

var MockAuthenticator *AuthImpl

func init() {
	MockAuthenticator = new(AuthImpl)
	cbauth.Default = MockAuthenticator
}

func (a *AuthImpl) AuthWebCreds(req *http.Request) (creds cbauth.Creds, err error) {
	return nil, nil
}

func (a *AuthImpl) AuthWebCredsGeneric(req httpreq.HttpRequest) (creds cbauth.Creds, err error) {
	return nil, nil
}

func (a *AuthImpl) Auth(user, pwd string) (creds cbauth.Creds, err error) {
	return nil, nil
}

func (a *AuthImpl) GetHTTPServiceAuth(hostport string) (user, pwd string, err error) {
	return "", "", nil
}

func (a *AuthImpl) GetMemcachedServiceAuth(hostport string) (user, pwd string, err error) {
	return "", "", nil
}

func (a *AuthImpl) RegisterTLSRefreshCallback(callback cbauth.TLSRefreshCallback) error {
	return nil
}

func (a *AuthImpl) GetClientCertAuthType() (tls.ClientAuthType, error) {
	return 0, nil
}

func (a *AuthImpl) GetClusterEncryptionConfig() (cbauth.ClusterEncryptionConfig, error) {
	return cbauth.ClusterEncryptionConfig{}, nil
}

func (a *AuthImpl) GetTLSConfig() (cbauth.TLSConfig, error) {
	return cbauth.TLSConfig{}, nil
}

func (a *AuthImpl) GetUserUuid(user, domain string) (string, error) {
	return "", nil
}

func (a *AuthImpl) GetUserBuckets(user, domain string) ([]string, error) {
	return nil, nil
}

func (a *AuthImpl) RegisterConfigRefreshCallback(callback cbauth.ConfigRefreshCallback) error {
	a.cfgRefreshCB = callback
	return nil
}

func (a *AuthImpl) GetGuardrailStatuses() (cbauth.GuardrailStatuses, error) {
	return a.grStatuses, a.grStatusErr
}

func (a *AuthImpl) TriggerConfigRefreshCallback(code uint64) error {
	return a.cfgRefreshCB(code)
}

func (a *AuthImpl) SetGuardrailStatuses(gss cbauth.GuardrailStatuses, err error) {
	a.grStatuses = gss
	a.grStatusErr = err
}
