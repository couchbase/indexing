package indexer

// EncryptionCtx is a local interface satisfied by gocbcrypto.EncryptionContext
// (enterprise) or nil (community / plaintext mode). Callers only need KeyID for
// tracking which key is in use, all crypto operations go through the helpers in
// encryption_enterprise.go / encryption_community.go.
type EncryptionCtx interface {
	KeyID() []byte
}
