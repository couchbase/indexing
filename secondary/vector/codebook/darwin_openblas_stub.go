//go:build darwin && !community
// +build darwin,!community

package codebook

// No-ops on mac (no OpenBLAS linked)
func SetOpenBLASThreadsInternal(n int) {}
func GetOpenBLASThreadsInternal() int  { return 0 }
