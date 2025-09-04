//go:build windows && !community
// +build windows,!community

package codebook

// No-ops on windows
func SetOpenBLASThreadsInternal(n int) {}
func GetOpenBLASThreadsInternal() int  { return 0 }
