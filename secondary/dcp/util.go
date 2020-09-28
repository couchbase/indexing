package couchbase

import (
	"fmt"
	"net/url"
	"strings"
)

// CleanupHost returns the hostname with the given suffix removed.
func CleanupHost(h, commonSuffix string) string {
	if strings.HasSuffix(h, commonSuffix) {
		return h[:len(h)-len(commonSuffix)]
	}
	return h
}

// FindCommonSuffix returns the longest common suffix from the given
// strings.
func FindCommonSuffix(input []string) string {
	rv := ""
	if len(input) < 2 {
		return ""
	}
	from := input
	for i := len(input[0]); i > 0; i-- {
		common := true
		suffix := input[0][i:]
		for _, s := range from {
			if !strings.HasSuffix(s, suffix) {
				common = false
				break
			}
		}
		if common {
			rv = suffix
		}
	}
	return rv
}

// ParseURL is a wrapper around url.Parse with some sanity-checking
func ParseURL(urlStr string) (result *url.URL, err error) {
	result, err = url.Parse(urlStr)
	if result != nil && result.Scheme == "" {
		result = nil
		err = fmt.Errorf("invalid URL <%s>", urlStr)
	}
	return
}

//
// Taken from golang implementation of url:unescape
//
func PathUnescape(s string) (string, error) {
	ishex := func(c byte) bool {
		switch {
		case '0' <= c && c <= '9':
			return true
		case 'a' <= c && c <= 'f':
			return true
		case 'A' <= c && c <= 'F':
			return true
		}
		return false
	}

	unhex := func(c byte) byte {
		switch {
		case '0' <= c && c <= '9':
			return c - '0'
		case 'a' <= c && c <= 'f':
			return c - 'a' + 10
		case 'A' <= c && c <= 'F':
			return c - 'A' + 10
		}
		return 0
	}

	// Count %, check that they're well-formed.
	n := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '%':
			n++
			if i+2 >= len(s) || !ishex(s[i+1]) || !ishex(s[i+2]) {
				s = s[i:]
				if len(s) > 3 {
					s = s[:3]
				}
				return "", url.EscapeError(s)
			}
			i += 3
		default:
			i++
		}
	}

	if n == 0 {
		return s, nil
	}

	t := make([]byte, len(s)-2*n)
	j := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '%':
			t[j] = unhex(s[i+1])<<4 | unhex(s[i+2])
			j++
			i += 3
		default:
			t[j] = s[i]
			j++
			i++
		}
	}

	return string(t), nil
}
