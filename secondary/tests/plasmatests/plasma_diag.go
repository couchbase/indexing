package indexer

import (
	"net/http"

	"github.com/couchbase/nitro/plasma"
)

func init() {
	http.HandleFunc("/plasmaDiag", plasma.Diag.HandleHttp)
	go func() {
		http.ListenAndServe(":8080", nil)
	}()
}
