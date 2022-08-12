module github.com/couchbase/indexing

go 1.18

replace github.com/couchbase/query => ../query

replace github.com/couchbase/cbas => ../cbas

replace github.com/couchbase/cbauth => ../cbauth

replace github.com/couchbase/goutils => ../goutils

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/gometa => ../gometa

replace github.com/couchbase/regulator => ../regulator

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/go_json => ../go_json

require (
	github.com/couchbase/cbauth v0.1.2
	github.com/couchbase/go-slab v0.0.0-20220303011136-e47646b420b3
	github.com/couchbase/goforestdb v0.0.0-20161215171854-0b501227de0e
	github.com/couchbase/gometa v0.0.0-20220803182802-05cb6b2e299f
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/logstats v0.0.0-20220303011129-24ba9753289f
	github.com/couchbase/nitro v0.0.0-20220707133503-f65f7a599cdf
	github.com/couchbase/plasma v0.0.0-00010101000000-000000000000
	github.com/couchbase/query v0.0.0-20220808005213-5fcf49bd5eb4
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/mschoch/smat v0.2.0
	github.com/prataprc/collatejson v0.0.0-20210210112148-85df4e1659d0
	github.com/prataprc/goparsec v0.0.0-20211219142520-daac0e635e7e
	github.com/prataprc/monster v0.0.0-20210210112206-07525cc27b6d
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	gopkg.in/couchbase/gocb.v1 v1.6.7
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/gocb/v2 v2.5.2 // indirect
	github.com/couchbase/gocbcore/v10 v10.1.4 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gomemcached v0.1.5-0.20220627085811-f29815b6005a // indirect
	github.com/couchbasedeps/go-curl v0.0.0-20190830233031-f0b2afc926ec // indirect
	github.com/couchbaselabs/c-forestdb v0.0.0-20160212203508-1b1267468faa // indirect
	github.com/couchbaselabs/c-snappy v0.0.0-20160212203049-a52f87e8ffc5 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/net v0.0.0-20220805013720-a33c5aa5df48 // indirect
	golang.org/x/sys v0.0.0-20220804214406-8e32c043e418 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect
)
