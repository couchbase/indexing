module github.com/couchbase/indexing

go 1.22.7

toolchain go1.24.4

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/cbauth => ../cbauth

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/gometa => ../gometa

replace github.com/couchbase/goutils => ../goutils

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/hebrew => ../../../../../hebrew

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/nitro => ../nitro

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/regulator => ../regulator

require (
	github.com/couchbase/cbauth v0.1.12
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/go-slab v0.0.0-20220303011136-e47646b420b3
	github.com/couchbase/goforestdb v0.0.0-20161215171854-0b501227de0e
	github.com/couchbase/gometa v0.0.0-20220803182802-05cb6b2e299f
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/logstats v1.0.0
	github.com/couchbase/nitro v0.0.0-20220707133503-f65f7a599cdf
	github.com/couchbase/plasma v0.0.0-00010101000000-000000000000
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4
	github.com/mschoch/smat v0.2.0
	github.com/prataprc/collatejson v0.0.0-20210210112148-85df4e1659d0
	github.com/prataprc/goparsec v0.0.0-20211219142520-daac0e635e7e
	github.com/prataprc/monster v0.0.0-20210210112206-07525cc27b6d
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	gopkg.in/couchbase/gocb.v1 v1.6.7
)

require (
	github.com/aws/aws-sdk-go-v2 v1.36.5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.11 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.29.17 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.70 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.32 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.83 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.83.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.34.0 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/gocb/v2 v2.5.4 // indirect
	github.com/couchbase/gocbcore/v10 v10.5.2 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gomemcached v0.2.2-0.20230407174933-7d7ce13da8cc // indirect
	github.com/couchbase/tools-common/cloud/v7 v7.2.0 // indirect
	github.com/couchbase/tools-common/fs v1.0.2 // indirect
	github.com/couchbase/tools-common/strings v1.0.0 // indirect
	github.com/couchbase/tools-common/sync/v2 v2.0.1 // indirect
	github.com/couchbase/tools-common/testing v1.0.2 // indirect
	github.com/couchbase/tools-common/types/v2 v2.2.1 // indirect
	github.com/couchbase/tools-common/utils/v3 v3.0.2 // indirect
	github.com/couchbaselabs/c-forestdb v0.0.0-20160212203508-1b1267468faa // indirect
	github.com/couchbaselabs/c-snappy v0.0.0-20160212203049-a52f87e8ffc5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/exp v0.0.0-20241204233417-43b7b7cde48d // indirect
	golang.org/x/net v0.32.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/protobuf v1.35.2 // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
