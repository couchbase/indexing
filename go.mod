module github.com/couchbase/indexing

go 1.23.0

toolchain go1.23.4

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

replace github.com/couchbase/bhive => ../bhive

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/regulator => ../regulator

require (
	github.com/couchbase/bhive v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbauth v0.1.13
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/go-slab v0.0.0-20220303011136-e47646b420b3
	github.com/couchbase/gocb/v2 v2.9.4
	github.com/couchbase/goforestdb v0.0.0-20161215171854-0b501227de0e
	github.com/couchbase/gometa v0.0.0-20220803182802-05cb6b2e299f
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/logstats v1.0.0
	github.com/couchbase/nitro v0.0.0-20220707133503-f65f7a599cdf
	github.com/couchbase/plasma v0.0.0-00010101000000-000000000000
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0
	github.com/kshard/fvecs v0.0.2
	github.com/mschoch/smat v0.2.0
	github.com/prataprc/collatejson v0.0.0-20210210112148-85df4e1659d0
	github.com/prataprc/goparsec v0.0.0-20211219142520-daac0e635e7e
	github.com/prataprc/monster v0.0.0-20210210112206-07525cc27b6d
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.10.0
	golang.org/x/sync v0.12.0
	gopkg.in/couchbase/gocb.v1 v1.6.7
)

require (
	github.com/aws/aws-sdk-go-v2 v1.32.6 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.28.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.47 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.21 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.43 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.71.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.2 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/gocbcore/v10 v10.7.0 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gocbcoreps v0.1.3 // indirect
	github.com/couchbase/gomemcached v0.3.3 // indirect
	github.com/couchbase/goprotostellar v1.0.2 // indirect
	github.com/couchbase/tools-common/cloud/v7 v7.2.0 // indirect
	github.com/couchbase/tools-common/fs v1.0.2 // indirect
	github.com/couchbase/tools-common/strings v1.0.0 // indirect
	github.com/couchbase/tools-common/sync/v2 v2.0.1 // indirect
	github.com/couchbase/tools-common/testing v1.0.2 // indirect
	github.com/couchbase/tools-common/types/v2 v2.2.1 // indirect
	github.com/couchbase/tools-common/utils/v3 v3.0.2 // indirect
	github.com/couchbaselabs/c-forestdb v0.0.0-20160212203508-1b1267468faa // indirect
	github.com/couchbaselabs/c-snappy v0.0.0-20160212203049-a52f87e8ffc5 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20240607131231-fb385523de28 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20250210230444-5fae499d98fc // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.57.0 // indirect
	go.opentelemetry.io/otel v1.34.0 // indirect
	go.opentelemetry.io/otel/metric v1.34.0 // indirect
	go.opentelemetry.io/otel/trace v1.34.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/exp v0.0.0-20241204233417-43b7b7cde48d // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250227231956-55c901821b1e // indirect
	google.golang.org/grpc v1.71.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/couchbase/gocbcore.v7 v7.1.18 // indirect
	gopkg.in/couchbaselabs/gocbconnstr.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/gojcbmock.v1 v1.0.4 // indirect
	gopkg.in/couchbaselabs/jsonx.v1 v1.0.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
