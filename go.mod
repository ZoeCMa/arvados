module git.arvados.org/arvados.git

go 1.17

require (
	github.com/AdRoll/goamz v0.0.0-20170825154802-2731d20f46f4
	github.com/Azure/azure-sdk-for-go v45.1.0+incompatible
	github.com/Azure/go-autorest/autorest v0.11.22
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.9
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/arvados/cgofuse v1.2.0-arvados1
	github.com/aws/aws-sdk-go v1.44.256
	github.com/aws/aws-sdk-go-v2 v1.26.1
	github.com/aws/aws-sdk-go-v2/config v1.27.12
	github.com/aws/aws-sdk-go-v2/credentials v1.17.12
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.16.16
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.160.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.53.2
	github.com/aws/smithy-go v1.20.2
	github.com/bmatcuk/doublestar/v4 v4.6.1
	github.com/bradleypeabody/godap v0.0.0-20170216002349-c249933bc092
	github.com/coreos/go-oidc/v3 v3.5.0
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e
	github.com/creack/pty v1.1.18
	github.com/docker/docker v24.0.9+incompatible
	github.com/dustin/go-humanize v1.0.0
	github.com/fsnotify/fsnotify v1.4.9
	github.com/ghodss/yaml v1.0.0
	github.com/go-ldap/ldap v3.0.3+incompatible
	github.com/gogo/protobuf v1.3.2
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-retryablehttp v0.7.6
	github.com/hashicorp/golang-lru v0.5.1
	github.com/hashicorp/yamux v0.0.0-20211028200310-0bc27b27de87
	github.com/imdario/mergo v0.3.12
	github.com/jmcvetta/randutil v0.0.0-20150817122601-2bb1b664bcff
	github.com/jmoiron/sqlx v1.2.0
	github.com/johannesboyne/gofakes3 v0.0.0-20240513200200-99de01ee122d
	github.com/julienschmidt/httprouter v1.3.0
	github.com/lib/pq v1.10.2
	github.com/msteinert/pam v0.0.0-20190215180659-f29b9f28d6f9
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.39.0
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/crypto v0.22.0
	golang.org/x/net v0.24.0
	golang.org/x/oauth2 v0.11.0
	golang.org/x/sys v0.20.0
	google.golang.org/api v0.126.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/square/go-jose.v2 v2.5.1
	rsc.io/getopt v0.0.0-20170811000552-20be20937449
)

require (
	cloud.google.com/go/compute v1.23.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.23 // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.4 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.3.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.17.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.20.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.23.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.28.7 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/dnaeon/go-vcr v1.2.0 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-connections v0.3.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/go-asn1-ber/asn1-ber v1.4.1 // indirect
	github.com/go-jose/go-jose/v3 v3.0.3 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/s2a-go v0.1.4 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.11.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/satori/go.uuid v1.2.1-0.20180404165556-75cca531ea76 // indirect
	github.com/shabbyrobe/gocovmerge v0.0.0-20190829150210-3e036491d500 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e // indirect
	golang.org/x/tools v0.8.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/grpc v1.59.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/asn1-ber.v1 v1.0.0-20181015200546-f715ec2f112d // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gotest.tools/v3 v3.0.3 // indirect
)

replace github.com/AdRoll/goamz => github.com/arvados/goamz v0.0.0-20190905141525-1bba09f407ef

replace gopkg.in/yaml.v2 => github.com/arvados/yaml v0.0.0-20210427145106-92a1cab0904b
