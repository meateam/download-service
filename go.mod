module github.com/meateam/download-service

go 1.12

require (
	github.com/aws/aws-sdk-go v1.19.6
	github.com/golang/protobuf v1.3.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/meateam/elasticsearch-logger v1.1.2
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/viper v1.3.2
	golang.org/x/net v0.0.0-20190327025741-74e053c68e29
	google.golang.org/grpc v1.21.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.12.0

replace github.com/meateam/download-service/proto => ./proto

replace github.com/meateam/download-service/download => ./download

replace github.com/meateam/download-service/server => ./server

replace go.elastic.co/apm/module/apmgrpc => github.com/omrishtam/apm-agent-go/module/apmgrpc v1.3.1-0.20190514172539-1b2e35db8668
