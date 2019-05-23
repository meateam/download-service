module github.com/meateam/download-service

go 1.12

require (
	github.com/aws/aws-sdk-go v1.19.6
	github.com/golang/protobuf v1.3.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/meateam/elogrus/v4 v4.0.2
	github.com/olivere/elastic/v7 v7.0.0
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/viper v1.3.2
	go.elastic.co/apm/module/apmgrpc v1.3.0
	go.elastic.co/apm/module/apmhttp v1.3.0
	golang.org/x/net v0.0.0-20190327025741-74e053c68e29
	google.golang.org/grpc v1.19.1
)

replace github.com/meateam/download-service/logger => ./logger

replace go.elastic.co/apm/module/apmgrpc => github.com/omrishtam/apm-agent-go/module/apmgrpc v1.3.1-0.20190514172539-1b2e35db8668
