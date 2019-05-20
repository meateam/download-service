package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	pb "github.com/meateam/download-service/proto"
	"github.com/meateam/elogrus/v4"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"go.elastic.co/apm/module/apmgrpc"
	"go.elastic.co/apm/module/apmhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	traceIDHeader = apmhttp.TraceparentHeader
)

var (
	logger = initLogger()
)

func main() {
	interval := os.Getenv("HEALTH_CHECK_INTERVAL")
	healthCheckInterval, err := strconv.Atoi(interval)
	if err != nil {
		healthCheckInterval = 3
	}
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	tcpPort := os.Getenv("TCP_PORT")
	s3Token := ""

	// Configure to use S3 Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3AccessKey, s3SecretKey, s3Token),
		Endpoint:         aws.String(s3Endpoint),
		Region:           aws.String("eu-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)
	logger.Infof("connected to S3 - %s", s3Endpoint)
	s3Client := s3.New(newSession)
	lis, err := net.Listen("tcp", ":"+tcpPort)
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}
	logger.Infof("listening on port %s", tcpPort)

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(
		apmgrpc.NewUnaryServerInterceptor(apmgrpc.WithRecovery()),
	),
		grpc.MaxRecvMsgSize(10<<20))
	server := &DownloadService{s3Client: s3Client}
	pb.RegisterDownloadServer(grpcServer, server)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	// Health validation GoRoutine
	go func() {
		for {
			_, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
			if err != nil {
				healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			} else {
				healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			}
			time.Sleep(time.Second * time.Duration(healthCheckInterval))
		}
	}()
	logger.Infof("serving grpc server on port %s", tcpPort)
	grpcServer.Serve(lis)
}

func initLogger() *logrus.Logger {
	log := logrus.New()
	logLevel, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = logrus.ErrorLevel
	}

	logIndex := os.Getenv("LOG_INDEX")
	if logIndex == "" {
		logIndex = "KDrive"
	}

	log.SetLevel(logLevel)
	log.SetFormatter(&logrus.JSONFormatter{})

	elasticURL := os.Getenv("ELASTICSEARCH_URL")
	if elasticURL == "" {
		elasticURL = "http://localhost:9200"
	}

	serviceName := os.Getenv("DS_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "download-service"
	}

	elasticClient, err := elastic.NewClient(elastic.SetURL(elasticURL))
	if err != nil {
		log.Panic(err)
		return log
	}

	hook, err := elogrus.NewElasticHookWithFunc(elasticClient, serviceName, logLevel, func() string {
		year, month, day := time.Now().Date()
		return fmt.Sprintf("%s-%04d.%02d.%02d", logIndex, year, month, day)
	})
	if err != nil {
		log.Panic(err)
		return log
	}

	log.Hooks.Add(hook)
	logger := log
	return logger
}
