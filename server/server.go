package server

import (
	"net"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/meateam/download-service/download"
	pb "github.com/meateam/download-service/proto"
	ilogger "github.com/meateam/elasticsearch-logger"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	configPort                 = "tcp_port"
	configHealthCheckInterval  = "health_check_interval"
	configElasticAPMIgnoreURLS = "elastic_apm_ignore_urls"
	configS3Endpoint           = "s3_endpoint"
	configS3Token              = "s3_token"
	configS3AccessKey          = "s3_access_key"
	configS3SecretKey          = "s3_secret_key"
)

func init() {
	viper.SetDefault(configPort, "8080")
	viper.SetDefault(configHealthCheckInterval, 3)
	viper.SetDefault(configElasticAPMIgnoreURLS, "/grpc.health.v1.Health/Check")
	viper.SetDefault(configS3Endpoint, "http://localhost:9000")
	viper.SetDefault(configS3Token, "")
	viper.SetDefault(configS3AccessKey, "")
	viper.SetDefault(configS3SecretKey, "")
	viper.AutomaticEnv()
}

// DownloadServer is a structure that holds the download server.
type DownloadServer struct {
	*grpc.Server
	logger              *logrus.Logger
	tcpPort             string
	healthCheckInterval int
	downloadService     *download.Service
}

// Serve accepts incoming connections on the self created listener, creating a new
// ServerTransport and service goroutine for each. The service goroutines
// read gRPC requests and then call the registered handlers to reply to them.
// Serve returns when lis.Accept fails with fatal errors.
// The listener will be closed when this method returns.
// Serve will log.Fatal a non-nil error unless Stop or GracefulStop is called.
func (s DownloadServer) Serve() {
	lis, err := net.Listen("tcp", ":"+s.tcpPort)
	if err != nil {
		s.logger.Fatalf("failed to listen: %v", err)
	}

	s.logger.Infof("listening and serving grpc server on port %s", s.tcpPort)
	if err := s.Server.Serve(lis); err != nil {
		s.logger.Fatalf(err.Error())
	}
}

// NewServer configures and creates a grpc.Server instance with the download service
// health check service.
// Configure using environment variables.
// `HEALTH_CHECK_INTERVAL`: Interval to update serving state of the health check server.
// `S3_ACCESS_KEY`: S3 accress key to connect with s3 backend.
// `S3_SECRET_KEY`: S3 secret key to connect with s3 backend.
// `S3_ENDPOINT`: S3 endpoint of s3 backend to connect to.
// `TCP_PORT`: TCO port on which the grpc server would serve on.
func NewServer() *DownloadServer {
	// Configuration variables
	s3AccessKey := viper.GetString(configS3AccessKey)
	s3SecretKey := viper.GetString(configS3SecretKey)
	s3Endpoint := viper.GetString(configS3Endpoint)
	s3Token := viper.GetString(configS3Token)

	// Configure to use S3 Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3AccessKey, s3SecretKey, s3Token),
		Endpoint:         aws.String(s3Endpoint),
		Region:           aws.String("eu-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	logger := ilogger.NewLogger()

	// Open a session to s3.
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	logger.Infof("connected to S3 - %s", s3Endpoint)

	// Create a client from the s3 session.
	s3Client := s3.New(newSession)

	// Set up grpc server opts with logger interceptor.
	serverOpts := append(
		serverLoggerInterceptor(logger),
		grpc.MaxRecvMsgSize(10<<20),
	)

	// Create a new grpc server.
	grpcServer := grpc.NewServer(
		serverOpts...,
	)

	// Create a download service and register it on the grpc server.
	downloadService := download.NewService(s3Client, logger)
	pb.RegisterDownloadServer(grpcServer, downloadService)

	// Create a health server and register it on the grpc server.
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	downloadServer := &DownloadServer{
		Server:              grpcServer,
		logger:              logger,
		tcpPort:             viper.GetString(configPort),
		healthCheckInterval: viper.GetInt(configHealthCheckInterval),
		downloadService:     downloadService,
	}

	// Health check validation goroutine worker.
	go downloadServer.healthCheckWorker(healthServer)

	return downloadServer
}

// serverLoggerInterceptor configures the logger interceptor for the download server.
func serverLoggerInterceptor(logger *logrus.Logger) []grpc.ServerOption {
	// Create new logrus entry for logger interceptor.
	logrusEntry := logrus.NewEntry(logger)

	ignorePayload := ilogger.IgnoreServerMethodsDecider(
		"/download.Download/Download",
		viper.GetString(configElasticAPMIgnoreURLS),
	)

	ignoreInitialRequest := ilogger.IgnoreServerMethodsDecider(
		viper.GetString(configElasticAPMIgnoreURLS),
	)

	// Shared options for the logger, with a custom gRPC code to log level function.
	loggerOpts := []grpc_logrus.Option{
		grpc_logrus.WithDecider(func(fullMethodName string, err error) bool {
			return ignorePayload(fullMethodName)
		}),
		grpc_logrus.WithLevels(grpc_logrus.DefaultCodeToLevel),
	}

	return ilogger.ElasticsearchLoggerServerInterceptor(
		logrusEntry,
		ignorePayload,
		ignoreInitialRequest,
		loggerOpts...,
	)
}

// healthCheckWorker is running an infinite loop that sets the serving status once
// in s.healthCheckInterval seconds.
func (s DownloadServer) healthCheckWorker(healthServer *health.Server) {
	s3Client := s.downloadService.GetS3Client()

	for {
		_, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		} else {
			healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		}

		time.Sleep(time.Second * time.Duration(s.healthCheckInterval))
	}
}
