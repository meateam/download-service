package logger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/meateam/elogrus/v4"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"go.elastic.co/apm/module/apmhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	traceIDHeader = apmhttp.TraceparentHeader
)

// NewLogger creates a `*logrus.Logger` with `elogrus` hook,
// which logs to elasticsearch, and returns it.
func NewLogger() *logrus.Logger {
	log := logrus.New()
	logLevel, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = logrus.ErrorLevel
	}

	logIndex := strings.ToLower(os.Getenv("LOG_INDEX"))
	if logIndex == "" {
		logIndex = "kdrive"
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

	elasticClient, err := elastic.NewClient(elastic.SetURL(elasticURL), elastic.SetSniff(false))
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

// WithElasticsearchLogger sets up a `grpc.ServerOption` to intercept streams with
// `*logrus.Entry` of the logger, created with `NewLogger`, and the options given to it.
// Returns the `grpc.ServerOption` which will be used in `grpc.NewServer`
// to log all incoming stream calls.
func WithElasticsearchLogger(logrusEntry *logrus.Entry, opts ...grpc_logrus.Option) grpc.ServerOption {
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	return grpc_middleware.WithStreamServerChain(
		func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			logCtx := ctxlogrus.ToContext(stream.Context(), logrusEntry)
			ctxlogrus.AddFields(logCtx, logrus.Fields{
				"trace.id": ExtractTraceParent(stream.Context()),
			})

			return grpc_logrus.StreamServerInterceptor(ctxlogrus.Extract(logCtx), opts...)(srv, stream, info, handler)
		},
		grpc_ctxtags.StreamServerInterceptor(
			grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor),
		),
	)
}

// ExtractTraceParent gets a `context.Context` which holds the "Elastic-Apm-Traceparent",
// which is the HTTP header for trace propagation, and returns the trace id.
func ExtractTraceParent(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(traceIDHeader); len(values) == 1 {
			traceCtx, err := apmhttp.ParseTraceparentHeader(values[0])
			if err == nil {
				return traceCtx.Trace.String()
			}
		}
	}

	return ""
}
