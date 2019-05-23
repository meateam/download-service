package logger

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/logging"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/meateam/elogrus/v4"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.elastic.co/apm/module/apmhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	traceIDHeader          = apmhttp.TraceparentHeader
	configLogLevel         = "log_level"
	configLogIndex         = "log_index"
	configElasticsearchURL = "elasticsearch_url"
	configHostName         = "host_name"
)

func init() {
	viper.SetDefault(configLogLevel, logrus.ErrorLevel)
	viper.SetDefault(configLogIndex, "log")
	viper.SetDefault(configElasticsearchURL, "http://localhost:9200")

	hostName := filepath.Base(os.Args[0])
	if runtime.GOOS == "windows" {
		hostName = strings.TrimSuffix(hostName, filepath.Ext(hostName))
	}

	viper.SetDefault(configHostName, hostName)
	viper.AutomaticEnv()
}

// JSONPbMarshaller is a struct used to marshal a protobuf message to JSON.
type JSONPbMarshaller struct {
	proto.Message
}

// MarshalJSON marshals a protobuf message to JSON.
func (j *JSONPbMarshaller) MarshalJSON() ([]byte, error) {
	b := &bytes.Buffer{}
	if err := grpc_logrus.JsonPbMarshaller.Marshal(b, j.Message); err != nil {
		return nil, fmt.Errorf("jsonpb serializer failed: %v", err)
	}

	return b.Bytes(), nil
}

// NewLogger creates a `*logrus.Logger` with `elogrus` hook,
// which logs to elasticsearch, and returns it.
func NewLogger() *logrus.Logger {
	logLevel, err := logrus.ParseLevel(viper.GetString(configLogLevel))
	if err != nil {
		logLevel = logrus.ErrorLevel
	}

	log := logrus.New()
	log.SetLevel(logLevel)
	log.SetFormatter(&logrus.JSONFormatter{})

	elasticURL := viper.GetString(configElasticsearchURL)

	elasticClient, err := elastic.NewClient(elastic.SetURL(elasticURL), elastic.SetSniff(false))
	if err != nil {
		log.Error(err)
		return log
	}

	logIndex := strings.ToLower(viper.GetString(configLogIndex))
	hostName := viper.GetString(configHostName)
	elasticsearchHook, err := elogrus.NewElasticHookWithFunc(elasticClient, hostName, logLevel, func() string {
		year, month, day := time.Now().Date()
		return fmt.Sprintf("%s-%04d.%02d.%02d", logIndex, year, month, day)
	})
	if err != nil {
		log.Error(err)
		return log
	}

	// Add elasticsearch log hook.
	log.Hooks.Add(elasticsearchHook)
	return log
}

// WithElasticsearchServerLogger sets up a `grpc.ServerOption` to intercept streams with
// `*logrus.Entry` of the logger, created with `NewLogger`, and the options given to it.
// Returns the `grpc.ServerOption` which will be used in `grpc.NewServer`
// to log all incoming stream calls.
func WithElasticsearchServerLogger(
	logrusEntry *logrus.Entry,
	serverPayloadLoggingDecider grpc_logging.ServerPayloadLoggingDecider,
	opts ...grpc_logrus.Option,
) []grpc.ServerOption {
	// Make sure that log statements internal to gRPC library are logged using the logrus Logger as well.
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)

	// Server stream interceptor set up for logging incoming initial requests,
	// and outgoing responses. It automatically adds the "trace.id" from
	// the stream's context, and logs payloads of streams.
	// Make sure we put the `grpc_ctxtags` context before everything else.
	grpcStreamLoggingInterceptor := grpc_middleware.WithStreamServerChain(
		// Log incoming initial requests.
		grpc_ctxtags.StreamServerInterceptor(
			grpc_ctxtags.WithFieldExtractorForInitialReq(RequestExtractor(logrusEntry)),
		),
		// Add the "trace.id" from the stream's context.
		func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			// Add logrusEntry to the context.
			logCtx := ctxlogrus.ToContext(stream.Context(), logrusEntry)

			// Extract the "trace.id" from the stream's context
			traceIDFields := logrus.Fields{
				"trace.id": ExtractTraceParent(stream.Context()),
			}

			// Overwrite the logrus entry to always log the "trace.id" field.
			*logrusEntry = *logrusEntry.WithFields(traceIDFields)

			// Add the "trace.id" field to logrusEntry.
			ctxlogrus.AddFields(logCtx, traceIDFields)

			return grpc_logrus.StreamServerInterceptor(ctxlogrus.Extract(logCtx), opts...)(srv, stream, info, handler)
		},
		// Log payload of stream requests.
		grpc_logrus.PayloadStreamServerInterceptor(logrusEntry, serverPayloadLoggingDecider),
	)

	// Server unary interceptor set up for logging incoming requests,
	// and outgoing responses. It automatically adds the "trace.id" from
	// the request's context, and logs payloads of request.
	// Make sure we put the `grpc_ctxtags` context before everything else.
	grpcUnaryLoggingInterceptor := grpc_middleware.WithUnaryServerChain(
		// Log incoming initial requests.
		grpc_ctxtags.UnaryServerInterceptor(
			grpc_ctxtags.WithFieldExtractorForInitialReq(grpc_ctxtags.CodeGenRequestFieldExtractor),
		),
		// Add the "trace.id" from the stream's context.
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			// Add logrusEntry to the context.
			logCtx := ctxlogrus.ToContext(ctx, logrusEntry)

			// Extract the "trace.id" from the stream's context.
			traceIDFields := logrus.Fields{
				"trace.id": ExtractTraceParent(ctx),
			}

			// Overwrite the logrus entry to always log the "trace.id" field.
			*logrusEntry = *logrusEntry.WithFields(traceIDFields)

			// Add the "trace.id" field to logrusEntry.
			ctxlogrus.AddFields(logCtx, traceIDFields)

			return grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...)(ctx, req, info, handler)
		},
		// Log payload of unrary requests.
		grpc_logrus.PayloadUnaryServerInterceptor(logrusEntry, serverPayloadLoggingDecider),
	)

	return []grpc.ServerOption{grpcUnaryLoggingInterceptor, grpcStreamLoggingInterceptor}
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

// DefaultServerPayloadLoggingDecider logs every payload.
func DefaultServerPayloadLoggingDecider(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
	return true
}

// IgnoreMethodServerPayloadLoggingDecider ignores logging the payload of method that is equal to fullIgnoredMethodName.
func IgnoreMethodServerPayloadLoggingDecider(fullIgnoredMethodName string) grpc_logging.ServerPayloadLoggingDecider {
	return func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
		return fullMethodName != fullIgnoredMethodName
	}
}

// RequestExtractor extracts the request and logs it as json under the key "grpc.request.content".
func RequestExtractor(entry *logrus.Entry) grpc_ctxtags.RequestFieldExtractorFunc {
	return func(fullMethod string, pbMsg interface{}) map[string]interface{} {
		if p, ok := pbMsg.(proto.Message); ok {
			entry.WithField("grpc.request.content", JSONPbMarshaller{p}).Info("server request payload logged as grpc.request.content field")
		}

		return nil
	}
}
