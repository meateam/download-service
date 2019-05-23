// Package logger is used to log all intercepted stream calls.
// The package exports the `NewLogger` function which sets up a logger with
// the elogrus hook and returns it.
//
// There's the `WithElasticsearchServerLogger` function which sets up a `grpc.ServerOption` to intercept streams with
// `*logrus.Entry` of the logger, created with `NewLogger`, and the options given to it.
// Returns the `grpc.ServerOption` which will be used in `grpc.NewServer`
// to log all incoming stream calls.
//
// The function `ExtractTraceParent` gets a `context.Context` which holds the "Elastic-Apm-Traceparent",
// which is the HTTP header for trace propagation, and returns the trace id.
package logger
