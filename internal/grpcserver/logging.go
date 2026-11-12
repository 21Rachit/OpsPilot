package grpcserver

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// LoggingInterceptor logs one line per gRPC call, like the HTTP logging middleware.
// An interceptor is gRPC's version of middleware: it wraps every call to a handler.
func LoggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req) // run the real method, e.g. CreateIncident
	code := status.Code(err)       // codes.OK when err is nil

	level := slog.LevelInfo
	switch code {
	case codes.OK:
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable:
		level = slog.LevelError // our fault
	default:
		level = slog.LevelWarn // the caller's fault, e.g. NotFound or InvalidArgument
	}
	slog.Log(ctx, level, "grpc request",
		"method", info.FullMethod, // e.g. /opspilot.incident.v1.IncidentService/CreateIncident
		"user", callerUser(ctx),
		"code", code.String(),
		"duration", time.Since(start),
	)
	return resp, err
}

// callerUser returns the user the gateway sent in the call's metadata, or "-" if there is
// none (for example when you call the server directly with grpcurl).
func callerUser(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "-"
	}
	if v := md.Get("x-user"); len(v) > 0 {
		return v[0]
	}
	return "-"
}
