package main

import (
	"context"
	"time"
)

const processShutdownTimeout = 75 * time.Second

type httpShutdownOwner interface {
	Shutdown(context.Context) error
}

type backendShutdownOwner interface {
	StopContext(context.Context) error
}

// drainHTTPAndBackend owns one deadline. Time spent finishing HTTP requests
// reduces the backend's remaining drain time instead of adding another budget
// beyond the deployed service manager's existing timeout.
func drainHTTPAndBackend(parent context.Context, server httpShutdownOwner, backend backendShutdownOwner) (error, error) {
	ctx, cancel := context.WithTimeout(parent, processShutdownTimeout)
	defer cancel()
	httpCtx, cancelHTTP := context.WithTimeout(ctx, 30*time.Second)
	httpErr := server.Shutdown(httpCtx)
	cancelHTTP()
	return httpErr, backend.StopContext(ctx)
}
