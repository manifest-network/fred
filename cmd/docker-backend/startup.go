package main

import (
	"context"
	"errors"
	"net"
	"net/http"
)

type backendStartupOwner interface {
	Start(context.Context) error
}

// startingBackendHandler can observe readiness but cannot publish it. Only the
// successful Start below releases requests to the identity-bound backend.
type startingBackendHandler struct {
	ready <-chan struct{}
	next  http.Handler
}

func (h startingBackendHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case <-h.ready:
		h.next.ServeHTTP(w, r)
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "backend startup in progress", http.StatusServiceUnavailable)
	}
}

// serveStartingBackend consumes an already-bound listener before Start can
// persist optional image pins. Requests receive a prompt, identity-free 503
// until Start succeeds; failed startup closes the listener without publishing
// the runtime handler. The readiness channel never escapes its startup owner.
func serveStartingBackend(ctx context.Context, listener net.Listener, server *http.Server, next http.Handler, owner backendStartupOwner) (<-chan error, error) {
	ready := make(chan struct{})
	server.Handler = startingBackendHandler{ready: ready, next: next}
	serverErr := make(chan error, 1)
	go func() {
		var err error
		if server.TLSConfig != nil {
			// Certificates have already been loaded and validated, before Start.
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	if err := owner.Start(ctx); err != nil {
		return nil, errors.Join(err, server.Close())
	}
	close(ready)
	return serverErr, nil
}
