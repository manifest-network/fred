package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
)

type backendStartupOwner interface {
	backendShutdownOwner
	Start(context.Context) error
}

// startAndServeBackend checks for an existing listener before Start can publish
// optional image pins, then relinquishes the probe. The serving listener is
// created only after successful recovery, so an open port retains its readiness
// meaning for deployment tooling. A concurrent process can still win the final
// bind; that is a startup failure, never a partially available HTTP service.
func startAndServeBackend(ctx context.Context, server *http.Server, next http.Handler, owner backendStartupOwner) (<-chan error, error) {
	probe, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, fmt.Errorf("probe HTTP listener: %w", err)
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf("close HTTP listener probe: %w", err)
	}
	if err := owner.Start(ctx); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		_, stopErr := drainHTTPAndBackend(context.WithoutCancel(ctx), server, owner)
		return nil, errors.Join(fmt.Errorf("bind ready HTTP listener: %w", err), stopErr)
	}
	server.Handler = next
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
	return serverErr, nil
}
