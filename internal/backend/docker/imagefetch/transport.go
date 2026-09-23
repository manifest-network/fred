package imagefetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const registryIdleTimeout = 30 * time.Second

// registryExchange owns one request's cancellation and no-progress timer. A
// single timer covers headers and body; reading never creates waiter goroutines.
type registryExchange struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelCauseFunc
	timer    *time.Timer
	deadline time.Time
	closed   bool
}

func newRegistryExchange(parent context.Context) *registryExchange {
	ctx, cancel := context.WithCancelCause(parent)
	exchange := &registryExchange{ctx: ctx, cancel: cancel, deadline: time.Now().Add(registryIdleTimeout)}
	exchange.mu.Lock()
	exchange.timer = time.AfterFunc(registryIdleTimeout, exchange.expire)
	exchange.mu.Unlock()
	return exchange
}

func (e *registryExchange) expire() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if remaining := time.Until(e.deadline); remaining > 0 {
		// A read can make progress while a previously queued callback waits for
		// this lock. That old callback cannot revoke the renewed idle allowance.
		e.timer.Reset(remaining)
		return
	}
	e.closed = true
	e.cancel(errors.New("registry exchange exceeded its no-progress timeout"))
}

func (e *registryExchange) progress() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.ctx.Err() != nil {
		return
	}
	e.deadline = time.Now().Add(registryIdleTimeout)
	e.timer.Reset(registryIdleTimeout)
}

func (e *registryExchange) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	e.timer.Stop()
	e.cancel(context.Canceled)
}

type boundedTransport struct {
	base  http.RoundTripper
	limit int64
}

func (t boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" {
		return nil, errors.New("image registries and redirects require HTTPS")
	}
	exchange := newRegistryExchange(req.Context())
	response, err := t.base.RoundTrip(req.WithContext(exchange.ctx))
	if err != nil {
		exchange.close()
		return nil, err
	}
	exchange.progress()
	limit := t.limit
	if response.StatusCode != http.StatusOK || strings.Contains(response.Header.Get("Content-Type"), "json") || strings.Contains(req.URL.Path, "/manifests/") {
		limit = min(limit, maxMetadataBytes)
	}
	response.Body = &boundedBody{body: response.Body, reader: budgetReader{reader: response.Body, remaining: limit}, exchange: exchange}
	return response, nil
}

type boundedBody struct {
	body     io.ReadCloser
	reader   budgetReader
	exchange *registryExchange
}

func (b *boundedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err != nil {
		b.exchange.close()
	} else if n > 0 {
		b.exchange.progress()
	}
	return n, err
}

func (b *boundedBody) Close() error { b.exchange.close(); return b.body.Close() }
