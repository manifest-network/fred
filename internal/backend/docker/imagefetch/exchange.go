package imagefetch

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// singleRegistryExchange owns a transport with no autonomous HTTP replay.
// Only its constructor can establish this authority in production. Copies
// share the same configured transport; caller mutations cannot change its
// protocol or connection-reuse policy after construction.
type singleRegistryExchange struct{ wire http.RoundTripper }

func defaultRegistryExchange() singleRegistryExchange {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return ownRegistryExchange(transport)
}

func newSingleRegistryExchange(transport *http.Transport) (singleRegistryExchange, error) {
	if transport == nil {
		return singleRegistryExchange{}, errors.New("nil registry transport")
	}
	if transport.DialTLS != nil || transport.DialTLSContext != nil {
		return singleRegistryExchange{}, errors.New("registry routing must use DialContext so the exchange owner controls TLS negotiation")
	}
	return ownRegistryExchange(transport), nil
}

func ownRegistryExchange(transport *http.Transport) singleRegistryExchange {
	owned := transport.Clone()
	// Go retries idempotent requests on reused HTTP/1 connections, and HTTP/2
	// can replay refused streams even on a fresh connection. A fresh HTTP/1
	// exchange makes one descriptor claim authorize at most one wire request.
	owned.DisableKeepAlives = true
	owned.ForceAttemptHTTP2 = false
	owned.HTTP2 = nil
	owned.Protocols = &http.Protocols{}
	owned.Protocols.SetHTTP1(true)
	owned.TLSNextProto = nil
	if owned.TLSClientConfig == nil {
		owned.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	owned.TLSClientConfig.NextProtos = []string{"http/1.1"}
	return singleRegistryExchange{wire: owned}
}

// interruptedRegistryHeaders is issued only before a response exists. Body
// EOF has different meaning: it terminates content and must still satisfy the
// exact descriptor size and digest instead of becoming a retry classification.
type interruptedRegistryHeaders struct{ cause error }

func (e interruptedRegistryHeaders) Error() string {
	return "registry exchange ended before response headers: " + e.cause.Error()
}
func (e interruptedRegistryHeaders) Unwrap() error { return e.cause }

func (e singleRegistryExchange) RoundTrip(request *http.Request) (*http.Response, error) {
	if e.wire == nil {
		return nil, errors.New("registry exchange is unavailable")
	}
	response, err := e.wire.RoundTrip(request)
	if response == nil && errors.Is(err, io.EOF) {
		return nil, interruptedRegistryHeaders{cause: err}
	}
	return response, err
}
