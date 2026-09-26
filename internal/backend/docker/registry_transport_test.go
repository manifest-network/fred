package docker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nativeRegistryTransport carries scripted registry fixtures over real HTTP/1
// and TLS. Pipes keep every network wait inside a synctest bubble; production
// still owns TLS negotiation, cancellation, and each individual HTTP exchange.
func nativeRegistryTransport(t *testing.T, fixture http.RoundTripper) *http.Transport {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"registry.example"},
		NotBefore:   time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:    time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	listener := &registryPipeListener{connections: make(chan net.Conn), done: make(chan struct{})}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		response, err := fixture.RoundTrip(request)
		if err != nil {
			panic(http.ErrAbortHandler)
		}
		defer response.Body.Close()
		for name, values := range response.Header {
			w.Header()[name] = append([]string(nil), values...)
		}
		w.WriteHeader(response.StatusCode)
		// Flush both headers and every body write: a progressing layer must
		// remain observable to the client's no-progress deadline.
		writer := registryFlushWriter{ResponseWriter: w}
		if err := http.NewResponseController(w).Flush(); err != nil {
			panic(http.ErrAbortHandler)
		}
		if _, err := io.Copy(writer, response.Body); err != nil {
			panic(http.ErrAbortHandler)
		}
	})}
	finished := make(chan error, 1)
	go func() {
		finished <- server.Serve(tls.NewListener(listener, &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		}))
	}()
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			client, server := net.Pipe()
			listener.mu.Lock()
			listener.pipes = append(listener.pipes, server)
			listener.mu.Unlock()
			select {
			case listener.connections <- server:
				return client, nil
			case <-listener.done:
				_ = client.Close()
				_ = server.Close()
				return nil, net.ErrClosed
			case <-ctx.Done():
				_ = client.Close()
				_ = server.Close()
				return nil, ctx.Err()
			}
		},
	}
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		require.NoError(t, server.Close())
		require.ErrorIs(t, <-finished, http.ErrServerClosed)
	})
	return transport
}

type registryFlushWriter struct{ http.ResponseWriter }

func (w registryFlushWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if err == nil {
		err = http.NewResponseController(w.ResponseWriter).Flush()
	}
	return n, err
}

type registryPipeListener struct {
	connections chan net.Conn
	done        chan struct{}
	close       sync.Once
	mu          sync.Mutex
	pipes       []net.Conn
}

func (l *registryPipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *registryPipeListener) Close() error {
	l.close.Do(func() {
		close(l.done)
		// TLS peers can both be writing close_notify into an unbuffered
		// pipe. Close the raw endpoints so cleanup joins those writers
		// before the virtual-time bubble exits.
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, pipe := range l.pipes {
			_ = pipe.Close()
		}
	})
	return nil
}

func (*registryPipeListener) Addr() net.Addr { return &net.TCPAddr{} }
