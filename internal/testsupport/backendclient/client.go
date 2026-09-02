// Package backendclient constructs identity-bound HTTP backend clients around
// legacy-shaped fixture servers. Production packages must never import it.
package backendclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const fixtureBoundarySecret = "fixture-boundary-secret-at-least-32-bytes" // gitleaks:allow -- synthetic test-support key

type resolver struct {
	backendName string
	identity    backendidentity.ID
}

func (r resolver) ExpectedBackendStorageIdentity(name string) (backendidentity.ID, bool) {
	return r.identity, name == r.backendName
}

// New wraps cfg.BaseURL with a compatibility proxy and constructs the client
// through the same identity-bound constructor used by providerd. The proxy
// translates the upgraded identity path and response header at the fixture
// boundary; the client itself never receives unbound mutation authority.
func New(
	cfg backend.HTTPClientConfig,
	identity backendidentity.ID,
) (*backend.HTTPClient, func(), error) {
	if !identity.Valid() {
		return nil, nil, fmt.Errorf("fixture backend storage identity: %w", backendidentity.ErrInvalidID)
	}
	target, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse fixture backend URL %q: %w", cfg.BaseURL, err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, nil, fmt.Errorf("fixture backend URL %q must be an absolute origin", cfg.BaseURL)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSClientConfig != nil {
		transport.TLSClientConfig = cfg.TLSClientConfig.Clone()
	}
	boundPrefix := backendidentity.BoundPathPrefix + identity.String()
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Director: func(request *http.Request) {
			request.URL.Scheme = target.Scheme
			request.URL.Host = target.Host
			request.Host = target.Host
			request.URL.Path = strings.TrimPrefix(request.URL.Path, boundPrefix)
			request.URL.RawPath = ""
			query := request.URL.Query()
			query.Del(backendidentity.QueryParameter)
			request.URL.RawQuery = query.Encode()

			if cfg.Secret == "" {
				// The production client must always carry a strong signing
				// capability. Legacy-shaped fixtures that deliberately omit HMAC
				// should not receive the compatibility proxy's boundary signature.
				request.Header.Del(hmacauth.SignatureHeader)
				return
			}
			var body []byte
			if request.Body != nil {
				var readErr error
				body, readErr = io.ReadAll(request.Body)
				if readErr != nil {
					request.Body = http.NoBody
					request.Header.Del(hmacauth.SignatureHeader)
					return
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
			}
			request.Header.Set(
				hmacauth.SignatureHeader,
				hmacauth.SignRequest(cfg.Secret, request, body),
			)
		},
		ModifyResponse: func(response *http.Response) error {
			response.Header.Set(backendidentity.ResponseHeader, identity.String())
			return nil
		},
	}
	server := httptest.NewServer(proxy)

	clientConfig := cfg
	clientConfig.BaseURL = server.URL
	clientConfig.TLSClientConfig = nil
	if clientConfig.Secret == "" {
		clientConfig.Secret = fixtureBoundarySecret
	}
	client, err := backend.NewIdentityBoundHTTPClient(clientConfig, resolver{
		backendName: cfg.Name,
		identity:    identity,
	})
	if err != nil {
		server.Close()
		transport.CloseIdleConnections()
		return nil, nil, err
	}
	cleanup := func() {
		server.Close()
		transport.CloseIdleConnections()
	}
	return client, cleanup, nil
}
