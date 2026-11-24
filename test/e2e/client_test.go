//go:build integration

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/hmacauth"
)

// TestRequestSignatureGoldenVector pins the wire format of X-Fred-Signature.
// The client signs with production code, so a change to that code would move
// both ends together and every other test would still pass. The expected
// value was computed outside Go, as HMAC-SHA256 over
// "<unix time>\n<method>\n<request URI>\n<hex sha256 of the body>".
func TestRequestSignatureGoldenVector(t *testing.T) {
	const (
		secret = "e2e-golden-secret-0123456789abcdef0123456789"
		uri    = "/_fred/storage/7b0e4c1e-3f5a-4d2b-9c6e-8a1f2b3c4d5e/provision" +
			"?backend_storage_id=7b0e4c1e-3f5a-4d2b-9c6e-8a1f2b3c4d5e"
		body = `{"lease_uuid":"0d3f6a52-91c4-4b7e-8f2a-5c6d7e8f9a0b"}`
		want = "t=1700000000,sha256=13e4ed1fcd58d02ee1391eb50a14db7838659f9e2dcf771c36d42aff850297d4"
	)
	got := hmacauth.SignWithTime(secret, "POST", uri, []byte(body), time.Unix(1700000000, 0))
	require.Equal(t, want, got)
}
