package api

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: api's production files must not carry code whose
// only caller is a test (ENG-354).

import (
	"errors"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/hmacauth"
)

// subscriberCount reports how many active subscribers a lease UUID has.
// Tests poll it to observe subscribe/unsubscribe bookkeeping that the
// production API surfaces only indirectly.
func (b *EventBroker) subscriberCount(leaseUUID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients[leaseUUID])
}

// NewCallbackAuthenticatorWithMaxAge builds a CallbackAuthenticator with
// a caller-supplied replay window so replay-protection tests can drive a
// short or an out-of-range max age. Production always uses
// NewCallbackAuthenticator, which pins DefaultCallbackMaxAge.
func NewCallbackAuthenticatorWithMaxAge(secret string, maxAge time.Duration) (*CallbackAuthenticator, error) {
	if err := validateCallbackSecret(secret); err != nil {
		return nil, err
	}
	if maxAge <= 0 {
		return nil, errors.New("callback max age must be positive")
	}
	if maxAge > MaxCallbackMaxAge {
		return nil, fmt.Errorf("callback max age %v exceeds maximum allowed %v", maxAge, MaxCallbackMaxAge)
	}
	return &CallbackAuthenticator{
		secret:  secret,
		maxAge:  maxAge,
		nowFunc: time.Now,
	}, nil
}

// ComputeSignatureWithTime is ComputeSignature with an explicit
// timestamp, so signature and replay-window tests can produce a
// deterministic (and deliberately stale or future-dated) signature.
func (a *CallbackAuthenticator) ComputeSignatureWithTime(method, uri string, payload []byte, t time.Time) string {
	return hmacauth.SignWithTime(a.secret, method, uri, payload, t)
}
