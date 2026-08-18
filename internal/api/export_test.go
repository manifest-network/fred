package api

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: api's production files must not carry code whose
// only caller is a test (ENG-354).

import (
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

// ComputeSignatureWithTime is ComputeSignature with an explicit
// timestamp, so signature and replay-window tests can produce a
// deterministic (and deliberately stale or future-dated) signature.
func (a *CallbackAuthenticator) ComputeSignatureWithTime(method, uri string, payload []byte, t time.Time) string {
	return hmacauth.SignWithTime(a.secret, method, uri, payload, t)
}
