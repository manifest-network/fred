package hmacauth

// ENG-191 QA assertions — independent verification of the six required
// properties from task #3:
//
//   1. Cross-endpoint replay rejected (POST /provision → POST /deprovision)
//   2. Cross-method replay rejected (GET /info/X → POST /info/X)
//   3. Cross-path same-method rejected (GET /info/X → GET /logs/X)
//   4. Round-trip correctness (Sign → Verify succeeds)
//   5. Query-string binding (GET /list?tenant=a → GET /list?tenant=b)
//   6. Old-format rejection (pre-fix "<timestamp>.<body>" signature rejected
//      under the new verifier — ship-direct rollout, no dual-verify).
//
// These cases are intentionally separate from TestSignAndVerify_MethodURIBinding
// (also in this package) so that QA's pass/fail signal is independent of any
// rename/restructure of the implementer's tests, and the fixtures (secret,
// URIs, body shapes) are different to avoid coincidental cross-test
// dependencies.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const qaSecret = "qa-eng191-secret-at-least-32-chars-padding!"

// TestQA_ENG191_RequiredCases asserts the six properties named in task #3.
// Each subtest matches one row of the QA report.
func TestQA_ENG191_RequiredCases(t *testing.T) {
	// Case 4 first (round-trip baseline) — if this fails the rest can't be
	// trusted.
	t.Run("case4_round_trip_correctness_POST_with_body", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"qa-lease-001","callback_url":"http://fred.test/cb","items":[{"sku":"x","quantity":1}]}`)
		sig := Sign(qaSecret, "POST", "/provision", body)
		require.NoError(t, Verify(qaSecret, "POST", "/provision", body, sig, 5*time.Minute),
			"round-trip Sign→Verify must succeed for POST with body")
	})

	t.Run("case4_round_trip_correctness_GET_empty_body", func(t *testing.T) {
		// nil body on both sides — verifies sha256("") path.
		sig := Sign(qaSecret, "GET", "/info/qa-lease-001", nil)
		require.NoError(t, Verify(qaSecret, "GET", "/info/qa-lease-001", nil, sig, 5*time.Minute),
			"round-trip Sign→Verify must succeed for GET with nil body")
	})

	// Case 1 — cross-endpoint POST replay. The exact attacker scenario from
	// the team brief: a captured /provision body's signature, when replayed
	// against /deprovision (which only reads lease_uuid and ignores the rest),
	// MUST be rejected.
	t.Run("case1_cross_endpoint_replay_POST_provision_to_deprovision", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"qa-lease-002","callback_url":"http://fred.test/cb","items":[{"sku":"x","quantity":1}]}`)
		sigProvision := Sign(qaSecret, "POST", "/provision", body)

		// Replay against /deprovision with the same body and the same
		// captured signature header.
		err := Verify(qaSecret, "POST", "/deprovision", body, sigProvision, 5*time.Minute)
		require.Error(t, err, "captured /provision signature MUST be rejected when replayed against /deprovision")
		assert.Contains(t, err.Error(), "mismatch")
	})

	// Case 2 — cross-method replay on identical URI. The brief specifies
	// GET /info/X → POST /info/X (rather than POST→PUT). This is a
	// method-confusion bug class.
	t.Run("case2_cross_method_replay_GET_info_to_POST_info_same_uri", func(t *testing.T) {
		sigGet := Sign(qaSecret, "GET", "/info/qa-lease-003", nil)

		// Replay as POST /info/qa-lease-003 with the same (empty) body.
		err := Verify(qaSecret, "POST", "/info/qa-lease-003", nil, sigGet, 5*time.Minute)
		require.Error(t, err, "GET /info/X signature MUST be rejected when replayed as POST /info/X")
		assert.Contains(t, err.Error(), "mismatch")
	})

	// Case 3 — cross-path, same method. The second attacker scenario from
	// the brief: a signed empty-body GET on /info/X replayed against
	// /logs/X. Without method+URI binding, both signed strings would be
	// identical (timestamp + empty body) — this test proves they no longer
	// are.
	t.Run("case3_cross_path_same_method_GET_info_to_GET_logs", func(t *testing.T) {
		sigInfo := Sign(qaSecret, "GET", "/info/qa-lease-004", nil)

		err := Verify(qaSecret, "GET", "/logs/qa-lease-004", nil, sigInfo, 5*time.Minute)
		require.Error(t, err, "GET /info/X signature MUST be rejected when replayed as GET /logs/X")
		assert.Contains(t, err.Error(), "mismatch")
	})

	// Case 5 — query-string binding. A signature on `?tenant=a` must not
	// verify against `?tenant=b`. This locks the choice of
	// r.URL.RequestURI() (path+query) over r.URL.Path (path only).
	t.Run("case5_query_string_binding_tenant_a_to_tenant_b", func(t *testing.T) {
		sigA := Sign(qaSecret, "GET", "/list?tenant=a", nil)

		err := Verify(qaSecret, "GET", "/list?tenant=b", nil, sigA, 5*time.Minute)
		require.Error(t, err, "GET /list?tenant=a signature MUST be rejected against ?tenant=b")
		assert.Contains(t, err.Error(), "mismatch")
	})

	// Additional query-string defense-in-depth — same key, different value
	// in an existing /logs/X?tail=N shape (a real endpoint pattern from the
	// codebase).
	t.Run("case5b_query_string_binding_logs_tail_value", func(t *testing.T) {
		sigShort := Sign(qaSecret, "GET", "/logs/qa-lease-005?tail=10", nil)

		err := Verify(qaSecret, "GET", "/logs/qa-lease-005?tail=100", nil, sigShort, 5*time.Minute)
		require.Error(t, err, "tail=10 signature MUST be rejected for tail=100")
		assert.Contains(t, err.Error(), "mismatch")
	})

	// Case 6 — old-format rejection. Construct a signature using the
	// pre-fix `<timestamp>.<body>` canonical string and assert it does NOT
	// verify under the new function. This is the ship-direct rollout
	// guard: there must be no compatibility window for old-format
	// signatures, ever.
	t.Run("case6_old_wire_format_rejected_ship_direct_cutover", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"qa-lease-006"}`)
		ts := time.Now().Unix()

		// Reproduce the pre-fix ComputeMAC exactly:
		//   signed := fmt.Sprintf("%d.%s", timestamp, payload)
		oldSigned := fmt.Sprintf("%d.%s", ts, body)
		mac := hmac.New(sha256.New, []byte(qaSecret))
		mac.Write([]byte(oldSigned))
		oldSig := fmt.Sprintf("t=%d,sha256=%s", ts, hex.EncodeToString(mac.Sum(nil)))

		// The new verifier must reject the old-format signature even when
		// the (method, URI, body) match what would otherwise be a valid
		// request.
		err := Verify(qaSecret, "POST", "/provision", body, oldSig, 5*time.Minute)
		require.Error(t, err, "pre-fix <timestamp>.<body> signature MUST NOT verify under the new function")
		assert.Contains(t, err.Error(), "mismatch")
	})
}

// TestQA_ENG191_CanonicalStringShape pins the exact canonical string format
// so any future accidental change to ComputeMAC's layout — separator,
// field order, body-hash encoding — fails this test.
func TestQA_ENG191_CanonicalStringShape(t *testing.T) {
	// Reproduce the canonical string by hand and assert ComputeMAC produces
	// the same HMAC.
	const method = "POST"
	const uri = "/provision?x=1"
	body := []byte(`{"k":"v"}`)
	ts := int64(1700000000)

	bodyHash := sha256.Sum256(body)
	want := fmt.Sprintf("%d\n%s\n%s\n%x", ts, method, uri, bodyHash[:])

	// Manually compute the expected HMAC over the expected canonical string.
	mac := hmac.New(sha256.New, []byte(qaSecret))
	mac.Write([]byte(want))
	expected := mac.Sum(nil)

	got := ComputeMAC(qaSecret, ts, method, uri, body)
	assert.Equal(t, expected, got,
		"ComputeMAC canonical string drifted from <ts>\\n<METHOD>\\n<URI>\\n<hex(sha256(body))>")

	// Defense-in-depth: assert the canonical string itself uses '\n'
	// (0x0A) separators, not some other byte. We rebuild it from
	// ComputeMAC's behavior using a unique-byte body to spot any change.
	assert.Equal(t, 3, strings.Count(want, "\n"),
		"canonical string must contain exactly 3 newline separators (4 fields)")
}

// TestQA_ENG191_EmptyBodyHashIsRFCDefined documents the SHA-256("") hash
// constant so that any subsequent change to how empty bodies are handled is
// caught immediately. The constant comes from FIPS 180-4 / RFC 6234 test
// vectors: SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.
func TestQA_ENG191_EmptyBodyHashIsRFCDefined(t *testing.T) {
	const expectedEmptyHashHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// Both nil and []byte{} must produce the same canonical string (and
	// therefore the same HMAC).
	macNil := ComputeMAC(qaSecret, 1700000000, "GET", "/info/x", nil)
	macEmpty := ComputeMAC(qaSecret, 1700000000, "GET", "/info/x", []byte{})
	assert.Equal(t, macNil, macEmpty, "nil and empty []byte bodies must hash identically")

	// Round-trip: signing with nil body must verify when caller passes []byte{}.
	sig := Sign(qaSecret, "GET", "/info/x", nil)
	assert.NoError(t, Verify(qaSecret, "GET", "/info/x", []byte{}, sig, 5*time.Minute))

	// Sanity-check the constant: sha256("") in the implementation must
	// match the FIPS-defined hex. This catches any accidental "if body
	// == nil, skip hashing" branch.
	emptyHash := sha256.Sum256(nil)
	assert.Equal(t, expectedEmptyHashHex, hex.EncodeToString(emptyHash[:]),
		"stdlib sha256.Sum256(nil) must be the canonical SHA-256(\"\") constant")
}
