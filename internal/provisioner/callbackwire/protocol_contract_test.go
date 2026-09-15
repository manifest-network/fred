package callbackwire

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestVerifiedCallbackRejectsAmbiguousPayloadAndSelectors(t *testing.T) {
	const lease = "d144291f-a36f-47a4-8ccf-48afe590e29d"
	valid := `{"lease_uuid":"` + lease + `","status":"success","backend_storage_id":"` + testStorageID + `"}`
	for _, tt := range []struct {
		name, body, query string
		want              error
	}{
		{"empty body", "", "", ErrInvalidPayload},
		{"null body", "null", "", ErrInvalidPayload},
		{"array body", "[]", "", ErrInvalidPayload},
		{"truncated body", strings.TrimSuffix(valid, "}"), "", ErrInvalidPayload},
		{"trailing object", valid + "{}", "", ErrInvalidPayload},
		{"trailing garbage", valid + "!", "", ErrInvalidPayload},
		{"duplicate status", strings.TrimSuffix(valid, "}") + `,"status":"failed"}`, "", ErrInvalidPayload},
		{"aliased status", strings.Replace(valid, `"status"`, `"Status"`, 1), "", ErrInvalidPayload},
		{"wrong field type", strings.Replace(valid, `"success"`, "17", 1), "", ErrInvalidPayload},
		{"unknown status", strings.Replace(valid, "success", "ready", 1), "", ErrInvalidPayload},
		{"noncanonical lease", strings.Replace(valid, lease, strings.ToUpper(lease), 1), "", ErrInvalidPayload},
		{"invalid storage identity", strings.Replace(valid, testStorageID, "invalid", 1), "", ErrInvalidPayload},
		{"retained active status", strings.TrimSuffix(valid, "}") + `,"retained":true}`, "", ErrInvalidPayload},
		{"operation cannot close", strings.Replace(valid, "success", "deprovisioned", 1), "?operation_id=" + testOperation, ErrInvalidPayload},
		{"malformed query", valid, "?x=%zz", ErrInvalidRoute},
		{"empty operation", valid, "?operation_id=", ErrInvalidRoute},
		{"duplicate operation", valid, "?operation_id=" + testOperation + "&operation_id=" + testOperation, ErrInvalidRoute},
		{"invalid lifecycle", valid, "?lifecycle_id=invalid", ErrInvalidRoute},
		{"duplicate lifecycle", valid, "?lifecycle_id=" + testLifecycle + "&lifecycle_id=" + testLifecycle, ErrInvalidRoute},
		{"mixed selectors", valid, "?operation_id=" + testOperation + "&lifecycle_id=" + testLifecycle, ErrInvalidRoute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proof := verifiedCallback(t, "/callbacks/provision"+tt.query, []byte(tt.body), "")
			observation, err := DecodeVerified(proof)
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, Observation{}, observation, "invalid authenticated input must not expose a usable observation")
		})
	}
	_, err := DecodeVerified(hmacauth.VerifiedRequest{})
	require.ErrorIs(t, err, ErrInvalidProof)
}

func TestVerifiedCallbackProjectionPreservesSignedSelectorAndDetachedPayload(t *testing.T) {
	for _, tt := range []struct {
		name     string
		selector Selector
	}{{"legacy", SelectorLegacy}, {"operation", SelectorOperation}, {"lifecycle", SelectorLifecycle}} {
		t.Run(tt.name, func(t *testing.T) {
			selector := tt.selector
			payload := backend.CallbackPayload{
				LeaseUUID: "d144291f-a36f-47a4-8ccf-48afe590e29d", Status: backend.CallbackStatusFailed,
				Backend: "backend-a", Error: "container failed", BackendStorageID: testStorageID,
			}
			query := ""
			switch selector {
			case SelectorOperation:
				query = "?operation_id=" + testOperation
				payload.OperationID = testOperation
			case SelectorLifecycle:
				query = "?lifecycle_id=" + testLifecycle
				payload.LifecycleID = testLifecycle
				payload.Status = backend.CallbackStatusDeprovisioned
				payload.Retained = true
			}
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			// Unknown additive fields remain forward compatible, but cannot
			// alter the selector authenticated in the query.
			body = append(body[:len(body)-1], []byte(`,"extension":{"future":true}}`)...)
			observation, err := DecodeVerified(verifiedCallback(t, "/callbacks/provision"+query, body, testStorageID))
			require.NoError(t, err)
			require.Equal(t, selector, observation.Selector())
			require.Equal(t, payload, observation.Payload())
			require.Equal(t, payload.LeaseUUID, observation.LeaseUUID())
			require.Equal(t, payload.Status, observation.Status())
			require.Equal(t, payload.Error, observation.Failure())
			require.Equal(t, payload.Backend, observation.BackendName())
			require.Equal(t, payload.Retained, observation.Retained())
			require.Equal(t, testStorageID, observation.StorageID().String())
			detached := observation.Payload()
			detached.BackendStorageID = testLifecycle
			detached.OperationID = ""
			require.NotEqual(t, detached, observation.Payload())
			require.Equal(t, payload, observation.Payload())
		})
	}
	for _, body := range []string{`{}`, `{"backend_storage_id":"invalid"}`} {
		identity, err := SelectUntrustedStorageRoute([]byte(body))
		require.Error(t, err)
		require.False(t, identity.Valid())
	}
}
