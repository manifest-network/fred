package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestProvisionManifestAdmissionHTTPErrorContract(t *testing.T) {
	// Obtain the refusal from the real strict-or-historical constructor. A
	// synthetic ErrInvalidManifest would miss an untyped constructor regression.
	policyErr := journalPendingError(t, func(operations *shared.OperationSettlement, _ *shared.MaintenanceSettlement, _ shared.OperationReleaseCandidate) error {
		_, err := operations.AdmitProvisionManifest(t.Context(), handlerTestLeaseUUID, "tenant-a", "22222222-2222-4222-8222-222222222222",
			[]backend.LeaseItem{{SKU: "small", ServiceName: "app", Quantity: 1}}, []byte(`{"image":42}`))
		return err
	})
	require.ErrorIs(t, policyErr, backend.ErrInvalidManifest)

	// Actual malformed journal bytes produce a storage failure, independently
	// of the request payload. Such failures must retain the server-error class.
	path := filepath.Join(t.TempDir(), "corrupt-releases.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("releases"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte(handlerTestLeaseUUID), []byte(`{"broken"`))
	}))
	require.NoError(t, db.Close())
	_, corruptionErr := shared.InspectReleaseStoreReadOnly(path)
	require.Error(t, corruptionErr)
	require.NotErrorIs(t, corruptionErr, backend.ErrValidation)

	for _, tc := range []struct {
		name string
		err  error
		code int
		kind backend.ValidationCode
	}{
		{"new malformed payload", policyErr, http.StatusBadRequest, backend.ValidationCodeInvalidManifest},
		{"corrupt stored journal", corruptionErr, http.StatusInternalServerError, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockBackend{ProvisionFunc: func(context.Context, backend.ProvisionRequest) error { return tc.err }}
			body := fmt.Sprintf(`{"lease_uuid":%q,"callback_url":"http://localhost/callbacks/provision","items":[{"sku":"small","quantity":1}]}`, handlerTestLeaseUUID)
			response := httptest.NewRecorder()
			newMockHandler(mock).ServeHTTP(response, signedPostRequest("/provision", body))
			require.Equal(t, tc.code, response.Code)
			var envelope ErrorResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
			require.Equal(t, tc.kind, envelope.ValidationCode)
		})
	}
}
