package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type completionPendingStorageVerifier struct {
	storage backendidentity.VerifiedStorage
	gate    *backendidentity.StorageAuthorityGate
}

func (v completionPendingStorageVerifier) StorageIdentity() backendidentity.ID { return v.storage.ID() }
func (v completionPendingStorageVerifier) StorageAuthorityGate() *backendidentity.StorageAuthorityGate {
	return v.gate
}
func (v completionPendingStorageVerifier) Verify(context.Context) error { return v.gate.Error() }

// This fixture obtains the diagnostic through real journal admission and
// publication; the HTTP test cannot construct the private error itself.
func journalCompletionPendingError(t *testing.T) error {
	t.Helper()
	dir := t.TempDir()
	pair, err := backendidentity.BindMarkerPair(filepath.Join(dir, "storage.json"), filepath.Join(dir, "anchor.json"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	callbackPath, releasePath := filepath.Join(dir, "callbacks.db"), filepath.Join(dir, "releases.db")
	callbacksBound, err := shared.BindAuthoritativeStorePath(callbackPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, callbacksBound.Close()) }()
	releasesBound, err := shared.BindAuthoritativeStorePath(releasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, releasesBound.Close()) }()
	storage, err := pair.InitializeWithStores("docker-a", "test-substrate", backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(callbacksBound, storage, profile); err != nil {
				return err
			}
			return shared.PrepareBoundReleaseStoreStorage(releasesBound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(callbacksBound, storage); err != nil {
				return err
			}
			return shared.CheckBoundReleaseStoreStorage(releasesBound, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			if err := shared.VerifyBoundCallbackStoreStorage(callbacksBound, storage); err != nil {
				return err
			}
			return shared.VerifyBoundReleaseStoreStorage(releasesBound, storage)
		},
	})
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(err error) { t.Errorf("storage authority failed: %v", err) })
	require.NoError(t, err)
	callbacks, err := shared.OpenIdentityBoundCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath}, storage, gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	releases, err := shared.OpenIdentityBoundReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath}, storage, gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	operations, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	attestor, err := shared.NewCallbackStorageAttestor(callbacks, completionPendingStorageVerifier{storage: storage, gate: gate}, t.Context())
	require.NoError(t, err)
	publisher, err := shared.NewCallbackPublisher(shared.CallbackPublisherConfig{
		OperationSettlement: operations, MaintenanceSettlement: maintenance,
		StorageAttestor: attestor, Logger: slog.Default(),
	})
	require.NoError(t, err)
	callback := "http://localhost/callbacks/provision?operation_id=" + uuid.NewString()
	lifecycle, err := backend.ResolveLifecycleCallbackURL(callback, "")
	require.NoError(t, err)
	candidate, err := operations.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: handlerTestLeaseUUID,
		CallbackURL: callback, LifecycleCallbackURL: lifecycle,
		Tenant: "tenant-a", ProviderUUID: "22222222-2222-4222-8222-222222222222",
		Items:            []backend.LeaseItem{{SKU: "small", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		Manifest:         []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)
	admission, err := operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	release, err := operations.PrepareOperationRelease(claim)
	require.NoError(t, err)
	refusal, err := operations.RefuseOperationExecution(release)
	require.NoError(t, err)
	uncommitted, err := operations.CommitOperationFailure(refusal)
	require.NoError(t, err)
	require.NoError(t, publisher.PublishOperationFailureContext(t.Context(), uncommitted, "capacity"))
	probe, err := operations.NewOperationIntentProbe(handlerTestLeaseUUID, "http://localhost/callbacks/provision?operation_id="+uuid.NewString())
	require.NoError(t, err)
	_, err = operations.ProbeOperationIntent(probe)
	require.True(t, shared.IsOperationCompletionPending(err))
	return fmt.Errorf("probe exact operation redelivery: %w", err)
}

func TestOperationCompletionPendingHTTPContract(t *testing.T) {
	pending := journalCompletionPendingError(t)
	for _, operation := range []string{"provision", "restore"} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
			code   string
		}{
			{"journal FIFO", pending, http.StatusConflict, backend.CodeOperationCompletionPending},
			{"generic conflict", fmt.Errorf("%w: an earlier operation completion is pending", shared.ErrOperationIntentConflict), http.StatusInternalServerError, ""},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				mock := &mockBackend{
					ProvisionFunc: func(context.Context, backend.ProvisionRequest) error { return tc.err },
					RestoreFunc:   func(context.Context, backend.RestoreRequest) error { return tc.err },
				}
				body := fmt.Sprintf(`{"lease_uuid":%q,"from_lease_uuid":"6ba7b811-9dad-41d1-80b4-00c04fd430c8","callback_url":"http://localhost/callbacks/provision","items":[{"sku":"small","quantity":1}]}`, handlerTestLeaseUUID)
				response := httptest.NewRecorder()
				newMockHandler(mock).ServeHTTP(response, signedPostRequest("/"+operation, body))
				require.Equal(t, tc.status, response.Code)
				var envelope ErrorResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
				require.Equal(t, tc.code, envelope.Code)
			})
		}
	}
}
