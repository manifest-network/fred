package k3s

import (
	"context"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestReleaseStoreFailureBlocksK3sCallbackSettlement(t *testing.T) {
	b := newBackendForTest(t, "")
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         testK3sProviderUUID,
		Items:                items,
		ResourceProfiles:     testK3sResourceProfiles(t, b, items),
		Manifest:             []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)

	var callbackRequests atomic.Int32
	attestor := shared.MustNewCallbackStorageAttestor(
		b.callbackStore,
		k3sCallbackStorageVerifier{verifier: b.storageVerifier, gate: b.storeAuthorityGate},
		b.stopCtx,
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           b.callbackStore,
		StorageAttestor: attestor,
		HTTPClient: &http.Client{Transport: k3sReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			callbackRequests.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		})},
		Secret: testCallbackSecret,
		Logger: b.logger,

		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})
	maintenanceSettlement, err := shared.NewMaintenanceSettlement(b.callbackStore, b.releaseStore)
	require.NoError(t, err)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   concreteK3sOperationSettlement(b),
		MaintenanceSettlement: maintenanceSettlement,
		StorageAttestor:       attestor,
		Logger:                b.logger,
	})

	uncommitted := commitK3sOperationRefusal(t, b, claim)
	require.NoError(t, os.Rename(b.cfg.ReleasesDBPath, b.cfg.ReleasesDBPath+".withdrawn"))
	_, triggerErr := b.releaseStore.LatestActive(leaseUUID)
	require.Error(t, triggerErr)
	assert.ErrorIs(t, triggerErr, backendidentity.ErrIdentityDrift)
	latched := b.terminalStorageAuthorityError()
	require.Error(t, latched)
	assert.EqualError(t, latched, triggerErr.Error())
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("release store failure did not cancel the K3s backend lifetime")
	}

	err = b.resolvePreEffectOperationRefusal(claim, "late terminal failure")
	require.Error(t, err)
	assert.EqualError(t, err, latched.Error())
	require.Error(t, b.callbackPublisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "late terminal failure",
	))
	assert.Zero(t, callbackRequests.Load())

	require.NoError(t, b.callbackStore.Close())
	require.NoError(t, b.releaseStore.Close())
	require.NoError(t, os.Rename(b.cfg.ReleasesDBPath+".withdrawn", b.cfg.ReleasesDBPath))
	restartGate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	reopened, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedReleases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedReleases.Close() })
	reopenedSettlement, err := shared.NewOperationSettlement(reopened, reopenedReleases)
	require.NoError(t, err)
	intents, err := reopenedSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, claim.OperationID(), intents[0].OperationID())
	pending, err := reopened.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}
