package k3s

import (
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
	b.operationIntents = b.callbackStore
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	admission, err := b.callbackStore.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Backend:              b.cfg.Name,
		BackendStorageID:     b.storageIdentity,
		Tenant:               "tenant-a",
		ProviderUUID:         testK3sProviderUUID,
		Items:                items,
		ResourceProfiles:     testK3sResourceProfiles(t, b, items),
		Manifest:             []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)

	var callbackRequests atomic.Int32
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: b.callbackStore,
		HTTPClient: &http.Client{Transport: k3sReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			callbackRequests.Add(1)
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		})},
		Secret:          testCallbackSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
		StorageIdentity: b.storageIdentity,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})

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

	_, err = b.operationIntents.ResolveOperationIntent(
		admission.Claim, backend.CallbackStatusFailed, "late terminal failure",
	)
	require.Error(t, err)
	assert.EqualError(t, err, latched.Error())
	b.callbackSender.SendOperationCallback(
		leaseUUID, callbackURL, b.cfg.Name, backend.CallbackStatusFailed, "late terminal failure",
	)
	assert.Zero(t, callbackRequests.Load())

	require.NoError(t, b.callbackStore.Close())
	restartGate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	reopened, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	intents, err := reopened.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, admission.Claim.OperationID(), intents[0].OperationID())
	pending, err := reopened.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}
