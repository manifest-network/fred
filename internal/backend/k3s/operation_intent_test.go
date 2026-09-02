package k3s

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRefuseProvisionIntent_StorageDriftPreservesIntent(t *testing.T) {
	b := newBackendForTest(t, "")
	bindK3sTestStorageIdentity(t, b)
	b.operationIntents = b.callbackStore
	callbackURL := "https://fred.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	admission, err := b.callbackStore.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
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

	driftErr := fmt.Errorf("%w: cluster lineage changed", backendidentity.ErrIdentityDrift)
	b.latchTerminalStorageAuthority(driftErr)
	cause := fmt.Errorf("%w: existing reservation", backend.ErrAlreadyProvisioned)
	err = b.refuseProvisionIntent(&admission.Claim, cause)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)

	// The backend-wide latch also makes the live journal unreadable. Reopen it
	// under the same verified storage capability to inspect the durable result
	// as the next process would.
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
	assert.Len(t, intents, 1)
	pending, err := reopened.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

type blockingK3sOperationIntentJournal struct {
	delegate operationIntentJournal
	began    chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (j *blockingK3sOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.delegate.ProbeOperationIntent(probe)
}

func (j *blockingK3sOperationIntentJournal) BeginOperationIntent(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	j.once.Do(func() { close(j.began) })
	<-j.release
	return j.delegate.BeginOperationIntent(spec)
}

func (j *blockingK3sOperationIntentJournal) ResolveOperationIntent(
	claim shared.OperationIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (shared.CallbackEntry, error) {
	return j.delegate.ResolveOperationIntent(claim, status, errMsg)
}

func TestRecoverOperationIntents_InterruptedStubProvisionBecomesExactFailure(t *testing.T) {
	b := newBackendForTest(t, "")
	bindK3sTestStorageIdentity(t, b)
	b.operationIntents = b.callbackStore
	const operationID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	_, err = b.callbackStore.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
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

	require.NoError(t, b.recoverOperationIntents())
	intents, err := b.callbackStore.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, callbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, stubProvisionerErrMsg, pending[0].Error)
}

func TestProvisionIntentToReservationWindowIsFencedAgainstDeprovision(t *testing.T) {
	b := newBackendForTest(t, "")
	bindK3sTestStorageIdentity(t, b)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: b.callbackStore,
		HTTPClient: &http.Client{Transport: k3sReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, assert.AnError
		})},
		Secret:          testCallbackSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
		StorageIdentity: b.storageIdentity,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
	})
	journal := &blockingK3sOperationIntentJournal{
		delegate: b.callbackStore,
		began:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	b.operationIntents = journal
	req := backend.ProvisionRequest{
		LeaseUUID:    "550e8400-e29b-41d4-a716-446655440000",
		Tenant:       "tenant-a",
		ProviderUUID: testK3sProviderUUID,
		CallbackURL:  "http://localhost/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		Items: []backend.LeaseItem{{
			SKU: "k3s-small", ServiceName: "app", Quantity: 1,
		}},
		Payload: []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	}

	provisionDone := make(chan error, 1)
	go func() { provisionDone <- b.Provision(context.Background(), req) }()
	select {
	case <-journal.began:
	case <-time.After(time.Second):
		t.Fatal("provision did not persist its intent")
	}

	deprovisionDone := make(chan error, 1)
	go func() { deprovisionDone <- b.Deprovision(context.Background(), req.LeaseUUID) }()
	select {
	case err := <-deprovisionDone:
		t.Fatalf("deprovision escaped through the intent-to-reservation window: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(journal.release)
	require.NoError(t, <-provisionDone)
	select {
	case err := <-deprovisionDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("deprovision did not complete after provision published its reservation")
	}
	b.provisionsMu.RLock()
	_, exists := b.provisions[req.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
	b.wg.Wait()
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "deprovision must not duplicate a worker's exact completion")
	assert.Equal(t, req.CallbackURL, pending[0].CallbackURL)
}
