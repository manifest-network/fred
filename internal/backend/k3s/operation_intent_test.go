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
	callbackURL := "https://fred.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
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

	driftErr := fmt.Errorf("%w: cluster lineage changed", backendidentity.ErrIdentityDrift)
	b.latchTerminalStorageAuthority(driftErr)
	cause := fmt.Errorf("%w: existing reservation", backend.ErrAlreadyProvisioned)
	err = b.refuseProvisionIntent(claim, cause)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)

	// The backend-wide latch also makes the live journal unreadable. Reopen it
	// under the same verified storage capability to inspect the durable result
	// as the next process would.
	require.NoError(t, b.callbackStore.Close())
	require.NoError(t, b.releaseStore.Close())
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
	assert.Len(t, intents, 1)
	pending, err := reopened.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

type blockingK3sOperationIntentJournal struct {
	operationSettlementService
	began   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (j *blockingK3sOperationIntentJournal) NewOperationIntentProbe(
	leaseUUID, callbackURL string,
) (shared.OperationIntentProbe, error) {
	return j.operationSettlementService.NewOperationIntentProbe(leaseUUID, callbackURL)
}

func (j *blockingK3sOperationIntentJournal) NewOperationIntentCandidate(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentCandidate, error) {
	return j.operationSettlementService.NewOperationIntentCandidate(spec)
}

func (j *blockingK3sOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.operationSettlementService.ProbeOperationIntent(probe)
}

func (j *blockingK3sOperationIntentJournal) BeginOperationIntent(
	candidate shared.OperationIntentCandidate,
) (shared.OperationIntentAdmission, error) {
	j.once.Do(func() { close(j.began) })
	<-j.release
	return j.operationSettlementService.BeginOperationIntent(candidate)
}

func TestRecoverOperationIntents_InterruptedStubProvisionBecomesExactFailure(t *testing.T) {
	b := newBackendForTest(t, "")
	bindK3sTestStorageIdentity(t, b)
	const operationID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         testK3sProviderUUID,
		Items:                items,
		ResourceProfiles:     testK3sResourceProfiles(t, b, items),
		Manifest:             []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)
	_, err = b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)

	require.NoError(t, b.recoverOperationIntents(t.Context()))
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, callbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, stubProvisionerErrMsg, pending[0].Error)
}

func TestRecoverOperationIntents_StartedStubProvisionUsesBoundAbsenceAttestor(t *testing.T) {
	b := newBackendForTest(t, "")
	const operationID = "7ba7b810-9dad-41d1-80b4-00c04fd430c8"
	const leaseUUID = "650e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
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
	releaseCandidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	_, err = concreteK3sOperationSettlement(b).StartOperationExecution(releaseCandidate)
	require.NoError(t, err)

	require.NoError(t, b.recoverOperationIntents(t.Context()))
	intents, err := b.operationSettlement.ListOperationIntents()
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
	attestor := shared.MustNewCallbackStorageAttestor(
		b.callbackStore,
		k3sCallbackStorageVerifier{verifier: b.storageVerifier, gate: b.storeAuthorityGate},
		b.stopCtx,
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           b.callbackStore,
		StorageAttestor: attestor,
		HTTPClient: &http.Client{Transport: k3sReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, assert.AnError
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
	journal := &blockingK3sOperationIntentJournal{
		operationSettlementService: b.operationSettlement,
		began:                      make(chan struct{}),
		release:                    make(chan struct{}),
	}
	b.operationSettlement = journal
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
