package docker

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestOperationRecoveryContainsUnknownLeaseAndSettlesSibling(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	uncertain := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, uncertain, storageID)
	require.NoError(t, err)
	sibling := dockerOperationIntentSpec(t, storageID)
	sibling.LeaseUUID = "223e4567-e89b-42d3-a456-426614174000"
	sibling.CallbackURL = "https://fred.example/callbacks/provision?operation_id=223e4567-e89b-42d3-a456-426614174001"
	sibling.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(sibling.CallbackURL, "")
	require.NoError(t, err)
	_, err = beginDockerTestOperationIntent(t, store, sibling, storageID)
	require.NoError(t, err)
	bad := dockerIntentContainer(uncertain, "contradictory-container", uncertain.Items[0].SKU, 0)
	bad.Image = "example.invalid/unexpected:latest"
	good := dockerIntentContainer(sibling, "sibling-container", sibling.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{bad, good}, nil)
	forbidOperationRecoveryTeardown(t, b)
	b.cfg.ProvisionTimeout = time.Nanosecond
	var warnings bytes.Buffer
	b.logger = slog.New(slog.NewTextHandler(&warnings, nil))
	require.NoError(t, b.recoverState(t.Context()))
	before, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	var unresolved shared.OperationIntentClaim
	for _, claim := range before {
		if claim.LeaseUUID() == uncertain.LeaseUUID {
			unresolved = claim
		}
	}
	require.NotEmpty(t, unresolved.LeaseUUID())
	require.NoError(t, b.recoverOperationIntents(t.Context()))
	after, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	require.Equal(t, []shared.OperationIntentClaim{unresolved}, after, "only the exact unresolved row must remain, without expiry or rewrite")
	callbacks, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, callbacks, 1)
	require.Equal(t, sibling.LeaseUUID, callbacks[0].LeaseUUID)
	require.Equal(t, backend.CallbackStatusSuccess, callbacks[0].Status)
	active, err := b.releaseStore.LatestActive(uncertain.LeaseUUID)
	require.NoError(t, err)
	require.Nil(t, active, "unknown physical evidence cannot mint a release")
	require.NotNil(t, b.pool.GetAllocation(uncertain.LeaseUUID+"-app-0"), "unknown lease keeps its full reservation")
	require.Contains(t, warnings.String(), uncertain.LeaseUUID)
	require.Contains(t, warnings.String(), unresolved.OperationID().Fingerprint())
	require.NotContains(t, warnings.String(), unresolved.OperationID().String())
	for range 2 {
		require.NoError(t, b.recoverLiveOperationIntents(t.Context()))
		remaining, err := listOperationIntentsForCallbackTest(t, store)
		require.NoError(t, err)
		require.Equal(t, after, remaining, "elapsed operation budgets cannot settle contradictory physical evidence")
	}
	// Re-admission sees the journal-owned pending fence and returns no new
	// execution capability, even though sibling recovery is now complete.
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(uncertain)
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	require.Equal(t, shared.OperationIntentAdmissionExisting, admission.Disposition())
	_, created := admission.CreatedClaim()
	require.False(t, created)
}

func TestStartPreservesUnresolvedOperationWithoutBlockingBackend(t *testing.T) {
	var actual ContainerInfo
	mock := &mockDockerClient{
		PingFn:                  func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return []ContainerInfo{actual}, nil },
		InspectContainerFn:      func(context.Context, string) (*ContainerInfo, error) { copy := actual; return &copy, nil },
		RemoveContainerFn:       func(context.Context, string) error { t.Error("unknown container must not be removed"); return nil },
		CloseFn:                 func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.volumes = &mockVolumeManager{}
	b.cfg.ProvisionTimeout = time.Nanosecond
	blockedDelivery := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	rebuildCallbackSender(b, blockedDelivery)
	attachReleaseStore(t, b)
	attachRetentionStore(t, b)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	_, err := beginDockerTestOperationIntent(t, b.callbackStore, spec, b.storageIdentity)
	require.NoError(t, err)
	startPendingOperationForRecoveryTest(t, b)
	actual = dockerIntentContainer(spec, "uncertain-startup-container", spec.Items[0].SKU, 0)
	actual.Image = "example.invalid/unexpected:latest"
	before, err := listOperationIntentsForCallbackTest(t, b.callbackStore)
	require.NoError(t, err)
	forbidOperationRecoveryTeardown(t, b)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, b.Stop()) })
	after, err := listOperationIntentsForCallbackTest(t, b.callbackStore)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NotNil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-0"))
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	require.NoError(t, b.recoverLiveOperationIntents(t.Context()))
	after, err = listOperationIntentsForCallbackTest(t, b.callbackStore)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func captureOperationRecoveryWarnings(b *Backend) *bytes.Buffer {
	warnings := new(bytes.Buffer)
	b.logger = slog.New(slog.NewTextHandler(warnings, nil))
	return warnings
}
