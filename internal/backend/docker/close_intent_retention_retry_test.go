package docker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func beginRetainedCloseRecoveryIntent(
	t *testing.T,
	stores closeRecoveryStores,
	callbackURL string,
) shared.CloseIntentClaim {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	resourceProfiles := testResourceProfiles(t, items)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	operationID := mustTestOperationIDFromCallbackURL(t, callbackURL)
	runtimeAuthority, err := shared.NewReleaseRuntimeAuthority(
		operationID,
		"tenant-a",
		closeRecoveryProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	seedProvisionReleaseForLeaseTest(
		t, stores.callbacks, stores.releases, stores.operations,
		closeRecoveryLeaseUUID, shared.Release{
			Manifest:         payload,
			Image:            "stack",
			OperationID:      operationID,
			Items:            items,
			ResourceProfiles: resourceProfiles,
			RuntimeAuthority: &runtimeAuthority,
			Status:           "active",
			CreatedAt:        time.Now(),
		})
	request, err := stores.close.NewCloseRequest(closeRecoveryLeaseUUID, true)
	require.NoError(t, err)
	admission, err := stores.close.BeginClose(request)
	require.NoError(t, err)
	return admission.Claim()
}

func seedCompletedRetention(
	t *testing.T,
	settlement *shared.CloseSettlement,
	claim shared.CloseIntentClaim,
	volumeNames ...string,
) {
	t.Helper()
	ok, err := settlement.RecordRetention(claim, "", volumeNames)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRecoverState_RetainedCloseAfterAllRenamesPublishesRetained(t *testing.T) {
	dir := t.TempDir()
	retainedVolume := retainedName(canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0))
	volumes := newVolumeSet(retainedVolume)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumes.manager())
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	claim := beginRetainedCloseRecoveryIntent(t, stores, operationURL)
	seedCompletedRetention(t, stores.close, claim, retainedVolume)

	// This is the post-crash state: PutActiveMerged and every rename committed,
	// while release retirement and close resolution did not. Recovery sees no
	// canonical volume and must derive retained=true from durable+physical proof.
	require.NoError(t, b.recoverState(context.Background()))
	_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
	require.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	require.True(t, pending[0].Retained,
		"retry must not downgrade a completed retained close to retained=false")
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_RetainedCloseMissingRecordedVolumeStaysPending(t *testing.T) {
	dir := t.TempDir()
	retainedVolume := retainedName(canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0))
	volumes := newVolumeSet() // the ACTIVE row's promised volume is absent
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumes.manager())
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	claim := beginRetainedCloseRecoveryIntent(t, stores, operationURL)
	seedCompletedRetention(t, stores.close, claim, retainedVolume)

	// Startup itself remains available, but this close must not consume either
	// finalizer or enqueue a terminal callback without physical completion proof.
	require.NoError(t, b.recoverState(context.Background()))
	claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, claim.ExecutionGeneration().Number())
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases, "the exact release fence must remain retryable")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_AmbiguousVolumeCleanupNeverConsumesClose(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var b *Backend
	volumes := &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
		DestroyFn: func(context.Context, string) error {
			return b.latchAmbiguousOperationOutcome(
				"test volume destroy",
				errors.New("post-mutation storage attestation unavailable"),
			)
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, volumes)
	seedCloseDeprovisionLease(t, b, stores)

	err := b.doDeprovisionForTest(t, context.Background(), closeDeprovisionLeaseUUID)
	require.Error(t, err)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	_, _, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.ErrorIs(t, readErr, backendidentity.ErrMutationOutcomeAmbiguous,
		"the ambiguous substrate outcome must revoke every authoritative store read in this backend lifetime")
	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	require.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	storageID := b.storageIdentity

	closeCloseRecoveryBackend(t, b, stores)

	// Durable preservation is observable only after a fresh process generation
	// reopens and re-attests the exact same storage lineage. Letting the latched
	// instance keep reading its journals would split substrate and durable
	// authority precisely when their ordering is unknown.
	b, stores = openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	require.Equal(t, storageID, b.storageIdentity, "restart must re-attest the same storage generation")
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found, "ambiguous cleanup must retain its durable finalizer across restart")
	require.Equal(t, 1, claim.ExecutionGeneration().Number(),
		"ambiguity must preserve the exact durable Started generation")
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.NotEmpty(t, releases, "ambiguous cleanup must retain its exact release fence")
	pending, readErr := stores.callbacks.ListPending()
	require.NoError(t, readErr)
	require.Empty(t, pending, "an unclassified close outcome cannot publish a terminal callback")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_RetainedCloseAuthorityReadFailurePreservesAllAuthority(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	volumes := volumeState.manager()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, volumes)
	retentionPath := filepath.Join(dir, "retention.db")
	retentions := stores.retentions
	b.cfg.RetainOnClose = true
	volumes.RenameVolumeFn = func(oldName, newName string) error {
		require.Equal(t, volumeName, oldName)
		require.Equal(t, retainedName(volumeName), newName)
		require.NoError(t, volumeState.rename(oldName, newName))
		// PutActiveMerged and the physical rename have committed at this point.
		// Closing the store makes strict terminal classification unavailable.
		return retentions.Close()
	}
	seedCloseDeprovisionLease(t, b, stores)
	require.NoError(t, b.pool.TryAllocate(
		closeDeprovisionLeaseUUID+"-app-0",
		"docker-small",
		"tenant-a",
	))

	err := b.doDeprovisionForTest(t, context.Background(), closeDeprovisionLeaseUUID)
	require.ErrorContains(t, err, "database not open")
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found)
	require.Equal(t, 1, claim.ExecutionGeneration().Number())
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.Len(t, releases, 1,
		"accounting uncertainty must be detected before release retirement")
	pending, readErr := stores.callbacks.ListPending()
	require.NoError(t, readErr)
	require.Empty(t, pending)
	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	require.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	require.Equal(t, 1, b.pool.Stats().AllocationCount,
		"live accounting must remain until retained accounting is readable")

	// The ACTIVE fact itself committed before the injected read failure.
	reopened, reopenErr := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: retentionPath},
		b.storageAuthority,
		b.storeAuthorityGate,
	)
	require.NoError(t, reopenErr)
	t.Cleanup(func() { _ = reopened.Close() })
	record, readErr := reopened.Get(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, record)
	require.Equal(t, shared.RetentionStatusActive, record.Status)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRefreshRetentionAccountingChecked_UnknownSKUKeepsLastProjection(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	retentions := stores.retentions
	profile := b.cfg.SKUProfiles["docker-small"]
	profile.DiskMB = 1024
	b.cfg.SKUProfiles["docker-small"] = profile
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
		OriginalLeaseUUID: "retained-lease",
		Tenant:            "tenant-a",
		ProviderUUID:      "provider-a",
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		Status:    shared.RetentionStatusActive,
		CreatedAt: time.Now(),
	}))
	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)

	delete(b.cfg.SKUProfiles, "docker-small")
	require.NoError(t, b.refreshRetentionAccountingChecked(),
		"durable retention profiles make accounting independent of mutable SKU configuration")
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB,
		"the exact stored profile must preserve the attested projection")

	closeCloseRecoveryBackend(t, b, stores)
}
