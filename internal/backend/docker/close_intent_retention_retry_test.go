package docker

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func beginRetainedCloseRecoveryIntent(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	callbackURL string,
) ([]backend.LeaseItem, []shared.SKUResourceSnapshot) {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	resourceProfiles := testResourceProfiles(t, items)
	require.NoError(t, stores.releases.Append(closeRecoveryLeaseUUID, shared.Release{
		Manifest:         payload,
		Image:            "stack",
		Items:            items,
		ResourceProfiles: resourceProfiles,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	release, err := stores.releases.LatestActive(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, release)
	version, digest, err := closeReleaseFence(release)
	require.NoError(t, err)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	resourceProfiles, err = b.resolveResourceProfiles(items)
	require.NoError(t, err)
	_, err = stores.callbacks.BeginCloseIntent(shared.CloseIntentSpec{
		LeaseUUID:             closeRecoveryLeaseUUID,
		Backend:               b.Name(),
		BackendStorageID:      b.storageIdentity,
		Tenant:                "tenant-a",
		ProviderUUID:          "provider-a",
		Items:                 items,
		ResourceProfiles:      resourceProfiles,
		Manifest:              payload,
		ActiveReleaseVersion:  version,
		ActiveReleaseDigest:   digest,
		CallbackURL:           callbackURL,
		LifecycleCallbackURL:  lifecycleCallbackURL,
		RetainOnClose:         true,
		LegacyRollbackTargets: nil,
	})
	require.NoError(t, err)
	return items, resourceProfiles
}

func seedCompletedRetention(
	t *testing.T,
	store *shared.RetentionStore,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	volumeNames ...string,
) {
	t.Helper()
	require.NoError(t, store.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   closeRecoveryLeaseUUID,
		Tenant:              "tenant-a",
		ProviderUUID:        "provider-a",
		Items:               items,
		ResourceProfiles:    resourceProfiles,
		RetainedVolumeNames: volumeNames,
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))
}

func admitCloseAtCleanupAttempt(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	leaseUUID string,
	attempts int,
) shared.CloseIntentClaim {
	t.Helper()
	b.provisionsMu.RLock()
	projection := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		leaseUUID,
		true,
		projection.Tenant,
		projection.ProviderUUID,
		projection.Items,
		projection.StackManifest,
		projection.CallbackURL,
		projection.LifecycleCallbackURL,
	)
	require.NoError(t, err)
	require.True(t, found)
	for range attempts {
		claim, err = stores.callbacks.IncrementCloseCleanupAttempts(claim)
		require.NoError(t, err)
	}
	b.provisionsMu.Lock()
	b.provisions[leaseUUID].VolumeCleanupAttempts = attempts
	b.provisionsMu.Unlock()
	return claim
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
	retentions := attachRetentionStore(t, b)
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	items, resourceProfiles := beginRetainedCloseRecoveryIntent(t, b, stores, operationURL)
	seedCompletedRetention(t, retentions, items, resourceProfiles, retainedVolume)

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
	retentions := attachRetentionStore(t, b)
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	items, resourceProfiles := beginRetainedCloseRecoveryIntent(t, b, stores, operationURL)
	seedCompletedRetention(t, retentions, items, resourceProfiles, retainedVolume)

	// Startup itself remains available, but this close must not consume either
	// finalizer or enqueue a terminal callback without physical completion proof.
	require.NoError(t, b.recoverState(context.Background()))
	claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, claim.CleanupAttempts())
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases, "the exact release fence must remain retryable")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_AmbiguousVolumeCleanupNeverConsumesCloseAtRetryLimit(t *testing.T) {
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

	admitCloseAtCleanupAttempt(
		t,
		b,
		stores,
		closeDeprovisionLeaseUUID,
		maxVolumeCleanupAttempts-1,
	)

	err := b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID)
	require.Error(t, err)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found, "ambiguous cleanup must retain its durable finalizer")
	require.Equal(t, maxVolumeCleanupAttempts-1, claim.CleanupAttempts(),
		"ambiguity is evidence to preserve, not a failed cleanup attempt")
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.NotEmpty(t, releases, "ambiguous cleanup must retain its exact release fence")
	pending, readErr := stores.callbacks.ListPending()
	require.NoError(t, readErr)
	require.Empty(t, pending, "an unclassified close outcome cannot publish a terminal callback")
	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	require.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	require.Equal(t, maxVolumeCleanupAttempts-1, projection.VolumeCleanupAttempts)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_RetainedCloseAccountingReadFailurePreservesAllAuthority(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	retentionPath := filepath.Join(dir, "retention.db")
	retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: retentionPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = retentions.Close() })

	volumes := &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
	}
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, volumes)
	b.cfg.RetainOnClose = true
	b.retentionStore = retentions
	volumes.RenameVolumeFn = func(oldName, newName string) error {
		require.Equal(t, volumeName, oldName)
		require.Equal(t, retainedName(volumeName), newName)
		// PutActiveMerged has committed at this point. Closing the store models an
		// EIO/read failure in the accounting refresh before terminal hand-off.
		return retentions.Close()
	}
	seedCloseDeprovisionLease(t, b, stores)
	require.NoError(t, b.pool.TryAllocate(
		closeDeprovisionLeaseUUID+"-app-0",
		"docker-small",
		"tenant-a",
	))

	err = b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID)
	require.ErrorContains(t, err, "refresh retained close accounting")
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found)
	require.Zero(t, claim.CleanupAttempts())
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
	reopened, reopenErr := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: retentionPath})
	require.NoError(t, reopenErr)
	t.Cleanup(func() { _ = reopened.Close() })
	record, readErr := reopened.Get(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, record)
	require.Equal(t, shared.RetentionStatusActive, record.Status)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_GiveUpTombstoneFailurePreservesAllAuthority(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(dir, "retention.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = retentions.Close() })

	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
		DestroyFn: func(context.Context, string) error {
			require.NoError(t, retentions.Close(), "inject failure after close admission")
			return errors.New("injected volume failure")
		},
	})
	b.retentionStore = retentions
	seedCloseDeprovisionLease(t, b, stores)
	admitCloseAtCleanupAttempt(
		t,
		b,
		stores,
		closeDeprovisionLeaseUUID,
		maxVolumeCleanupAttempts-1,
	)
	require.NoError(t, b.pool.TryAllocate(
		closeDeprovisionLeaseUUID+"-app-0",
		"docker-small",
		"tenant-a",
	))

	err = b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID)
	require.ErrorContains(t, err, "record counted give-up footprint")
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found)
	require.Equal(t, maxVolumeCleanupAttempts, claim.CleanupAttempts())
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.Len(t, releases, 1)
	pending, readErr := stores.callbacks.ListPending()
	require.NoError(t, readErr)
	require.Empty(t, pending)
	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	require.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	require.Equal(t, 1, b.pool.Stats().AllocationCount,
		"failed tombstone ownership must keep the live reservation")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRefreshRetentionAccountingChecked_UnknownSKUKeepsLastProjection(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	retentions := attachRetentionStore(t, b)
	profile := b.cfg.SKUProfiles["docker-small"]
	profile.DiskMB = 1024
	b.cfg.SKUProfiles["docker-small"] = profile
	require.NoError(t, retentions.Put(shared.RetentionEntry{
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
	err := b.refreshRetentionAccountingChecked()
	require.ErrorContains(t, err, "unresolved SKU profiles")
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB,
		"an unknown SKU must keep the last attested projection, never publish an undercount")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestValidateCompletedRetention(t *testing.T) {
	leaseUUID := closeRecoveryLeaseUUID
	tenant := "tenant-a"
	providerUUID := "provider-a"
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}}
	resourceProfiles := []shared.SKUResourceSnapshot{{
		SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024,
	}}
	retainedVolume := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	physical := map[string]struct{}{retainedVolume: {}}
	valid := func() *shared.RetentionEntry {
		return &shared.RetentionEntry{
			OriginalLeaseUUID:   leaseUUID,
			Tenant:              tenant,
			ProviderUUID:        providerUUID,
			Items:               slices.Clone(items),
			ResourceProfiles:    shared.CloneSKUResourceSnapshot(resourceProfiles),
			Status:              shared.RetentionStatusActive,
			RetainedVolumeNames: []string{retainedVolume},
		}
	}
	withMutation := func(mutate func(*shared.RetentionEntry)) *shared.RetentionEntry {
		record := valid()
		mutate(record)
		return record
	}

	tests := []struct {
		name       string
		record     *shared.RetentionEntry
		physical   map[string]struct{}
		wantErrSub string
	}{
		{name: "valid", record: valid(), physical: physical},
		{name: "nil record", physical: physical, wantErrSub: "requires a record"},
		{name: "foreign identity", record: &shared.RetentionEntry{
			OriginalLeaseUUID: "different-lease", Status: shared.RetentionStatusActive,
			RetainedVolumeNames: []string{retainedVolume},
		}, physical: physical, wantErrSub: "identity mismatch"},
		{name: "non-active", record: &shared.RetentionEntry{
			OriginalLeaseUUID: leaseUUID, Status: shared.RetentionStatusRestoring,
			RetainedVolumeNames: []string{retainedVolume},
		}, physical: physical, wantErrSub: "is not active"},
		{name: "empty names", record: &shared.RetentionEntry{
			OriginalLeaseUUID: leaseUUID, Status: shared.RetentionStatusActive,
		}, physical: physical, wantErrSub: "has no retained volume names"},
		{name: "foreign volume name", record: &shared.RetentionEntry{
			OriginalLeaseUUID: leaseUUID, Status: shared.RetentionStatusActive,
			RetainedVolumeNames: []string{"fred-retained-different-lease-app-0"},
		}, physical: physical, wantErrSub: "malformed or foreign"},
		{name: "empty volume suffix", record: &shared.RetentionEntry{
			OriginalLeaseUUID: leaseUUID, Status: shared.RetentionStatusActive,
			RetainedVolumeNames: []string{retainedVolumePrefix + leaseUUID + "-"},
		}, physical: physical, wantErrSub: "malformed or foreign"},
		{name: "missing physical volume", record: valid(), physical: map[string]struct{}{}, wantErrSub: "references missing"},
		{name: "divergent tenant", record: withMutation(func(record *shared.RetentionEntry) {
			record.Tenant = "other"
		}), physical: physical, wantErrSub: "divergent lease identity"},
		{name: "divergent provider", record: withMutation(func(record *shared.RetentionEntry) {
			record.ProviderUUID = "other"
		}), physical: physical, wantErrSub: "divergent lease identity"},
		{name: "divergent items", record: withMutation(func(record *shared.RetentionEntry) {
			record.Items[0].Quantity = 2
		}), physical: physical, wantErrSub: "divergent items"},
		{name: "divergent resource snapshot", record: withMutation(func(record *shared.RetentionEntry) {
			record.ResourceProfiles[0].DiskMB++
		}), physical: physical, wantErrSub: "divergent resource snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCompletedRetention(
				leaseUUID, tenant, providerUUID, items, resourceProfiles, tt.record, tt.physical,
			)
			if tt.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErrSub)
		})
	}
}
