package docker

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func mustReleaseRuntimeAuthorityForIntent(
	t *testing.T,
	claim shared.OperationIntentClaim,
) *shared.ReleaseRuntimeAuthority {
	t.Helper()
	authority, err := releaseRuntimeAuthorityForIntent(claim)
	require.NoError(t, err)
	require.NotNil(t, authority)
	return authority
}

func reopenProvisionCommitBackend(
	t *testing.T,
	dir string,
	storageID backendidentity.ID,
	mock *mockDockerClient,
) (*Backend, closeRecoveryStores) {
	t.Helper()
	b := newBackendForTest(mock, nil)
	b.cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	b.cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	b.storageIdentity = storageID
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath})
	require.NoError(t, err)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath})
	require.NoError(t, err)
	b.callbackStore = callbacks
	b.operationIntents = callbacks
	b.releaseStore = releases
	return b, closeRecoveryStores{callbacks: callbacks, releases: releases}
}

func TestRecoverCommittedProvisionConvergesAcrossTwoRestarts(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	writer, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	storageID := writer.storageIdentity
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := stores.callbacks.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
		Manifest:         spec.Manifest,
		Image:            "stack",
		OperationID:      admission.Claim.OperationID(),
		Items:            admission.Claim.EffectiveItems(),
		ResourceProfiles: admission.Claim.ResourceProfiles(),
		RuntimeAuthority: mustReleaseRuntimeAuthorityForIntent(t, admission.Claim),
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	// Exact crash injection: AppendActive crossed the durable success boundary,
	// but ResolveOperationIntent has not replaced the intent with its callback.
	closeCloseRecoveryBackend(t, writer, stores)

	firstRestart, firstStores := reopenProvisionCommitBackend(t, dir, storageID, mock)
	require.NoError(t, firstRestart.recoverState(context.Background()))
	require.NoError(t, firstRestart.recoverOperationIntents(context.Background(), nil))
	active, err := firstStores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NotNil(t, active.RuntimeAuthority)
	assert.Equal(t, spec.Tenant, active.RuntimeAuthority.Tenant())
	assert.Equal(t, spec.ProviderUUID, active.RuntimeAuthority.ProviderUUID())
	assert.Equal(t, spec.CallbackURL, active.RuntimeAuthority.CallbackURL())
	assert.Equal(t, spec.LifecycleCallbackURL, active.RuntimeAuthority.LifecycleCallbackURL())
	closeCloseRecoveryBackend(t, firstRestart, firstStores)

	// A second process has no operation intent left and no container identity to
	// consult. The committed Release alone must still reconstruct the terminal
	// projection, exact callback authority, and complete pinned reservation.
	secondRestart, secondStores := reopenProvisionCommitBackend(t, dir, storageID, mock)
	t.Cleanup(func() { closeCloseRecoveryBackend(t, secondRestart, secondStores) })
	require.NoError(t, secondRestart.recoverState(context.Background()))
	projection, err := secondRestart.GetProvision(context.Background(), spec.LeaseUUID)
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	assert.Equal(t, spec.ProviderUUID, projection.ProviderUUID)
	secondRestart.provisionsMu.RLock()
	recovered := secondRestart.provisions[spec.LeaseUUID]
	secondRestart.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, spec.Tenant, recovered.Tenant)
	assert.Equal(t, spec.CallbackURL, recovered.CallbackURL)
	assert.Equal(t, spec.LifecycleCallbackURL, recovered.LifecycleCallbackURL)
	allocation := secondRestart.pool.GetAllocation(spec.LeaseUUID + "-app-0")
	require.NotNil(t, allocation)
	assert.Equal(t, spec.Tenant, allocation.Tenant)
	intents, err := secondStores.callbacks.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := secondStores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestRecoverCommittedProvisionAfterRestart(t *testing.T) {
	tests := []struct {
		name       string
		containers func(shared.OperationIntentSpec) []ContainerInfo
	}{
		{
			name: "exited survivor",
			containers: func(spec shared.OperationIntentSpec) []ContainerInfo {
				container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
				container.Status = "exited"
				return []ContainerInfo{container}
			},
		},
		{
			name: "zero survivors",
			containers: func(shared.OperationIntentSpec) []ContainerInfo {
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			var containers []ContainerInfo
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return append([]ContainerInfo(nil), containers...), nil
				},
				InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
					for _, container := range containers {
						if container.ContainerID == containerID {
							copy := container
							return &copy, nil
						}
					}
					return nil, assert.AnError
				},
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			admission, err := stores.callbacks.BeginOperationIntent(spec)
			require.NoError(t, err)
			containers = tt.containers(spec)
			require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
				Manifest:         spec.Manifest,
				Image:            "stack",
				OperationID:      admission.Claim.OperationID(),
				Items:            admission.Claim.EffectiveItems(),
				ResourceProfiles: admission.Claim.ResourceProfiles(),
				RuntimeAuthority: mustReleaseRuntimeAuthorityForIntent(t, admission.Claim),
				Status:           "active",
				CreatedAt:        time.Now(),
			}))

			// Model the new process startup order. recoverState must first publish a
			// terminal projection and its full conservative reservation; only then may
			// operation recovery replace the intent with Success.
			require.NoError(t, b.recoverState(context.Background()))
			projection, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusFailed, projection.Status)
			assert.Equal(t, spec.ProviderUUID, projection.ProviderUUID)
			assert.Equal(t, admission.Claim.EffectiveItems(), projection.Items)
			b.provisionsMu.RLock()
			recoveredTenant := b.provisions[spec.LeaseUUID].Tenant
			recoveredLifecycleCallbackURL := b.provisions[spec.LeaseUUID].LifecycleCallbackURL
			b.provisionsMu.RUnlock()
			assert.Equal(t, spec.Tenant, recoveredTenant)
			assert.Equal(t, spec.LifecycleCallbackURL, recoveredLifecycleCallbackURL)
			allocation := b.pool.GetAllocation(spec.LeaseUUID + "-app-0")
			require.NotNil(t, allocation)
			assert.Equal(t, spec.Tenant, allocation.Tenant)
			assert.Equal(t, spec.ResourceProfiles[0].CPUCores, allocation.CPUCores)
			assert.Equal(t, spec.ResourceProfiles[0].MemoryMB, allocation.MemoryMB)

			before, err := stores.releases.List(spec.LeaseUUID)
			require.NoError(t, err)
			require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
			after, err := stores.releases.List(spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, before, after, "recovery must not mint or replace the committed Release")
			intents, err := stores.callbacks.ListOperationIntents()
			require.NoError(t, err)
			assert.Empty(t, intents)
			pending, err := stores.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
			assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
		})
	}
}

func TestRecoverProvisionDoesNotCommitFromAnotherReleaseGeneration(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		operationID           shared.OperationID
		expectUnresolvedError string
	}{
		{
			name:                  "legacy empty operation ID",
			operationID:           "",
			expectUnresolvedError: "legacy predecessor has no durable runtime authority",
		},
		{name: "different operation ID", operationID: "9a72fbc1-38c8-4f31-87f7-f689979b9324"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			admission, err := stores.callbacks.BeginOperationIntent(spec)
			require.NoError(t, err)
			var runtimeAuthority *shared.ReleaseRuntimeAuthority
			if tt.operationID.Valid() {
				oldID := admission.Claim.OperationID().String()
				callbackURL := strings.Replace(spec.CallbackURL, oldID, tt.operationID.String(), 1)
				lifecycleCallbackURL := strings.Replace(spec.LifecycleCallbackURL, oldID, tt.operationID.String(), 1)
				runtimeAuthority, err = releaseRuntimeAuthorityForOperation(
					tt.operationID, spec.Tenant, spec.ProviderUUID, callbackURL, lifecycleCallbackURL,
				)
				require.NoError(t, err)
			}
			require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
				Manifest:         spec.Manifest,
				Image:            "stack",
				OperationID:      tt.operationID,
				Items:            admission.Claim.EffectiveItems(),
				ResourceProfiles: admission.Claim.ResourceProfiles(),
				RuntimeAuthority: runtimeAuthority,
				Status:           "active",
				CreatedAt:        time.Now(),
			}))

			committed, err := b.operationIntentHasCommittedRelease(admission.Claim)
			require.NoError(t, err)
			assert.False(t, committed, "another generation must not commit the pending operation")

			require.NoError(t, b.recoverState(context.Background()))
			candidate, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusProvisioning, candidate.Status)
			assert.Equal(t, admission.Claim.EffectiveItems(), candidate.Items)
			b.provisionsMu.RLock()
			candidateCallbackURL := b.provisions[spec.LeaseUUID].CallbackURL
			b.provisionsMu.RUnlock()
			assert.Equal(t, spec.CallbackURL, candidateCallbackURL,
				"the temporary cleanup projection must come from the pending intent")

			recoveryErr := b.recoverOperationIntents(context.Background(), nil)
			if tt.expectUnresolvedError != "" {
				require.ErrorContains(t, recoveryErr, tt.expectUnresolvedError)
				intents, listErr := stores.callbacks.ListOperationIntents()
				require.NoError(t, listErr)
				require.Len(t, intents, 1,
					"ambiguous legacy authority must retain the exact operation evidence")
				pending, listErr := stores.callbacks.ListPending()
				require.NoError(t, listErr)
				assert.Empty(t, pending)
				return
			}
			require.NoError(t, recoveryErr)
			pending, err := stores.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
			assert.Equal(t, interruptedOperationFailure, pending[0].Error)

			recovered, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
			b.provisionsMu.RLock()
			recoveredCallbackURL := b.provisions[spec.LeaseUUID].CallbackURL
			b.provisionsMu.RUnlock()
			assert.Equal(t, runtimeAuthority.CallbackURL(), recoveredCallbackURL,
				"failed candidate recovery must restore the older release authority")
			assert.NotEqual(t, spec.CallbackURL, recoveredCallbackURL)
			active, err := stores.releases.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Equal(t, tt.operationID, active.OperationID,
				"failure settlement must not append a candidate release")
		})
	}
}

func TestRecoverProvisionPublishesExactLineageOverByteIdenticalOlderRelease(t *testing.T) {
	dir := t.TempDir()
	var containers []ContainerInfo
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return append([]ContainerInfo(nil), containers...), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for _, container := range containers {
				if container.ContainerID == containerID {
					copy := container
					return &copy, nil
				}
			}
			return nil, assert.AnError
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	admission, err := stores.callbacks.BeginOperationIntent(spec)
	require.NoError(t, err)
	containers = []ContainerInfo{dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)}

	const olderID = shared.OperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	newID := admission.Claim.OperationID().String()
	olderCallbackURL := strings.Replace(spec.CallbackURL, newID, olderID.String(), 1)
	olderLifecycleURL := strings.Replace(spec.LifecycleCallbackURL, newID, olderID.String(), 1)
	olderAuthority, err := releaseRuntimeAuthorityForOperation(
		olderID, spec.Tenant, spec.ProviderUUID, olderCallbackURL, olderLifecycleURL,
	)
	require.NoError(t, err)
	require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
		Manifest:         spec.Manifest,
		Image:            "stack",
		OperationID:      olderID,
		Items:            admission.Claim.EffectiveItems(),
		ResourceProfiles: admission.Claim.ResourceProfiles(),
		RuntimeAuthority: olderAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
	history, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	latest := history[len(history)-1]
	assert.Equal(t, admission.Claim.OperationID(), latest.OperationID)
	assert.True(t, latest.CreatedAt.Equal(admission.Claim.CreatedAt()),
		"cold recovery must reuse the admission timestamp proven before side effects")
	require.NotNil(t, latest.RuntimeAuthority)
	assert.Equal(t, admission.Claim.CallbackURL(), latest.RuntimeAuthority.CallbackURL())
	assert.Equal(t, "superseded", history[0].Status)
	assert.Equal(t, "active", latest.Status)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestRecoverProvisionFailsClosedOnSameOperationIDDivergence(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*shared.Release)
	}{
		{
			name: "manifest",
			mutate: func(release *shared.Release) {
				release.Manifest = validStackManifestJSON(map[string]string{
					"app": "docker.io/library/busybox:1.37",
				})
			},
		},
		{
			name: "effective items",
			mutate: func(release *shared.Release) {
				release.Items[0].Quantity = 2
			},
		},
		{
			name: "resource profiles",
			mutate: func(release *shared.Release) {
				release.ResourceProfiles[0].CPUCores++
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			admission, err := stores.callbacks.BeginOperationIntent(spec)
			require.NoError(t, err)
			release := shared.Release{
				Manifest:         spec.Manifest,
				Image:            "stack",
				OperationID:      admission.Claim.OperationID(),
				Items:            admission.Claim.EffectiveItems(),
				ResourceProfiles: admission.Claim.ResourceProfiles(),
				RuntimeAuthority: mustReleaseRuntimeAuthorityForIntent(t, admission.Claim),
				Status:           "active",
				CreatedAt:        time.Now(),
			}
			tt.mutate(&release)
			require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, release))

			err = b.recoverState(context.Background())
			require.ErrorContains(t, err, "active release with matching operation ID differs")
			intents, listErr := stores.callbacks.ListOperationIntents()
			require.NoError(t, listErr)
			assert.Len(t, intents, 1, "ambiguous same-token evidence must remain durable")
			pending, listErr := stores.callbacks.ListPending()
			require.NoError(t, listErr)
			assert.Empty(t, pending)
		})
	}
}

func TestRestoreFinalizerAcceptsCommittedMaintenanceBaseMove(t *testing.T) {
	const (
		operationID  = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + operationID.String()
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + operationID.String()
	newOperationURL := "https://new.example/callbacks/provision?operation_id=" + operationID.String()
	newLifecycleURL := "https://new.example/callbacks/provision?lifecycle_id=" + operationID.String()
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}}
	profiles := testResourceProfiles(t, items)
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	finalizer := shared.RetentionEntry{
		Tenant:                          "tenant-a",
		ProviderUUID:                    providerUUID,
		StackManifest:                   stack,
		DestinationItems:                items,
		DestinationResourceProfiles:     profiles,
		DestinationOperationID:          operationID,
		DestinationCallbackURL:          oldOperationURL,
		DestinationLifecycleCallbackURL: oldLifecycleURL,
	}
	for _, tt := range []struct {
		name                              string
		callbackURL, lifecycleCallbackURL string
	}{
		{name: "rejected or failed maintenance leaves old active route", callbackURL: oldOperationURL, lifecycleCallbackURL: oldLifecycleURL},
		{name: "successful maintenance moves active route", callbackURL: newOperationURL, lifecycleCallbackURL: newLifecycleURL},
	} {
		t.Run(tt.name, func(t *testing.T) {
			authority, authorityErr := shared.NewReleaseRuntimeAuthority(
				operationID,
				finalizer.Tenant,
				finalizer.ProviderUUID,
				tt.callbackURL,
				tt.lifecycleCallbackURL,
			)
			require.NoError(t, authorityErr)
			matches, matchErr := restoreReleaseMatchesAuthority(&shared.Release{
				Manifest:         payload,
				OperationID:      operationID,
				Items:            items,
				ResourceProfiles: profiles,
				RuntimeAuthority: &authority,
			}, finalizer)
			require.NoError(t, matchErr)
			assert.True(t, matches)
		})
	}
}

func TestRecoverCommittedRestoreWithNoSurvivorsUsesMovedReleaseRoute(t *testing.T) {
	const (
		sourceLease  = "0192f1a0-1111-4abc-8def-000000000201"
		destination  = "0192f1a0-2222-4abc-8def-000000000202"
		operationID  = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + operationID.String()
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + operationID.String()
	newOperationURL := "https://new.example/callbacks/provision?operation_id=" + operationID.String()
	newLifecycleURL := "https://new.example/callbacks/provision?lifecycle_id=" + operationID.String()
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}}
	profiles := testResourceProfiles(t, items)
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	dir := t.TempDir()
	retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(dir, "retentions.db"),
	})
	require.NoError(t, err)
	defer retentions.Close()
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      providerUUID,
		Items:             items,
		ResourceProfiles:  profiles,
		StackManifest:     stack,
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	claimed, err := retentions.ClaimForRestoreWithAuthority(
		sourceLease,
		destination,
		0,
		items,
		profiles,
		operationID,
		oldOperationURL,
		oldLifecycleURL,
	)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "releases.db"),
	})
	require.NoError(t, err)
	defer releases.Close()
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, newOperationURL, newLifecycleURL,
	)
	require.NoError(t, err)
	require.NoError(t, releases.AppendActive(destination, shared.Release{
		Manifest: payload, Image: "stack", OperationID: operationID,
		Items: items, ResourceProfiles: profiles, RuntimeAuthority: &authority,
		Status: "active", CreatedAt: time.Now(),
	}))
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, nil)
	defer b.stopCancel()
	b.retentionStore = retentions
	b.releaseStore = releases
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	recovered := b.provisions[destination]
	b.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
	assert.Equal(t, newOperationURL, recovered.CallbackURL)
	assert.Equal(t, newLifecycleURL, recovered.LifecycleCallbackURL)
	allocation := b.pool.GetAllocation(destination + "-app-0")
	require.NotNil(t, allocation)
	assert.Equal(t, "tenant-a", allocation.Tenant)
}
