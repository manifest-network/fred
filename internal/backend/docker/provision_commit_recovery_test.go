package docker

import (
	"context"
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
	authority, err := releaseRuntimeAuthorityForOperation(
		claim.OperationID(),
		claim.Tenant(),
		claim.ProviderUUID(),
		claim.CallbackURL(),
		claim.LifecycleCallbackURL(),
	)
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
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	require.Equal(t, storageID, b.storageIdentity,
		"reopening the same journal set must recover the same storage authority")
	return b, stores
}

func TestRecoverCommittedProvisionConvergesAcrossTwoRestarts(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	writer, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	storageID := writer.storageIdentity
	spec := dockerOperationIntentSpec(t, storageID)
	admission := beginOperationIntentForSettlementTest(t, stores.operations, spec)
	commitOperationReleaseWithoutPublishingTest(
		t, stores.operations, createdDockerOperationClaim(t, admission),
	)
	// Exact crash injection: the typed release commit crossed the durable success boundary,
	// but ResolveOperationIntent has not replaced the intent with its callback.
	closeCloseRecoveryBackend(t, writer, stores)

	firstRestart, firstStores := reopenProvisionCommitBackend(t, dir, storageID, mock)
	require.NoError(t, firstRestart.recoverState(context.Background()))
	require.NoError(t, firstRestart.recoverOperationIntents(context.Background()))
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
	intents, err := secondStores.operations.ListOperationIntents()
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
			admission := beginOperationIntentForSettlementTest(t, stores.operations, spec)
			containers = tt.containers(spec)
			commitOperationReleaseWithoutPublishingTest(
				t, stores.operations, createdDockerOperationClaim(t, admission),
			)

			// Model the new process startup order. recoverState must first publish a
			// terminal projection and its full conservative reservation; only then may
			// operation recovery replace the intent with Success.
			require.NoError(t, b.recoverState(context.Background()))
			projection, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusFailed, projection.Status)
			assert.Equal(t, spec.ProviderUUID, projection.ProviderUUID)
			assert.Equal(t, createdDockerOperationClaim(t, admission).EffectiveItems(), projection.Items)
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
			require.NoError(t, b.recoverOperationIntents(context.Background()))
			after, err := stores.releases.List(spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, before, after, "recovery must not mint or replace the committed Release")
			intents, err := stores.operations.ListOperationIntents()
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
		name        string
		operationID shared.OperationID
	}{
		{name: "migrated legacy predecessor", operationID: shared.OperationID{}},
		{name: "different operation ID", operationID: mustDockerOperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			inventoryReads := 0
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					inventoryReads++
					return nil, nil
				},
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			// An empty exact-candidate inventory is not absence proof: Docker may
			// still publish a Create accepted by the process that wrote the intent.
			// Keep the production recovery semantics while bounding this fixture.
			b.cfg.ContainerStartTimeout = 5 * time.Millisecond
			b.cfg.ProvisionTimeout = 40 * time.Millisecond
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			if tt.operationID.Valid() {
				currentID := mustTestOperationIDFromCallbackURL(t, spec.CallbackURL)
				callbackURL := strings.Replace(spec.CallbackURL, currentID.String(), tt.operationID.String(), 1)
				lifecycleCallbackURL := strings.Replace(spec.LifecycleCallbackURL, currentID.String(), tt.operationID.String(), 1)
				runtimeAuthority, authorityErr := releaseRuntimeAuthorityForOperation(
					tt.operationID, spec.Tenant, spec.ProviderUUID, callbackURL, lifecycleCallbackURL,
				)
				require.NoError(t, authorityErr)
				seedProvisionReleaseForLeaseTest(t, stores.callbacks, stores.releases, stores.operations,
					spec.LeaseUUID, shared.Release{
						Manifest: spec.Manifest, Image: "stack", OperationID: tt.operationID,
						Items: spec.Items, ResourceProfiles: spec.ResourceProfiles,
						RuntimeAuthority: runtimeAuthority, Status: "active", CreatedAt: time.Now(),
					})
			} else {
				legacyAuthority, authorityErr := shared.NewLegacyRuntimeAuthority(
					spec.Tenant, spec.ProviderUUID,
					"https://fred.example/callbacks/provision",
					"https://fred.example/callbacks/provision",
				)
				require.NoError(t, authorityErr)
				seedUpgradedV013ReleaseForBackendTest(t, b, spec.LeaseUUID, shared.Release{
					Manifest: spec.Manifest, Image: "stack", Status: "active", CreatedAt: time.Now(),
				}, spec.Items, spec.ResourceProfiles, legacyAuthority)
				stores.releases = b.releaseStore
				var ok bool
				stores.operations, ok = concreteOperationSettlementForTest(b.operationSettlement)
				require.True(t, ok)
				stores.restore = b.restoreSettlement
				stores.maintenance = b.maintenanceSettlement
				stores.close = b.closeSettlement
			}
			admission := beginOperationIntentForSettlementTest(t, stores.operations, spec)

			committed, err := b.operationIntentHasCommittedRelease(createdDockerOperationClaim(t, admission))
			require.NoError(t, err)
			assert.False(t, committed, "another generation must not commit the pending operation")

			require.NoError(t, b.recoverState(context.Background()))
			candidate, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusProvisioning, candidate.Status)
			assert.Equal(t, createdDockerOperationClaim(t, admission).EffectiveItems(), candidate.Items)
			b.provisionsMu.RLock()
			candidateCallbackURL := b.provisions[spec.LeaseUUID].CallbackURL
			b.provisionsMu.RUnlock()
			assert.Equal(t, spec.CallbackURL, candidateCallbackURL,
				"the temporary cleanup projection must come from the pending intent")

			readsBeforeRecovery := inventoryReads
			recoveryErr := b.recoverOperationIntents(context.Background())
			require.NoError(t, recoveryErr)
			assert.GreaterOrEqual(t, inventoryReads-readsBeforeRecovery, 2,
				"a different active generation must not turn the first empty candidate inventory into absence proof")
			pending, err := stores.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
			assert.Equal(t, interruptedOperationFailure, pending[0].Error)
			intents, err := stores.operations.ListOperationIntents()
			require.NoError(t, err)
			assert.Empty(t, intents,
				"the bounded ambiguity window must terminate in an exact failed receipt")

			recovered, err := b.GetProvision(context.Background(), spec.LeaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
			b.provisionsMu.RLock()
			recoveredCallbackURL := b.provisions[spec.LeaseUUID].CallbackURL
			b.provisionsMu.RUnlock()
			active, err := stores.releases.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			activeAuthority, ok := active.RuntimeIdentity()
			require.True(t, ok)
			assert.Equal(t, activeAuthority.CallbackURL(), recoveredCallbackURL,
				"failed candidate recovery must restore the older release authority")
			assert.NotEqual(t, spec.CallbackURL, recoveredCallbackURL)
			assert.Equal(t, tt.operationID, active.OperationID,
				"failure settlement must not append a candidate release")
			history, err := stores.releases.List(spec.LeaseUUID)
			require.NoError(t, err)
			require.Len(t, history, 1,
				"recovery must preserve the predecessor without minting a candidate generation")
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
	olderID := mustDockerOperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	newID := mustTestOperationIDFromCallbackURL(t, spec.CallbackURL).String()
	olderCallbackURL := strings.Replace(spec.CallbackURL, newID, olderID.String(), 1)
	olderLifecycleURL := strings.Replace(spec.LifecycleCallbackURL, newID, olderID.String(), 1)
	olderAuthority, err := releaseRuntimeAuthorityForOperation(
		olderID, spec.Tenant, spec.ProviderUUID, olderCallbackURL, olderLifecycleURL,
	)
	require.NoError(t, err)
	seedProvisionReleaseForLeaseTest(t, stores.callbacks, stores.releases, stores.operations,
		spec.LeaseUUID, shared.Release{
			Manifest:         spec.Manifest,
			Image:            "stack",
			OperationID:      olderID,
			Items:            spec.Items,
			ResourceProfiles: spec.ResourceProfiles,
			RuntimeAuthority: olderAuthority,
			Status:           "active",
			CreatedAt:        time.Now(),
		})
	admission := beginOperationIntentForSettlementTest(t, stores.operations, spec)
	started := startPendingOperationForRecoveryTest(t, b)
	containers = []ContainerInfo{dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)}

	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	history, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	latest := history[len(history)-1]
	assert.Equal(t, started.OperationID(), latest.OperationID)
	assert.True(t, latest.CreatedAt.Equal(createdDockerOperationClaim(t, admission).CreatedAt()),
		"cold recovery must reuse the admission timestamp proven before side effects")
	require.NotNil(t, latest.RuntimeAuthority)
	assert.Equal(t, createdDockerOperationClaim(t, admission).CallbackURL(), latest.RuntimeAuthority.CallbackURL())
	assert.Equal(t, "superseded", history[0].Status)
	assert.Equal(t, "active", latest.Status)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestRestoreFinalizerAcceptsCommittedMaintenanceBaseMove(t *testing.T) {
	const providerUUID = "22222222-2222-4222-8222-222222222222"
	operationID := mustDockerOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
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
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	operationID := mustDockerOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + operationID.String()
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + operationID.String()
	newOperationURL := "https://new.example/callbacks/provision?operation_id=" + operationID.String()
	newLifecycleURL := "https://new.example/callbacks/provision?lifecycle_id=" + operationID.String()
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}}
	profiles := testResourceProfiles(t, items)
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	b := newBackendForProvisionTest(t, &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, nil)
	defer b.stopCancel()
	retentions := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      providerUUID,
		Items:             items,
		ResourceProfiles:  profiles,
		StackManifest:     stack,
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	operationCandidate, err := b.operationSettlement.NewOperationIntentCandidate(
		shared.OperationIntentSpec{
			Kind: shared.OperationIntentRestore, LeaseUUID: destination,
			CallbackURL: oldOperationURL, LifecycleCallbackURL: oldLifecycleURL,
			Tenant: "tenant-a", ProviderUUID: providerUUID,
			Items: items, ResourceProfiles: profiles, EffectiveItems: items,
			Manifest: payload, SourceLeaseUUID: sourceLease, SourceGeneration: 1,
		},
	)
	require.NoError(t, err)
	operationAdmission, err := b.operationSettlement.BeginOperationIntent(operationCandidate)
	require.NoError(t, err)
	operationClaim, created := operationAdmission.CreatedClaim()
	require.True(t, created)
	restoreCandidate, err := b.restoreSettlement.PrepareRestoreClaim(operationClaim)
	require.NoError(t, err)
	claimedProof, err := b.restoreSettlement.ClaimForRestore(restoreCandidate, 0)
	require.NoError(t, err)
	require.True(t, claimedProof.Valid())
	committed := commitOperationReleaseWithoutPublishingTest(t, b.operationSettlement, operationClaim)
	require.NoError(t, b.callbackPublisher.PublishOperationSuccessContext(context.Background(), committed))
	acknowledgePendingCallbacksForTest(t, b.callbackStore)
	active, err := b.releaseStore.LatestActive(destination)
	require.NoError(t, err)
	require.NotNil(t, active)
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, newOperationURL, newLifecycleURL,
	)
	require.NoError(t, err)
	target := *active
	target.Version = 0
	target.RuntimeAuthority = &authority
	activateMaintenanceReleaseForTest(
		t, b.maintenanceSettlement, destination, shared.MaintenanceIntentUpdate, target,
	)
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
