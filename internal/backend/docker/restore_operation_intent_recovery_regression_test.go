package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type interruptedRestoreRecoveryFixture struct {
	b             *Backend
	retentions    *shared.RetentionStore
	releases      *shared.ReleaseStore
	callbacks     *shared.CallbackStore
	retentionPath string
	claim         shared.OperationIntentClaim
	source        *shared.RetentionEntry
	spec          shared.OperationIntentSpec
	containers    []ContainerInfo
	allocation    []string
	addContainer  func(ContainerInfo)

	eventsMu sync.Mutex
	events   []string
}

func newInterruptedRestoreRecoveryFixture(
	t *testing.T,
	quantity int,
	containerStatuses []string,
	projectionStatus backend.ProvisionStatus,
	withSourceFinalizer bool,
) *interruptedRestoreRecoveryFixture {
	t.Helper()

	fixture := &interruptedRestoreRecoveryFixture{}
	var substrateMu sync.Mutex
	var inventory []ContainerInfo
	var volumeNames []string
	captured := make(map[string]bool)
	fixture.addContainer = func(container ContainerInfo) {
		substrateMu.Lock()
		defer substrateMu.Unlock()
		inventory = append(inventory, container)
	}
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			return slices.Clone(inventory), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			for i := range inventory {
				if inventory[i].ContainerID == containerID {
					container := inventory[i]
					return &container, nil
				}
			}
			return nil, fmt.Errorf("unknown test container %q", containerID)
		},
		ContainerLogsFn: func(_ context.Context, containerID string, tail int) (string, error) {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			for _, container := range inventory {
				if container.ContainerID == containerID {
					if tail == persistedLogTail {
						captured[containerID] = true
					}
					return "interrupted restore startup logs", nil
				}
			}
			return "", fmt.Errorf("unknown log container %q", containerID)
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			for index, container := range inventory {
				if container.ContainerID != containerID {
					continue
				}
				if !captured[containerID] {
					t.Errorf("restore cleanup removed %q before capturing its logs", containerID)
					return errors.New("restore cleanup requires captured exact-container logs")
				}
				inventory = slices.Delete(inventory, index, index+1)
				fixture.record("remove:" + containerID)
				return nil
			}
			return nil // Docker removal is idempotent for an already absent ID.
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = 20 * time.Millisecond
	fixture.b = b
	// bindTestStorageIdentity already installed one construction-bound callback,
	// release, and retention journal lineage. Replacing just the retention store
	// would leave RestoreSettlement and CloseSettlement authorized against the
	// old store and create a fixture state production cannot construct.
	fixture.retentionPath = b.cfg.RetentionDBPath
	fixture.retentions = b.retentionStore
	fixture.releases = b.releaseStore
	fixture.callbacks = attachRestoreAuthorityCallbackStore(t, b)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174001"
	spec.SourceGeneration = 1
	spec.Items[0].Quantity = quantity
	spec.ResourceProfiles = testResourceProfiles(t, spec.Items)
	spec.EffectiveItems = slices.Clone(spec.Items)
	if slices.Contains(containerStatuses, "health-starting") {
		spec.HealthCheckServices = []string{spec.Items[0].ServiceName}
	}
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	if slices.Contains(containerStatuses, "health-starting") {
		stack.Services[spec.Items[0].ServiceName].HealthCheck = &manifest.HealthCheckConfig{
			Test: []string{"CMD", "true"},
		}
	}
	spec.Manifest, err = json.Marshal(stack)
	require.NoError(t, err)
	fixture.spec = spec

	retainedVolume := retainedName(canonicalVolumeName(
		spec.SourceLeaseUUID, spec.Items[0].ServiceName, 0,
	))
	// ClaimForRestore has already adopted the source volume before the actor can
	// start the destination operation. Model that exact physical state: operation
	// recovery must re-quarantine the canonical destination name before it may
	// seal an exact-absence failure. A nil ListForProof fixture would instead
	// describe lost source data and correctly latch ambiguity.
	volumeNames = []string{retainedToNewCanonical(
		retainedVolume, spec.SourceLeaseUUID, spec.LeaseUUID,
	)}
	source := shared.RetentionEntry{
		OriginalLeaseUUID: spec.SourceLeaseUUID,
		NewLeaseUUID:      spec.LeaseUUID,
		Tenant:            spec.Tenant,
		ProviderUUID:      spec.ProviderUUID,
		Items:             slices.Clone(spec.Items),
		ResourceProfiles:  shared.CloneSKUResourceSnapshot(spec.ResourceProfiles),
		RetainedVolumeNames: []string{
			retainedVolume,
		},
		Status:     shared.RetentionStatusRestoring,
		Generation: spec.SourceGeneration,
	}
	if withSourceFinalizer {
		fixture.source = putRestoreIntentFinalizer(t, fixture.retentions, spec, source)
		require.Equal(t, spec.SourceGeneration, fixture.source.Generation)
	} else {
		// Restore admits its operation WAL before claiming Active -> Restoring.
		// A crash in that exact window leaves a recoverable operation with no
		// source finalizer and must fail closed without touching substrate.
		candidate, candidateErr := operations.NewOperationIntentCandidate(spec)
		require.NoError(t, candidateErr)
		admission, admissionErr := operations.BeginOperationIntent(candidate)
		require.NoError(t, admissionErr)
		require.Equal(t, shared.OperationIntentAdmissionCreated, admission.Disposition())
		claim := createdDockerOperationClaim(t, admission)
		releaseCandidate, prepareErr := operations.PrepareOperationRelease(claim)
		require.NoError(t, prepareErr)
		_, startErr := operations.StartOperationExecution(releaseCandidate)
		require.NoError(t, startErr)
		fixture.source = &source
	}

	// ClaimForRestore atomically admitted the destination operation through the
	// same journal pair. Recovery starts from that durable claim; admitting a
	// second copy through an independently assembled test settlement would model
	// a state the production constructor deliberately makes impossible.
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	for _, claim := range claims {
		if claim.LeaseUUID() == spec.LeaseUUID &&
			claim.OperationID() == dockerOperationIntentID {
			fixture.claim = claim
			break
		}
	}
	require.NotNil(t, fixture.claim)
	if fixture.claim.ExecutionPhase() == shared.OperationExecutionBeforeEffects {
		candidate, prepareErr := operations.PrepareOperationRelease(fixture.claim)
		require.NoError(t, prepareErr)
		_, startErr := operations.StartOperationExecution(candidate)
		require.NoError(t, startErr)
		started, listErr := operations.ListOperationIntents()
		require.NoError(t, listErr)
		fixture.claim = shared.OperationIntentClaim{}
		for _, claim := range started {
			if claim.LeaseUUID() == spec.LeaseUUID &&
				claim.OperationID() == dockerOperationIntentID {
				fixture.claim = claim
				break
			}
		}
		require.Equal(t, shared.OperationExecutionStarted, fixture.claim.ExecutionPhase())
	}

	containerIDs := make([]string, 0, len(containerStatuses))
	for index, status := range containerStatuses {
		containerID := fmt.Sprintf("restore-container-%d", index)
		container := dockerIntentContainer(spec, containerID, spec.Items[0].SKU, index)
		if status == "health-starting" {
			container.Status = "running"
			container.Health = HealthStatusStarting
		} else {
			container.Status = status
		}
		inventory = append(inventory, container)
		containerIDs = append(containerIDs, containerID)
	}
	fixture.containers = slices.Clone(inventory)

	projection := readyIntentProjection(spec, containerIDs...)[spec.LeaseUUID]
	projection.Status = projectionStatus
	b.provisionsMu.Lock()
	b.provisions[spec.LeaseUUID] = projection
	b.provisionsMu.Unlock()

	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		t.Error("interrupted restore recovery attempted broad Compose teardown")
		return errors.New("restore recovery requires captured exact-container removal")
	}}
	b.volumes = &mockVolumeManager{
		ListForProofFn: func(context.Context) ([]string, error) {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			return slices.Clone(volumeNames), nil
		},
		RenameVolumeFn: func(oldName, newName string) error {
			substrateMu.Lock()
			defer substrateMu.Unlock()
			for index, name := range volumeNames {
				if name == newName {
					return nil
				}
				if name == oldName {
					volumeNames[index] = newName
					fixture.record("re-quarantine")
					return nil
				}
			}
			return fmt.Errorf("test volume %q is absent from both namespaces", oldName)
		},
		UsageFn: func(context.Context, string) (int64, error) {
			fixture.record("measure-source-quota")
			return 0, nil
		},
		EnsureQuotaFn: func(context.Context, string, int64) error {
			fixture.record("restore-source-quota")
			return nil
		},
	}

	for index := range quantity {
		allocationID := fmt.Sprintf("%s-%s-%d", spec.LeaseUUID, spec.Items[0].ServiceName, index)
		require.NoError(t, b.pool.TryAllocateResolved(
			allocationID, spec.Tenant, spec.ResourceProfiles[0],
		))
		fixture.allocation = append(fixture.allocation, allocationID)
	}

	// Stop actors before the real stores registered above are closed.
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	return fixture
}

func TestInterruptedRestoreRecovery_MissingSourceFinalizerFailsStartupClosed(t *testing.T) {
	for _, runtimeStatus := range []string{"created", "exited"} {
		t.Run(runtimeStatus, func(t *testing.T) {
			f := newInterruptedRestoreRecoveryFixture(
				t, 1, []string{runtimeStatus}, backend.ProvisionStatusFailed, false,
			)

			err := f.b.Start(context.Background())
			require.ErrorContains(t, err, "restore source finalizer is absent")
			assert.Empty(t, f.snapshotEvents(),
				"startup must not tear down or re-quarantine substrate without source authority")

			intents, listErr := f.b.operationSettlement.ListOperationIntents()
			require.NoError(t, listErr)
			require.Len(t, intents, 1)
			assert.Equal(t, f.claim.OperationID(), intents[0].OperationID())
			for _, allocationID := range f.allocation {
				assert.NotNil(t, f.b.pool.GetAllocation(allocationID))
			}
		})
	}
}

// record is intentionally a method rather than exposing the slice to callback
// closures: the active-worker test observes the same fixture concurrently.
func (f *interruptedRestoreRecoveryFixture) record(event string) {
	f.eventsMu.Lock()
	defer f.eventsMu.Unlock()
	f.events = append(f.events, event)
}

func (f *interruptedRestoreRecoveryFixture) snapshotEvents() []string {
	f.eventsMu.Lock()
	defer f.eventsMu.Unlock()
	return slices.Clone(f.events)
}

func assertInterruptedRestoreSettled(t *testing.T, f *interruptedRestoreRecoveryFixture) {
	t.Helper()

	record, err := f.retentions.Get(f.spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusActive, record.Status)
	assert.Equal(t, f.source.Generation+1, record.Generation)
	assert.Empty(t, record.NewLeaseUUID)

	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := f.callbacks.ListPending()
	require.NoError(t, err)
	var destination *shared.CallbackEntry
	for i := range pending {
		if pending[i].LeaseUUID == f.spec.LeaseUUID {
			destination = &pending[i]
			break
		}
	}
	require.NotNil(t, destination)
	assert.Equal(t, backend.CallbackStatusFailed, destination.Status)
	assert.Equal(t, interruptedOperationFailure, destination.Error)

	active, err := f.releases.LatestActive(f.spec.LeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, active, "rollback must not publish destination ownership")
	for _, allocationID := range f.allocation {
		assert.Nil(t, f.b.pool.GetAllocation(allocationID))
	}
	f.b.provisionsMu.RLock()
	_, exists := f.b.provisions[f.spec.LeaseUUID]
	f.b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func assertInterruptedRestoreRolledBack(t *testing.T, f *interruptedRestoreRecoveryFixture) {
	t.Helper()
	assertInterruptedRestoreSettled(t, f)

	wantEvents := make([]string, 0, len(f.containers)+3)
	for _, container := range f.containers {
		wantEvents = append(wantEvents, "remove:"+container.ContainerID)
	}
	wantEvents = append(wantEvents,
		"re-quarantine",
		"measure-source-quota",
		"restore-source-quota",
	)
	assert.Equal(t, wantEvents, f.snapshotEvents(), "rollback must remove each captured target before quarantine and quota handback")
}

func TestInterruptedRestoreRecovery_NonterminalExactCohortRollsBack(t *testing.T) {
	for _, runtimeStatus := range []string{"created", "paused", "restarting", "health-starting"} {
		t.Run(runtimeStatus, func(t *testing.T) {
			f := newInterruptedRestoreRecoveryFixture(
				t, 1, []string{runtimeStatus}, backend.ProvisionStatusFailed, true,
			)
			// These are deliberately stale interrupted generations. Expire the
			// configured recovery visibility window without changing production
			// timing or sleeping in the test.
			f.b.cfg.ProvisionTimeout = time.Nanosecond

			require.NoError(t, f.b.recoverOperationIntents(context.Background()),
				"typed exact nonterminal evidence must converge before the restore finalizer")
			require.NoError(t, f.b.reconcileRestoringWithAuthority(context.Background(), *f.source))
			assertInterruptedRestoreRolledBack(t, f)
		})
	}
}

func TestInterruptedRestoreRecovery_IncompleteOrFailedCohortRollsBack(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
	}{
		{name: "partial exact cohort", statuses: []string{"running"}},
		{name: "mixed ready and failed cohort", statuses: []string{"running", "exited"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newInterruptedRestoreRecoveryFixture(
				t, 2, test.statuses, backend.ProvisionStatusFailed, true,
			)

			classification, err := f.b.classifyOperationIntentSubstrate(
				context.Background(), f.claim, f.containers,
			)
			require.NoError(t, err)
			assert.Equal(t, backend.CallbackStatusFailed, classification.status)
			require.NoError(t, f.b.recoverOperationIntents(context.Background()))
			require.NoError(t, f.b.reconcileRestoringWithAuthority(context.Background(), *f.source))
			assertInterruptedRestoreRolledBack(t, f)
		})
	}
}

func TestInterruptedRestoreRecovery_HandbackCapturesLateExactTarget(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 1, []string{"exited"}, backend.ProvisionStatusFailed, true,
	)
	require.NoError(t, f.b.recoverOperationIntents(t.Context()))
	require.Equal(t, []string{"remove:restore-container-0", "re-quarantine"}, f.snapshotEvents())
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Empty(t, intents, "the late target arrives after durable operation settlement")
	source, err := f.retentions.Get(f.source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, source)
	require.Equal(t, shared.RetentionStatusRestoring, source.Status,
		"operation cleanup renamed the volume, but source quota and accounting handback remain pending")

	// An accepted Docker create can appear after operation settlement but before
	// retention handback. The permanent exact receipt still owns that target.
	late := f.containers[0]
	late.ContainerID = "restore-container-late"
	f.addContainer(late)

	require.NoError(t, f.b.reconcileRestoringWithAuthority(t.Context(), *f.source))
	assertInterruptedRestoreSettled(t, f)
	assert.Equal(t, []string{
		"remove:restore-container-0",
		"re-quarantine",
		"remove:restore-container-late",
		"measure-source-quota",
		"restore-source-quota",
	}, f.snapshotEvents(), "the late exact target must be captured and removed before source quota and accounting handback")
}

func TestInterruptedRestoreRecovery_HandbackPreservesForeignCallbackGeneration(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 1, []string{"exited"}, backend.ProvisionStatusFailed, true,
	)
	require.NoError(t, f.b.recoverOperationIntents(t.Context()))
	before := f.snapshotEvents()

	foreign := f.containers[0]
	foreign.ContainerID = "foreign-restore-generation"
	foreign.CallbackURL = "https://fred.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c9"
	var err error
	foreign.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(foreign.CallbackURL, "")
	require.NoError(t, err)
	f.addContainer(foreign)

	err = f.b.reconcileRestoringWithAuthority(t.Context(), *f.source)
	require.ErrorContains(t, err, "still prevents source handback")
	assert.Equal(t, before, f.snapshotEvents(), "foreign ownership must prevent both deletion and source volume handback")
	inventory, err := f.b.docker.ListManagedContainers(t.Context())
	require.NoError(t, err)
	require.Len(t, inventory, 1)
	assert.Equal(t, foreign.ContainerID, inventory[0].ContainerID)
	source, err := f.retentions.Get(f.source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, source)
	assert.Equal(t, shared.RetentionStatusRestoring, source.Status)
	for _, allocationID := range f.allocation {
		assert.NotNil(t, f.b.pool.GetAllocation(allocationID), "uncertain destination bytes retain their allocation")
	}
}

func TestInterruptedRestoreRecovery_CommittedReleaseNeverRollsBack(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 1, []string{"running"}, backend.ProvisionStatusReady, true,
	)
	acquired, err := f.b.recoveryCoordinator.WithLease(
		context.Background(), f.claim.LeaseUUID(),
		func(scope shared.LeaseRecoveryScope) error {
			outcome, recoverErr := f.b.operationSettlement.RecoverOperationExecution(
				context.Background(), scope, f.claim,
			)
			if recoverErr != nil {
				return recoverErr
			}
			success, ok := outcome.(shared.OperationExecutionSuccess)
			if !ok {
				return fmt.Errorf("recovered operation outcome = %T, want success", outcome)
			}
			_, commitErr := f.b.operationSettlement.CommitOperationSuccess(success)
			return commitErr
		},
	)
	require.NoError(t, err)
	require.True(t, acquired)
	// Simulate the crash/rebuild window in which the exact Release is durable
	// but the volatile projection has not yet been promoted to Ready. The Release
	// must remain the irreversible boundary and prevent rollback.
	f.b.provisionsMu.Lock()
	f.b.provisions[f.spec.LeaseUUID].Status = backend.ProvisionStatusFailed
	f.b.provisionsMu.Unlock()

	require.NoError(t, f.b.recoverOperationIntents(context.Background()))
	require.NoError(t, f.b.reconcileRestoringWithAuthority(context.Background(), *f.source))
	assert.Empty(t, f.snapshotEvents(), "an exact Release is an irreversible success boundary")

	record, err := f.retentions.Get(f.spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusRestoring, record.Status,
		"a non-Ready projection keeps the source finalizer as reconstruction authority")
	assert.Equal(t, f.source.Generation, record.Generation)
	for _, allocationID := range f.allocation {
		assert.NotNil(t, f.b.pool.GetAllocation(allocationID))
	}
	f.b.provisionsMu.RLock()
	_, exists := f.b.provisions[f.spec.LeaseUUID]
	f.b.provisionsMu.RUnlock()
	assert.True(t, exists)
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := f.callbacks.ListPending()
	require.NoError(t, err)
	var destination *shared.CallbackEntry
	for i := range pending {
		if pending[i].LeaseUUID == f.spec.LeaseUUID {
			destination = &pending[i]
			break
		}
	}
	require.NotNil(t, destination)
	assert.Equal(t, backend.CallbackStatusSuccess, destination.Status)
}

func TestInterruptedRestoreRecovery_FailedSettlementSurvivesSourceHandbackError(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 1, []string{"paused"}, backend.ProvisionStatusReady, true,
	)
	f.b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, f.b.recoverOperationIntents(context.Background()),
		"operation recovery must seal exact failure before retention rollback")

	closed := false
	f.b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error {
			f.record("re-quarantine")
			return nil
		},
		UsageFn: func(context.Context, string) (int64, error) {
			f.record("measure-source-quota")
			return 0, nil
		},
		EnsureQuotaFn: func(context.Context, string, int64) error {
			f.record("restore-source-quota")
			if !closed {
				closed = true
				return f.retentions.Close()
			}
			return nil
		},
	}
	err := f.b.reconcileRestoringWithAuthority(context.Background(), *f.source)
	require.ErrorContains(t, err, "finalizer")
	intents, listErr := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "the first pass deliberately crosses failed settlement")
	for _, allocationID := range f.allocation {
		assert.NotNil(t, f.b.pool.GetAllocation(allocationID),
			"failed handback must retain the live reservation")
	}

	reopened, reopenErr := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: f.retentionPath},
		f.b.storageAuthority,
		f.b.storeAuthorityGate,
	)
	require.NoError(t, reopenErr)
	t.Cleanup(func() { _ = reopened.Close() })
	f.retentions = reopened
	f.b.retentionStore = reopened
	operations, ok := concreteOperationSettlementForTest(f.b.operationSettlement)
	require.True(t, ok)
	restoreSettlement, rebuildErr := shared.NewRestoreSettlement(operations, reopened)
	require.NoError(t, rebuildErr)
	f.b.restoreSettlement = restoreSettlement
	closeSettlement, rebuildErr := shared.NewCloseSettlement(
		f.callbacks, f.releases, reopened,
	)
	require.NoError(t, rebuildErr)
	f.b.closeSettlement = closeSettlement
	bindBackendTestCloseExecutor(t, f.b, closeSettlement)
	retentionFixtureAuthorities.Store(reopened, &retentionFixtureAuthority{
		backend: f.b, callbacks: f.callbacks, releases: f.releases, operations: operations,
		restore: restoreSettlement, close: closeSettlement,
	})
	f.b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error { return nil },
		UsageFn:        func(context.Context, string) (int64, error) { return 0, nil },
	}
	stillRestoring, readErr := reopened.Get(f.spec.SourceLeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, stillRestoring)
	require.Equal(t, shared.RetentionStatusRestoring, stillRestoring.Status)

	require.NoError(t, f.b.reconcileRestoringWithAuthority(context.Background(), *stillRestoring))
	record, readErr := reopened.Get(f.spec.SourceLeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusActive, record.Status)
	assert.Empty(t, record.NewLeaseUUID)
	active, readErr := f.releases.LatestActive(f.spec.LeaseUUID)
	require.NoError(t, readErr)
	assert.Nil(t, active, "stale Ready projection must not reverse a settled rollback")
	for _, allocationID := range f.allocation {
		assert.Nil(t, f.b.pool.GetAllocation(allocationID))
	}
}
