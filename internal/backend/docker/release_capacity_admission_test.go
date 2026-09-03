package docker

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type refusingReleaseHistoryCapacityPlanner struct {
	appendCalls    int
	migrationCalls int
}

func (*refusingReleaseHistoryCapacityPlanner) capacityError() error {
	return &shared.ReleaseHistoryCapacityError{LimitBytes: 64, RequiredBytes: 65}
}

func (p *refusingReleaseHistoryCapacityPlanner) CheckAppendActiveCapacity(
	string,
	shared.Release,
) error {
	p.appendCalls++
	return p.capacityError()
}

func (p *refusingReleaseHistoryCapacityPlanner) CheckRecordLegacyMigrationCapacity(
	string,
	[]byte,
	[]backend.LeaseItem,
	[]shared.SKUResourceSnapshot,
	shared.LegacyRuntimeAuthority,
	time.Time,
) error {
	p.migrationCalls++
	return p.capacityError()
}

func attachCapacityAdmissionCallbackStore(t *testing.T, b *Backend) *shared.CallbackStore {
	t.Helper()
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "capacity_callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store
	return store
}

func assertCapacityRefusalSettled(
	t *testing.T,
	store *shared.CallbackStore,
) {
	t.Helper()
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "a pre-mutation capacity refusal must consume its intent")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, refusedOperationFailure, pending[0].Error)
}

func TestProvisionReleaseCapacityRefusalPrecedesSubstrateMutation(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	callbacks := attachCapacityAdmissionCallbackStore(t, b)
	planner := &refusingReleaseHistoryCapacityPlanner{}
	b.releaseCapacityPlanner = planner

	var composeCalls, volumeCalls int
	b.compose = &mockComposeExecutor{UpFn: func(
		context.Context,
		*composetypes.Project,
		composeUpOpts,
	) error {
		composeCalls++
		return nil
	}}
	b.volumes = &mockVolumeManager{CreateFn: func(
		context.Context,
		string,
		int64,
	) (string, bool, error) {
		volumeCalls++
		return "", false, nil
	}}

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	err := b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                spec.Items,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Payload:              spec.Manifest,
	})
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	require.ErrorIs(t, err, shared.ErrReleaseHistoryCapacity)
	assert.Equal(t, 1, planner.appendCalls)
	assert.Zero(t, composeCalls)
	assert.Zero(t, volumeCalls)
	b.provisionsMu.RLock()
	_, reserved := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, reserved)
	assertCapacityRefusalSettled(t, callbacks)
}

func TestRestoreReleaseCapacityRefusalPrecedesSubstrateMutation(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	retentions := attachRetentionStore(t, b)
	callbacks := b.callbackStore
	require.NotNil(t, callbacks)
	planner := &refusingReleaseHistoryCapacityPlanner{}
	b.releaseCapacityPlanner = planner

	var composeCalls, volumeCalls int
	b.compose = &mockComposeExecutor{UpFn: func(
		context.Context,
		*composetypes.Project,
		composeUpOpts,
	) error {
		composeCalls++
		return nil
	}}
	b.volumes = &mockVolumeManager{RenameVolumeFn: func(string, string) error {
		volumeCalls++
		return nil
	}}

	const (
		sourceLease      = "11111111-1111-4111-8111-111111111111"
		destinationLease = "22222222-2222-4222-8222-222222222222"
	)
	wantSource := seedActiveRetained(t, retentions, sourceLease)
	err := b.Restore(context.Background(), restoreRequest(
		destinationLease,
		sourceLease,
		"https://fred.example/callbacks/provision",
	))
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	require.ErrorIs(t, err, shared.ErrReleaseHistoryCapacity)
	assert.Equal(t, 1, planner.appendCalls)
	assert.Zero(t, composeCalls)
	assert.Zero(t, volumeCalls)
	gotSource, getErr := retentions.Get(sourceLease)
	require.NoError(t, getErr)
	require.NotNil(t, gotSource)
	assert.Equal(t, wantSource.Generation, gotSource.Generation)
	assert.Equal(t, shared.RetentionStatusActive, gotSource.Status)
	assert.Empty(t, gotSource.NewLeaseUUID)
	b.provisionsMu.RLock()
	_, reserved := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, reserved)
	assertCapacityRefusalSettled(t, callbacks)
}

func TestLegacyMigrationReleaseCapacityRefusalPrecedesSubstrateMutation(t *testing.T) {
	b, dockerState, volumes, _ := newMigrationTestBackend(t)
	planner := &refusingReleaseHistoryCapacityPlanner{}
	b.releaseCapacityPlanner = planner
	stopCalls := 0
	dockerState.stopContainer = func(context.Context, string, time.Duration) error {
		stopCalls++
		return nil
	}

	migration := &legacyMigration{
		LeaseUUID:    "lease-capacity-migration",
		Tenant:       "tenant-a",
		ProviderUUID: nominalDockerProviderUUID,
		SKU:          "docker-micro",
		Stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			manifest.DefaultServiceName: {Image: "docker.io/library/nginx:1.27"},
		}},
		Instances: []legacyMigrationInstance{{
			LegacyContainer: ContainerInfo{
				ContainerID:   "legacy-container",
				InstanceIndex: 0,
				CallbackURL:   "https://fred.example/callbacks/provision",
			},
			NewContainerName: "fred-lease-capacity-migration-app-0",
			PrevName:         "fred-lease-capacity-migration-app-0-prev",
		}},
	}

	err := b.executeLegacyMigration(context.Background(), migration, slog.Default())
	require.ErrorIs(t, err, shared.ErrReleaseHistoryCapacity)
	assert.Equal(t, 1, planner.migrationCalls)
	assert.Zero(t, stopCalls)
	assert.Empty(t, volumes.renames)
	assert.Nil(t, dockerState.lastComposeProject)
}
