package docker

import (
	"context"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

type refusingReleaseHistoryCapacityPlanner struct {
	appendCalls int
}

type blockingRefusingReleaseHistoryCapacityPlanner struct {
	refusingReleaseHistoryCapacityPlanner
	entered chan struct{}
	release chan struct{}
}

func (p *blockingRefusingReleaseHistoryCapacityPlanner) CheckOperationReleaseCapacity(
	candidate shared.OperationReleaseCandidate,
) error {
	close(p.entered)
	<-p.release
	return p.refusingReleaseHistoryCapacityPlanner.CheckOperationReleaseCapacity(candidate)
}

func (*refusingReleaseHistoryCapacityPlanner) capacityError() error {
	return &shared.ReleaseHistoryCapacityError{LimitBytes: 64, RequiredBytes: 65}
}

func (p *refusingReleaseHistoryCapacityPlanner) CheckOperationReleaseCapacity(
	shared.OperationReleaseCandidate,
) error {
	p.appendCalls++
	return p.capacityError()
}

func attachCapacityAdmissionCallbackStore(t *testing.T, b *Backend) *shared.CallbackStore {
	t.Helper()
	attachBoundOperationHandoffStores(t, b)
	require.NotNil(t, b.callbackStore)
	return b.callbackStore
}

func assertCapacityRefusalSettled(
	t *testing.T,
	store *shared.CallbackStore,
) {
	t.Helper()
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents, "a pre-mutation capacity refusal must leave no pending intent")
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

func TestProvisionCapacityRefusalCannotRaceRecoveryPublication(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}, nil)
	callbacks := attachCapacityAdmissionCallbackStore(t, b)
	planner := &blockingRefusingReleaseHistoryCapacityPlanner{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	b.releaseCapacityPlanner = planner
	spec := dockerOperationIntentSpec(t, b.storageIdentity)

	provisionDone := make(chan error, 1)
	go func() {
		provisionDone <- b.Provision(context.Background(), backend.ProvisionRequest{
			LeaseUUID:            spec.LeaseUUID,
			Tenant:               spec.Tenant,
			ProviderUUID:         spec.ProviderUUID,
			Items:                spec.Items,
			CallbackURL:          spec.CallbackURL,
			LifecycleCallbackURL: spec.LifecycleCallbackURL,
			Payload:              spec.Manifest,
		})
	}()
	select {
	case <-planner.entered:
	case <-time.After(time.Second):
		t.Fatal("provision did not reach the post-intent capacity boundary")
	}

	// The intent is Pending but no projection exists. Recovery must wait for the
	// typed admission-to-projection bridge instead of manufacturing a synthetic
	// Provisioning generation from an intent that can settle synchronously.
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- b.recoverState(context.Background()) }()
	select {
	case err := <-recoveryDone:
		t.Fatalf("recovery crossed an unsettled pre-projection admission: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(planner.release)
	require.ErrorIs(t, <-provisionDone, shared.ErrReleaseHistoryCapacity)
	require.NoError(t, <-recoveryDone)
	b.provisionsMu.RLock()
	_, phantom := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, phantom, "terminal refusal must not reappear as a recovered pending projection")
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
