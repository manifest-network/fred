package docker

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type stalledConstructionIdentityResolver struct{}

func (stalledConstructionIdentityResolver) resolve(
	ctx context.Context,
	_ Config,
	_ dockerClient,
	_ volumeManager,
) (backendidentity.VerifiedStorage, error) {
	<-ctx.Done()
	return backendidentity.VerifiedStorage{}, ctx.Err()
}

func statelessConstructionTestConfig() Config {
	cfg := validConfig()
	cfg.SKUProfiles = map[string]SKUProfile{
		"small": {CPUCores: 0.5, MemoryMB: 512},
	}
	cfg.VolumeDataPath = ""
	cfg.VolumeMountPath = ""
	return cfg
}

func TestNew_DefaultConstructionPathCancelsStalledIdentityRead(t *testing.T) {
	const timeout = 20 * time.Millisecond
	cfg := statelessConstructionTestConfig()
	cfg.DockerHost = newStorageIdentityDockerServer(t, nil).URL
	started := time.Now()
	_, err := newBackendWithConstructionTimeout(
		cfg,
		slog.Default(),
		timeout,
		stalledConstructionIdentityResolver{},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second,
		"New's shared construction path must not leave a stalled identity read unbounded")
	assert.Positive(t, defaultBackendConstructionTimeout,
		"the exported New constructor must always install a finite default")
}

func TestStartupPhaseBudget_HonorsConfiguredContainerStopGrace(t *testing.T) {
	b := &Backend{
		cfg:                 Config{ContainerStopTimeout: 5 * time.Minute},
		startupPhaseTimeout: 2 * time.Minute,
	}
	assert.Equal(t, 5*time.Minute, b.startupPhaseBudget())

	b.cfg.ContainerStopTimeout = 30 * time.Second
	assert.Equal(t, 2*time.Minute, b.startupPhaseBudget(),
		"ordinary stop grace must not inflate the configured aggregate phase budget")

	b.cfg.ContainerStopTimeout = 0
	b.startupPhaseTimeout = 0
	assert.Equal(t, defaultStartupPhaseTimeout, b.startupPhaseBudget(),
		"zero values must select both production defaults")
}

func TestStorageAttestationBudgetIsConfigurableAndBounded(t *testing.T) {
	for _, test := range []struct {
		name         string
		configured   time.Duration
		construction time.Duration
		proof        time.Duration
	}{
		{name: "default", construction: 30 * time.Second, proof: 2 * time.Minute},
		{name: "large fleet", configured: 8 * time.Minute, construction: 8 * time.Minute, proof: 8 * time.Minute},
		{name: "short construction", configured: time.Second, construction: time.Second, proof: 2 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.StorageAttestationTimeout = test.configured
			require.NoError(t, cfg.Validate())
			b := &Backend{cfg: cfg}
			assert.Equal(t, test.construction, cfg.storageAttestationBudget())
			assert.Equal(t, test.proof, b.startupVolumeProofBudget())
			assert.Equal(t, defaultRecoveryDockerReadTimeout, b.recoveryDockerReadBudget(),
				"fleet size must not inflate an individual Docker call")
			baseline := &Backend{cfg: cfg}
			baseline.cfg.StorageAttestationTimeout = 0
			assert.Equal(t, baseline.startupRecoveryBudget()+test.proof-2*time.Minute, b.startupRecoveryBudget())
		})
	}
	cfg := validConfig()
	cfg.StorageAttestationTimeout = -time.Nanosecond
	require.ErrorContains(t, cfg.Validate(), "storage_attestation_timeout")
}

func TestStartupVolumeMutationContextHasFixedFilesystemOnlyCap(t *testing.T) {
	started := time.Now()
	ctx, cancel := startupVolumeMutationContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, started.Add(defaultStartupVolumeMutationTimeout), deadline, time.Second)
}

func TestOperationRecoveryPhaseBudgetUsesConfiguredDockerReadTimeout(t *testing.T) {
	const dockerReadTimeout = 3 * time.Minute
	assert.Equal(t,
		13*time.Minute+volumeCleanupTimeout,
		operationRecoveryPhaseBudget(10*time.Minute, dockerReadTimeout),
	)
}

func TestOperationRecoveryPhaseBudgetSaturates(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	assert.Equal(t, maxDuration,
		operationRecoveryPhaseBudget(maxDuration, defaultRecoveryDockerReadTimeout))
	assert.Equal(t, maxDuration, saturatingDurationSum(maxDuration-1, 2))
}

func TestStartupRecoveryBudgetDefaultIsExactSequentialPhaseSum(t *testing.T) {
	b := &Backend{}
	assert.Equal(t, 51*time.Minute+10*time.Second, b.startupRecoveryBudget(),
		"the aggregate must equal the sum of the two volume phases, state recovery, three ordinary phases, operation recovery, and final identity read")
}

func TestStartupRecoveryBudgetIncludesConfiguredDockerReadTimeout(t *testing.T) {
	b := &Backend{recoveryDockerReadTimeout: 3 * time.Minute}
	assert.Equal(t, 56*time.Minute+10*time.Second, b.startupRecoveryBudget(),
		"the configured read budget applies once inside operation recovery and once to the final identity proof")
}

func TestStartupRecoveryBudgetSaturates(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	b := &Backend{cfg: Config{ProvisionTimeout: maxDuration}}
	assert.Equal(t, maxDuration, b.startupRecoveryBudget())
}

func TestStartupRecoveryBudgetHonorsExplicitOverride(t *testing.T) {
	b := &Backend{
		cfg:                    Config{ProvisionTimeout: 24 * time.Hour},
		startupRecoveryTimeout: 25 * time.Millisecond,
	}
	assert.Equal(t, 25*time.Millisecond, b.startupRecoveryBudget(),
		"an explicit whole-startup override must remain authoritative")
}

func TestStartupStateRecoveryKeepsIndependentFleetBound(t *testing.T) {
	b := &Backend{startupRecoveryTimeout: 24 * time.Hour}
	parent, cancelParent := context.WithTimeout(context.Background(), b.startupRecoveryBudget())
	defer cancelParent()
	started := time.Now()
	ctx, cancel := b.startupStateRecoveryContext(parent)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, started.Add(defaultStartupRecoveryTimeout), deadline, time.Second,
		"a longer aggregate override must not enlarge ordinary fleet recovery")
}

func TestStartupRecoveryBudgetLeavesFullOperationWindowAfterPreOperationPhases(t *testing.T) {
	b := &Backend{
		cfg:                       Config{ProvisionTimeout: 45 * time.Minute},
		recoveryDockerReadTimeout: 3 * time.Minute,
	}
	preOperationBudget := saturatingDurationSum(
		defaultStartupVolumeMutationTimeout,
		defaultStartupVolumeMutationTimeout,
		defaultStartupRecoveryTimeout,
	)
	operationBudget := b.startupOperationRecoveryBudget()

	// Move the aggregate start into the past instead of sleeping through the
	// preceding phases. The resulting parent deadline models every earlier
	// phase consuming its full local allowance.
	aggregateDeadline := time.Now().Add(b.startupRecoveryBudget() - preOperationBudget)
	parent, cancelParent := context.WithDeadline(context.Background(), aggregateDeadline)
	defer cancelParent()
	started := time.Now()
	ctx, cancel := b.startupOperationRecoveryContext(parent)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, started.Add(operationBudget), deadline, time.Second,
		"earlier phases consuming their complete budgets must not starve operation recovery")
}

func TestStart_OverallRecoveryBudgetIncludesInterruptedVolumeRecovery(t *testing.T) {
	entered := make(chan struct{})
	var observedDeadline time.Time
	b, _ := newInterruptedVolumeStartBackend(t)
	b.startupRecoveryTimeout = 25 * time.Millisecond
	// Neither setting may inflate the filesystem-only recovery budget or let it
	// escape the aggregate startup deadline.
	b.startupPhaseTimeout = time.Hour
	b.cfg.ContainerStopTimeout = time.Hour
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(ctx context.Context) error {
			observedDeadline, _ = ctx.Deadline()
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	started := time.Now()
	err := b.Start(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "recover interrupted managed-volume mutations")
	assert.Less(t, time.Since(started), time.Second)
	select {
	case <-entered:
	default:
		t.Fatal("startup did not reach interrupted-volume recovery")
	}
	assert.False(t, observedDeadline.IsZero())
	assert.Less(t, observedDeadline.Sub(started), time.Second,
		"interrupted-volume recovery must inherit the aggregate startup deadline")
}

func TestStart_StopContextCancelsInterruptedVolumeRecovery(t *testing.T) {
	entered := make(chan struct{})
	b, _ := newInterruptedVolumeStartBackend(t)
	b.startupRecoveryTimeout = time.Minute
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	result := make(chan error, 1)
	go func() { result <- b.Start(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("startup did not reach interrupted-volume recovery")
	}
	b.stopCancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "recover interrupted managed-volume mutations")
	case <-time.After(time.Second):
		t.Fatal("canceling stopCtx did not unblock interrupted-volume recovery")
	}
}

func TestStart_OverallRecoveryBudgetCancelsStalledInventory(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	var inventoryCalls atomic.Int32
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			if inventoryCalls.Add(1) == 1 {
				// The read-only supported-topology preflight deliberately precedes the
				// mutating/convergent startup budget. Stall the following recoverState
				// inventory, which is the phase this test is proving bounded.
				return nil, nil
			}
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.recoveryDockerReadTimeout = time.Second
	b.startupRecoveryTimeout = 25 * time.Millisecond
	t.Cleanup(b.stopCancel)

	started := time.Now()
	err := b.Start(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second,
		"the overall startup deadline must cap a longer per-read deadline")
	select {
	case <-entered:
	default:
		t.Fatal("startup did not reach the deliberately stalled inventory read")
	}
}

func TestStart_StopContextCancelsRecoveryBudget(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	var inventoryCalls atomic.Int32
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			if inventoryCalls.Add(1) == 1 {
				// Let the caller-scoped read-only topology preflight complete, then
				// block inside the stopCtx-derived startup recovery lifetime.
				return nil, nil
			}
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.recoveryDockerReadTimeout = time.Minute
	b.startupRecoveryTimeout = time.Minute

	result := make(chan error, 1)
	go func() { result <- b.Start(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("startup did not reach the deliberately stalled inventory read")
	}
	b.stopCancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceling stopCtx did not unblock startup recovery")
	}
}

func TestStart_OverallBudgetCapsOperationRecoveryInventory(t *testing.T) {
	var inventoryCalls atomic.Int32
	phaseEntered := make(chan struct{})
	var phaseOnce sync.Once
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			if inventoryCalls.Add(1) <= 3 {
				// The supported-topology preflight, recoverState's projection
				// inventory, and its independent strict closed-substrate confirmation
				// all precede operation recovery.
				return nil, nil
			}
			phaseOnce.Do(func() { close(phaseEntered) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.recoveryDockerReadTimeout = time.Second
	b.startupRecoveryTimeout = 100 * time.Millisecond
	b.startupPhaseTimeout = 25 * time.Millisecond
	b.cfg.ContainerStopTimeout = time.Millisecond
	t.Cleanup(b.stopCancel)

	store := b.callbackStore
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
	releaseStore, err := newBoundReleaseStoreForTest(t, shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releaseStore.Close()) })
	b.releaseStore = releaseStore

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	_, err = beginDockerTestOperationIntent(t, store, spec, b.storageIdentity)
	require.NoError(t, err)

	started := time.Now()
	err = b.Start(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "recover interrupted operations")
	assert.Less(t, time.Since(started), time.Second,
		"a stalled operation fleet inventory must inherit the finite startup budget")
	select {
	case <-phaseEntered:
	default:
		t.Fatal("startup did not reach the deliberately stalled operation inventory")
	}
}
