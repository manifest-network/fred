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
	started := time.Now()
	_, err := newBackendWithConstructionTimeout(
		statelessConstructionTestConfig(),
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

func TestStart_OverallRecoveryBudgetCancelsStalledInventory(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
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
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
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

func TestStart_PhaseBudgetCapsRestorePreflightInventory(t *testing.T) {
	var inventoryCalls atomic.Int32
	phaseEntered := make(chan struct{})
	var phaseOnce sync.Once
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			if inventoryCalls.Add(1) == 1 {
				return nil, nil // recoverState inventory
			}
			phaseOnce.Do(func() { close(phaseEntered) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.recoveryDockerReadTimeout = time.Second
	b.startupRecoveryTimeout = time.Second
	b.startupPhaseTimeout = 25 * time.Millisecond
	b.cfg.ContainerStopTimeout = time.Millisecond
	t.Cleanup(b.stopCancel)

	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 1
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)

	started := time.Now()
	err = b.Start(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "preflight interrupted operations")
	assert.Less(t, time.Since(started), time.Second,
		"a stalled fleet preflight must share one finite phase budget")
	select {
	case <-phaseEntered:
	default:
		t.Fatal("startup did not reach the deliberately stalled preflight inventory")
	}
}
