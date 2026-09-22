package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestLaunchPreparationFailureRemainsOwnedByOperationRecovery(t *testing.T) {
	for _, kind := range []string{"provision", "restore"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var inspected atomic.Bool
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
					inspected.Store(true)
					return nil, errors.New("unresolved foreign writer inventory")
				})
				f.start(t, kind)
				synctest.Wait()
				require.True(t, inspected.Load())
				intents, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, intents, 1, "the Started operation remains the sole cleanup owner after volume preparation")
				require.Equal(t, shared.OperationExecutionStarted, intents[0].ExecutionPhase())
				debt, err := f.b.volumeLaunches.pendingCount()
				require.NoError(t, err)
				require.Zero(t, debt, "writer inventory failed before Docker Create/Start admission")
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Empty(t, callbacks, "a preparation error after volume mutation is not pre-effect refusal authority")

				// Drive the production recovery lane after its configured visibility
				// horizon. Virtual time preserves the real policy without waiting or
				// shortening production timeouts to manufacture settlement.
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				intents, err = f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, intents, "typed recovery must settle the exact operation without a backend restart")
				callbacks, err = f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Len(t, callbacks, 1)
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
				if kind == "restore" {
					source, err := f.b.retentionStore.Get(f.source)
					require.NoError(t, err)
					require.NotNil(t, source)
					require.Equal(t, shared.RetentionStatusActive, source.Status,
						"the same scheduled state/operation pass must hand the failed restore back without waiting for hourly reaping")
					destinationOwner, err := f.b.retentionStore.RestoringSourceByDestination(f.target)
					require.NoError(t, err)
					require.Nil(t, destinationOwner)
					require.Zero(t, f.b.pool.Stats().AllocationCount)
					require.EqualValues(t, 2048, f.b.pool.Stats().RetainedDiskMB)
					for _, name := range source.RetainedVolumeNames {
						require.FileExists(t, filepath.Join(f.root, name, "sentinel"))
					}
				}
			})
		})
	}
}

func TestPublicLaunchSurvivesConcurrentUnrelatedVolumeRemoval(t *testing.T) {
	for _, kind := range []string{"provision", "restore"} {
		t.Run(kind, func(t *testing.T) {
			closing := newMaintenanceRecoveryHarness(t)
			closing.appendTarget(true)
			closing.inventory.containers = closing.containersFor(closing.source, 2, "running", "")
			writerID := closing.inventory.containers[0].ContainerID
			seedMaintenanceCloseProjection(t, closing, shared.MaintenanceIntentRestart)
			bindBackendTestCloseExecutor(t, closing.b, closing.b.closeSettlement)
			listed, removed := make(chan struct{}), make(chan struct{})
			finishRemoval := sync.OnceFunc(func() { close(removed) })
			t.Cleanup(finishRemoval)
			var containerInspections atomic.Int32
			cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/containers/json"):
					body, err := json.Marshal([]map[string]any{{
						"Id": writerID, "State": "running", "Mounts": []map[string]any{{
							"Type": "volume", "Name": "closing-peer-volume", "Driver": "local", "RW": true,
						}},
					}})
					require.NoError(t, err)
					close(listed)
					return imageSecurityResponse(http.StatusOK, string(body)), nil
				case strings.HasSuffix(req.URL.Path, "/volumes/closing-peer-volume"):
					select {
					case <-removed:
						return imageSecurityResponse(http.StatusNotFound, `{"message":"no such volume"}`), nil
					case <-req.Context().Done():
						return nil, req.Context().Err()
					}
				case strings.HasSuffix(req.URL.Path, "/containers/"+writerID+"/json"):
					containerInspections.Add(1)
					containers, err := closing.inventory.list(req.Context())
					require.NoError(t, err)
					require.False(t, slices.ContainsFunc(containers, func(info ContainerInfo) bool { return info.ContainerID == writerID }),
						"the observed absence must follow the other lease's actual public close")
					return imageSecurityResponse(http.StatusNotFound, `{"message":"no such container"}`), nil
				default:
					return nil, fmt.Errorf("unexpected writer inventory request: %s", req.URL.Path)
				}
			})
			f := newVolumeWriterLaunchFixture(t, cli.ListVolumeWriters)
			f.start(t, kind)
			waitForTestSignal(t, listed, "launch's fleet writer snapshot")
			require.NoError(t, closing.b.Deprovision(t.Context(), closing.leaseUUID))
			finishRemoval()
			require.Eventually(t, func() bool {
				f.b.provisionsMu.RLock()
				p := f.b.provisions[f.target]
				ready := p != nil && p.Status == backend.ProvisionStatusReady
				f.b.provisionsMu.RUnlock()
				intents, err := f.b.operationSettlement.ListOperationIntents()
				return ready && err == nil && len(intents) == 0
			}, 5*time.Second, 10*time.Millisecond, "an unrelated completed close must not strand a fresh or restored operation")
			require.EqualValues(t, 1, containerInspections.Load())
			intents, err := f.b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Empty(t, intents)
			debt, err := f.b.volumeLaunches.pendingCount()
			require.NoError(t, err)
			require.Zero(t, debt)
			active, err := f.b.releaseStore.LatestActive(f.target)
			require.NoError(t, err)
			require.NotNil(t, active)
			require.Len(t, active.Items, 2)
			if kind == "restore" {
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.Nil(t, source, "successful destination authority consumes its source finalizer")
				for _, item := range f.items {
					bytes, err := os.ReadFile(filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0), "sentinel"))
					require.NoError(t, err)
					require.Equal(t, item.ServiceName, string(bytes))
				}
			}
			require.NoError(t, f.b.Deprovision(t.Context(), f.target), "committed restore ownership must allow the destination's subsequent close")
			pending, err := f.b.callbackStore.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 2)
			require.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
			require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
			require.Less(t, pending[0].Sequence, pending[1].Sequence)
		})
	}
}

type volumeWriterLaunchFixture struct {
	b      *Backend
	root   string
	source string
	target string
	items  []backend.LeaseItem
	stack  *manifest.StackManifest
}

func newVolumeWriterLaunchFixture(t *testing.T, inventory func(context.Context) ([]ContainerInfo, error)) *volumeWriterLaunchFixture {
	t.Helper()
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		ListVolumeWritersFn: inventory,
	}
	b := newBackendForProvisionTest(t, mock, nil)
	f := &volumeWriterLaunchFixture{
		b: b, root: t.TempDir(), source: "11111111-1111-4111-8111-111111111171", target: "22222222-2222-4222-8222-222222222172",
		items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web"}, {SKU: "docker-small", Quantity: 1, ServiceName: "db"}},
		stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:latest"}, "db": {Image: "redis:7"}}},
	}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.VolumeDataPath = f.root
	b.cfg.SKUProfiles["docker-small"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}
	var composeMu sync.Mutex
	var downProjects []string
	b.compose = happyComposeMock(t, mock, &composeMu, &downProjects, nil)
	b.volumes = &mockVolumeManager{
		defaultDir: f.root,
		CreateFn: func(_ context.Context, name string, _ int64) (string, bool, error) {
			path := filepath.Join(f.root, name)
			_, statErr := os.Stat(path)
			return path, os.IsNotExist(statErr), os.MkdirAll(path, 0o700)
		},
		ListFn: func() ([]string, error) {
			entries, err := os.ReadDir(f.root)
			var names []string
			for _, entry := range entries {
				names = append(names, entry.Name())
			}
			return names, err
		},
		DestroyFn: func(_ context.Context, name string) error { return os.RemoveAll(filepath.Join(f.root, name)) },
		UsageFn: func(_ context.Context, name string) (int64, error) {
			data, err := os.ReadFile(filepath.Join(f.root, name, "sentinel"))
			return int64(len(data)), err
		},
		RenameVolumeFn: func(from, to string) error {
			oldName, err := parseManagedVolumeName(from)
			if err != nil {
				return err
			}
			newName, err := parseManagedVolumeName(to)
			if err != nil {
				return err
			}
			// Match the real managers' idempotent no-replace namespace
			// operation, including replay after operation cleanup moved it.
			return renameAtStorageRoot(t.Context(), f.root, oldName, newName)
		},
	}
	attachRetentionStore(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
	return f
}

func (f *volumeWriterLaunchFixture) start(t *testing.T, kind string) {
	t.Helper()
	_, callback, lifecycleCallback := newTestRestoreCallbackAuthority(t)
	if kind == "provision" {
		payload, err := json.Marshal(f.stack)
		require.NoError(t, err)
		require.NoError(t, f.b.Provision(t.Context(), backend.ProvisionRequest{
			LeaseUUID: f.target, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Items: f.items, Payload: payload, CallbackURL: callback, LifecycleCallbackURL: lifecycleCallback,
		}))
		return
	}
	profiles, err := f.b.snapshotResourceProfiles(f.items, map[string]SKUProfile{"docker-small": f.b.cfg.SKUProfiles["docker-small"]})
	require.NoError(t, err)
	var names []string
	for _, item := range f.items {
		name := retainedName(canonicalVolumeName(f.source, item.ServiceName, 0))
		names = append(names, name)
		require.NoError(t, os.MkdirAll(filepath.Join(f.root, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(f.root, name, "sentinel"), []byte(item.ServiceName), 0o600))
	}
	require.NoError(t, putRetentionForTest(t, f.b.retentionStore, shared.RetentionEntry{
		OriginalLeaseUUID: f.source, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: f.items, ResourceProfiles: profiles, StackManifest: f.stack, RetainedVolumeNames: names,
		Status: shared.RetentionStatusActive, Generation: 1, CreatedAt: time.Now(),
	}))
	require.NoError(t, f.b.Restore(t.Context(), backend.RestoreRequest{
		LeaseUUID: f.target, FromLeaseUUID: f.source, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: f.items, CallbackURL: callback, LifecycleCallbackURL: lifecycleCallback,
	}))
}
