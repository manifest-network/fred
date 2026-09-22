package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func retainedRestoreRequest(t *testing.T, f *volumeWriterLaunchFixture, services ...string) backend.RestoreRequest {
	t.Helper()
	profiles, err := f.b.snapshotResourceProfiles(f.items, f.b.cfg.SKUProfiles)
	require.NoError(t, err)
	var names []string
	for _, service := range services {
		name := retainedName(canonicalVolumeName(f.source, service, 0))
		names = append(names, name)
		require.NoError(t, os.MkdirAll(filepath.Join(f.root, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(f.root, name, "sentinel"), []byte(service), 0o600))
	}
	require.NoError(t, putRetentionForTest(t, f.b.retentionStore, shared.RetentionEntry{
		OriginalLeaseUUID: f.source, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: f.items, ResourceProfiles: profiles, StackManifest: f.stack, RetainedVolumeNames: names,
		Status: shared.RetentionStatusActive, Generation: 1, CreatedAt: time.Now(),
	}))
	return newFixtureRestoreRequest(t, f)
}

func newFixtureRestoreRequest(t *testing.T, f *volumeWriterLaunchFixture) backend.RestoreRequest {
	t.Helper()
	_, callback, lifecycle := newTestRestoreCallbackAuthority(t)
	return backend.RestoreRequest{
		LeaseUUID: f.target, FromLeaseUUID: f.source, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: f.items, CallbackURL: callback, LifecycleCallbackURL: lifecycle,
	}
}

// The production debt was a diskless service's writable-path scaffold, which
// is absent from the retained source volume list but present in target topology.
func configureDisklessRestoreScratch(t *testing.T, f *volumeWriterLaunchFixture) {
	t.Helper()
	require.Equal(t, "web", f.items[0].ServiceName)
	f.items[0].SKU = "docker-diskless"
	f.b.cfg.SKUProfiles["docker-diskless"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}
	f.b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	webImageID := fixtureImageID("diskless-restore-web")
	mock := f.b.docker.(*mockDockerClient)
	mock.InspectImageFn = func(_ context.Context, image string) (*ImageInfo, error) {
		if strings.Contains(image, "nginx") || image == webImageID {
			return &ImageInfo{ID: webImageID, Volumes: map[string]struct{}{}}, nil
		}
		return &ImageInfo{ID: fixtureImageID("stateful-restore-db"), Volumes: map[string]struct{}{"/data": {}}}, nil
	}
	mock.DetectWritablePathsFn = func(_ context.Context, image string, _ int, _ []string) ([]string, error) {
		if image == webImageID {
			return []string{"/var/lib/app"}, nil
		}
		return nil, nil
	}
	mock.ExtractImageContentFn = func(_ context.Context, _ string, paths []string, dest string, _, _ int64) map[string]error {
		failures := make(map[string]error)
		for _, path := range paths {
			if err := os.MkdirAll(filepath.Join(dest, sanitizeVolumePath(path)), 0o700); err != nil {
				failures[path] = err
			}
		}
		return failures
	}
}

func TestRestoreRecoveryRemovesDestinationCreatedVolumes(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "live"
		if reopen {
			name = "reopened"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var preparationFailed atomic.Bool
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
					if !preparationFailed.Load() {
						return nil, errors.New("writer inventory failed after all restore volumes were materialized")
					}
					return nil, nil
				})
				configureDisklessRestoreScratch(t, f)
				require.NoError(t, f.b.Restore(t.Context(), retainedRestoreRequest(t, f, "db")))
				synctest.Wait()
				targetScratch := filepath.Join(f.root, canonicalVolumeName(f.target, "web", 0))
				require.DirExists(t, targetScratch, "the writable-path-only service creates new target-owned state")
				// Writer inventory runs after root materialization but before
				// content extraction; no _wp subtree exists at this failure point.
				require.NoDirExists(t, filepath.Join(targetScratch, writablePathSubdir))
				before, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, before, 1)
				require.Equal(t, shared.OperationExecutionStarted, before[0].ExecutionPhase())
				web, found := shared.LookupSKUResourceSnapshotRow(before[0].ResourceProfiles(), "docker-diskless")
				require.True(t, found)
				require.Zero(t, web.DiskMB)
				require.Positive(t, web.ScratchDiskMB)
				if reopen {
					f.reopen(t)
				}
				preparationFailed.Store(true)
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, pending, "target scratch must not strand otherwise complete exact restore cleanup")
				require.NoDirExists(t, targetScratch)
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.NotNil(t, source)
				require.Equal(t, shared.RetentionStatusActive, source.Status)
				require.Len(t, source.RetainedVolumeNames, 1)
				data, err := os.ReadFile(filepath.Join(f.root, source.RetainedVolumeNames[0], "sentinel"))
				require.NoError(t, err)
				require.Equal(t, "db", string(data))
				require.Zero(t, f.b.pool.Stats().AllocationCount)
				require.EqualValues(t, 1024, f.b.pool.Stats().RetainedDiskMB,
					"only retained database storage survives; diskless target scratch is released")
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Len(t, callbacks, 1)
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				replayed, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Equal(t, callbacks, replayed)
				require.NoError(t, f.b.Deprovision(t.Context(), f.target), "the failed attempt no longer owns an uncommitted source")
			})
		})
	}
}

func TestRestoreCreatedVolumesRetainUnknownLaunchDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) { return nil, nil })
		f.b.compose.(*mockComposeExecutor).UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
			return errors.New("daemon reply lost after accepting restore launch")
		}
		require.NoError(t, f.b.Restore(t.Context(), retainedRestoreRequest(t, f, "db")))
		synctest.Wait()
		f.reopen(t)
		time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
		_ = f.b.reconcileStateAndOperations(t.Context())
		pending, err := f.b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		debt, err := f.b.volumeLaunches.pendingCount()
		require.NoError(t, err)
		require.Positive(t, debt)
		for _, item := range f.items {
			require.DirExists(t, filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0)))
		}
		source, err := f.b.retentionStore.Get(f.source)
		require.NoError(t, err)
		require.Equal(t, shared.RetentionStatusRestoring, source.Status)
		callbacks, err := f.b.callbackStore.ListPending()
		require.NoError(t, err)
		require.Empty(t, callbacks, "an absence snapshot cannot settle an outstanding launch request")
	})
}

func TestRestoreCreatedVolumeRecoveryPreservesForeignWriters(t *testing.T) {
	for _, obstruction := range []string{"writer", "alias", "unknown inventory", "unexpected target name"} {
		t.Run(obstruction, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var preparationDone atomic.Bool
				var writers []ContainerInfo
				var writerErr error
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
					if !preparationDone.Load() {
						return nil, errors.New("launch preparation failed")
					}
					return writers, writerErr
				})
				require.NoError(t, f.b.Restore(t.Context(), retainedRestoreRequest(t, f, "db")))
				synctest.Wait()
				preparationDone.Store(true)
				scratch := filepath.Join(f.root, canonicalVolumeName(f.target, "web", 0))
				protected := scratch
				switch obstruction {
				case "writer", "alias":
					mount := scratch
					if obstruction == "alias" {
						mount = filepath.Join(t.TempDir(), "alias")
						require.NoError(t, os.Symlink(scratch, mount))
					}
					writers = []ContainerInfo{{ContainerID: "unmanaged-writer", Mounts: []ContainerMount{{Type: "bind", Source: mount}}}}
				case "unknown inventory":
					writerErr = errors.New("daemon inventory unavailable")
				case "unexpected target name":
					protected = filepath.Join(f.root, canonicalVolumeName(f.target, "foreign", 0))
					require.NoError(t, os.Mkdir(protected, 0o700))
				}
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				_ = f.b.reconcileStateAndOperations(t.Context())
				require.DirExists(t, protected)
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, pending, 1)
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Empty(t, callbacks)
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.Equal(t, shared.RetentionStatusRestoring, source.Status)
				require.FileExists(t, filepath.Join(f.root, source.RetainedVolumeNames[0], "sentinel"))
			})
		})
	}
}

// This is the actual record-first source close, stopped at its rename boundary.
func pendingRetainedCloseFixture(t *testing.T, partial bool) (*volumeWriterLaunchFixture, func()) {
	t.Helper()
	f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) { return nil, nil })
	f.b.cfg.RetainOnClose = true
	f.start(t, "provision")
	awaitProvisionWorkerQuiescence(t, f.b, f.target)
	for _, item := range f.items {
		require.NoError(t, os.WriteFile(filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0), "sentinel"), []byte(item.ServiceName), 0o600))
	}
	volumes := f.b.volumes.(*mockVolumeManager)
	rename := volumes.RenameVolumeFn
	var failed atomic.Bool
	volumes.RenameVolumeFn = func(from, to string) error {
		if !failed.Load() && (!partial || from == canonicalVolumeName(f.target, "web", 0)) {
			return errors.New("source close rename interrupted")
		}
		return rename(from, to)
	}
	require.Error(t, f.b.Deprovision(t.Context(), f.target))
	f.source, f.target = f.target, "33333333-3333-4333-8333-333333333183"
	_, found, err := f.b.closeSettlement.GetCloseIntent(f.source)
	require.NoError(t, err)
	require.True(t, found)
	return f, func() { failed.Store(true) }
}

func TestRestoreRejectsPendingRetainedSourceClose(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "all canonical", true: "partial rename"}[partial], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, resumeClose := pendingRetainedCloseFixture(t, partial)
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.Equal(t, shared.RetentionStatusActive, source.Status)
				require.ErrorIs(t, f.b.Restore(t.Context(), newFixtureRestoreRequest(t, f)), backend.ErrInvalidState)
				synctest.Wait()
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, pending, "known pending source close is refused before destination admission")
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				for _, callback := range callbacks {
					require.NotEqual(t, f.target, callback.LeaseUUID, "preflight cannot create a doomed target callback")
				}
				after, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.Equal(t, source, after, "refused source transfer cannot interrupt the close's retry owner")
				for _, item := range f.items {
					require.NoDirExists(t, filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0)))
				}
				resumeClose()
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				_, found, err := f.b.closeSettlement.GetCloseIntent(f.source)
				require.NoError(t, err)
				require.False(t, found)
				f.target = "44444444-4444-4444-8444-444444444184"
				require.NoError(t, f.b.Restore(t.Context(), newFixtureRestoreRequest(t, f)))
				awaitProvisionWorkerQuiescence(t, f.b, f.target)
				p, err := f.b.GetProvision(t.Context(), f.target)
				require.NoError(t, err)
				require.Equal(t, backend.ProvisionStatusReady, p.Status)
			})
		})
	}
}

func TestInterruptedSourceRecoveryPreservesUnprovenBytes(t *testing.T) {
	for _, obstruction := range []string{"source container", "unknown inventory", "foreign writer", "mismatched close", "missing close", "conflicting namespace"} {
		t.Run(obstruction, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _ := pendingRetainedCloseFixture(t, false)
				closeHead := editHistoricalSourceClose(t, f, nil)
				require.NoError(t, f.b.Restore(t.Context(), newFixtureRestoreRequest(t, f)))
				synctest.Wait()
				if obstruction == "mismatched close" {
					// Keep a valid close wire shape while changing its immutable
					// principal: a same-lease name is insufficient source authority.
					var head map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(closeHead, &head))
					changed := bytes.Replace(closeHead, []byte(`"tenant":"tenant-a"`), []byte(`"tenant":"other-tenant"`), 1)
					require.NotEqual(t, closeHead, changed)
					closeHead = changed
				}
				if obstruction != "missing close" {
					editHistoricalSourceClose(t, f, closeHead)
				}
				mock := f.b.docker.(*mockDockerClient)
				sourcePath := filepath.Join(f.root, canonicalVolumeName(f.source, "web", 0))
				switch obstruction {
				case "source container":
					mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
						return []ContainerInfo{{ContainerID: "source-still-visible", LeaseUUID: f.source, Status: "running"}}, nil
					}
				case "unknown inventory":
					mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
						return nil, errors.New("source inventory cannot establish absence")
					}
				case "foreign writer":
					mock.ListVolumeWritersFn = func(context.Context) ([]ContainerInfo, error) {
						return []ContainerInfo{{ContainerID: "foreign-source-writer", Mounts: []ContainerMount{{Type: "bind", Source: sourcePath}}}}, nil
					}
				case "conflicting namespace":
					require.NoError(t, os.Mkdir(filepath.Join(f.root, retainedName(canonicalVolumeName(f.source, "web", 0))), 0o700))
				}
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				// Exercise the normal operation owner in isolation: the separate
				// close owner is not allowed to resolve the deliberately retained
				// source obstruction before this negative assertion.
				var recoveryLog bytes.Buffer
				f.b.logger = slog.New(slog.NewTextHandler(&recoveryLog, nil))
				recoveryErr := f.b.recoverLiveOperationIntents(t.Context())
				if obstruction == "source container" {
					require.Contains(t, recoveryLog.String()+fmt.Sprint(recoveryErr), "still prevents namespace repair",
						"the complete operation pass must reach the source-presence branch, not fail on fixture metadata")
				}
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, pending, 1)
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.Equal(t, shared.RetentionStatusRestoring, source.Status)
				for _, item := range f.items {
					name := canonicalVolumeName(f.source, item.ServiceName, 0)
					data, readErr := os.ReadFile(filepath.Join(f.root, name, "sentinel"))
					require.NoError(t, readErr)
					require.Equal(t, item.ServiceName, string(data))
				}
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				for _, callback := range callbacks {
					require.NotEqual(t, f.target, callback.LeaseUUID)
				}
			})
		})
	}
}

// Preserve a real close head while reproducing the old admission interleaving
// offline. No caller-authored claim is minted: reopened production selectors
// decode the exact bytes issued by public Deprovision. Current admission
// excludes this historical state, so it cannot be constructed through new APIs.
func editHistoricalSourceClose(t *testing.T, f *volumeWriterLaunchFixture, restore []byte) []byte {
	t.Helper()
	f.b.stopCancel()
	f.b.wg.Wait()
	require.NoError(t, f.b.callbackStore.Close())
	db, err := bolt.Open(f.b.cfg.CallbackDBPath, 0o600, nil)
	require.NoError(t, err)
	var old []byte
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		heads := tx.Bucket([]byte("callback_lease_mutation_heads"))
		old = bytes.Clone(heads.Get([]byte(f.source)))
		if restore == nil {
			return heads.Delete([]byte(f.source))
		}
		return heads.Put([]byte(f.source), restore)
	}))
	require.NoError(t, db.Close())
	f.reopen(t)
	return old
}

func TestRestoreRecoveryReturnsInterruptedSourceCanonicalVolumes(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "all canonical", true: "partially adopted"}[partial], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, resumeClose := pendingRetainedCloseFixture(t, partial)
				closeHead := editHistoricalSourceClose(t, f, nil)
				require.NotEmpty(t, closeHead)
				require.NoError(t, f.b.Restore(t.Context(), newFixtureRestoreRequest(t, f)))
				synctest.Wait()
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, pending, 1)
				require.Equal(t, shared.OperationExecutionStarted, pending[0].ExecutionPhase())
				editHistoricalSourceClose(t, f, closeHead)
				resumeClose()
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.NotNil(t, source)
				require.Equal(t, shared.RetentionStatusActive, source.Status)
				pending, err = f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, pending)
				// The next ordinary cadence resumes the exact original source
				// close now that source ownership has returned from the restore.
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				_, found, err := f.b.closeSettlement.GetCloseIntent(f.source)
				require.NoError(t, err)
				require.False(t, found)
				for _, item := range f.items {
					name := retainedName(canonicalVolumeName(f.source, item.ServiceName, 0))
					data, readErr := os.ReadFile(filepath.Join(f.root, name, "sentinel"))
					require.NoError(t, readErr)
					require.Equal(t, item.ServiceName, string(data))
					require.NoDirExists(t, filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0)))
				}
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				var targetFailures int
				for _, callback := range callbacks {
					if callback.LeaseUUID == f.target && callback.Status == backend.CallbackStatusFailed {
						targetFailures++
					}
				}
				require.Equal(t, 1, targetFailures)
			})
		})
	}
}
