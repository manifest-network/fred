//go:build integration

package docker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/testutil"
)

// Real XFS project volumes exercise the guarded rollback's rename, descriptor
// release and deletion boundaries. Docker is mocked only to stop the public
// restore after volume creation, before Compose can own an uncertain launch.
func TestIntegration_Docker_RestoreFailureRemovesCreatedVolume(t *testing.T) {
	mount := testutil.SetupXFSLoopback(t)
	var sourceProjID, scratchProjID uint32
	synctest.Test(t, func(t *testing.T) {
		var preparationComplete atomic.Bool
		f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
			if !preparationComplete.Load() {
				return nil, errors.New("writer inventory unavailable after restore volume creation")
			}
			return nil, nil
		})
		configureDisklessRestoreScratch(t, f)
		mgr, err := newVolumeManager(mount, "xfs", 1024, slog.Default())
		require.NoError(t, err)
		require.NoError(t, mgr.Validate())
		f.root = mount
		f.b.cfg.VolumeDataPath = mount
		f.b.cfg.VolumeMountPath = mount
		f.b.cfg.VolumeFilesystem = "xfs"
		useVolumeManager(f.b, mgr)
		sourceName := retainedName(canonicalVolumeName(f.source, "db", 0))
		sourcePath, created, err := mgr.Create(t.Context(), sourceName, 1024)
		require.NoError(t, err)
		require.True(t, created)
		req := retainedRestoreRequest(t, f, "db")
		writeNonSparse(t, filepath.Join(sourcePath, "data.bin"), 1)
		sourceInode := inodeNumber(t, sourcePath)
		sourceProjID, err = readProjectIDFile(sourcePath)
		require.NoError(t, err)
		require.NotZero(t, sourceProjID)

		require.NoError(t, f.b.Restore(t.Context(), req))
		synctest.Wait()
		scratchName := canonicalVolumeName(f.target, "web", 0)
		scratchPath := mgr.HostPath(scratchName)
		require.DirExists(t, scratchPath)
		// The refusal precedes extraction. Seed real charged extents into the
		// materialized target root to exercise the driver's deletion boundary.
		require.NoDirExists(t, filepath.Join(scratchPath, writablePathSubdir))
		writeNonSparse(t, filepath.Join(scratchPath, "discard.bin"), 2)
		scratchProjID, err = readProjectIDFile(scratchPath)
		require.NoError(t, err)
		require.NotZero(t, scratchProjID)
		require.NotEqual(t, sourceProjID, scratchProjID)
		used, err := mgr.Usage(t.Context(), scratchName)
		require.NoError(t, err)
		require.GreaterOrEqual(t, used, int64(2*bytesPerMiB), "the target must own real charged extents before rollback")
		pending, err := f.b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, shared.OperationExecutionStarted, pending[0].ExecutionPhase())
		web, found := shared.LookupSKUResourceSnapshotRow(pending[0].ResourceProfiles(), "docker-diskless")
		require.True(t, found)
		require.Zero(t, web.DiskMB)
		require.Positive(t, web.ScratchDiskMB)

		preparationComplete.Store(true)
		time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		require.NoError(t, f.b.reconcileStateAndOperations(ctx))
		pending, err = f.b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Empty(t, pending, "strict target absence must complete the same durable operation")
		require.NoDirExists(t, scratchPath)
		require.NoDirExists(t, mgr.HostPath(canonicalVolumeName(f.target, "db", 0)))
		source, err := f.b.retentionStore.Get(f.source)
		require.NoError(t, err)
		require.NotNil(t, source)
		require.Equal(t, shared.RetentionStatusActive, source.Status)
		require.Equal(t, []string{sourceName}, source.RetainedVolumeNames)
		require.Equal(t, sourceInode, inodeNumber(t, sourcePath), "returning source bytes preserves the original volume root")
		restoredProjID, err := readProjectIDFile(sourcePath)
		require.NoError(t, err)
		require.Equal(t, sourceProjID, restoredProjID, "returning source bytes preserves the original project")
		data, err := os.ReadFile(filepath.Join(sourcePath, "sentinel"))
		require.NoError(t, err)
		require.Equal(t, "db", string(data))
		data, err = os.ReadFile(filepath.Join(sourcePath, "data.bin"))
		require.NoError(t, err)
		require.Equal(t, make([]byte, bytesPerMiB), data)
		require.Zero(t, f.b.pool.Stats().AllocationCount)
		callbacks, err := f.b.callbackStore.ListPending()
		require.NoError(t, err)
		require.Len(t, callbacks, 1)
		require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
	})

	// Deletion removes the name before XFS finishes inactivating the
	// unlinked inodes. Wait for this exact project's quota entry to retire;
	// a lingering entry would mean the deleted target still holds usage. The
	// wait is real kernel work, so it runs outside the synctest bubble.
	requireXFSProjectEventuallyAbsent(t, mount, scratchProjID)
	require.True(t, xfsReportListsProject(t, mount, sourceProjID),
		"the returned source must keep its project-quota entry")
}
