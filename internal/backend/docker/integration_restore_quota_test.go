//go:build integration

package docker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// parseXfsReportHardBlocks returns the HARD block-limit column for projID from
// `xfs_quota report -p -b -N` output. Rows are `#<projid> <used> <soft> <hard>
// <warn/grace>` in 1 KiB blocks, so the hard limit is fields[3]. This mirrors
// the production parseXfsReportUsedBlocks (which reads fields[1]) but lives in
// the test file to honor the zero-prod-change goal.
func parseXfsReportHardBlocks(out string, projID uint32) (int64, error) {
	want := strconv.FormatUint(uint64(projID), 10)
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 { // hard is fields[3]; skip short lines/headers
			continue
		}
		if strings.TrimPrefix(fields[0], "#") != want {
			continue
		}
		return strconv.ParseInt(fields[3], 10, 64)
	}
	return 0, fmt.Errorf("project id %d not found in xfs_quota report output", projID)
}

// xfsBhardBytes reads back the enforced hard limit (in bytes) for the volume's
// XFS project. It resolves the projID from the volume's .fred-project-id marker
// (guaranteed present after a successful Create) and runs the SAME report flags
// as production Usage() — `report -p -b -N` (never -h, which scales to strings
// and breaks exact equality).
func xfsBhardBytes(t *testing.T, mount string, mgr volumeManager, volName string) int64 {
	t.Helper()
	projID, err := readProjectIDFile(mgr.HostPath(volName))
	require.NoError(t, err, "read project-id marker for %s", volName)
	out, err := exec.Command("xfs_quota", xfsQuotaArgs("report -p -b -N", mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota report -p: %s", out)
	blocks, err := parseXfsReportHardBlocks(string(out), projID)
	require.NoError(t, err, "parse hard-limit blocks from: %s", out)
	return blocks * xfsBlockBytes
}

// ddWrite writes mib MiB of zeros to dir/name via dd. Returns dd's combined
// output and error. Used both to seed and to probe quota enforcement: on an
// over-cap write dd fails partway with EDQUOT ("Disk quota exceeded").
func ddWrite(dir, name string, mib int64) ([]byte, error) {
	return exec.Command("dd", "if=/dev/zero",
		"of="+filepath.Join(dir, name), "bs=1M", fmt.Sprintf("count=%d", mib)).CombinedOutput()
}

// writeNonSparse writes mib MiB of real (allocated, fsync'd) bytes to path.
// XFS project "used" counts only ALLOCATED blocks, so a sparse/truncated seed
// would read back ~0 and make the demote-refused subtest vacuous. fsync forces
// delalloc to real allocation before any Usage() measurement.
func writeNonSparse(t *testing.T, path string, mib int64) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	_, werr := f.Write(make([]byte, mib*bytesPerMiB))
	require.NoError(t, werr)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// newRestoreQuotaBackend wires a Backend with mock Docker/compose but the REAL
// xfs volume manager, and an HMAC-verifying callback sink. The two mock fields
// below are LOAD-BEARING:
//   - InspectImageFn Volumes:{"/data":{}} -> setupVolBinds creates a stateful
//     volume (and thus re-applies the quota); drop it and Create is skipped and
//     the quota assertions become vacuous.
//   - InspectContainerFn Status:"running" -> verifyStartup sees Ready; a wrong
//     status fails the worker, whose rollback renames the adopted volume back
//     out of newCanon, turning every probe into a confusing ENOENT.
//
// CallbackSecret is set to testCallbackSecret (and the sender rebuilt) so
// startCallbackServer's HMAC verification passes; StartupVerifyDuration=10ms
// avoids the 5s default wait per subtest. A parent-level t.Cleanup drains the
// async restore workers (stopCancel + wg.Wait) before setupXFSLoopback unmounts.
func newRestoreQuotaBackend(t *testing.T, mgr volumeManager) (*Backend, <-chan backend.CallbackPayload, string) {
	t.Helper()
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: "img-1", Volumes: map[string]struct{}{"/data": {}}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.CallbackSecret = testCallbackSecret
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.volumes = mgr
	var mu sync.Mutex
	var down []string
	b.compose = happyComposeMock(&mu, &down, nil)
	rebuildCallbackSender(b) // pick up testCallbackSecret so callback HMAC verifies
	attachRetentionStore(t, b)
	b.cfg.SKUProfiles["test-large"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 100}
	b.cfg.SKUProfiles["test-medium"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 20}
	server, ch := startCallbackServer(t)
	// Drain async restore workers before setupXFSLoopback's cleanup unmounts the
	// loopback. Registered after the mount cleanup, so LIFO runs this first.
	t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
	return b, ch, server.URL
}

// seedRetainedForRestore physically creates a retained volume on the real mount
// (Create at the OLD tier's cap -> write a non-sparse data.bin -> rename into
// the retained namespace) and Puts a matching Active retention record. Tenant
// and ProviderUUID MUST equal restoreRequest's hardcodes ("tenant-a"/"prov-1")
// or Restore() rejects at validation before reaching the quota routing.
func seedRetainedForRestore(t *testing.T, b *Backend, mgr volumeManager, orig, oldSKU string, oldCapMiB, dataMiB int64) {
	t.Helper()
	ctx := context.Background()
	canon := canonicalVolumeName(orig, manifest.DefaultServiceName, 0)
	hostPath, _, err := mgr.Create(ctx, canon, oldCapMiB)
	require.NoError(t, err)
	writeNonSparse(t, filepath.Join(hostPath, "data.bin"), dataMiB)
	require.NoError(t, mgr.RenameVolume(canon, retainedName(canon)))
	t.Cleanup(func() { _ = mgr.Destroy(ctx, retainedName(canon)) })
	require.NoError(t, b.retentionStore.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		Tenant:              "tenant-a",
		ProviderUUID:        "prov-1",
		Items:               []backend.LeaseItem{{SKU: oldSKU, ServiceName: manifest.DefaultServiceName, Quantity: 1}},
		StackManifest:       restoreStackManifest(),
		CallbackURL:         "http://unused/callback",
		RetainedVolumeNames: []string{retainedName(canon)},
		Status:              shared.RetentionStatusActive,
		Generation:          1,
		CreatedAt:           time.Now(),
	}))
}

// TestIntegration_Restore_DemotePromote_EnforcesQuota_XFS drives the REAL
// Backend.Restore() on a real XFS loopback and proves the restore-time quota
// re-apply routing (Restore -> checkDemoteFit -> adopt -> setupVolBinds ->
// Create) lands the NEW tier's bhard on the adopted volume. This is the gap the
// hand-stitched TestIntegration_DemotePromote_XFS (never calls Restore) and the
// mock-FS TestRestore_HonorsNewSKU_Promote (no real quota) each leave open.
//
// Subtests are SERIAL (no t.Parallel): they mutate the shared, filesystem-
// global XFS project-quota table on one mount. They stay order-independent only
// because each uses DISTINCT lease/volume/project names — never reuse a lease
// name across subtests, and never assert used==dataMiB on a seed (XFS speculative
// preallocation can transiently inflate "used"; assert on bhard instead).
func TestIntegration_Restore_DemotePromote_EnforcesQuota_XFS(t *testing.T) {
	mount := setupXFSLoopback(t) // root-gated; skips if root/mkfs.xfs/xfs_quota/loop absent
	mgr, err := newVolumeManager(mount, "xfs", slog.Default())
	require.NoError(t, err)
	b, callbackCh, callbackURL := newRestoreQuotaBackend(t, mgr)

	// (a) demote: retained test-large(100)+5 MiB, restore at test-medium(20).
	//     Gate passes (5 <= 20); Create re-applies bhard=20 MiB; a 25 MiB write
	//     must be quota-rejected.
	t.Run("demote_enforces", func(t *testing.T) {
		const orig, newLease = "int-rq-demote", "int-rq-demote-new"
		seedRetainedForRestore(t, b, mgr, orig, "test-large", 100, 5)
		newCanon := canonicalVolumeName(newLease, manifest.DefaultServiceName, 0)
		t.Cleanup(func() { _ = mgr.Destroy(context.Background(), newCanon) })

		req := restoreRequest(newLease, orig, callbackURL)
		req.Items = []backend.LeaseItem{{SKU: "test-medium", ServiceName: manifest.DefaultServiceName, Quantity: 1}}
		require.NoError(t, b.Restore(context.Background(), req))

		cb := waitForCallback(t, callbackCh, newLease, 30*time.Second)
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "restore must succeed; err=%s", cb.Error)

		require.Equal(t, int64(20)*bytesPerMiB, xfsBhardBytes(t, mount, mgr, newCanon),
			"restore must lower the XFS bhard to test-medium (20 MiB)")

		out, werr := ddWrite(mgr.HostPath(newCanon), "big.bin", 25)
		require.Error(t, werr,
			"writing 25 MiB to a 20 MiB-quota XFS volume must fail (EDQUOT/ENOSPC); dd: %s", out)
		// XFS project quota surfaces as EDQUOT ("quota") or ENOSPC ("no space left")
		// depending on the kernel; accept both, as the sibling enforcement tests do
		// (integration_volume_test.go). A spurious failure (missing binary/bad path)
		// matches neither string.
		outLower := strings.ToLower(string(out))
		require.True(t, strings.Contains(outLower, "quota") || strings.Contains(outLower, "no space left"),
			"dd must fail on the quota/space cap, not a spurious error; dd: %s", out)
	})

	// (b) promote: retained test-medium(20)+5 MiB, restore at test-large(100).
	//     Baseline proves 25 MiB does not fit the old 20 MiB cap; after the
	//     promote, Create raises bhard=100 MiB and the same write succeeds.
	t.Run("promote_enforces", func(t *testing.T) {
		const orig, newLease = "int-rq-promote", "int-rq-promote-new"
		seedRetainedForRestore(t, b, mgr, orig, "test-medium", 20, 5)

		retainedDir := mgr.HostPath(retainedName(canonicalVolumeName(orig, manifest.DefaultServiceName, 0)))
		out, werr := ddWrite(retainedDir, "big.bin", 25)
		require.Error(t, werr, "25 MiB must not fit the 20 MiB cap before promote; dd: %s", out)
		// Accept EDQUOT or ENOSPC (see the demote_enforces note above).
		outLower := strings.ToLower(string(out))
		require.True(t, strings.Contains(outLower, "quota") || strings.Contains(outLower, "no space left"),
			"pre-promote dd must fail on the quota/space cap; dd: %s", out)
		require.NoError(t, os.Remove(filepath.Join(retainedDir, "big.bin"))) // free blocks back to ~5 MiB

		newCanon := canonicalVolumeName(newLease, manifest.DefaultServiceName, 0)
		t.Cleanup(func() { _ = mgr.Destroy(context.Background(), newCanon) })

		req := restoreRequest(newLease, orig, callbackURL)
		req.Items = []backend.LeaseItem{{SKU: "test-large", ServiceName: manifest.DefaultServiceName, Quantity: 1}}
		require.NoError(t, b.Restore(context.Background(), req))

		cb := waitForCallback(t, callbackCh, newLease, 30*time.Second)
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "restore must succeed; err=%s", cb.Error)

		require.Equal(t, int64(100)*bytesPerMiB, xfsBhardBytes(t, mount, mgr, newCanon),
			"restore must raise the XFS bhard to test-large (100 MiB)")

		out, werr = ddWrite(mgr.HostPath(newCanon), "big2.bin", 25)
		require.NoError(t, werr, "25 MiB must fit the promoted 100 MiB cap; dd: %s", out)
	})

	// (c) refuse: retained test-large(100)+25 MiB, restore at test-medium(20).
	//     The real Usage() measurement (~25 MiB > 20 MiB cap) makes checkDemoteFit
	//     refuse in the read-only prelude: b.Restore() returns ErrDemoteDataExceedsTier
	//     synchronously (asserted below) and mutates nothing. Assert the
	//     "measured_exceeds" reason too: an unmeasurable Usage() returns the SAME
	//     sentinel, so the sentinel alone would not prove the gate measured 25 > 20.
	t.Run("demote_refused", func(t *testing.T) {
		const orig, newLease = "int-rq-refuse", "int-rq-refuse-new"
		seedRetainedForRestore(t, b, mgr, orig, "test-large", 100, 25)

		req := restoreRequest(newLease, orig, callbackURL)
		req.Items = []backend.LeaseItem{{SKU: "test-medium", ServiceName: manifest.DefaultServiceName, Quantity: 1}}

		var err error
		assertDemoteRefused(t, mgr.Kind(), "measured_exceeds", func() {
			err = b.Restore(context.Background(), req)
		})
		require.ErrorIs(t, err, backend.ErrDemoteDataExceedsTier)

		// No mutation: retained volume survives under its retained name; no new
		// canonical volume was created for the refused lease.
		_, statErr := os.Stat(mgr.HostPath(retainedName(canonicalVolumeName(orig, manifest.DefaultServiceName, 0))))
		require.NoError(t, statErr, "retained volume must survive a refused restore")
		_, statErr = os.Stat(mgr.HostPath(canonicalVolumeName(newLease, manifest.DefaultServiceName, 0)))
		require.True(t, os.IsNotExist(statErr), "a refused restore must not create the new canonical volume")
	})
}
