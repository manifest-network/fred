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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/testutil"
)

// parseXfsReportHardBlocks returns the HARD block-limit column for projID from
// `xfs_quota report -p -b -n -N` output. Rows are `#<projid> <used> <soft> <hard>
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
// as production Usage() — `report -p -b -n -N` (never -h, which scales to strings
// and breaks exact equality).
func xfsBhardBytes(t *testing.T, mount string, mgr volumeReader, volName string) int64 {
	t.Helper()
	projID, err := readProjectIDFile(mgr.HostPath(volName))
	require.NoError(t, err, "read project-id marker for %s", volName)
	out, err := exec.Command("xfs_quota", xfsQuotaArgs("report -p -b -n -N", mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota report -p: %s", out)
	blocks, err := parseXfsReportHardBlocks(string(out), projID)
	require.NoError(t, err, "parse hard-limit blocks from: %s", out)
	return blocks * xfsBlockBytes
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

// TestIntegration_RestoreRollback_ReappliesSourceQuota drives the rollback
// quota helper through the production XFS volumeManager. It models a
// failed promote after Create raised the adopted volume to 100 MiB: once the
// volume is re-quarantined, rollback must restore the retained source's exact
// 20 MiB cap before that source can become authoritative again.
func TestIntegration_RestoreRollback_ReappliesSourceQuota(t *testing.T) {
	const oldCapMiB, promotedCapMiB = int64(20), int64(100)
	ctx := context.Background()
	mount := testutil.SetupXFSLoopback(t)
	mgr, err := newVolumeManager(mount, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	orig := newIntegrationLeaseUUID()
	destination := newIntegrationLeaseUUID()
	canonical := canonicalVolumeName(orig, "app", 0)
	retained := retainedName(canonical)
	newCanonical := canonicalVolumeName(destination, "app", 0)

	hostPath, _, err := mgr.Create(ctx, canonical, oldCapMiB)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(hostPath, "seed.bin"), make([]byte, 5*bytesPerMiB), 0o600,
	))
	require.NoError(t, mgr.RenameVolume(ctx, canonical, retained))
	require.NoError(t, mgr.RenameVolume(ctx, retained, newCanonical))
	_, _, err = mgr.Create(ctx, newCanonical, promotedCapMiB)
	require.NoError(t, err, "promote must first raise the physical quota")
	require.NoError(t, mgr.RenameVolume(ctx, newCanonical, retained))
	t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, retained) })

	b := newBackendForTest(&mockDockerClient{}, nil)
	useVolumeManager(b, mgr)
	rec := &shared.RetentionEntry{
		OriginalLeaseUUID: orig,
		NewLeaseUUID:      destination,
		Items: []backend.LeaseItem{{
			SKU: "old-tier", ServiceName: "app", Quantity: 1,
		}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "old-tier", CPUCores: 1, MemoryMB: 512, DiskMB: oldCapMiB,
		}},
		RetainedVolumeNames: []string{retained},
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
	}
	profiles, err := b.restoreRetainedVolumeQuotas(ctx, rec)
	require.NoError(t, err)
	require.Equal(t, rec.ResourceProfiles, profiles)

	require.Equal(t, oldCapMiB*bytesPerMiB,
		xfsBhardBytes(t, mount, mgr, retained),
		"XFS hard quota must be restored exactly")
	// Existing data is ~5 MiB; a fresh 25 MiB incompressible write must
	// cross the restored 20 MiB cap.
	out, writeErr := exec.Command(
		"dd", "if=/dev/urandom", "of="+filepath.Join(mgr.HostPath(retained), "probe.bin"),
		"bs=1M", "count=25",
	).CombinedOutput()
	require.Error(t, writeErr,
		"XFS must enforce the restored source cap; dd output: %s", out)
}
