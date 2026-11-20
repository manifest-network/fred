//go:build integration

package e2e

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ProjectID reads the XFS project a host path belongs to. Project 0 is no
// project.
func ProjectID(t *testing.T, path string) int {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "xfs_io", "-r", "-c", "lsproj", path).CombinedOutput() //nolint:gosec // G204: a fixed tool on a fixture-built path
	require.NoError(t, err, "xfs_io lsproj %s: %s", path, out)
	_, value, found := strings.Cut(strings.TrimSpace(string(out)), "=")
	require.True(t, found, "unexpected xfs_io output: %s", out)
	id, err := strconv.Atoi(strings.TrimSpace(value))
	require.NoError(t, err, "unexpected xfs_io output: %s", out)
	return id
}

// QuotaLimitKB reads the hard block limit, in KiB, of the XFS project a
// volume directory of this backend belongs to.
func (b *Backend) QuotaLimitKB(t *testing.T, root string) int {
	t.Helper()
	project := ProjectID(t, root)
	require.NotZero(t, project, "%s belongs to no project", root)
	report := b.quotaReport(t)
	quota, reported := report[project]
	require.True(t, reported, "xfs_quota reports no project %d: %v", project, report)
	return quota.limitKB
}

// QuotaUsedKB reads how much, in KiB, is charged to an XFS project on this
// backend's storage. A project the report does not list has nothing charged
// to it and no limit, and reads as 0. The project is given by number, so a
// test can ask about a volume whose directory is gone.
func (b *Backend) QuotaUsedKB(t *testing.T, project int) int {
	t.Helper()
	return b.quotaReport(t)[project].usedKB
}

// QuotaEntries counts the XFS projects on this backend's storage that carry
// a block limit. Every volume directory has one, and nothing else may.
func (b *Backend) QuotaEntries(t *testing.T) int {
	t.Helper()
	entries := 0
	for _, quota := range b.quotaReport(t) {
		if quota.limitKB > 0 {
			entries++
		}
	}
	return entries
}

// projectQuota is one project's line of xfs_quota's block report, in KiB.
type projectQuota struct {
	usedKB  int
	limitKB int
}

// quotaReport maps each project xfs_quota reports on this backend's storage
// to its usage and its hard block limit.
func (b *Backend) quotaReport(t *testing.T) map[int]projectQuota {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "xfs_quota", "-x", "-c", "report -p -b -n -N", filepath.Dir(b.VolumePath)).CombinedOutput() //nolint:gosec // G204: a fixed tool on the fixture's mount
	require.NoError(t, err, "xfs_quota report: %s", out)
	report := map[int]projectQuota{}
	// One line per project: "#<id> <used> <soft> <hard> ...", in KiB.
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "#") {
			continue
		}
		project, err := strconv.Atoi(strings.TrimPrefix(fields[0], "#"))
		require.NoError(t, err, "xfs_quota line: %s", line)
		used, err := strconv.Atoi(fields[1])
		require.NoError(t, err, "xfs_quota line: %s", line)
		limit, err := strconv.Atoi(fields[3])
		require.NoError(t, err, "xfs_quota line: %s", line)
		report[project] = projectQuota{usedKB: used, limitKB: limit}
	}
	return report
}
