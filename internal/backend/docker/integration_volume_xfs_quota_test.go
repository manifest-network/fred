//go:build integration

package docker

import (
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise the real kernel attributes and xfsprogs depth semantics. Root repair
// must restore inheritance without recursively retagging historical descendants;
// such descendants require offline repair outside the startup readiness budget.
func TestIntegration_XFS_QuotaRepairTouchesOnlyRoot(t *testing.T) {
	mount := setupXFSLoopback(t)
	mgr, err := newVolumeManager(mount, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	const name = "fred-550e8400-e29b-41d4-a716-446655440107-app-0"
	hostPath, created, err := mgr.Create(t.Context(), name, 100)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(t.Context(), name) })
	projID, err := readProjectIDFile(hostPath)
	require.NoError(t, err)

	// Model a root left untagged by an older daemon, with an existing child
	// created before root inheritance has been repaired.
	output, err := exec.CommandContext(t.Context(), "xfs_quota",
		xfsQuotaArgs(xfsProjectResetToDefaultCmd(hostPath), mount)...).CombinedOutput()
	require.NoError(t, err, "%s", output)
	oldChild := filepath.Join(hostPath, "historical-child")
	require.NoError(t, os.Mkdir(oldChild, 0o700))
	readAttr := func(path string) linuxFSXAttr {
		t.Helper()
		root, err := os.OpenRoot(path)
		require.NoError(t, err)
		defer func() { _ = root.Close() }()
		attr, err := (linuxXFSProjectAttributeReader{}).ReadProjectAttributes(root)
		require.NoError(t, err)
		return attr
	}
	require.Zero(t, readAttr(oldChild).ProjectID)

	require.NoError(t, mgr.EnsureQuota(t.Context(), name, 100))
	rootAttr := readAttr(hostPath)
	assert.Equal(t, projID, rootAttr.ProjectID)
	assert.NotZero(t, rootAttr.XFlags&linuxFSXFlagProjInherit)
	assert.Zero(t, readAttr(oldChild).ProjectID, "quota repair must not visit existing tenant entries")

	newChild := filepath.Join(hostPath, "new-child")
	require.NoError(t, os.Mkdir(newChild, 0o700))
	assert.Equal(t, projID, readAttr(newChild).ProjectID, "future children must inherit the enforced project")
	_, created, err = mgr.Create(t.Context(), name, 100)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Zero(t, readAttr(oldChild).ProjectID, "reuse must not retag existing tenant entries")
}
