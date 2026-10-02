//go:build integration

package docker

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// ENG-1118: the runtime policy cannot repair historical escapes. Use real
// kernel attributes and the production inventory, opener, walker and getter
// to prove that the audit reports these volumes without changing them.
func TestIntegration_XFS_ProjectIDAuditDetectsHistoricalDrift(t *testing.T) {
	mount := setupXFSLoopback(t)
	mgr, err := newVolumeManager(mount, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	// Cleanup runs in reverse order, removing foreign-tagged inodes before
	// trying to retire the clean volume's project ID.
	clean := createIntegrationXFSVolume(t, mgr)
	zero := createIntegrationXFSVolume(t, mgr)
	foreign := createIntegrationXFSVolume(t, mgr)
	inheritance := createIntegrationXFSVolume(t, mgr)
	volumes := []integrationXFSVolume{clean, zero, foreign, inheritance}
	for _, volume := range volumes {
		for _, dir := range []string{"_wp/etc", "data/db"} {
			require.NoError(t, os.MkdirAll(filepath.Join(volume.path, dir), 0o700))
		}
		for _, file := range []string{"_wp/etc/app.conf", "data/db/table"} {
			require.NoError(t, os.WriteFile(filepath.Join(volume.path, file), []byte("existing tenant data\n"), 0o600))
		}
	}
	var logs bytes.Buffer
	auditor := newProjidAuditor(mgr, xfsProjectAttributeGetter{}, slog.New(slog.NewJSONHandler(&logs, nil)))
	previousGauge := testutil.ToFloat64(volumesWithProjidDrift)
	t.Cleanup(func() { volumesWithProjidDrift.Set(previousGauge) })
	before := auditCounters()
	auditor.pass(t.Context())
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 4})
	require.Zero(t, testutil.ToFloat64(volumesWithProjidDrift))
	require.Empty(t, logs.String())

	// Plant the on-disk results of old tenant ioctls, after all descendants
	// exist. The valid roots and markers cannot reveal this descendant drift.
	change := func(path string, mutate func(*linuxFSXAttr)) {
		t.Helper()
		file, err := os.Open(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, file.Close()) }()
		attr, err := readXFSProjectAttributes(file)
		require.NoError(t, err)
		mutate(&attr)
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), linuxFSIOCFSSetXAttr,
			uintptr(unsafe.Pointer(&attr))) // #nosec G103 -- test fixture Linux fsxattr UAPI buffer
		require.Zero(t, errno)
	}
	change(filepath.Join(zero.path, "data/db/table"), func(attr *linuxFSXAttr) { attr.ProjectID = 0 })
	for _, path := range []string{"_wp/etc", "_wp/etc/app.conf"} {
		change(filepath.Join(foreign.path, path), func(attr *linuxFSXAttr) { attr.ProjectID = clean.projID })
	}
	change(filepath.Join(inheritance.path, "data/db"), func(attr *linuxFSXAttr) { attr.XFlags &^= linuxFSXFlagProjInherit })

	attributes := make(map[string]linuxFSXAttr)
	contents := make(map[string][]byte)
	for _, volume := range volumes {
		root := integrationXFSAttributes(t, volume.path)
		require.Equal(t, volume.projID, root.ProjectID)
		require.NotZero(t, root.XFlags&linuxFSXFlagProjInherit)
		for _, path := range []string{".", "_wp", "_wp/etc", "_wp/etc/app.conf", "data", "data/db", "data/db/table", projectIDFile} {
			path = filepath.Join(volume.path, path)
			attributes[path] = integrationXFSAttributes(t, path)
		}
		for _, path := range []string{"_wp/etc/app.conf", "data/db/table", projectIDFile} {
			path = filepath.Join(volume.path, path)
			contents[path], err = os.ReadFile(path)
			require.NoError(t, err)
		}
	}
	before = auditCounters()
	auditor.pass(t.Context())
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 1, projidAuditDrift: 3})
	require.Equal(t, 3.0, testutil.ToFloat64(volumesWithProjidDrift))
	require.Equal(t, map[string]projidAuditOutcome{
		clean.name.value(): projidAuditClean, zero.name.value(): projidAuditDrift,
		foreign.name.value(): projidAuditDrift, inheritance.name.value(): projidAuditDrift,
	}, auditor.last)
	type finding struct {
		Volume  string `json:"volume"`
		Project uint32 `json:"project_id"`
		Dirs    uint64 `json:"drifted_directories"`
		Files   uint64 `json:"drifted_files"`
	}
	want := map[string]finding{
		zero.name.value():        {Volume: zero.name.value(), Project: zero.projID, Files: 1},
		foreign.name.value():     {Volume: foreign.name.value(), Project: foreign.projID, Dirs: 1, Files: 1},
		inheritance.name.value(): {Volume: inheritance.name.value(), Project: inheritance.projID, Dirs: 1},
	}
	decoder := json.NewDecoder(&logs)
	for range 3 {
		var got finding
		require.NoError(t, decoder.Decode(&got))
		require.Contains(t, want, got.Volume)
		require.Equal(t, want[got.Volume], got)
		delete(want, got.Volume)
	}
	require.Empty(t, want)

	// A second pass must report the same historical damage, not silently heal
	// it or count the same affected volumes twice in the gauge.
	before = auditCounters()
	auditor.pass(t.Context())
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 1, projidAuditDrift: 3})
	require.Equal(t, 3.0, testutil.ToFloat64(volumesWithProjidDrift))
	for path, want := range attributes {
		require.Equal(t, want, integrationXFSAttributes(t, path), "audit changed attributes at %s", path)
	}
	for path, want := range contents {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got, "audit changed contents at %s", path)
	}
}
