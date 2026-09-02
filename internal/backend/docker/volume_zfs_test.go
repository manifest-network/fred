package docker

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testZFSLeaseUUID = "550e8400-e29b-41d4-a716-446655440000"

func interruptedCreateZFSManager(t *testing.T, mode string) (*zfsVolumeManager, string, string) {
	t.Helper()

	root := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"

inventory="list -H -r -d 1 -o name $FRED_TEST_ZFS_PARENT"
state="list -H -p -o name,mounted,mountpoint $FRED_TEST_ZFS_PARENT/$FRED_TEST_ZFS_CHILD"
mount="mount $FRED_TEST_ZFS_PARENT/$FRED_TEST_ZFS_CHILD"

if [ "$*" = "$inventory" ]; then
  printf '%s\n%s/%s\n' "$FRED_TEST_ZFS_PARENT" "$FRED_TEST_ZFS_PARENT" "$FRED_TEST_ZFS_CHILD"
  exit 0
fi
if [ "$*" = "$state" ]; then
  if [ "$FRED_TEST_ZFS_MODE" = "ambiguous" ] && [ -e "$FRED_TEST_ZFS_STATE" ]; then
    printf '%s\n' 'post-mount substrate inventory unavailable' >&2
    exit 1
  fi
  mounted=no
  if [ -e "$FRED_TEST_ZFS_STATE" ]; then mounted=yes; fi
  printf '%s/%s\t%s\t%s\n' \
    "$FRED_TEST_ZFS_PARENT" "$FRED_TEST_ZFS_CHILD" "$mounted" "$FRED_TEST_ZFS_MOUNTPOINT"
  exit 0
fi
if [ "$*" = "$mount" ]; then
  case "$FRED_TEST_ZFS_MODE" in
    success|ambiguous)
      /bin/mkdir -p "$FRED_TEST_ZFS_MOUNTPOINT" || exit 98
      : > "$FRED_TEST_ZFS_STATE"
      if [ "$FRED_TEST_ZFS_MODE" = "ambiguous" ]; then
        printf '%s\n' 'mount acknowledgement lost' >&2
        exit 1
      fi
      exit 0
      ;;
    failed)
      printf '%s\n' 'mount failed' >&2
      exit 1
      ;;
  esac
fi
printf '%s\n' "unexpected zfs invocation: $*" >&2
exit 97
`), 0o700))

	const parent = "tank/fred"
	name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)
	t.Setenv("FRED_TEST_ZFS_PARENT", parent)
	t.Setenv("FRED_TEST_ZFS_CHILD", name)
	t.Setenv("FRED_TEST_ZFS_MOUNTPOINT", filepath.Join(root, name))
	t.Setenv("FRED_TEST_ZFS_STATE", filepath.Join(t.TempDir(), "mounted"))
	t.Setenv("FRED_TEST_ZFS_MODE", mode)

	return &zfsVolumeManager{
		dataPath:      root,
		parentDataset: parent,
		logger:        slog.Default(),
	}, name, logPath
}

func TestManagedVolumeNameRejectsZFSDatasetSelectors(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"fred-" + testZFSLeaseUUID + "-app-0@snapshot",
		"fred-" + testZFSLeaseUUID + "-app-0#bookmark",
	} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, err := parseManagedVolumeName(value)
			require.Error(t, err)
		})
	}
}

func TestZFSVolumeManagerRejectsInvalidNameAtEveryDatasetBoundary(t *testing.T) {
	t.Parallel()

	mgr := &zfsVolumeManager{
		dataPath:      t.TempDir(),
		parentDataset: "tank/fred",
		logger:        slog.Default(),
	}
	const invalid = "fred-" + testZFSLeaseUUID + "-app-0/../../foreign"
	valid := canonicalVolumeName(testZFSLeaseUUID, "app", 0)

	_, _, err := mgr.Create(context.Background(), invalid, 10)
	require.ErrorContains(t, err, "storage path component")
	require.ErrorContains(t, mgr.EnsureQuota(context.Background(), invalid, 10), "storage path component")
	require.ErrorContains(t, mgr.Destroy(context.Background(), invalid), "storage path component")
	_, err = mgr.Usage(context.Background(), invalid)
	require.ErrorContains(t, err, "storage path component")
	require.ErrorContains(t, mgr.RenameVolume(context.Background(), invalid, valid), "old volume")
	require.ErrorContains(t, mgr.RenameVolume(context.Background(), valid, invalid), "new volume")
}

func TestZFSVolumeManagerCreateRefusesSymlinkMountpoint(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		outsideRoot bool
	}{
		{name: "in_root_target"},
		{name: "escaping_target", outsideRoot: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			target := filepath.Join(root, "another-volume")
			if test.outsideRoot {
				target = t.TempDir()
			} else {
				require.NoError(t, os.Mkdir(target, 0o700))
			}
			name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
			require.NoError(t, os.Symlink(target, filepath.Join(root, name)))
			mgr := &zfsVolumeManager{
				dataPath:      root,
				parentDataset: "tank/fred",
				logger:        slog.Default(),
			}

			_, _, err := mgr.Create(context.Background(), name, 10)
			require.Error(t, err)
			assert.False(t, errors.Is(err, fs.ErrNotExist),
				"a symlink must be rejected, not treated as an absent volume")
			assert.ErrorContains(t, err, "not a real directory")
		})
	}
}

func TestZFSVolumeManagerCreateRefusesExistingDatasetMountedElsewhere(t *testing.T) {
	root := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
case "$1" in
  list)
    for arg do dataset="$arg"; done
    printf '%s\n' "$dataset"
    ;;
  mount)
    exit 0
    ;;
  *)
    exit 97
    ;;
esac
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)

	name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
	mgr := &zfsVolumeManager{
		dataPath:      root,
		parentDataset: "tank/fred",
		logger:        slog.Default(),
	}

	_, _, err := mgr.Create(context.Background(), name, 10)
	require.ErrorContains(t, err, "did not appear at managed mountpoint")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(commands), "list -H -o name tank/fred/"+name)
	assert.Contains(t, string(commands), "mount tank/fred/"+name)
	assert.NotContains(t, string(commands), "set ", "quota must not be changed until the bind source is proved")
	assert.NotContains(t, string(commands), "destroy ", "an existing dataset must be preserved for operator recovery")
}

func TestZFSVolumeManagerCreatePreservesAmbiguousDataset(t *testing.T) {
	root := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
case "$1" in
  list)
    printf '%s\n' 'no datasets available' >&2
    exit 1
    ;;
  create)
    printf '%s\n' 'dataset already exists' >&2
    exit 1
    ;;
  destroy)
    exit 97
    ;;
esac
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)

	name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
	mgr := &zfsVolumeManager{
		dataPath:      root,
		parentDataset: "tank/fred",
		logger:        slog.Default(),
	}

	_, _, err := mgr.Create(context.Background(), name, 10)
	require.ErrorContains(t, err, "zfs create")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(commands), "list -H -o name tank/fred/"+name)
	assert.Contains(t, string(commands), "create -o refquota=10M tank/fred/"+name)
	assert.NotContains(t, string(commands), "destroy ",
		"a failed create is ambiguous and must preserve the dataset for retry")
}

func TestZFSVolumeManagerCreateStopsOnDatasetInventoryError(t *testing.T) {
	root := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
printf '%s\n' 'temporary inventory I/O error' >&2
exit 1
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)

	name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
	mgr := &zfsVolumeManager{
		dataPath:      root,
		parentDataset: "tank/fred",
		logger:        slog.Default(),
	}

	_, _, err := mgr.Create(context.Background(), name, 10)
	require.ErrorContains(t, err, "check for existing unmounted zfs dataset")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(commands), "list -H -o name tank/fred/"+name)
	assert.NotContains(t, string(commands), "create ",
		"inventory uncertainty must stop before a storage mutation")
	assert.NotContains(t, string(commands), "destroy ")
}

func TestZFSVolumeManagerRequireNoInterruptedVolumeMutationsDetectsExactUnmountedDataset(t *testing.T) {
	mgr, name, logPath := interruptedCreateZFSManager(t, "readonly")

	err := mgr.RequireNoInterruptedVolumeMutations(t.Context())
	require.ErrorContains(t, err, "interrupted unmounted create")
	require.ErrorContains(t, err, name)

	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	logText := string(commands)
	assert.Contains(t, logText, "list -H -r -d 1 -o name tank/fred")
	assert.Contains(t, logText,
		"list -H -p -o name,mounted,mountpoint tank/fred/"+name)
	assert.NotContains(t, logText, "mount ", "the publication gate must remain read-only")
	assert.NotContains(t, logText, "destroy ")
}

func TestZFSVolumeManagerRecoverInterruptedCreateRemountsAndReattests(t *testing.T) {
	mgr, name, logPath := interruptedCreateZFSManager(t, "success")

	require.NoError(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))
	assert.DirExists(t, filepath.Join(mgr.dataPath, name))

	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	logText := string(commands)
	stateProbe := "list -H -p -o name,mounted,mountpoint tank/fred/" + name
	assert.Equal(t, 2, strings.Count(logText, stateProbe),
		"recovery must attest both before and after mounting")
	assert.Contains(t, logText, "mount tank/fred/"+name)
	assert.NotContains(t, logText, "destroy ")
}

func TestZFSVolumeManagerRecoverInterruptedCreateRefusesUnsafeStubWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name      string
		prepare   func(t *testing.T, mountpoint string)
		wantError string
	}{
		{
			name: "nonempty",
			prepare: func(t *testing.T, mountpoint string) {
				require.NoError(t, os.Mkdir(mountpoint, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "tenant-data"), []byte("keep"), 0o600))
			},
			wantError: "would hide existing data",
		},
		{
			name: "symlink",
			prepare: func(t *testing.T, mountpoint string) {
				outside := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(outside, "tenant-data"), []byte("keep"), 0o600))
				require.NoError(t, os.Symlink(outside, mountpoint))
			},
			wantError: "not a real directory",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr, name, logPath := interruptedCreateZFSManager(t, "readonly")
			mountpoint := filepath.Join(mgr.dataPath, name)
			test.prepare(t, mountpoint)

			err := mgr.RecoverInterruptedVolumeMutations(t.Context())
			require.ErrorContains(t, err, test.wantError)

			info, statErr := os.Lstat(mountpoint)
			require.NoError(t, statErr)
			if test.name == "symlink" {
				assert.NotZero(t, info.Mode()&os.ModeSymlink)
			} else {
				contents, readErr := os.ReadFile(filepath.Join(mountpoint, "tenant-data"))
				require.NoError(t, readErr)
				assert.Equal(t, "keep", string(contents))
			}

			commands, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			logText := string(commands)
			assert.NotContains(t, logText, "mount ")
			assert.NotContains(t, logText, "destroy ")
		})
	}
}

func TestZFSVolumeManagerRecoverInterruptedCreateRetainsFailedOrAmbiguousMount(t *testing.T) {
	for _, test := range []struct {
		mode      string
		wantError string
	}{
		{mode: "failed", wantError: "dataset remains unmounted"},
		{mode: "ambiguous", wantError: "post-mount substrate inventory unavailable"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			mgr, name, logPath := interruptedCreateZFSManager(t, test.mode)

			err := mgr.RecoverInterruptedVolumeMutations(t.Context())
			require.ErrorContains(t, err, "recover interrupted zfs create tank/fred/"+name)
			require.ErrorContains(t, err, test.wantError)

			commands, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			logText := string(commands)
			assert.Contains(t, logText, "mount tank/fred/"+name)
			assert.NotContains(t, logText, "destroy ",
				"recovery uncertainty must preserve the exact dataset")
			if test.mode == "ambiguous" {
				assert.DirExists(t, filepath.Join(mgr.dataPath, name),
					"an unconfirmed mount must be preserved for the next exact re-attestation")
			}
		})
	}
}

func TestZFSVolumeManagerCreateRefusesPlainDirectoryWithoutExactMountedDataset(t *testing.T) {
	for _, test := range []struct {
		name       string
		mounted    string
		mountpoint func(root, name string) string
		wantError  string
	}{
		{
			name:    "unmounted",
			mounted: "no",
			mountpoint: func(root, name string) string {
				return filepath.Join(root, name)
			},
			wantError: "is not mounted",
		},
		{
			name:    "mounted elsewhere",
			mounted: "yes",
			mountpoint: func(root, _ string) string {
				return filepath.Join(root, "foreign")
			},
			wantError: "does not equal exact managed path",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			binDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "zfs.log")
			fakeZFS := filepath.Join(binDir, "zfs")
			require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
for arg do dataset="$arg"; done
case "$*" in
  *"name,mounted,mountpoint"*)
    printf '%s\t%s\t%s\n' "$dataset" "$FRED_TEST_ZFS_MOUNTED" "$FRED_TEST_ZFS_MOUNTPOINT"
    ;;
  *)
    printf '%s\n' "$dataset"
    ;;
esac
`), 0o700))
			t.Setenv("PATH", binDir)
			t.Setenv("FRED_TEST_ZFS_LOG", logPath)
			t.Setenv("FRED_TEST_ZFS_MOUNTED", test.mounted)

			name := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
			t.Setenv("FRED_TEST_ZFS_MOUNTPOINT", test.mountpoint(root, name))
			require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
			mgr := &zfsVolumeManager{
				dataPath: root, parentDataset: "tank/fred", logger: slog.Default(),
			}

			_, _, err := mgr.Create(t.Context(), name, 10)
			require.ErrorContains(t, err, test.wantError)
			commands, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			assert.Contains(t, string(commands),
				"list -H -p -o name,mounted,mountpoint tank/fred/"+name)
			assert.NotContains(t, string(commands), "set ",
				"quota mutation must follow exact dataset and mountpoint proof")
		})
	}
}

func TestParseZFSDatasetAttestation(t *testing.T) {
	t.Parallel()

	const dataset = "tank/fred/fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	const mountpoint = "/srv/fred/volumes/fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	tests := []struct {
		name      string
		out       string
		wantError string
	}{
		{name: "exact", out: dataset + "\tyes\t" + mountpoint + "\n"},
		{name: "wrong dataset", out: "tank/fred/foreign\tyes\t" + mountpoint + "\n", wantError: "unexpected dataset"},
		{name: "unmounted", out: dataset + "\tno\t" + mountpoint + "\n", wantError: "is not mounted"},
		{name: "wrong mountpoint", out: dataset + "\tyes\t/srv/foreign\n", wantError: "does not equal exact managed path"},
		{name: "extra row", out: dataset + "\tyes\t" + mountpoint + "\nforeign\tyes\t/srv/foreign\n", wantError: "invalid row"},
		{name: "missing field", out: dataset + "\tyes\n", wantError: "expected 3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := parseZFSDatasetAttestation(test.out, dataset, mountpoint)
			if test.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestZFSVolumeManagerRenameRefusesIdempotentDestinationMountedElsewhere(t *testing.T) {
	root := t.TempDir()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
for arg do dataset="$arg"; done
case "$*" in
  *"name,mounted,mountpoint"*)
    printf '%s\tyes\t%s\n' "$dataset" "$FRED_TEST_ZFS_MOUNTPOINT"
    ;;
  *"tank/fred/$FRED_TEST_ZFS_OLD")
    printf '%s\n' 'dataset does not exist' >&2
    exit 1
    ;;
  *)
    printf '%s\n' "$dataset"
    ;;
esac
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)
	oldName := canonicalVolumeName(testZFSLeaseUUID, "app", 0)
	newName := retainedName(oldName)
	t.Setenv("FRED_TEST_ZFS_OLD", oldName)
	t.Setenv("FRED_TEST_ZFS_MOUNTPOINT", filepath.Join(root, "foreign"))
	require.NoError(t, os.Mkdir(filepath.Join(root, newName), 0o700))
	mgr := &zfsVolumeManager{
		dataPath: root, parentDataset: "tank/fred", logger: slog.Default(),
	}

	err := mgr.RenameVolume(t.Context(), oldName, newName)
	require.ErrorContains(t, err, "does not equal exact managed path")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.NotContains(t, string(commands), "rename ",
		"an idempotent destination is not authoritative until its exact mountpoint is proved")
}

func TestZFSVolumeManagerHostPathFailsClosed(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mgr := &zfsVolumeManager{dataPath: root}
	valid := canonicalVolumeName(testZFSLeaseUUID, "api", 3)
	assert.Equal(t, filepath.Join(root, valid), mgr.HostPath(valid))

	rejected := mgr.HostPath("../../etc")
	rel, err := filepath.Rel(root, rejected)
	require.NoError(t, err)
	assert.True(t, filepath.IsLocal(rel))
	assert.Equal(t, filepath.Base(rel), rel)
	assert.Contains(t, rel, ".fred-rejected-volume-")
	assert.NotEqual(t, root, rejected)
}
