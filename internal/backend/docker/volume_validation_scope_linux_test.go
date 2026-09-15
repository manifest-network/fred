package docker

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// Kernel open notifications observe the real Directory.VerifyPath syscall
// boundary. The test has no clock threshold and no injected production probe.
func TestProtectedVolumeValidationOpensOnlyItsOwnedRoot(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}, nil)
	defer b.stopCancel()
	lease := durableCallbackTestLeaseUUID
	base := t.TempDir()
	paths := make(map[string]string)
	for index := range 64 {
		name := canonicalVolumeName(lease, "app", index)
		paths[name] = filepath.Join(base, name)
		require.NoError(t, os.Mkdir(paths[name], 0o700))
	}
	b.volumes = &mockVolumeManager{defaultDir: base}
	runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q, err := b.quiesceLaunchVolumes(t.Context(), mutations, paths, nil, nil)
		require.NoError(t, err)
		defer q.release()
		watcher, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
		require.NoError(t, err)
		defer unix.Close(watcher) //nolint:errcheck // read-only test observation
		watched := make(map[int]string)
		for name, path := range paths {
			watch, err := unix.InotifyAddWatch(watcher, path, unix.IN_OPEN)
			require.NoError(t, err)
			watched[watch] = name
		}
		for index := range 64 {
			name := canonicalVolumeName(lease, "app", index)
			volume, err := q.lookup(name)
			require.NoError(t, err)
			for range 8 {
				path, err := volume.rootPath()
				require.NoError(t, err)
				require.Equal(t, paths[name], path)
			}
			opened := openedVolumeRoots(t, watcher, watched)
			require.Equal(t, map[string]bool{name: true}, opened,
				"per-volume preparation must not rescan the other reserved directories")
		}
		return true
	})
}

func openedVolumeRoots(t *testing.T, watcher int, watched map[int]string) map[string]bool {
	t.Helper()
	opened := make(map[string]bool)
	var buffer [64 << 10]byte
	for {
		count, err := unix.Read(watcher, buffer[:])
		if errors.Is(err, unix.EAGAIN) {
			return opened
		}
		require.NoError(t, err)
		require.Positive(t, count)
		for offset := 0; offset < count; {
			require.GreaterOrEqual(t, count-offset, unix.SizeofInotifyEvent)
			header := buffer[offset : offset+unix.SizeofInotifyEvent]
			watch := int(int32(binary.NativeEndian.Uint32(header)))
			mask := binary.NativeEndian.Uint32(header[4:])
			length := int(binary.NativeEndian.Uint32(header[12:]))
			require.Zero(t, mask&unix.IN_Q_OVERFLOW)
			if mask&unix.IN_OPEN != 0 {
				name, ok := watched[watch]
				require.True(t, ok)
				opened[name] = true
			}
			offset += unix.SizeofInotifyEvent + length
			require.LessOrEqual(t, offset, count)
		}
	}
}

func TestProtectedVolumeScopedValidationPreservesWholeLaunchAndLifetimeChecks(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}, nil)
	defer b.stopCancel()
	lease := durableCallbackTestLeaseUUID
	base := t.TempDir()
	first, second := canonicalVolumeName(lease, "app", 0), canonicalVolumeName(lease, "app", 1)
	paths := map[string]string{first: filepath.Join(base, first), second: filepath.Join(base, second)}
	for _, path := range paths {
		require.NoError(t, os.Mkdir(path, 0o700))
	}
	b.volumes = &mockVolumeManager{defaultDir: base}
	runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q, err := b.quiesceLaunchVolumes(t.Context(), mutations, paths, nil, nil)
		require.NoError(t, err)
		defer q.release()
		firstVolume, err := q.lookup(first)
		require.NoError(t, err)
		secondVolume, err := q.lookup(second)
		require.NoError(t, err)
		copied := firstVolume
		require.NoError(t, os.Rename(paths[second], paths[second]+"-old"))
		require.NoError(t, os.Mkdir(paths[second], 0o700))
		require.NoError(t, firstVolume.requireActive(), "a per-volume capability revalidates only its own directory")
		_, err = q.lookup(first)
		require.NoError(t, err)
		require.ErrorIs(t, secondVolume.requireActive(), fsidentity.ErrDirectoryIdentityChanged)
		require.ErrorIs(t, q.requireActive(), fsidentity.ErrDirectoryIdentityChanged,
			"complete dispatch still requires every reserved root")
		require.ErrorIs(t, q.validateProject(&composetypes.Project{}), fsidentity.ErrDirectoryIdentityChanged)
		q.release()
		require.Error(t, copied.requireActive(), "copies cannot outlive the shared reservation")
		return true
	})
}
