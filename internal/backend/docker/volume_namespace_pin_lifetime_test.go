package docker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fsidentity"
)

type recordedNamespaceVolumes struct {
	aliasedNamespaceVolumes
	mu    sync.Mutex
	pins  []*fsidentity.Directory
	paths map[string]string
}

func requireNoOpenNamespaceInode(t *testing.T, identity fsidentity.Identity) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		require.NoError(t, err)
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); errors.Is(err, unix.EBADF) {
			continue // The descriptor used to list /proc/self/fd is already closed.
		} else {
			require.NoError(t, err)
		}
		require.False(t, identity.Equal(fsidentity.Identity{Device: stat.Dev, Inode: stat.Ino}),
			"descriptor %d still pins the namespace inode", fd)
	}
}

func TestVolumeAccessWaitingLaunchClosesDiscoveryPins(t *testing.T) {
	name, _ := namespaceNames(t)
	root := filepath.Join(t.TempDir(), name.value())
	require.NoError(t, os.Mkdir(root, 0o700))
	identity, err := fsidentity.InspectDirectory(root)
	require.NoError(t, err)
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) {
		return nil, nil
	}}, nil)
	defer b.stopCancel()
	installProtectedVolumeFixture(t, b, root)
	synctest.Test(t, func(t *testing.T) {
		reserved, err := b.volumeAccess.reserve(t.Context(), []fsidentity.Identity{identity})
		require.NoError(t, err)
		defer reserved.release()
		finished := make(chan error, 1)
		go func() {
			finished <- runSubjectStorageObservationForTest(t, b, managedVolumeLeaseUUID(name), func(mutations *storageMutations) error {
				q, err := b.quiesceLaunchVolumes(t.Context(), mutations, map[string]string{name.value(): root}, nil, nil)
				if err != nil {
					return err
				}
				owned := q.volumes[name.value()].root
				q.release()
				if !errors.Is(owned.VerifyPath(), fsidentity.ErrDirectoryClosed) {
					return errors.New("released launch retained its directory descriptor")
				}
				return nil
			})
		}()
		synctest.Wait()
		requireNoOpenNamespaceInode(t, identity)
		reserved.release()
		require.NoError(t, <-finished)
		requireNoOpenNamespaceInode(t, identity)
		require.Empty(t, b.volumeAccess.active)
		require.Empty(t, b.volumeAccess.scopes)
	})
}

func (v *recordedNamespaceVolumes) PinNamespaceRoot(name managedVolumeName) (*fsidentity.Directory, error) {
	path := v.root
	if v.paths != nil {
		path = v.paths[name.value()]
	}
	root, err := pinManagedNamespaceRoot(path)
	if root != nil {
		v.mu.Lock()
		v.pins = append(v.pins, root)
		v.mu.Unlock()
	}
	return root, err
}

func (v *recordedNamespaceVolumes) requireClosedPins(t *testing.T) {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	require.NotEmpty(t, v.pins)
	for _, root := range v.pins {
		require.ErrorIs(t, root.VerifyPath(), fsidentity.ErrDirectoryClosed,
			"a namespace probe must not retain the inode during a reservation wait or effect")
	}
}

func TestVolumeAccessNamespaceMutationClosesPinsWithoutReleasingExclusion(t *testing.T) {
	first, second := namespaceNames(t)
	root := filepath.Join(t.TempDir(), "volume")
	require.NoError(t, os.Mkdir(root, 0o700))
	identity, err := fsidentity.InspectDirectory(root)
	require.NoError(t, err)
	volumes := &recordedNamespaceVolumes{aliasedNamespaceVolumes: aliasedNamespaceVolumes{
		mockVolumeManager: &mockVolumeManager{}, root: root,
	}}
	synctest.Test(t, func(t *testing.T) {
		b := &Backend{volumes: volumes, volumeLaunches: emptyVolumeLaunchCoordinatorForTest()}
		entered := make(chan struct{})
		finishEffect := make(chan struct{})
		defer close(finishEffect)
		finished := make(chan error, 1)
		go func() {
			finished <- b.mutateManagedVolumeNamespace(t.Context(), []string{first.value()}, func(context.Context) error {
				close(entered)
				<-finishEffect
				return nil
			})
		}()
		<-entered
		volumes.requireClosedPins(t)
		require.Contains(t, b.volumeAccess.active, identity,
			"closing directory probes must retain the physical reservation")

		// A same-lease launch remains excluded throughout the effect.
		readerCtx, cancelReader := context.WithCancel(t.Context())
		defer cancelReader()
		reader := make(chan error, 1)
		go func() {
			release, err := b.volumeAccess.retainNamespace(readerCtx, []managedVolumeName{first})
			if err == nil {
				release()
			}
			reader <- err
		}()
		// A different lease name for the same physical directory must neither
		// enter its effect nor pin the retiring inode while it waits.
		aliasCtx, cancelAlias := context.WithCancel(t.Context())
		defer cancelAlias()
		alias := make(chan error, 1)
		go func() {
			alias <- b.mutateManagedVolumeNamespace(aliasCtx, []string{second.value()}, func(context.Context) error {
				return errors.New("alias mutation escaped physical exclusion")
			})
		}()
		synctest.Wait()
		volumes.requireClosedPins(t)
		select {
		case err := <-reader:
			t.Fatalf("same-lease reader escaped namespace exclusion: %v", err)
		case err := <-alias:
			t.Fatalf("alias mutation escaped physical exclusion: %v", err)
		default:
		}
		cancelReader()
		cancelAlias()
		require.ErrorIs(t, <-reader, context.Canceled)
		require.ErrorIs(t, <-alias, context.Canceled)
		finishEffect <- struct{}{}
		require.NoError(t, <-finished)
		require.Empty(t, b.volumeAccess.active)
		require.Empty(t, b.volumeAccess.scopes)
	})
}

func TestVolumeAccessNamespaceMutationRechecksRootAfterPhysicalWait(t *testing.T) {
	first, _ := namespaceNames(t)
	root := filepath.Join(t.TempDir(), "volume")
	require.NoError(t, os.Mkdir(root, 0o700))
	identity, err := fsidentity.InspectDirectory(root)
	require.NoError(t, err)
	volumes := &recordedNamespaceVolumes{aliasedNamespaceVolumes: aliasedNamespaceVolumes{
		mockVolumeManager: &mockVolumeManager{}, root: root,
	}}
	synctest.Test(t, func(t *testing.T) {
		b := &Backend{volumes: volumes, volumeLaunches: emptyVolumeLaunchCoordinatorForTest()}
		reserved, err := b.volumeAccess.reserve(t.Context(), []fsidentity.Identity{identity})
		require.NoError(t, err)
		defer reserved.release()
		finished := make(chan error, 1)
		go func() {
			finished <- b.mutateManagedVolumeNamespace(t.Context(), []string{first.value()}, func(context.Context) error {
				return errors.New("namespace effect entered with a replaced directory")
			})
		}()
		synctest.Wait()
		volumes.requireClosedPins(t)
		// Retain the original inode under another path so replacement cannot
		// accidentally reuse its device/inode pair.
		require.NoError(t, os.Rename(root, root+"-previous"))
		require.NoError(t, os.Mkdir(root, 0o700))
		reserved.release()
		require.ErrorContains(t, <-finished, "namespace mutation root changed")
		volumes.requireClosedPins(t)
		require.Empty(t, b.volumeAccess.active)
		require.Empty(t, b.volumeAccess.scopes)
	})
}

func TestVolumeAccessNamespaceMutationRejectsNewRootAfterPhysicalWait(t *testing.T) {
	first, second := namespaceNames(t)
	base := t.TempDir()
	existing := filepath.Join(base, "existing")
	absent := filepath.Join(base, "absent")
	require.NoError(t, os.Mkdir(existing, 0o700))
	identity, err := fsidentity.InspectDirectory(existing)
	require.NoError(t, err)
	volumes := &recordedNamespaceVolumes{
		aliasedNamespaceVolumes: aliasedNamespaceVolumes{mockVolumeManager: &mockVolumeManager{}},
		paths:                   map[string]string{first.value(): existing, second.value(): absent},
	}
	synctest.Test(t, func(t *testing.T) {
		b := &Backend{volumes: volumes, volumeLaunches: emptyVolumeLaunchCoordinatorForTest()}
		reserved, err := b.volumeAccess.reserve(t.Context(), []fsidentity.Identity{identity})
		require.NoError(t, err)
		defer reserved.release()
		finished := make(chan error, 1)
		go func() {
			finished <- b.mutateManagedVolumeNamespace(t.Context(), []string{first.value(), second.value()}, func(context.Context) error {
				return errors.New("namespace effect entered without the new root's physical reservation")
			})
		}()
		synctest.Wait()
		volumes.requireClosedPins(t)
		require.NoError(t, os.Mkdir(absent, 0o700))
		reserved.release()
		require.ErrorIs(t, <-finished, fsidentity.ErrDirectoryIdentityChanged)
		volumes.requireClosedPins(t)
		require.Empty(t, b.volumeAccess.active)
		require.Empty(t, b.volumeAccess.scopes)
	})
}
