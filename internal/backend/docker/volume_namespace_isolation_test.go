package docker

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func namespaceNames(t *testing.T) (managedVolumeName, managedVolumeName) {
	t.Helper()
	first, err := parseManagedVolumeName(canonicalVolumeName(durableCallbackTestLeaseUUID, "app", 0))
	require.NoError(t, err)
	second, err := parseManagedVolumeName(canonicalVolumeName(durableCallbackTestLeaseUUID2, "app", 0))
	require.NoError(t, err)
	return first, second
}

func TestVolumeAccessQueuedNamespaceWriterDoesNotFenceOtherLeases(t *testing.T) {
	first, second := namespaceNames(t)
	synctest.Test(t, func(t *testing.T) {
		var access volumeAccessCoordinator
		release, err := access.retainNamespace(t.Context(), []managedVolumeName{first})
		require.NoError(t, err)
		writer := make(chan error, 1)
		go func() {
			writer <- access.mutateNamespace(t.Context(), []managedVolumeName{first}, func(context.Context) error { return nil })
		}()
		synctest.Wait()
		other, err := access.retainNamespace(t.Context(), []managedVolumeName{second})
		require.NoError(t, err, "a queued destructor must not block an unrelated launch")
		other()
		select {
		case <-writer:
			t.Fatal("same-lease namespace mutation escaped the active launch")
		default:
		}
		// Same-lease readers still respect the queued writer, preventing starvation.
		ctx, cancel := context.WithCancel(t.Context())
		reader := make(chan error, 1)
		go func() {
			done, err := access.retainNamespace(ctx, []managedVolumeName{first})
			if err == nil {
				done()
			}
			reader <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-reader, context.Canceled)
		release()
		require.NoError(t, <-writer)
		require.Empty(t, access.scopes)
	})
}

// The fixture maps two separately scoped manager names to one actual directory.
// Namespace exclusion must use its physical identity as well as lease names.
type aliasedNamespaceVolumes struct {
	*mockVolumeManager
	root string
}

func (v aliasedNamespaceVolumes) PinNamespaceRoot(managedVolumeName) (*fsidentity.Directory, error) {
	return pinManagedNamespaceRoot(v.root)
}

func TestVolumeAccessNamespaceMutationWaitsForPhysicalAlias(t *testing.T) {
	first, second := namespaceNames(t)
	root := t.TempDir()
	identity, err := fsidentity.InspectDirectory(root)
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		b := &Backend{volumes: aliasedNamespaceVolumes{mockVolumeManager: &mockVolumeManager{}, root: root}, volumeLaunches: emptyVolumeLaunchCoordinatorForTest()}
		release, err := b.volumeAccess.retainNamespace(t.Context(), []managedVolumeName{first})
		require.NoError(t, err)
		reserved, err := b.volumeAccess.reserve(t.Context(), []fsidentity.Identity{identity})
		require.NoError(t, err)
		entered := make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			finished <- b.mutateManagedVolumeNamespace(t.Context(), []string{second.value()}, func(context.Context) error {
				close(entered)
				return nil
			})
		}()
		synctest.Wait()
		select {
		case <-entered:
			t.Fatal("a differently named alias mutated a reserved physical root")
		default:
		}
		reserved.release()
		release()
		require.NoError(t, <-finished)
		<-entered
		require.Empty(t, b.volumeAccess.active)
		require.Empty(t, b.volumeAccess.scopes)
	})
}

func TestVolumeAccessRenameCancellationReleasesEveryNamespace(t *testing.T) {
	first, second := namespaceNames(t)
	synctest.Test(t, func(t *testing.T) {
		var access volumeAccessCoordinator
		release, err := access.retainNamespace(t.Context(), []managedVolumeName{second})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- access.mutateNamespace(ctx, []managedVolumeName{second, first, second}, func(context.Context) error {
				t.Error("canceled rename entered its effect")
				return nil
			})
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		other, err := access.retainNamespace(t.Context(), []managedVolumeName{first})
		require.NoError(t, err)
		other()
		release()
		require.Empty(t, access.scopes)
	})
}

func TestVolumeAccessStartupRecoveryKeepsGlobalLayoutExclusion(t *testing.T) {
	first, _ := namespaceNames(t)
	synctest.Test(t, func(t *testing.T) {
		var access volumeAccessCoordinator
		release, err := access.retainNamespace(t.Context(), []managedVolumeName{first})
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- access.recoverNamespace(t.Context(), func(context.Context) error { return nil }) }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("startup remount entered while a launch retained its paths")
		default:
		}
		release()
		require.NoError(t, <-done)
	})
}
