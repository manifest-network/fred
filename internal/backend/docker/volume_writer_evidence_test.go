package docker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// tenantLinkedVolumeSource plants, inside leaseUUID's managed volume under
// root, the links a running workload can create in its own volume (ENG-2181):
// data/a -> b/l and data/b/l -> target. It returns the bind source Docker
// reports for a mount of data/a joined with tail.
//
// The tenant re-points data/b/l after its container started, so the string
// Docker keeps reporting resolves wherever the tenant last chose.
func tenantLinkedVolumeSource(t *testing.T, root, leaseUUID, target string, tail ...string) string {
	t.Helper()
	volume := filepath.Join(root, canonicalVolumeName(leaseUUID, "app", 0))
	require.NoError(t, os.MkdirAll(filepath.Join(volume, "data", "b"), 0o700))
	require.NoError(t, os.Symlink(target, filepath.Join(volume, "data", "b", "l")))
	require.NoError(t, os.Symlink(filepath.Join("b", "l"), filepath.Join(volume, "data", "a")))
	return filepath.Join(append([]string{volume, "data", "a"}, tail...)...)
}

func TestVolumeWriterMountEvidenceStopsAtTheOwnManagedVolumeRoot(t *testing.T) {
	lease, other := uuid.NewString(), uuid.NewString()
	root := filepath.Join("/srv/volumes", canonicalVolumeName(lease, "app", 0))
	for _, tc := range []struct {
		name, leaseUUID, source, want string
	}{
		{"tail below the root is dropped", lease, root + "/data/a/c", root},
		{"writable-path subtree", lease, root + "/" + writablePathSubdir + "/var/cache", root},
		{"the root itself", lease, root, root},
		{"unclean source", lease, root + "/data/../data/./a", root},
		{
			"a tenant-made directory of the same shape does not move the root",
			lease, root + "/data/" + canonicalVolumeName(lease, "app", 1) + "/x", root,
		},
		{"container fred did not launch", "", root + "/data/a/c", root + "/data/a/c"},
		{"malformed lease", "not-a-lease", root + "/data/a/c", root + "/data/a/c"},
		{"non-canonical lease", strings.ToUpper(lease), root + "/data/a/c", root + "/data/a/c"},
		{
			"first managed root belongs to another lease",
			other, root + "/data/" + canonicalVolumeName(other, "app", 0), root + "/data/" + canonicalVolumeName(other, "app", 0),
		},
		{
			"retained namespace",
			lease, "/srv/volumes/" + retainedVolumePrefix + lease + "-app-0/data", "/srv/volumes/" + retainedVolumePrefix + lease + "-app-0/data",
		},
		{"no managed volume in the path", lease, "/var/lib/docker/volumes/cache/_data", "/var/lib/docker/volumes/cache/_data"},
		{"relative source", lease, canonicalVolumeName(lease, "app", 0) + "/data", canonicalVolumeName(lease, "app", 0) + "/data"},
		{"empty source", lease, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, volumeWriterMountEvidence(tc.leaseUUID, tc.source))
		})
	}
}

// The verdicts below are the classifier's own, without the launch workflow
// around them: what a tenant link changes, and for which containers.
func TestProtectedVolumeIgnoresTenantLinksBelowAFredLaunchedVolumeRoot(t *testing.T) {
	storage := t.TempDir()
	protectedPath := filepath.Join(storage, canonicalVolumeName(uuid.NewString(), "app", 0))
	require.NoError(t, os.Mkdir(protectedPath, 0o700))
	protected, err := fsidentity.OpenDirectory(protectedPath)
	require.NoError(t, err)
	defer protected.Close()
	volumes := map[string]protectedVolume{"protected": {root: protected}}

	writer := func(leaseUUID, source string) ContainerInfo {
		return ContainerInfo{
			ContainerID: "writer", LeaseUUID: leaseUUID,
			Mounts: []ContainerMount{{Type: "bind", Source: source}},
		}
	}

	t.Run("loop", func(t *testing.T) {
		lease := uuid.NewString()
		source := tenantLinkedVolumeSource(t, storage, lease, "l", "c")

		_, err := protectedVolumesAffectWriter(volumes, writer("", source))
		require.Error(t, err, "an unvalidated source that cannot be resolved stays uncertain")

		affected, err := protectedVolumesAffectWriter(volumes, writer(lease, source))
		require.NoError(t, err)
		require.False(t, affected)
	})

	t.Run("link to an ancestor of the protected root", func(t *testing.T) {
		lease := uuid.NewString()
		source := tenantLinkedVolumeSource(t, storage, lease, storage)

		affected, err := protectedVolumesAffectWriter(volumes, writer("", source))
		require.NoError(t, err)
		require.True(t, affected, "an unvalidated source is classified where it resolves")

		affected, err = protectedVolumesAffectWriter(volumes, writer(lease, source))
		require.NoError(t, err)
		require.False(t, affected)
	})

	t.Run("a looped tail does not hide a writer of its own protected volume", func(t *testing.T) {
		lease := uuid.NewString()
		source := tenantLinkedVolumeSource(t, storage, lease, "l", "c")
		own, err := fsidentity.OpenDirectory(filepath.Join(storage, canonicalVolumeName(lease, "app", 0)))
		require.NoError(t, err)
		defer own.Close()

		affected, err := protectedVolumesAffectWriter(
			map[string]protectedVolume{"own": {root: own}}, writer(lease, source))
		require.NoError(t, err)
		require.True(t, affected)
	})

	t.Run("a source under another lease's volume is resolved whole", func(t *testing.T) {
		source := tenantLinkedVolumeSource(t, storage, uuid.NewString(), "l", "c")

		_, err := protectedVolumesAffectWriter(volumes, writer(uuid.NewString(), source))
		require.Error(t, err, "fred never emits a bind into another lease's volume")
	})

	t.Run("read-only and non-filesystem mounts are not writers", func(t *testing.T) {
		looped := tenantLinkedVolumeSource(t, storage, uuid.NewString(), "l", "c")

		affected, err := protectedVolumesAffectWriter(volumes, ContainerInfo{
			ContainerID: "reader",
			Mounts: []ContainerMount{
				{Type: "bind", Source: looped, ReadOnly: true},
				{Type: "tmpfs", Source: looped},
			},
		})
		require.NoError(t, err)
		require.False(t, affected)
	})
}

// One lease's workload must not be able to stop another lease's launch by
// re-pointing a link inside its own volume. A container fred did not launch
// keeps the conservative verdict: nothing validated where its source led.
func TestVolumeWriterLinkedSourceOfAnotherLeaseCannotBlockLaunch(t *testing.T) {
	for _, shape := range []struct {
		name string
		// plant returns the source reported for the other container.
		plant func(t *testing.T, root, leaseUUID string) string
	}{
		{"link loop", func(t *testing.T, root, leaseUUID string) string {
			return tenantLinkedVolumeSource(t, root, leaseUUID, "l", "c")
		}},
		{"link to an ancestor of every volume", func(t *testing.T, root, leaseUUID string) string {
			return tenantLinkedVolumeSource(t, root, leaseUUID, root)
		}},
	} {
		for _, launched := range []bool{true, false} {
			name := shape.name + "/unmanaged container"
			if launched {
				name = shape.name + "/container fred launched"
			}
			t.Run(name, func(t *testing.T) {
				f := newWriterRetirementHarness(t)
				other := uuid.NewString()
				candidate := ContainerInfo{
					ContainerID: "other-lease-workload", Status: "running",
					Mounts: []ContainerMount{{
						Type: "bind", Target: "/data/a/c",
						Source: shape.plant(t, f.h.b.cfg.VolumeDataPath, other),
					}},
				}
				if launched {
					candidate.LeaseUUID = other
				}
				f.h.inventory.containers = append(f.h.inventory.containers, candidate)

				f.execute(t)

				if !launched {
					require.Zero(t, f.stops, "classification precedes every retirement")
					require.Zero(t, f.launches, "an unvalidated source keeps the conservative refusal")
					return
				}
				require.Equal(t, 2, f.stops, "retire only this lease's prior writers")
				require.Equal(t, 2, f.removes)
				require.Equal(t, 1, f.launches)
			})
		}
	}
}

// The classifier trusts ContainerInfo.LeaseUUID as "fred launched this
// container", so the inventory may set it only from the pair of labels every
// fred launch writes and no tenant can (the fred. label namespace is reserved
// in manifests and in image metadata).
func TestVolumeWriterInventoryNamesTheLeaseOnlyForFredLaunchedContainers(t *testing.T) {
	lease := uuid.NewString()
	cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/containers/json") {
			return nil, fmt.Errorf("unexpected Docker read: %s", req.URL.Path)
		}
		body, err := json.Marshal([]map[string]any{
			{"Id": "launched", "Labels": map[string]string{LabelManaged: "true", LabelLeaseUUID: lease}},
			{"Id": "unlabelled"},
			{"Id": "lease label alone", "Labels": map[string]string{LabelLeaseUUID: lease}},
			{"Id": "managed label not true", "Labels": map[string]string{LabelManaged: "false", LabelLeaseUUID: lease}},
		})
		require.NoError(t, err)
		return imageSecurityResponse(http.StatusOK, string(body)), nil
	})

	all, err := cli.ListVolumeWriters(t.Context())
	require.NoError(t, err)

	got := make(map[string]string, len(all))
	for _, info := range all {
		got[info.ContainerID] = info.LeaseUUID
	}
	require.Equal(t, map[string]string{
		"launched": lease, "unlabelled": "", "lease label alone": "", "managed label not true": "",
	}, got)
}
