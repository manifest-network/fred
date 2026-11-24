//go:build integration

package e2e

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are about the image a lease names (G21) and about
// what running one leaves on the daemon (G34). A backend fetches an image
// itself, reads what the image says about itself before it lets Docker have
// it, and from then on runs that exact image. The images come from the
// fixture's own registry, where a test decides their labels and platforms
// and moves their tags.

// What a backend reports for a lease whose image it did not accept.
const (
	reasonImagePullFailed  = "ImagePullFailed"
	messageImagePullFailed = "image pull failed"
)

// A label an image may not carry: it would switch on the host's ingress for
// the container.
var refusedLabel = map[string]string{"traefik.enable": "true"}

// requireRuns asserts that a container was created from the pushed image.
// The daemon must name the image by its identity, which a moved tag cannot
// change, and the container must still say which reference its lease asked
// for.
func requireRuns(t *testing.T, b *Backend, c container.InspectResponse, image PushedImage) {
	t.Helper()
	id := b.ImageID(t, image)
	require.Equal(t, id, c.Image, "the image container %s was created from", c.Name)
	require.Equal(t, id, c.Config.Image, "container %s must be created from an image's identity, not from a tag", c.Name)
	require.Equal(t, image.Reference, c.Config.Labels[labelImageReference], "the reference container %s records", c.Name)
}

// requireImageRefused waits until the backend reports the lease failed
// because of its image, and checks that the backend read that image and let
// nothing of it reach Docker: no container since the lease was applied, of
// any kind, and no image under the reference.
func requireImageRefused(t *testing.T, b *Backend, leaseUUID string, applied time.Time, image PushedImage) {
	t.Helper()
	failed := requireFailed(t, b, leaseUUID)
	require.Equal(t, reasonImagePullFailed, failed.Reason)
	require.Equal(t, messageImagePullFailed, failed.Message)
	require.True(t, b.Registry.Requested(image.Config), "the backend must have read the image's configuration, which holds its labels")
	require.Empty(t, b.Created(t, applied), "no container may be created from a refused image")
	require.False(t, b.HoldsImage(t, image.Reference), "a refused image must not reach the daemon")
}

// closeRefused closes a lease whose image the backend refused. The backend
// stops listing the lease and goes on answering for it with why it failed,
// as for any failed deploy (TestBackend_FailedDeployStaysExplained).
func closeRefused(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	closeLease(t, b, leaseUUID)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		listed, err := b.Client.List(t.Context())
		if assert.NoError(collect, err) {
			for _, lease := range listed {
				assert.NotEqual(collect, leaseUUID, lease.UUID, "the backend still lists the closed lease")
			}
		}
	}, time.Minute, 500*time.Millisecond)
	closed, err := b.Client.Get(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, statusFailed, closed.Status)
	require.Equal(t, reasonImagePullFailed, closed.Reason, "a closed lease must still say why its deploy failed")
}

// TestBackend_ImageWithAReservedLabelIsNeverRun deploys images that carry a
// label the platform keeps for itself: one of the ingress's, one of the
// backend's own and one of Compose's, in the spelling an image chose. Each
// lease must fail for its image, before any container exists, the backend's
// own inspection containers included. An image with an ordinary label, from
// the same registry, runs and passes the label on.
func TestBackend_ImageWithAReservedLabelIsNeverRun(t *testing.T) {
	b := StartBackend(t)

	plain := b.Registry.Push(t, "plain:v1", Image{Labels: map[string]string{"app.owner": "tenant"}})
	accepted := stateless("tenant-a")
	accepted.Manifest = sleeper(plain.Reference)
	require.NoError(t, b.Client.Apply(t.Context(), accepted))
	requireStatus(t, b, accepted.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, accepted.LeaseUUID)
	requireRuns(t, b, workload, plain)
	require.Equal(t, "tenant", workload.Config.Labels["app.owner"], "an image's own labels reach its container")

	for i, key := range []string{"TrAeFiK.enable", "fred.managed", "com.docker.compose.oneoff"} {
		t.Run(key, func(t *testing.T) {
			reserved := b.Registry.Push(t, fmt.Sprintf("reserved-%d:v1", i), Image{Labels: map[string]string{key: "true"}})
			lease := stateless("tenant-a")
			lease.Manifest = sleeper(reserved.Reference)
			applied := time.Now()
			require.NoError(t, b.Client.Apply(t.Context(), lease))
			requireImageRefused(t, b, lease.LeaseUUID, applied, reserved)
			require.Empty(t, b.Containers(t, lease.LeaseUUID))

			closeRefused(t, b, lease.LeaseUUID)
		})
	}
	requireOneRunningContainer(t, b, accepted.LeaseUUID)
}

// TestBackend_ImageCannotClaimAnotherComposeProject deploys an image that
// carries the labels Compose puts on what it builds, naming a project and a
// service of the image's choosing. Compose finds a project's containers by
// those labels, and a container inherits its image's labels unless it is
// given its own. So no container created from the image may carry the
// image's values: not the workload, and not a container the backend inspects
// the image in. The image's other labels are passed on.
func TestBackend_ImageCannotClaimAnotherComposeProject(t *testing.T) {
	b := StartBackend(t)
	const projectLabel = "com.docker.compose.project"
	claimed := map[string]string{
		projectLabel:                       "foreign-" + randomHex(t, 4),
		"com.docker.compose.service":       "foreign-service",
		"com.docker.compose.version":       "foreign-version",
		"com.docker.compose.image.builder": "classic",
	}
	labels := maps.Clone(claimed)
	labels["app.owner"] = "tenant"
	// The volume gives the backend one more reason to inspect the image.
	image := b.Registry.Push(t, "built:v1", Image{Labels: labels, Volumes: []string{"/data"}})

	lease := stateless("tenant-a")
	lease.Manifest = sleeper(image.Reference)
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
	requireRuns(t, b, workload, image)
	require.Equal(t, "tenant", workload.Config.Labels["app.owner"], "an image's own labels reach its container")
	for key, value := range claimed {
		require.NotEqual(t, value, workload.Config.Labels[key], "the workload inherited %s from its image", key)
	}

	helpers, workloads := 0, 0
	for _, created := range b.Created(t, applied) {
		if InspectionHelper(created) {
			helpers++
		} else {
			workloads++
			require.Equal(t, workload.ID, created.ID)
		}
		for key, value := range claimed {
			require.NotEqual(t, value, created.Attributes[key], "container %s was created with %s of its image", created.Attributes["name"], key)
		}
	}
	require.Equal(t, 1, workloads, "the lease's one container")
	require.NotZero(t, helpers, "the backend inspects an image it has not seen in containers of its own")
	require.Empty(t, b.ContainersLabeled(t, projectLabel, claimed[projectLabel]), "nothing on the daemon may answer to the image's project")
}

// TestBackend_LeaseKeepsItsImageWhenItsTagMoves deploys an image by a tag
// and then moves the tag, to an image the backend refuses. The lease was
// given the image the tag named when it was deployed, and it must keep that
// one through everything that creates its container again: a backend
// restart, a new deploy after its container died, and a restore of its data
// into a new lease. A new lease that asks for the tag gets what the tag
// names now, and is refused.
func TestBackend_LeaseKeepsItsImageWhenItsTagMoves(t *testing.T) {
	b := StartBackend(t)
	const name = "app:stable"
	first := b.Registry.Push(t, name, Image{Labels: map[string]string{"app.release": "first"}, Volumes: []string{"/data"}})
	lease := stateful("tenant-a")
	lease.Manifest = sleeper(first.Reference)
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	original := requireOneRunningContainer(t, b, lease.LeaseUUID)
	requireRuns(t, b, original, first)
	requireVolumeBind(t, b, original, "/data")
	b.Exec(t, original.ID, "sh", "-c", "echo kept > /data/sentinel")

	moved := b.Registry.Push(t, name, Image{Labels: refusedLabel, Volumes: []string{"/data"}})
	require.Equal(t, first.Reference, moved.Reference)

	// A new lease gets what the tag names now.
	newcomer := stateless("tenant-b")
	newcomer.Manifest = sleeper(moved.Reference)
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), newcomer))
	refused := requireFailed(t, b, newcomer.LeaseUUID)
	require.Equal(t, reasonImagePullFailed, refused.Reason)
	require.True(t, b.Registry.Requested(moved.Config), "the backend must have read the image the tag names now")
	require.Empty(t, b.Created(t, applied), "no container may be created from a refused image")
	closeRefused(t, b, newcomer.LeaseUUID)

	// A backend restart leaves the lease in its container.
	b.Stop()
	b.Start()
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	now := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, original.ID, now.ID)
	require.Equal(t, original.State.StartedAt, now.State.StartedAt, "the container must not have been restarted")
	requireRuns(t, b, now, first)

	// A new container, after the first died, is created from the same image.
	b.KillContainers(t, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusFailed)
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	now = requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.NotEqual(t, original.ID, now.ID)
	requireRuns(t, b, now, first)
	require.Equal(t, "kept\n", b.Exec(t, now.ID, "cat", "/data/sentinel"))

	// So is the container of the lease its data is restored into.
	closeLease(t, b, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusRetained)
	target := Spec{LeaseUUID: uuid.NewString(), Tenant: lease.Tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(t.Context(), target, lease.LeaseUUID))
	requireStatus(t, b, target.LeaseUUID, statusReady)
	restored := requireOneRunningContainer(t, b, target.LeaseUUID)
	requireRuns(t, b, restored, first)
	require.Equal(t, "kept\n", b.Exec(t, restored.ID, "cat", "/data/sentinel"))
}

// TestBackend_MultiPlatformImageRunsTheLeafThatWasChecked deploys an image
// published for two architectures. The backend must check, and run, the one
// image built for its host: not the index that lists both, and not the other
// architecture's, whatever that one carries and wherever the index lists it.
func TestBackend_MultiPlatformImageRunsTheLeafThatWasChecked(t *testing.T) {
	b := StartBackend(t)
	native := Image{Labels: map[string]string{"app.owner": "native"}}
	foreign := Image{Architecture: ForeignArchitecture(), Labels: map[string]string{"app.owner": "foreign"}}

	t.Run("the other architecture's labels do not count", func(t *testing.T) {
		// The image for the other architecture carries a label the backend
		// refuses, and comes first in the index.
		refused := foreign
		refused.Labels = refusedLabel
		pushed := b.Registry.PushIndex(t, "multi:v1", refused, native)
		other, own := pushed[0], pushed[1]

		lease := stateless("tenant-a")
		lease.Manifest = sleeper(own.Reference)
		require.NoError(t, b.Client.Apply(t.Context(), lease))
		requireStatus(t, b, lease.LeaseUUID, statusReady)
		workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
		requireRuns(t, b, workload, own)
		require.Equal(t, "native", workload.Config.Labels["app.owner"])
		for key := range refusedLabel {
			require.NotContains(t, workload.Config.Labels, key, "the workload carries a label of the other architecture's image")
		}
		require.False(t, b.Registry.Requested(other.Config), "the backend has no use for the other architecture's image")
	})

	t.Run("this host's image is the one checked", func(t *testing.T) {
		refused := native
		refused.Labels = refusedLabel
		pushed := b.Registry.PushIndex(t, "multi:v2", foreign, refused)
		lease := stateless("tenant-a")
		lease.Manifest = sleeper(pushed[1].Reference)
		applied := time.Now()
		require.NoError(t, b.Client.Apply(t.Context(), lease))
		requireImageRefused(t, b, lease.LeaseUUID, applied, pushed[1])
	})
}

// requireNoNewAnonymousVolume asserts that the daemon holds no anonymous
// volume it did not hold before.
func requireNoNewAnonymousVolume(t *testing.T, b *Backend, before []string, when string) {
	t.Helper()
	var added []string
	for _, name := range b.AnonymousVolumes(t) {
		if !slices.Contains(before, name) {
			added = append(added, name)
		}
	}
	require.Empty(t, added, "anonymous volumes on the daemon %s", when)
}

// TestBackend_ClosedLeaseLeavesNoAnonymousVolume runs images that declare
// volumes, on a lease without disk and on leases with disk, and closes them
// (G34). Docker gives a container an anonymous volume for every declared
// volume that nothing else is mounted on, and keeps it after the container
// unless told otherwise: thousands of them once piled up on a host
// (ENG-372). No lease may hold one while it runs, and none may be on the
// daemon after the close, whether the close destroys the lease's data or
// retains it.
func TestBackend_ClosedLeaseLeavesNoAnonymousVolume(t *testing.T) {
	b := StartBackend(t)
	before := b.AnonymousVolumes(t)

	withoutDisk := stateless("tenant-a")
	withoutDisk.Manifest = sleeper(RedisImage)
	withDisk := stateful("tenant-a")
	twoVolumes := stateful("tenant-a")
	twoVolumes.Manifest = sleeper(NestedVolumesImage)
	leases := []Spec{withoutDisk, withDisk, twoVolumes}
	for _, lease := range leases {
		require.NoError(t, b.Client.Apply(t.Context(), lease))
		requireStatus(t, b, lease.LeaseUUID, statusReady)
		workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
		for _, mount := range workload.Mounts {
			require.NotEqual(t, "volume", string(mount.Type), "container %s mounts a Docker volume at %s", workload.Name, mount.Destination)
		}
	}
	requireNoNewAnonymousVolume(t, b, before, "while the leases run")

	closeLease(t, b, withoutDisk.LeaseUUID)
	requireGone(t, b, withoutDisk.LeaseUUID)
	for _, lease := range []Spec{withDisk, twoVolumes} {
		closeLease(t, b, lease.LeaseUUID)
		requireStatus(t, b, lease.LeaseUUID, statusRetained)
	}
	for _, lease := range leases {
		require.Empty(t, b.Containers(t, lease.LeaseUUID))
	}
	requireNoNewAnonymousVolume(t, b, before, "after the leases closed")
}

// TestBackend_ImageInspectionLeavesNoAnonymousVolume deploys an image that
// declares a volume in the two ways that make a backend look into it: for
// who owns the volume's directory, and for the account a user name stands
// for. The backend looks into an image by creating a container from it that
// it never starts, and Docker gives that container the image's volumes too.
// Those containers must be gone once the leases run, and no anonymous volume
// may be left (G34, ENG-372).
func TestBackend_ImageInspectionLeavesNoAnonymousVolume(t *testing.T) {
	b := StartBackend(t)
	before := b.AnonymousVolumes(t)
	started := time.Now()

	byOwner := stateless("tenant-a")
	byOwner.Manifest = sleeper(RedisImage)
	byName := stateless("tenant-a")
	byName.Manifest = map[string]any{"image": RedisImage, "user": "redis", "command": []string{"sleep", "3600"}}
	for _, lease := range []Spec{byOwner, byName} {
		require.NoError(t, b.Client.Apply(t.Context(), lease))
		requireStatus(t, b, lease.LeaseUUID, statusReady)
		requireOneRunningContainer(t, b, lease.LeaseUUID)
	}

	helpers := 0
	for _, created := range b.Created(t, started) {
		if InspectionHelper(created) {
			helpers++
		}
	}
	require.NotZero(t, helpers, "the backend must have inspected the image in containers of its own")
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Empty(collect, b.ContainersLabeled(t, labelInspectionSchema, ""), "inspection containers left on the daemon")
	}, 30*time.Second, 500*time.Millisecond)
	requireNoNewAnonymousVolume(t, b, before, "after the images were inspected")

	for _, lease := range []Spec{byOwner, byName} {
		closeLease(t, b, lease.LeaseUUID)
		requireGone(t, b, lease.LeaseUUID)
	}
	requireNoNewAnonymousVolume(t, b, before, "after the leases closed")
}
