package docker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// This file covers ENG-681: the owner table is a snapshot, so a destroy authorized by it
// must not be able to act on a claim fact that has since changed. Every test here needs a
// deterministic interleaving, which comes from parking a goroutine inside the mock's
// DestroyFn — a test closure that volumeOp.destroy calls from inside its per-name loop,
// after the table is resolved. That is a real seam, not a production hook.
//
// Note the lease UUIDs: liveClaim walks a volume name back to its lease with
// leaseUUIDFromVolumeName, which requires the canonical 36-char form (the same dependency
// cleanupOrphanedVolumes' release probe already has). Tests using placeholder IDs like
// "u1" exercise the cached table only.

// destroyBarrier parks the first Destroy call and releases it on demand. The ctx arm keeps
// a wedged test failing rather than hanging.
type destroyBarrier struct {
	reached chan struct{}
	release chan struct{}
	once    sync.Once

	mu        sync.Mutex
	destroyed []string
}

func newDestroyBarrier() *destroyBarrier {
	return &destroyBarrier{reached: make(chan struct{}), release: make(chan struct{})}
}

func (d *destroyBarrier) destroyFn(ctx context.Context, id string) error {
	d.once.Do(func() {
		close(d.reached)
		select {
		case <-d.release:
		case <-ctx.Done():
		}
	})
	d.mu.Lock()
	defer d.mu.Unlock()
	d.destroyed = append(d.destroyed, id)
	return nil
}

func (d *destroyBarrier) names() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.destroyed...)
}

// TestDestroyReapingVolumes_RefusesANameClaimedAfterTheSnapshot is the ENG-681 regression.
//
// The finalizer resolves the owner table once and then loops. Park it inside the first
// name's Destroy and re-provision the lease underneath it — which is precisely the
// scenario the give-up tombstone creates, since deleting the provision while the lease is
// still ACTIVE on chain makes the reconciler bring it straight back. Before the
// destroy-time re-check, the second name was deleted against a table that had already
// stopped being true, taking a running tenant's data with it.
//
// The first name IS destroyed, deliberately asserted: its Destroy was already in flight
// when the claim appeared, and a "fix" that simply stopped destroying anything must not
// pass this test.
func TestDestroyReapingVolumes_RefusesANameClaimedAfterTheSnapshot(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000101"
	first := canonicalVolumeName(lease, "app", 0)
	second := canonicalVolumeName(lease, "app", 1)

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: []string{first, second},
	}))

	bar := newDestroyBarrier()
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{first, second}, nil },
		DestroyFn: bar.destroyFn,
	}

	ownerBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed))
	refusedBefore := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteReaping, destroyRefusedClaimed))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var reaped bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		reaped = b.destroyReapingVolumes(ctx, b.newManagedVolumeIndex(), lease)
	}()

	<-bar.reached
	// The reconciler re-provisions the still-ACTIVE lease. This is the reservation
	// Provision publishes, claim included (provision.go).
	b.provisionsMu.Lock()
	b.provisions[lease] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusProvisioning, Quantity: 2,
		Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
	}}
	b.provisionsMu.Unlock()
	close(bar.release)
	<-done

	assert.Equal(t, []string{first}, bar.names(),
		"the second name became live-claimed mid-loop and must be refused; the first was already in flight")
	assert.False(t, reaped, "a refused name means the record is kept for retry")
	assert.Equal(t, ownerBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed)),
		"a live provision holds it — the label the stuck-reaping runbook triages on")
	assert.Equal(t, refusedBefore+1, testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteReaping, destroyRefusedClaimed)),
		"the re-check refusal is counted like any other, per volume")

	tomb, err := rs.Get(lease)
	require.NoError(t, err)
	require.NotNil(t, tomb, "the tombstone survives so the surviving name stays counted")
	assert.Equal(t, shared.RetentionStatusReaping, tomb.Status)
}

// TestDestroyReapingVolumes_UnreadableClaims_IgnoresALateClaim is the error-branch twin.
// An unreadable retention store refuses the whole batch before the loop, so no Destroy
// runs at all and there is nothing for a late claim to race — the fail-safe still wins.
func TestDestroyReapingVolumes_UnreadableClaims_IgnoresALateClaim(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000102"
	name := canonicalVolumeName(lease, "app", 0)

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	b.volumes = &mockVolumeManager{
		// Enumerable on disk; only the ownership table is unreadable.
		ListFn: func() ([]string, error) { return []string{name}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when ownership could not be established", id)
			return nil
		},
	}
	unreadableBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	assert.False(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), lease))
	assert.Equal(t, unreadableBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
}

// TestCreateManagedVolume_SerializesAgainstAnInFlightDestroy pins the other half of the
// serialization. volumeManager.Create ADOPTS a pre-existing directory (created=false), so
// a create that ran while a destroy was midway through RemoveAll would mount a tree that
// keeps shrinking under the container. The two must order, not interleave.
func TestCreateManagedVolume_SerializesAgainstAnInFlightDestroy(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000103"
	name := canonicalVolumeName(lease, "app", 0)

	b := newBackendForTest(&mockDockerClient{}, nil)
	bar := newDestroyBarrier()

	var mu sync.Mutex
	var order []string
	b.volumes = &mockVolumeManager{
		defaultDir: t.TempDir(),
		DestroyFn: func(ctx context.Context, id string) error {
			err := bar.destroyFn(ctx, id)
			mu.Lock()
			order = append(order, "destroy:"+id)
			mu.Unlock()
			return err
		},
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			mu.Lock()
			order = append(order, "create:"+id)
			mu.Unlock()
			return t.TempDir(), false, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	destroyDone := make(chan struct{})
	go func() {
		defer close(destroyDone)
		// Nothing claims the name, so the collector is entitled to it.
		b.volumeOp("", b.logger).destroy(ctx, destroySiteOrphanGC, name)
	}()
	<-bar.reached

	createDone := make(chan struct{})
	go func() {
		defer close(createDone)
		_, _, err := b.createManagedVolume(ctx, name, 512)
		assert.NoError(t, err)
	}()

	select {
	case <-createDone:
		t.Fatal("createManagedVolume completed while a destroy of the same name was in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(bar.release)
	<-destroyDone
	<-createDone

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"destroy:" + name, "create:" + name}, order,
		"the stripe must order the two; adopting a half-removed directory is silent data loss")
}

// TestProvision_ReservationPublishesTheOwnershipClaim pins that a lease's claim exists for
// the WHOLE of Provision, not just its tail. A safe re-provision keeps the predecessor
// projection authoritative until its exact cohort is proven absent, then atomically
// publishes the candidate projection and pool generation. Both projections claim the
// same canonical volume names, so the handoff cannot expose reusable tenant data to an
// orphan collector (ENG-681).
func TestProvision_ReservationPublishesTheOwnershipClaim(t *testing.T) {
	const lease = "0192f1a0-4444-7abc-8def-000000000104"
	const candidateOperationID = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	svc := manifest.DefaultServiceName
	oldItems := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: svc}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := shared.OperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://old.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID, "tenant-a", nominalDockerProviderUUID, oldCallbackURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	oldManifest := validManifestJSON("nginx:latest")
	oldContainer := ContainerInfo{
		ContainerID: "container-1", Name: "fred-" + lease + "-app-0", LeaseUUID: lease,
		Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		SKU: "docker-micro", ServiceName: svc, InstanceIndex: 0,
		Image: "nginx:latest", CallbackURL: oldCallbackURL,
		LifecycleCallbackURL: oldLifecycleURL, Status: "exited",
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{oldContainer}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := oldContainer
			return &copy, nil
		},
		RemoveContainerFn: func(ctx context.Context, _ string) error {
			close(reached)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	}
	oldProjection := &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusFailed, Quantity: 1,
			ContainerIDs: []string{oldContainer.ContainerID}, Items: oldItems,
			ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
			CallbackURL:      oldCallbackURL, LifecycleCallbackURL: oldLifecycleURL,
		},
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{lease: oldProjection})
	withMicroSKU(b, 512)
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	nominalIntents := b.operationIntents.(noopOperationIntentJournal)
	b.operationIntents = durableTestOperationIntentJournal{store: nominalIntents.store, storageID: storageID}
	releases := attachReleaseStore(t, b)
	require.NoError(t, releases.AppendActive(lease, shared.Release{
		Manifest: oldManifest, Image: "stack", OperationID: oldOperationID,
		Items: oldItems, ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority, Status: "active", CreatedAt: time.Now().Add(-time.Hour),
	}))
	compose := newNominalProvisionComposeExecutor()
	compose.DownFn = func(context.Context, string, time.Duration) error {
		return errors.New("force strict predecessor fallback")
	}
	b.compose = compose

	req := newProvisionRequest(lease, "tenant-a", "docker-micro", 1, oldManifest)
	req.CallbackURL = "https://new.example/callbacks/provision?operation_id=" + candidateOperationID.String()
	provisionErr := make(chan error, 1)
	go func() { provisionErr <- b.Provision(context.Background(), req) }()

	select {
	case <-reached:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("re-provision did not enter predecessor teardown")
	}
	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)
	claim, claimed := claims.owner(canonicalVolumeName(lease, svc, 0))
	assert.True(t, claimed)
	assert.Equal(t, volumeClaim{kind: claimLive, owner: lease}, claim)
	b.provisionsMu.RLock()
	prov := b.provisions[lease]
	b.provisionsMu.RUnlock()
	assert.Same(t, oldProjection, prov,
		"the predecessor projection remains authoritative until teardown is proven complete")

	close(release)
	require.NoError(t, <-provisionErr)
	claims, err = b.snapshotVolumeClaims()
	require.NoError(t, err)
	claim, claimed = claims.owner(canonicalVolumeName(lease, svc, 0))
	assert.True(t, claimed, "the candidate must take over the volume claim without a gap")
	assert.Equal(t, volumeClaim{kind: claimLive, owner: lease}, claim)

	b.stopCancel()
	b.wg.Wait()
}

func TestLiveClaim(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000105"
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		}},
	})

	tests := []struct {
		name  string
		vol   string
		want  bool
		notes string
	}{
		{"claimed instance 0", canonicalVolumeName(lease, "app", 0), true, ""},
		{"claimed instance 1", canonicalVolumeName(lease, "app", 1), true, ""},
		{"index beyond quantity", canonicalVolumeName(lease, "app", 2), false, "the lease owns 2 instances, not 3"},
		{"different service", canonicalVolumeName(lease, "db", 0), false, ""},
		{"untracked lease", canonicalVolumeName("0192f1a0-4444-7abc-8def-000000000106", "app", 0), false, ""},
		{"retained namespace", retainedName(canonicalVolumeName(lease, "app", 0)), false,
			"a fred-retained- name carries no lease UUID in the position the parser reads"},
		{"not a managed name", "some-other-directory", false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claim, claimed := b.liveClaim(tc.vol)
			assert.Equal(t, tc.want, claimed, tc.notes)
			if tc.want {
				assert.Equal(t, volumeClaim{kind: claimLive, owner: lease}, claim)
			}
		})
	}
}

// The kept-record WARN is operator triage, and the two holds resolve differently: a
// restore-held name clears when that restore rolls back, an owner-held one only when the
// owning lease is next closed. ENG-658 split the metric for exactly that reason; the log
// line kept saying "the restore's rollback resolves it" either way, which is the sentence
// that sends someone to reclaim a running tenant's volume by hand.
func TestDestroyReapingVolumes_KeptRecordLogNamesTheRightHold(t *testing.T) {
	t.Run("held by a live provision", func(t *testing.T) {
		lease := "0192f1a0-4444-7abc-8def-000000000108"
		live := canonicalVolumeName(lease, "app", 0)

		b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
			lease: {ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease, Status: backend.ProvisionStatusReady, Quantity: 1,
				Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			}},
		})
		rs := attachRetentionStore(t, b)
		require.NoError(t, rs.Put(shared.RetentionEntry{
			OriginalLeaseUUID: lease, Tenant: "tenant-a", Status: shared.RetentionStatusReaping,
			RetainedVolumeNames: []string{live},
		}))
		b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return []string{live}, nil }}

		var buf bytes.Buffer
		b.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		require.False(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), lease))

		out := buf.String()
		assert.Contains(t, out, "do NOT reclaim by hand")
		assert.NotContains(t, out, "the restore's rollback resolves it",
			"there is no restore here; naming one sends the runbook after something that does not exist")
	})

	t.Run("held by an in-flight restore", func(t *testing.T) {
		orig := "0192f1a0-1111-7abc-8def-000000000109"
		newLease := "0192f1a0-2222-7abc-8def-000000000110"

		b := newBackendForTest(&mockDockerClient{}, nil)
		rs := attachRetentionStore(t, b)
		adopted, ownLeak := seedClaimedTombstone(t, rs, orig, newLease)
		b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return []string{adopted, ownLeak}, nil }}

		var buf bytes.Buffer
		b.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		require.False(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), newLease))

		assert.Contains(t, buf.String(), "the restore's rollback resolves it",
			"this hold really does clear on rollback, and the operator must be told to wait")
	})
}

// A lease destroying its OWN volumes must sail through the re-check — the close path runs
// while the provision is still tracked, so the fresh read finds exactly the claim the
// asking lease holds. Without this, every deprovision would refuse its own data.
func TestVolumeOp_Destroy_ReCheckPermitsTheOwningLease(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000107"
	name := canonicalVolumeName(lease, "app", 0)

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Status: backend.ProvisionStatusDeprovisioning, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	var destroyed []string
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, id string) error {
		destroyed = append(destroyed, id)
		return nil
	}}

	rep := b.volumeOp(lease, b.logger).destroy(context.Background(), destroySiteDeprovisionDestroy, name)
	assert.Equal(t, []string{name}, destroyed)
	assert.Zero(t, rep.refused())
	assert.False(t, rep.leftOnDisk())
}
