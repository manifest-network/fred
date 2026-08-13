package docker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// errComposeDown is the canonical "compose gave up part-way" failure. Under compose
// v5 this is what a teardown looks like when the errgroup's derived context cancels
// the removals still in flight (pkg/compose/down.go), which is the failure that
// leaves containers — and their anonymous volumes — behind.
var errComposeDown = errors.New("compose down canceled after first removal failed")

// failingDown returns a mockComposeExecutor whose Down always fails.
func failingDown() *mockComposeExecutor {
	return &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return errComposeDown },
	}
}

// managedContainer builds the ListManagedContainers shape the discovery path reads.
func managedContainer(id, leaseUUID string) ContainerInfo {
	return ContainerInfo{ContainerID: id, LeaseUUID: leaseUUID, SKU: "docker-small"}
}

// TestTeardownLeaseContainers_DownSucceeds_SkipsDiscovery pins that the hot path pays
// nothing for the fallback. compose Down is itself a label sweep, so re-listing every
// container on the daemon after a SUCCESSFUL Down would tax every lease close to
// re-derive what compose just did.
//
// The assertion is the mock's own strictness: mockDockerClient panics on an
// unconfigured ListManagedContainers/RemoveContainer, so leaving both hooks nil makes
// any call on this path fail the test.
func TestTeardownLeaseContainers_DownSucceeds_SkipsDiscovery(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	remaining, err := b.teardownLeaseContainers(context.Background(), "lease-1",
		[]string{"c-recorded"}, time.Second, teardownOpDeprovision, slog.Default())

	require.NoError(t, err, "a successful compose Down is a successful teardown")
	assert.Empty(t, remaining, "nothing can remain when Down reported success")
}

// TestTeardownLeaseContainers_DownFails_RemovesDiscoveredAndRecorded is the core
// ENG-647 behaviour: the fallback finds containers the RECORD does not name.
//
// A failed restore's provision carries no ContainerIDs at all (Restore reserves it
// empty; only the success paths fill it in), so a fallback that walks the recorded
// list removes nothing precisely when a container leaked. Discovery by fred label is
// what closes that, and it is the same identity recoverState rebuilds state from.
func TestTeardownLeaseContainers_DownFails_RemovesDiscoveredAndRecorded(t *testing.T) {
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				managedContainer("c-discovered-0", "lease-1"),
				managedContainer("c-discovered-1", "lease-1"),
			}, nil
		},
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
	}
	b := newBackendForTest(mock, nil)
	b.compose = failingDown()

	// "c-recorded" is known to the caller but absent from the listing (labels stripped
	// out-of-band); the union must still reap it.
	remaining, err := b.teardownLeaseContainers(context.Background(), "lease-1",
		[]string{"c-recorded", "c-discovered-0"}, time.Second, teardownOpRestoreReconcile, slog.Default())

	require.NoError(t, err, "every container was removed, so the teardown succeeded")
	assert.Empty(t, remaining)
	assert.ElementsMatch(t, []string{"c-recorded", "c-discovered-0", "c-discovered-1"}, removed,
		"the fallback must cover the union of recorded and label-discovered containers, deduplicated; "+
			"a container skipped here keeps its anonymous volumes forever (ENG-372)")
}

// TestTeardownLeaseContainers_DownFails_LeavesOtherLeasesAlone pins the filter. The
// listing is host-wide (fred.managed=true), so an unfiltered fallback would tear down
// every tenant on the box the first time one lease's compose Down failed.
func TestTeardownLeaseContainers_DownFails_LeavesOtherLeasesAlone(t *testing.T) {
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				managedContainer("c-mine", "lease-1"),
				managedContainer("c-theirs", "lease-2"),
				managedContainer("c-unlabelled", ""),
			}, nil
		},
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
	}
	b := newBackendForTest(mock, nil)
	b.compose = failingDown()

	_, err := b.teardownLeaseContainers(context.Background(), "lease-1", nil,
		time.Second, teardownOpDeprovision, slog.Default())

	require.NoError(t, err)
	assert.Equal(t, []string{"c-mine"}, removed,
		"only the target lease's containers may be removed")
}

// TestTeardownLeaseContainers_DiscoveryFails_ReturnsError pins the fail-closed
// direction. If we cannot enumerate, we cannot prove the host is clean, and every
// caller's next step (re-quarantine, record revert, pool release, dropping the
// provision) assumes we can.
func TestTeardownLeaseContainers_DiscoveryFails_ReturnsError(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return nil, errors.New("daemon unreachable")
		},
		// RemoveContainerFn deliberately nil: the mock panics if the code removes
		// anything on a listing it could not read.
	}
	b := newBackendForTest(mock, nil)
	b.compose = failingDown()

	remaining, err := b.teardownLeaseContainers(context.Background(), "lease-1",
		[]string{"c-recorded"}, time.Second, teardownOpRestoreReconcile, slog.Default())

	require.Error(t, err, "an unreadable daemon must not be reported as a clean teardown")
	assert.ErrorIs(t, err, errComposeDown, "the originating Down failure stays in the chain")
	assert.Equal(t, []string{"c-recorded"}, remaining,
		"report what we know of as possibly-present; an empty list reads as 'nothing left'")
}

// TestTeardownLeaseContainers_RemovalFails_ReturnsRemaining pins that a partial
// removal is reported as a failure naming exactly the survivors — deprovision writes
// that list back to ContainerIDs so the retry re-attempts only what is stuck.
func TestTeardownLeaseContainers_RemovalFails_ReturnsRemaining(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				managedContainer("c-ok", "lease-1"),
				managedContainer("c-stuck", "lease-1"),
			}, nil
		},
		RemoveContainerFn: func(_ context.Context, id string) error {
			if id == "c-stuck" {
				return errors.New("device or resource busy")
			}
			return nil
		},
	}
	b := newBackendForTest(mock, nil)
	b.compose = failingDown()

	remaining, err := b.teardownLeaseContainers(context.Background(), "lease-1", nil,
		time.Second, teardownOpDeprovision, slog.Default())

	require.Error(t, err)
	assert.Equal(t, []string{"c-stuck"}, remaining,
		"remaining names the survivors only — c-ok was confirmed gone")
}

// TestTeardownLeaseContainers_NeverDestroysManagedVolumes is a data-safety guard.
// Tenant data lives in bind-mounted host directories owned by the volumeManager, and
// RemoveContainer's RemoveVolumes:true reaps a container's ANONYMOUS volumes only.
// Teardown must therefore never reach the volume manager: destroying or renaming a
// managed volume here would be silent tenant data loss.
func TestTeardownLeaseContainers_NeverDestroysManagedVolumes(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{managedContainer("c-1", "lease-1")}, nil
		},
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForTest(mock, nil)
	b.compose = failingDown()
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("teardown must never destroy a managed volume, got %q", id)
			return nil
		},
		RenameVolumeFn: func(old, new string) error {
			t.Errorf("teardown must never rename a managed volume, got %q -> %q", old, new)
			return nil
		},
	}

	_, err := b.teardownLeaseContainers(context.Background(), "lease-1", nil,
		time.Second, teardownOpDeprovision, slog.Default())
	require.NoError(t, err)
}

// TestTeardownLeaseContainers_CountsOutcome pins the operator signal: "the fallback
// ran and saved us" and "the fallback ran and did not" must be distinguishable, since
// only the latter means containers are pinned on the host.
func TestTeardownLeaseContainers_CountsOutcome(t *testing.T) {
	recoveredBefore := testutil.ToFloat64(teardownFallbackTotal.WithLabelValues(teardownOpDeprovision, teardownOutcomeRecovered))
	failedBefore := testutil.ToFloat64(teardownFallbackTotal.WithLabelValues(teardownOpDeprovision, teardownOutcomeFailed))

	listing := func(_ context.Context) ([]ContainerInfo, error) {
		return []ContainerInfo{managedContainer("c-1", "lease-1")}, nil
	}
	okMock := &mockDockerClient{
		ListManagedContainersFn: listing,
		RemoveContainerFn:       func(_ context.Context, _ string) error { return nil },
	}
	bOK := newBackendForTest(okMock, nil)
	bOK.compose = failingDown()
	_, err := bOK.teardownLeaseContainers(context.Background(), "lease-1", nil,
		time.Second, teardownOpDeprovision, slog.Default())
	require.NoError(t, err)

	stuckMock := &mockDockerClient{
		ListManagedContainersFn: listing,
		RemoveContainerFn:       func(_ context.Context, _ string) error { return errors.New("busy") },
	}
	bStuck := newBackendForTest(stuckMock, nil)
	bStuck.compose = failingDown()
	_, err = bStuck.teardownLeaseContainers(context.Background(), "lease-1", nil,
		time.Second, teardownOpDeprovision, slog.Default())
	require.Error(t, err)

	assert.InDelta(t, recoveredBefore+1,
		testutil.ToFloat64(teardownFallbackTotal.WithLabelValues(teardownOpDeprovision, teardownOutcomeRecovered)), 0.0001)
	assert.InDelta(t, failedBefore+1,
		testutil.ToFloat64(teardownFallbackTotal.WithLabelValues(teardownOpDeprovision, teardownOutcomeFailed)), 0.0001)
}

// TestRestoringClaimedVolumes_ClaimsOnlyMatchingRestoringRecords pins the guard that
// keeps a close from eating an in-flight restore's data. While a record is restoring,
// the ORIGINAL lease's retained data sits under the NEW lease's canonical names, so a
// close of the new lease sees volumes that look like its own (ENG-647).
func TestRestoringClaimedVolumes_ClaimsOnlyMatchingRestoringRecords(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	// The record we must protect: orig=u1 being restored into u2.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		NewLeaseUUID:        "u2",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web"}},
		RetainedVolumeNames: []string{"fred-retained-u1-web-0"},
	}))
	// An ACTIVE record for another lease: not in flight, claims nothing.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u3",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u3-web-0"},
	}))

	claimed, err := b.restoringClaimedVolumes("u2")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"fred-u2-web-0": true}, claimed,
		"the adopted canonical name under the NEW lease is what a close of u2 would otherwise destroy")

	// A lease with no in-flight restore pointed at it claims nothing.
	other, err := b.restoringClaimedVolumes("u3")
	require.NoError(t, err)
	assert.Empty(t, other)
}
