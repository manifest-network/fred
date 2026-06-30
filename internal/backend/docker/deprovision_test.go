package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory proves ENG-410's
// close-time fix: a lease whose container was already gone at on-chain close has release
// history but no provision entry (recoverState rebuilds b.provisions from live containers
// only), so a deprovision RPC hits the !exists short-circuit ~before the terminal
// releaseStore.Delete and leaves a stale "active" record that audit-lease-status flags
// until the 90-day RemoveOlderThan TTL. The short-circuit must still purge that history.
func TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	rel := attachReleaseStore(t, b)
	require.NoError(t, rel.Append("u1", shared.Release{Image: "stack", Status: "active", CreatedAt: time.Now()}))

	// No provision entry for u1 → doDeprovision takes the !exists path.
	require.NoError(t, b.doDeprovision(context.Background(), "u1"))

	releases, err := rel.List("u1")
	require.NoError(t, err)
	assert.Empty(t, releases, "containerless deprovision must purge stranded release history (ENG-410)")
}

// TestDeprovisionGiveUp_WritesReapingTombstone verifies a give-up (max volume
// cleanup attempts) writes a reaping tombstone for the leaked canonical volumes so
// the footprint keeps counting + the sweep auto-retries, instead of a silent
// uncounted leak. ENG-376 site 3.
func TestDeprovisionGiveUp_WritesReapingTombstone(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b) // RetainOnClose stays false → non-retain destroy arm

	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	// The give-up branch returns nil to the actor (it abandons to manual cleanup and
	// fires a failed callback), so do not assert on Deprovision's return value here —
	// the load-bearing assertions are the tombstone + the leak counter below.
	_ = b.Deprovision(context.Background(), "u1")

	// Poll for the reaping tombstone.
	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get("u1")
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone for u1 must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.ElementsMatch(t, []string{"fred-u1-app-0"}, got.RetainedVolumeNames)
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// TestDeprovisionGiveUp_ListFails_RecordsBothNamespaces verifies the recordGiveUpLeak
// fallback (volumes.List error): the tombstone records BOTH the canonical name and the
// fred-retained-* name per item. A retain-path partial rename may have moved a volume
// into the retained namespace before failing, so recording only the canonical name would
// let the sweep "succeed" against the (idempotent) non-existent canonical name, drop the
// tombstone, and leave the fred-retained-* volume on disk and untracked. ENG-376.
func TestDeprovisionGiveUp_ListFails_RecordsBothNamespaces(t *testing.T) {
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return nil, errors.New("statfs EIO") }, // force the fallback
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	_ = b.Deprovision(context.Background(), "u1")

	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get("u1")
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone for u1 must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.ElementsMatch(t,
		[]string{"fred-u1-app-0", "fred-retained-u1-app-0"},
		got.RetainedVolumeNames,
		"fallback must record BOTH the canonical and the fred-retained- name so whichever exists is destroyed before the tombstone is deleted")
}
