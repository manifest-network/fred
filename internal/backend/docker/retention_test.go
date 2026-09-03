package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// TestListRetentions_AllStatuses asserts ListRetentions surfaces every retained
// lease UUID regardless of status (active, restoring, reaping) — the all-statuses
// contract the consumer (reconciler restore affinity, ENG-333) relies on. It
// also pins the provider/tenant identity needed by offline repair; projecting
// the value-bearing records must not silently filter by status.
func TestListRetentions_AllStatuses(t *testing.T) {
	b, rs := newBackendWithRetention(t)

	active := retentionEntryFixture("la", "tenant-a", time.Now())
	require.NoError(t, rs.Put(active))

	restoring := retentionEntryFixture("lr", "tenant-a", time.Now())
	restoring.Status = shared.RetentionStatusRestoring
	putRestoringRetention(t, rs, restoring)

	reaping := retentionEntryFixture("lp", "tenant-a", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	require.NoError(t, rs.Put(reaping))

	got, err := b.ListRetentions(context.Background())
	require.NoError(t, err)
	ids := make([]string, 0, len(got))
	for _, r := range got {
		ids = append(ids, r.LeaseUUID)
		assert.Equal(t, "prov-1", r.ProviderUUID)
		assert.Equal(t, "tenant-a", r.Tenant)
	}
	assert.ElementsMatch(t, []string{"la", "lr", "lp"}, ids) // all statuses, like the old List()
}
