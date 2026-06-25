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
// guards the Keys()-based body's equivalence with the prior List()-based one: a
// keys-only walk must not silently filter by status.
func TestListRetentions_AllStatuses(t *testing.T) {
	b, rs := newBackendWithRetention(t)

	active := retentionEntryFixture("la", "tenant-a", time.Now())
	require.NoError(t, rs.Put(active))

	restoring := retentionEntryFixture("lr", "tenant-a", time.Now())
	restoring.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(restoring))

	reaping := retentionEntryFixture("lp", "tenant-a", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	require.NoError(t, rs.Put(reaping))

	got, err := b.ListRetentions(context.Background())
	require.NoError(t, err)
	ids := make([]string, 0, len(got))
	for _, r := range got {
		ids = append(ids, r.LeaseUUID)
	}
	assert.ElementsMatch(t, []string{"la", "lr", "lp"}, ids) // all statuses, like the old List()
}
