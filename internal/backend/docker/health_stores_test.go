package docker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// TestHealth_ProbesStores is the F31 regression: Backend.Health must fail when a
// persistence store is unreadable (e.g. a closed/locked bbolt file), not only
// when the Docker daemon is down. Otherwise the backend reports healthy while
// soft-delete/restore silently fail against the broken store.
func TestHealth_ProbesStores(t *testing.T) {
	mock := &mockDockerClient{PingFn: func(context.Context) error { return nil }}
	b := newBackendForProvisionTest(t, mock, nil)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	b.retentionStore = rs

	// Docker reachable + store open → healthy.
	require.NoError(t, b.Health(context.Background()))

	// Closing the store makes read transactions fail → backend must report
	// unhealthy, naming the offending subsystem.
	require.NoError(t, rs.Close())
	err = b.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retention store unhealthy")
}
