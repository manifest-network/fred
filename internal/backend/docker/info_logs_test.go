package docker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestGetLogs_AggregateByteBudget_BoundsCrossTenantMemory pins ENG-590: GetLogs
// buffers every container's logs into one map, and maxContainerLogBytes caps
// each container individually but NOT the aggregate. A lease may have up to
// maxLeaseQuantity (1024) containers, so without an aggregate budget one
// authenticated GET /logs can materialize gigabytes and OOM the shared docker
// backend host — cross-tenant denial of service. GetLogs must bound the total
// bytes it accumulates across all of a lease's containers.
func TestGetLogs_AggregateByteBudget_BoundsCrossTenantMemory(t *testing.T) {
	const perContainer = 1 << 20 // 1 MiB each, comfortably under the 5 MiB per-container cap
	const containers = 40        // 40 MiB uncapped — well over the aggregate budget
	big := strings.Repeat("x", perContainer)

	cids := make([]string, containers)
	for i := range cids {
		cids[i] = fmt.Sprintf("cid-%d", i)
	}

	mock := &mockDockerClient{
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return big, nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusReady,
			ContainerIDs:      cids,
			ServiceContainers: map[string][]string{"app": cids},
		}},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 100)
	require.NoError(t, err)

	total := 0
	for _, v := range logs {
		total += len(v)
	}
	// Uncapped this is 40 MiB (containers * perContainer). The aggregate budget
	// must bound a single /logs call so a tenant cannot OOM the shared host.
	assert.LessOrEqual(t, total, 33<<20,
		"GetLogs must bound aggregate log bytes across all containers (ENG-590); got %d bytes", total)
	// ...but it must still return substantial content, not drop everything.
	assert.Greater(t, total, 8<<20,
		"aggregate budget should still return real log content, got only %d bytes", total)
}

// TestGetLogs_AggregateBudget_TruncationIsDeterministic pins that when the
// aggregate budget (ENG-590) truncates a multi-service lease, the SAME
// services are truncated on every call. GetLogs ranges the serviceContainers
// map, whose iteration order Go randomizes per range, so without a stable
// ordering which services get real logs vs the truncation placeholder varies
// call-to-call — a confusing experience for a tenant polling /logs.
func TestGetLogs_AggregateBudget_TruncationIsDeterministic(t *testing.T) {
	const perContainer = 4 << 20 // 4 MiB, under the 5 MiB per-container cap
	const services = 12          // 48 MiB total; the 32 MiB budget fits 8 → 4 truncated
	big := strings.Repeat("x", perContainer)

	svcContainers := make(map[string][]string, services)
	var allCids []string
	for i := range services {
		name := fmt.Sprintf("svc-%02d", i)
		cid := fmt.Sprintf("cid-%02d", i)
		svcContainers[name] = []string{cid}
		allCids = append(allCids, cid)
	}

	mock := &mockDockerClient{
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return big, nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusReady,
			ContainerIDs:      allCids,
			ServiceContainers: svcContainers,
		}},
	})

	first, err := b.GetLogs(context.Background(), "lease-1", 100)
	require.NoError(t, err)
	for range 5 {
		again, err := b.GetLogs(context.Background(), "lease-1", 100)
		require.NoError(t, err)
		assert.Equal(t, first, again,
			"GetLogs truncation must be deterministic across calls (which services are truncated must not depend on map iteration order)")
	}
}
