package api

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func testAPICallbackPair(t *testing.T, id operation.OperationID) placement.CallbackPair {
	t.Helper()
	pair, err := makeAPICallbackPair(id)
	require.NoError(t, err)
	return pair
}

func testAPIBackendRequestSnapshot(t *testing.T) placement.BackendRequestSnapshot {
	t.Helper()
	return testAPIBackendRequestSnapshotFromValues()
}

func testAPIBackendRequestSnapshotFromValues() placement.BackendRequestSnapshot {
	snapshot, err := placement.NewBackendRequestSnapshot(
		"tenant-1", "provider-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1, ServiceName: "app"}},
	)
	if err != nil {
		panic(err)
	}
	return snapshot
}

func makeAPICallbackPair(id operation.OperationID) (placement.CallbackPair, error) {
	pair, err := placement.NewCallbackPair(
		id,
		"https://provider.test/callbacks/provision?operation_id="+id.String(),
		"https://provider.test/callbacks/provision?lifecycle_id="+id.String(),
	)
	return pair, err
}

func testAPICallbackPairFromID(id operation.OperationID) placement.CallbackPair {
	pair, err := makeAPICallbackPair(id)
	if err != nil {
		panic(err)
	}
	return pair
}

func testAPIBackendStorageID(name string) backendidentity.ID {
	digest := sha256.Sum256([]byte("fred-api-test-storage:" + name))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	id, err := backendidentity.Parse(fmt.Sprintf("%x-%x-%x-%x-%x",
		digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]))
	if err != nil {
		panic(err)
	}
	return id
}

func testAPIBackendStorageIDs(names ...string) map[string]backendidentity.ID {
	identities := make(map[string]backendidentity.ID, len(names))
	for _, name := range names {
		identities[name] = testAPIBackendStorageID(name)
	}
	return identities
}

func testAPIEmptyBackends(
	names []string,
	placements map[string]string,
) []string {
	nonempty := make(map[string]struct{}, len(placements))
	for _, backendName := range placements {
		nonempty[backendName] = struct{}{}
	}
	empty := make([]string, 0, len(names))
	for _, backendName := range names {
		if _, present := nonempty[backendName]; !present {
			empty = append(empty, backendName)
		}
	}
	return empty
}

func configureAPIPlacementTopology(
	t *testing.T,
	store *placement.Store,
	names []string,
) {
	t.Helper()
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		names, testAPIBackendStorageIDs(names...),
	))
}
