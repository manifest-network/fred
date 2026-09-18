package api

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

type workloadLookupRecorder struct {
	backend.Backend
	name  string
	err   error
	rows  []backend.ProvisionInfo
	mu    sync.Mutex
	calls [][]string
}

func (b *workloadLookupRecorder) Name() string { return b.name }

func (b *workloadLookupRecorder) LookupProvisions(_ context.Context, ids []string) ([]backend.ProvisionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, append([]string(nil), ids...))
	if b.err != nil {
		return nil, b.err
	}
	if b.rows != nil {
		return b.rows, nil
	}
	rows := make([]backend.ProvisionInfo, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, backend.ProvisionInfo{LeaseUUID: id, BackendName: b.name, Status: backend.ProvisionStatusReady})
	}
	return rows, nil
}

func TestWorkloadLookupBindsResponseToAssignedLeaseSet(t *testing.T) {
	for _, extra := range []string{testutil.ValidUUID1, testutil.ValidUUID3} {
		t.Run(extra, func(t *testing.T) {
			owner := &workloadLookupRecorder{name: "owner", rows: []backend.ProvisionInfo{{LeaseUUID: testutil.ValidUUID1}}}
			peer := &workloadLookupRecorder{name: "discovery", rows: []backend.ProvisionInfo{
				{LeaseUUID: testutil.ValidUUID2}, {LeaseUUID: extra},
			}}
			h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: owner}, {Backend: peer}})
			h.placementLookup = &mockPlacementLookup{lookupFunc: func(id string) placement.Placement {
				if id == testutil.ValidUUID1 {
					return placement.Placement{Backend: "owner"}
				}
				return placement.Placement{}
			}}
			status, response := callGetWorkloads(t, h, testutil.ValidUUID1, testutil.ValidUUID2)
			require.Equal(t, http.StatusOK, status)
			require.Len(t, response.Warnings, 1, "off-contract discovery is not a complete observation")
			require.Len(t, response.Workloads, 1, "reject the entire batch containing an unsolicited row")
			require.Equal(t, "owner", response.Workloads[testutil.ValidUUID1].BackendName)
			require.Equal(t, [][]string{{testutil.ValidUUID2}}, peer.calls)
		})
	}
}

func TestWorkloadLookupIsolatesConfirmedOwner(t *testing.T) {
	healthy := &workloadLookupRecorder{name: "healthy"}
	unavailable := &workloadLookupRecorder{name: "unavailable", err: backend.ErrCircuitOpen}
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: healthy}, {Backend: unavailable}})
	h.placementLookup = &mockPlacementLookup{getFunc: func(string) string { return "healthy" }}
	status, response := callGetWorkloads(t, h, testutil.ValidUUID1)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, response.Warnings, "an unrelated unavailable backend cannot taint the confirmed owner's observation")
	require.Equal(t, "healthy", response.Workloads[testutil.ValidUUID1].BackendName)
	require.Equal(t, [][]string{{testutil.ValidUUID1}}, healthy.calls)
	require.Empty(t, unavailable.calls)
}

func TestWorkloadLookupGroupsOnlyRequestedOwners(t *testing.T) {
	first, second := &workloadLookupRecorder{name: "first"}, &workloadLookupRecorder{name: "second"}
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: first}, {Backend: second}})
	h.placementLookup = &mockPlacementLookup{getFunc: func(id string) string {
		if id == testutil.ValidUUID1 {
			return "first"
		}
		return "second"
	}}
	_, response := callGetWorkloads(t, h, testutil.ValidUUID1, testutil.ValidUUID2, testutil.ValidUUID1)
	require.Empty(t, response.Warnings)
	require.Len(t, response.Workloads, 2)
	require.Equal(t, [][]string{{testutil.ValidUUID1}}, first.calls)
	require.Equal(t, [][]string{{testutil.ValidUUID2}}, second.calls)
}

func TestWorkloadLookupRetainsUncertaintyAndMissingOwnerWarnings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner placement.Placement
		query bool
	}{
		{name: "absent", query: true},
		{name: "attempt", owner: placement.Placement{Attempt: "unavailable"}, query: true},
		{name: "confirmed with attempt", owner: placement.Placement{Backend: "healthy", Attempt: "unavailable"}, query: true},
		{name: "conflict", owner: placement.Placement{Backend: "healthy", Conflict: true, ConflictBackends: []string{"healthy", "unavailable"}}, query: true},
		{name: "missing owner", owner: placement.Placement{Backend: "removed"}},
		{name: "unavailable owner", owner: placement.Placement{Backend: "unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			healthy := &workloadLookupRecorder{name: "healthy"}
			unavailable := &workloadLookupRecorder{name: "unavailable", err: errors.New("backend unavailable")}
			h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: healthy}, {Backend: unavailable}})
			h.placementLookup = &mockPlacementLookup{lookupFunc: func(string) placement.Placement { return tc.owner }}
			status, response := callGetWorkloads(t, h, testutil.ValidUUID1)
			require.Equal(t, http.StatusOK, status)
			require.Len(t, response.Warnings, 1, "uncertainty is not a complete observation")
			if tc.query {
				require.Len(t, healthy.calls, 1)
				require.Len(t, unavailable.calls, 1)
			} else {
				require.Empty(t, healthy.calls, "a named unavailable owner must never fall back to another machine")
				require.Empty(t, response.Workloads)
			}
		})
	}
}
