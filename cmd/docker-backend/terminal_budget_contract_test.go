package main

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/terminalverdict"
)

// contractBudgetCase is one lease in the backend's inventory and what
// providerd must conclude from it after the round trip.
type contractBudgetCase struct {
	name      string
	info      backend.ProvisionInfo
	wantLabel string
}

// contractBudgetCases covers every verdict class providerd distinguishes,
// sorted by lease UUID as the keyset walk requires. Only the first may close.
func contractBudgetCases() []contractBudgetCase {
	budget := func(verdict backend.TerminalVerdict, count int) *backend.TerminalBudgetObservation {
		return &backend.TerminalBudgetObservation{Verdict: verdict, ConsecutiveFailures: count}
	}
	return []contractBudgetCase{
		{
			name: "exhausted failed lease",
			info: backend.ProvisionInfo{
				LeaseUUID: "11111111-1111-4111-8111-111111111111", Status: backend.ProvisionStatusFailed,
				FailCount: 9, TerminalBudget: budget(backend.TerminalVerdictExhausted, 3),
			},
			wantLabel: "exhausted",
		},
		{
			name: "retry",
			info: backend.ProvisionInfo{
				LeaseUUID: "22222222-2222-4222-8222-222222222222", Status: backend.ProvisionStatusFailed,
				FailCount: 9, TerminalBudget: budget(backend.TerminalVerdictRetry, 2),
			},
			wantLabel: "retry",
		},
		{
			// An older backend reports fail_count but no budget.
			name: "absent",
			info: backend.ProvisionInfo{
				LeaseUUID: "33333333-3333-4333-8333-333333333333", Status: backend.ProvisionStatusFailed,
				FailCount: 9,
			},
			wantLabel: "absent",
		},
		{
			name: "exhausted contradicts a ready status",
			info: backend.ProvisionInfo{
				LeaseUUID: "44444444-4444-4444-8444-444444444444", Status: backend.ProvisionStatusReady,
				TerminalBudget: budget(backend.TerminalVerdictExhausted, 3),
			},
			wantLabel: "unknown",
		},
		{
			name: "unrecognized verdict",
			info: backend.ProvisionInfo{
				LeaseUUID: "55555555-5555-4555-8555-555555555555", Status: backend.ProvisionStatusFailed,
				TerminalBudget: budget("close-now", 3),
			},
			wantLabel: "unknown",
		},
		{
			name: "exhausted without a counted failure",
			info: backend.ProvisionInfo{
				LeaseUUID: "66666666-6666-4666-8666-666666666666", Status: backend.ProvisionStatusFailed,
				TerminalBudget: budget(backend.TerminalVerdictExhausted, 0),
			},
			wantLabel: "unknown",
		},
	}
}

// TestTerminalBudgetContract_DockerBackendServerToVerdict pins the ENG-799 wire
// contract across the real process boundary: what the docker-backend HTTP
// server reports on every provision read path reaches terminalverdict through
// providerd's real identity-bound HTTP client unchanged, and only an exhausted
// budget on a Failed lease becomes a close proof. The backend half (the
// state machine minting the observation into ProvisionInfo) is pinned by the
// docker package's terminal budget tests; the providerd half (inventory to a
// close) by the reconciler fleet harness.
func TestTerminalBudgetContract_DockerBackendServerToVerdict(t *testing.T) {
	cases := contractBudgetCases()
	infos := make([]backend.ProvisionInfo, 0, len(cases))
	byLease := make(map[string]backend.ProvisionInfo, len(cases))
	for _, tc := range cases {
		infos = append(infos, tc.info)
		byLease[tc.info.LeaseUUID] = tc.info
	}
	mock := &mockBackend{
		ListProvisionsFunc: func(context.Context) ([]backend.ProvisionInfo, error) {
			return slices.Clone(infos), nil
		},
		LookupProvisionsFunc: func(_ context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
			found := make([]backend.ProvisionInfo, 0, len(uuids))
			for _, uuid := range uuids {
				if info, ok := byLease[uuid]; ok {
					found = append(found, info)
				}
			}
			return found, nil
		},
		GetProvisionFunc: func(_ context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
			info, ok := byLease[leaseUUID]
			if !ok {
				return nil, backend.ErrNotProvisioned
			}
			return &info, nil
		},
		VerifyStorageIdentityFunc: func(context.Context) error { return nil },
	}
	identity, err := backendidentity.Parse("a8ff9194-0f55-4a31-854e-5f63b236ef3b")
	require.NoError(t, err)
	server, err := NewIdentityBoundServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize, identity)
	require.NoError(t, err)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
		Name: "budget-contract", BaseURL: httpServer.URL, Secret: testSecret,
	})
	require.NoError(t, err)
	// A page size of two forces a multi-page keyset walk.
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{
		CBFailureThresh: 1, CBTimeout: time.Hour, ProvisionsPageLimit: 2,
	}, lifecycleTestIdentity{identity})
	require.NoError(t, err)

	listed, observed, err := client.ListProvisionsWithIdentity(t.Context())
	require.NoError(t, err)
	assert.Equal(t, identity, observed)
	require.Len(t, listed, len(cases))

	uuids := make([]string, 0, len(cases))
	for _, tc := range cases {
		uuids = append(uuids, tc.info.LeaseUUID)
	}
	lookedUp, err := client.LookupProvisions(t.Context(), uuids)
	require.NoError(t, err)
	require.Len(t, lookedUp, len(cases))

	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := client.GetProvision(t.Context(), tc.info.LeaseUUID)
			require.NoError(t, err)
			for path, info := range map[string]backend.ProvisionInfo{
				"list": listed[index], "lookup": lookedUp[index], "get": *got,
			} {
				require.Equal(t, tc.info.LeaseUUID, info.LeaseUUID, path)
				assert.Equal(t, tc.info.TerminalBudget, info.TerminalBudget,
					"%s: the budget must cross the wire unchanged", path)
				assert.Equal(t, tc.info.FailCount, info.FailCount, "%s: fail_count is unchanged", path)

				verdict := terminalverdict.FromProvision(info)
				assert.Equal(t, tc.wantLabel, verdict.Label(), path)
				proof, exhausted := verdict.Exhausted()
				assert.Equal(t, tc.wantLabel == "exhausted", exhausted, "%s: only exhausted closes", path)
				if exhausted {
					assert.True(t, proof.Valid(), path)
					assert.Equal(t, tc.info.LeaseUUID, proof.LeaseUUID(), path)
					assert.Equal(t, 3, proof.ConsecutiveFailures(), path)
				}
			}
		})
	}
}
