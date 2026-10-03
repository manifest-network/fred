package provisioner

import (
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// TestFleet_TerminalBudgetDecidesFailedLeaseClose drives the providerd half of
// the ENG-799 contract end to end: signed HTTP inventory, through the real
// identity-bound client and the sealed inventory collector, into the
// reconciler's decision. Only a Failed lease whose backend reports an exhausted
// budget is closed, with the fixed chain reason, and eagerly deprovisioned on
// its own backend. An absent budget (an older backend), an unrecognized
// verdict, and an exhausted verdict on a lease that is not Failed never close,
// however large fail_count is.
func TestFleet_TerminalBudgetDecidesFailedLeaseClose(t *testing.T) {
	// Not parallel: it asserts deltas of a process-wide counter.
	f := newFleet(t, fleetOptions{})
	const owner = 2
	budget := func(verdict backend.TerminalVerdict, count int) *backend.TerminalBudgetObservation {
		return &backend.TerminalBudgetObservation{Verdict: verdict, ConsecutiveFailures: count}
	}
	leases := []struct {
		name      string
		status    backend.ProvisionStatus
		failCount int
		budget    *backend.TerminalBudgetObservation
	}{
		{"lease-exhausted", backend.ProvisionStatusFailed, 3, budget(backend.TerminalVerdictExhausted, 3)},
		{"lease-retry", backend.ProvisionStatusFailed, 50, budget(backend.TerminalVerdictRetry, 2)},
		{"lease-absent", backend.ProvisionStatusFailed, 50, nil},
		{"lease-unknown", backend.ProvisionStatusFailed, 50, budget("close-now", 3)},
		{"lease-ready-exhausted", backend.ProvisionStatusReady, 50, budget(backend.TerminalVerdictExhausted, 3)},
	}
	labels := []string{"exhausted", "retry", "absent", "unknown"}
	before := make(map[string]float64, len(labels))
	for _, label := range labels {
		before[label] = promtestutil.ToFloat64(metrics.ReconcilerTerminalVerdictsTotal.WithLabelValues(label))
	}
	for _, lease := range leases {
		f.addLease(lease.name, billingtypes.LEASE_STATE_ACTIVE)
		f.backendAt(owner).seedProvision(t, lease.name, f.providerUUID, lease.status)
		f.backendAt(owner).seedFailureReport(lease.name, lease.failCount, lease.budget)
	}

	require.NoError(t, f.sweep())

	_, rejected, closed := f.chainCalls()
	assert.Empty(t, rejected)
	assert.Equal(t, []string{fleetLeaseUUID("lease-exhausted")}, closed,
		"only the lease whose backend reported an exhausted budget is closed")
	reason, ok := f.closeReason("lease-exhausted")
	require.True(t, ok)
	assert.Equal(t, "workload failed repeatedly", reason)
	assert.Equal(t, 1, f.backendAt(owner).deprovisionCount("lease-exhausted"),
		"an exhausted lease is deprovisioned eagerly on its own backend")
	assert.Zero(t, f.backendAt(owner).provisionCount("lease-exhausted"),
		"an exhausted lease is never re-provisioned")
	for _, name := range []string{"lease-retry", "lease-absent", "lease-unknown"} {
		assert.Positive(t, f.backendAt(owner).provisionCount(name),
			"%s: a failed lease without an exhausted verdict is re-provisioned", name)
		assert.Zero(t, f.backendAt(owner).deprovisionCount(name), name)
	}
	assert.Zero(t, f.backendAt(owner).provisionCount("lease-ready-exhausted"),
		"a ready lease is left alone whatever its budget says")
	assert.Zero(t, f.backendAt(owner).deprovisionCount("lease-ready-exhausted"))
	for index := 1; index <= 3; index++ {
		if index == owner {
			continue
		}
		for _, lease := range leases {
			assert.Zero(t, f.backendAt(index).provisionCount(lease.name),
				"%s must never be substituted onto backend %d", lease.name, index)
		}
	}
	for _, label := range labels {
		got := promtestutil.ToFloat64(metrics.ReconcilerTerminalVerdictsTotal.WithLabelValues(label)) - before[label]
		assert.Equal(t, 1.0, got, "one failed lease per verdict %s", label)
	}

	// The chain applies the close. A later sweep closes nothing more: the
	// absent and unrecognized verdicts stay non-closing on every pass.
	f.closeLease("lease-exhausted")
	require.NoError(t, f.sweep())
	_, _, closed = f.chainCalls()
	assert.Equal(t, []string{fleetLeaseUUID("lease-exhausted")}, closed)
}
