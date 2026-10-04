package provisioner

import (
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/terminalverdict"
)

const planTestLease = "11111111-1111-4111-8111-111111111111"

// planVerdict reads a Failed lease's budget observation exactly as the
// reconciler does. A nil observation is an absent verdict.
func planVerdict(observation *backend.TerminalBudgetObservation) terminalverdict.Verdict {
	return terminalverdict.FromProvision(backend.ProvisionInfo{
		LeaseUUID: planTestLease, Status: backend.ProvisionStatusFailed, TerminalBudget: observation,
	})
}

func exhaustedVerdict() terminalverdict.Verdict {
	return planVerdict(&backend.TerminalBudgetObservation{
		Verdict: backend.TerminalVerdictExhausted, ConsecutiveFailures: 3,
	})
}

func TestPlanLease_DecisionTable(t *testing.T) {
	t.Parallel()

	exhaustion, ok := exhaustedVerdict().Exhausted()
	require.True(t, ok)

	tests := []struct {
		name  string
		facts leaseFacts
		want  leasePlan
	}{
		{
			name: "recordless active lease a retired backend may have held closes as lost",
			facts: leaseFacts{
				authority:          lifecycleAuthorityDurable,
				chain:              billingtypes.LEASE_STATE_ACTIVE,
				recordlessUnproven: true,
			},
			want: leasePlan{
				action: reconcileActionCloseLost, anomaly: true,
				reason: "active lease may have lived on a retired backend's lost storage",
			},
		},
		{
			name: "recordless pending lease still starts after a retirement",
			facts: leaseFacts{
				authority:          lifecycleAuthorityDurable,
				chain:              billingtypes.LEASE_STATE_PENDING,
				recordlessUnproven: true,
			},
			want: leasePlan{action: reconcileActionStart},
		},
		{
			name: "pending payload-free lease starts",
			facts: leaseFacts{
				authority: lifecycleAuthorityDurable,
				chain:     billingtypes.LEASE_STATE_PENDING,
			},
			want: leasePlan{action: reconcileActionStart},
		},
		{
			name: "pending manifest waits for payload",
			facts: leaseFacts{
				authority:   lifecycleAuthorityDurable,
				chain:       billingtypes.LEASE_STATE_PENDING,
				hasMetaHash: true,
				payload:     payloadEvidenceAbsent,
			},
			want: leasePlan{action: reconcileActionWait, reason: "awaiting payload"},
		},
		{
			name: "pending manifest starts with durable payload",
			facts: leaseFacts{
				authority:   lifecycleAuthorityDurable,
				chain:       billingtypes.LEASE_STATE_PENDING,
				hasMetaHash: true,
				payload:     payloadEvidencePresent,
			},
			want: leasePlan{action: reconcileActionStart, withPayload: true},
		},
		{
			name: "payload read uncertainty defers",
			facts: leaseFacts{
				authority:   lifecycleAuthorityDurable,
				chain:       billingtypes.LEASE_STATE_PENDING,
				hasMetaHash: true,
			},
			want: leasePlan{reason: "payload evidence unavailable"},
		},
		{
			name: "pending ready acknowledges",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_PENDING,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusReady,
			},
			want: leasePlan{action: reconcileActionAcknowledge},
		},
		{
			name: "pending ready in-flight defers to callback",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_PENDING,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusReady,
				inFlight:        true,
			},
			want: leasePlan{reason: "callback operation still owns acknowledgement"},
		},
		{
			name: "pending provisioning waits",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_PENDING,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusProvisioning,
			},
			want: leasePlan{action: reconcileActionWait, reason: "backend operation in progress"},
		},
		{
			name: "pending failure rejects",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_PENDING,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
			},
			want: leasePlan{action: reconcileActionReject, reason: "provisioning failed"},
		},
		{
			name: "active absence reprovisions with payload",
			facts: leaseFacts{
				authority: lifecycleAuthorityDurable,
				chain:     billingtypes.LEASE_STATE_ACTIVE,
			},
			want: leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active lease is absent from backend inventory",
			},
		},
		{
			name: "active failure with a retry verdict re-provisions",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				terminal: planVerdict(&backend.TerminalBudgetObservation{
					Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 2,
				}),
			},
			want: leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active provision failed",
			},
		},
		{
			name: "active failure with an absent verdict re-provisions",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				terminal:        planVerdict(nil),
			},
			want: leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active provision failed",
			},
		},
		{
			name: "active failure with an unrecognized verdict re-provisions",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				terminal: planVerdict(&backend.TerminalBudgetObservation{
					Verdict: "closed", ConsecutiveFailures: 3,
				}),
			},
			want: leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active provision failed",
			},
		},
		{
			name: "active failure with an exhausted verdict closes and deprovisions",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				terminal:        exhaustedVerdict(),
			},
			want: leasePlan{
				action:     reconcileActionCloseAndDeprovision,
				anomaly:    true,
				reason:     "workload failure budget exhausted",
				exhaustion: exhaustion,
			},
		},
		{
			name: "an exhausted verdict never closes a lease that is not Failed",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusReady,
				terminal:        exhaustedVerdict(),
			},
			want: leasePlan{action: reconcileActionReconcileCustomDomain},
		},
		{
			name: "a pending failure rejects whatever the verdict",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_PENDING,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				terminal:        exhaustedVerdict(),
			},
			want: leasePlan{action: reconcileActionReject, reason: "provisioning failed"},
		},
		{
			name: "active healthy reconciles custom domain",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusReady,
			},
			want: leasePlan{action: reconcileActionReconcileCustomDomain},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, planLease(tt.facts))
		})
	}
}

func TestPlanLease_MissingAuthorityAlwaysDefers(t *testing.T) {
	t.Parallel()

	chainStates := []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_UNSPECIFIED,
		billingtypes.LEASE_STATE_PENDING,
		billingtypes.LEASE_STATE_ACTIVE,
		billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED,
	}
	statuses := []backend.ProvisionStatus{
		backend.ProvisionStatusUnknown,
		backend.ProvisionStatusProvisioning,
		backend.ProvisionStatusReady,
		backend.ProvisionStatusFailed,
		backend.ProvisionStatusRestarting,
		backend.ProvisionStatusUpdating,
		backend.ProvisionStatusDeprovisioning,
	}

	for _, chainState := range chainStates {
		for _, status := range statuses {
			for _, hasProvision := range []bool{false, true} {
				plan := planLease(leaseFacts{
					chain:           chainState,
					hasProvision:    hasProvision,
					provisionStatus: status,
					terminal:        exhaustedVerdict(),
					hasMetaHash:     true,
					payload:         payloadEvidencePresent,
				})
				require.Equalf(t, reconcileActionDefer, plan.action,
					"chain=%s status=%s hasProvision=%t", chainState, status, hasProvision)
			}
		}
	}
}

func TestLeasePlan_ZeroValueDefers(t *testing.T) {
	t.Parallel()

	var plan leasePlan
	assert.Equal(t, reconcileActionDefer, plan.action)
	assert.False(t, plan.withPayload)
	assert.False(t, plan.anomaly)
}

func FuzzPlanLease_SafetyInvariants(f *testing.F) {
	f.Add(uint8(lifecycleAuthorityNone), uint8(billingtypes.LEASE_STATE_PENDING),
		false, string(backend.ProvisionStatusUnknown), true,
		string(backend.TerminalVerdictRetry), 0, true,
		uint8(payloadEvidencePresent), false)
	f.Add(uint8(lifecycleAuthorityDurable), uint8(billingtypes.LEASE_STATE_ACTIVE),
		true, string(backend.ProvisionStatusFailed), true,
		string(backend.TerminalVerdictExhausted), 3, false,
		uint8(payloadEvidenceUnknown), false)
	f.Add(uint8(lifecycleAuthorityDurable), uint8(billingtypes.LEASE_STATE_ACTIVE),
		true, string(backend.ProvisionStatusFailed), false,
		"", 100, false,
		uint8(payloadEvidenceUnknown), false)

	f.Fuzz(func(t *testing.T, authority, chain uint8, hasProvision bool,
		status string, hasBudget bool, verdict string, consecutive int, hasMetaHash bool,
		payload uint8, inFlight bool,
	) {
		info := backend.ProvisionInfo{LeaseUUID: planTestLease, Status: backend.ProvisionStatus(status)}
		if hasBudget {
			info.TerminalBudget = &backend.TerminalBudgetObservation{
				Verdict: backend.TerminalVerdict(verdict), ConsecutiveFailures: consecutive,
			}
		}
		terminal := terminalverdict.FromProvision(info)
		facts := leaseFacts{
			authority:       lifecycleAuthority(authority),
			chain:           billingtypes.LeaseState(chain),
			hasProvision:    hasProvision,
			provisionStatus: info.Status,
			terminal:        terminal,
			hasMetaHash:     hasMetaHash,
			payload:         payloadEvidence(payload),
			inFlight:        inFlight,
		}

		plan := planLease(facts)
		if facts.authority != lifecycleAuthorityDurable {
			assert.Equal(t, reconcileActionDefer, plan.action,
				"untrusted evidence authorized a lifecycle action")
		}
		if plan.withPayload {
			assert.Equal(t, reconcileActionStart, plan.action,
				"payload flag escaped the start action")
		}
		if plan.action > reconcileActionReconcileCustomDomain {
			t.Fatalf("planner returned unknown action %d", plan.action)
		}
		// ENG-799: the failure-budget close requires an ACTIVE, provisioned,
		// Failed lease with an exhausted verdict, and carries its proof.
		_, exhausted := terminal.Exhausted()
		if plan.action == reconcileActionCloseAndDeprovision {
			assert.Equal(t, lifecycleAuthorityDurable, facts.authority)
			assert.Equal(t, billingtypes.LEASE_STATE_ACTIVE, facts.chain)
			assert.True(t, facts.hasProvision)
			assert.Equal(t, backend.ProvisionStatusFailed, facts.provisionStatus)
			assert.True(t, exhausted, "a close without an exhausted verdict")
			assert.True(t, plan.exhaustion.Valid())
			assert.Equal(t, planTestLease, plan.exhaustion.LeaseUUID())
		} else {
			assert.False(t, plan.exhaustion.Valid(), "only the close carries a close proof")
		}
		if !hasBudget || verdict != string(backend.TerminalVerdictExhausted) || consecutive < 1 {
			assert.NotEqual(t, reconcileActionCloseAndDeprovision, plan.action,
				"an absent, unrecognized or zero-count verdict closed a lease")
		}
	})
}
