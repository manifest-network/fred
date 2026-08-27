package provisioner

import (
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestPlanLease_DecisionTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts leaseFacts
		want  leasePlan
	}{
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
			name: "active failure retries",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				failCount:       1,
				maxFailures:     3,
			},
			want: leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active provision failed",
			},
		},
		{
			name: "active failure exhaustion closes and deprovisions",
			facts: leaseFacts{
				authority:       lifecycleAuthorityDurable,
				chain:           billingtypes.LEASE_STATE_ACTIVE,
				hasProvision:    true,
				provisionStatus: backend.ProvisionStatusFailed,
				failCount:       3,
				maxFailures:     3,
			},
			want: leasePlan{
				action:  reconcileActionCloseAndDeprovision,
				anomaly: true,
				reason:  "reprovision attempts exhausted",
			},
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
					hasMetaHash:     true,
					payload:         payloadEvidencePresent,
					failCount:       99,
					maxFailures:     1,
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
		false, string(backend.ProvisionStatusUnknown), 0, 3, true,
		uint8(payloadEvidencePresent), false)
	f.Add(uint8(lifecycleAuthorityDurable), uint8(billingtypes.LEASE_STATE_ACTIVE),
		true, string(backend.ProvisionStatusFailed), 3, 3, false,
		uint8(payloadEvidenceUnknown), false)

	f.Fuzz(func(t *testing.T, authority, chain uint8, hasProvision bool,
		status string, failCount, maxFailures int, hasMetaHash bool,
		payload uint8, inFlight bool,
	) {
		facts := leaseFacts{
			authority:       lifecycleAuthority(authority),
			chain:           billingtypes.LeaseState(chain),
			hasProvision:    hasProvision,
			provisionStatus: backend.ProvisionStatus(status),
			failCount:       failCount,
			maxFailures:     maxFailures,
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
	})
}
