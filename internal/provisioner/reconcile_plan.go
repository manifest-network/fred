package provisioner

import (
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/terminalverdict"
)

// lifecycleAuthority is deliberately not a bool. Its conservative zero value
// means the planner has no durable proof that the observed fleet state may
// drive a lifecycle mutation.
type lifecycleAuthority uint8

const (
	lifecycleAuthorityNone lifecycleAuthority = iota
	lifecycleAuthorityDurable
)

// payloadEvidence captures the only payload distinction needed by the planner
// for a new PENDING lease. Unknown includes a store read failure and therefore
// defaults to deferral.
type payloadEvidence uint8

const (
	payloadEvidenceUnknown payloadEvidence = iota
	payloadEvidenceAbsent
	payloadEvidencePresent
)

// reconcileAction is the pure desired action for one lease. Zero is Defer so
// omitted or partially constructed facts can never authorize a side effect.
type reconcileAction uint8

const (
	reconcileActionDefer reconcileAction = iota
	reconcileActionWait
	reconcileActionStart
	reconcileActionAcknowledge
	reconcileActionReject
	reconcileActionCloseAndDeprovision
	reconcileActionReconcileCustomDomain
	// reconcileActionCloseLost closes a live lease whose data may have lived
	// on a retired backend's lost storage, instead of provisioning it empty.
	reconcileActionCloseLost
)

// leaseFacts is the immutable input to planLease. I/O adapters are responsible
// for deriving these facts; the planner never reads stores or calls a backend.
type leaseFacts struct {
	authority lifecycleAuthority
	chain     billingtypes.LeaseState

	hasProvision    bool
	provisionStatus backend.ProvisionStatus
	// terminal is the backend's consecutive-failure verdict (ENG-799). The
	// lifetime FailCount is deliberately not a fact: it never decides a close.
	// The zero value is an absent verdict, which never closes.
	terminal terminalverdict.Verdict

	hasMetaHash bool
	payload     payloadEvidence
	inFlight    bool
	// recordlessUnproven: the lease has no placement row and a retired
	// backend may have held it, so its absence is not "never placed".
	recordlessUnproven bool
}

// leasePlan is deliberately small and closed within this package. withPayload
// only has meaning for reconcileActionStart; anomaly controls accounting for
// ACTIVE drift and failed runtime recovery. exhaustion is the proof the
// failure-budget close requires; only reconcileActionCloseAndDeprovision
// carries one.
type leasePlan struct {
	action      reconcileAction
	withPayload bool
	anomaly     bool
	reason      string
	exhaustion  terminalverdict.Exhaustion
}

// planLease is the level-triggered lease decision table. It is intentionally a
// pure function: persistence/freshness proof is represented by authority, and
// execution must still revalidate the proof with a CAS before any side effect.
func planLease(f leaseFacts) leasePlan {
	if f.authority != lifecycleAuthorityDurable {
		return leasePlan{reason: "lifecycle authority unavailable"}
	}

	switch f.chain {
	case billingtypes.LEASE_STATE_PENDING:
		if !f.hasProvision {
			if !f.hasMetaHash {
				return leasePlan{action: reconcileActionStart}
			}
			switch f.payload {
			case payloadEvidencePresent:
				return leasePlan{action: reconcileActionStart, withPayload: true}
			case payloadEvidenceAbsent:
				return leasePlan{action: reconcileActionWait, reason: "awaiting payload"}
			default:
				return leasePlan{reason: "payload evidence unavailable"}
			}
		}

		switch f.provisionStatus {
		case backend.ProvisionStatusReady:
			if f.inFlight {
				return leasePlan{reason: "callback operation still owns acknowledgement"}
			}
			return leasePlan{action: reconcileActionAcknowledge}
		case backend.ProvisionStatusProvisioning,
			backend.ProvisionStatusRestarting,
			backend.ProvisionStatusUpdating:
			return leasePlan{action: reconcileActionWait, reason: "backend operation in progress"}
		case backend.ProvisionStatusFailed:
			return leasePlan{action: reconcileActionReject, reason: "provisioning failed"}
		default:
			return leasePlan{action: reconcileActionWait, reason: "backend status is not actionable"}
		}

	case billingtypes.LEASE_STATE_ACTIVE:
		if !f.hasProvision && f.recordlessUnproven {
			return leasePlan{
				action:  reconcileActionCloseLost,
				anomaly: true,
				reason:  "active lease may have lived on a retired backend's lost storage",
			}
		}
		if !f.hasProvision {
			return leasePlan{
				action:      reconcileActionStart,
				withPayload: true,
				anomaly:     true,
				reason:      "active lease is absent from backend inventory",
			}
		}
		if f.provisionStatus != backend.ProvisionStatusFailed {
			return leasePlan{action: reconcileActionReconcileCustomDomain}
		}
		// Only the backend's exhausted verdict closes: its tenant workload failed
		// consecutively. An absent or unrecognized verdict re-provisions.
		if exhaustion, exhausted := f.terminal.Exhausted(); exhausted {
			return leasePlan{
				action:     reconcileActionCloseAndDeprovision,
				anomaly:    true,
				reason:     "workload failure budget exhausted",
				exhaustion: exhaustion,
			}
		}
		return leasePlan{
			action:      reconcileActionStart,
			withPayload: true,
			anomaly:     true,
			reason:      "active provision failed",
		}

	default:
		return leasePlan{reason: "chain state is not actionable"}
	}
}
