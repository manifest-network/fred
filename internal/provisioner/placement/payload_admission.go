package placement

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// A PENDING lease gets the input of its first deployment from exactly one of
// two admissions: a tenant payload or a restore (ENG-2117). A restore redeploys
// the source lease's retained manifest and never reads a payload, so a payload
// stored for a lease a restore owns would be answered 202 yet never deployed,
// and a later re-provision would deploy it in place of the restored manifest.
// Both admissions therefore decide while holding the lease's exact Registry
// claim, which neither can take while the other holds it:
// RestoreCoordinator.ExecuteApplication refuses a target that already holds a
// payload, and PayloadAdmitter.Admit refuses a lease a restore owns. A restore
// keeps owning the lease after it releases the claim, through its Registry
// operation until the callback settles, which is after the lease is
// acknowledged, and through its durable placement, which survives a restart.
//
// Admit is the only tenant path that inserts a lease's payload; an update of
// an ACTIVE lease overwrites it through the maintenance persister. The
// forbidigo rule tagged [payload-insert] pins every raw insert to this file
// and to the reconciler's rebuild of an ACTIVE lease's payload (ENG-1442).

// payloadAdmissionPollInterval paces the retry while other lifecycle work
// holds the lease claim. On a lease that awaits its payload those claims are
// short: the lease-created event releases its claim after one chain read.
const payloadAdmissionPollInterval = 25 * time.Millisecond

// PayloadAdmission is the closed result of PayloadAdmitter.Admit. The zero
// value is invalid and never means admitted.
type PayloadAdmission uint8

const (
	PayloadAdmissionInvalid PayloadAdmission = iota
	// PayloadAdmitted: the payload is stored. The caller announces it.
	PayloadAdmitted
	// PayloadAlreadyStored: the lease already holds a payload.
	PayloadAlreadyStored
	// PayloadNotStored: no payload store is configured, or it failed the write.
	PayloadNotStored
	// PayloadRestoreOwned: a restore owns the lease. It deploys the source
	// lease's retained manifest and takes no payload.
	PayloadRestoreOwned
	// PayloadLeaseDeployed: the lease already has a confirmed deployment, so a
	// payload can no longer be the input of its first one.
	PayloadLeaseDeployed
	// PayloadPlacementUnusable: the lease's placement awaits operator repair,
	// and nothing is admitted for it until then.
	PayloadPlacementUnusable
	// PayloadLeaseUnavailable: the lease claim was not acquired, because other
	// lifecycle work held it until ctx ended or the provider is shutting down.
	PayloadLeaseUnavailable
)

func (admission PayloadAdmission) String() string {
	switch admission {
	case PayloadAdmitted:
		return "admitted"
	case PayloadAlreadyStored:
		return "already_stored"
	case PayloadNotStored:
		return "not_stored"
	case PayloadRestoreOwned:
		return "restore_owned"
	case PayloadLeaseDeployed:
		return "lease_deployed"
	case PayloadPlacementUnusable:
		return "placement_unusable"
	case PayloadLeaseUnavailable:
		return "lease_unavailable"
	default:
		return "invalid"
	}
}

// PayloadAdmitter is the least-authority capability tenant ingress needs to
// store a payload. It inserts one only through Admit, and cannot route,
// invoke, or settle lifecycle work. Its zero value is invalid.
type PayloadAdmitter struct {
	coordinator *OperationCoordinator
	payloads    *payload.Store
}

// PayloadAdmitter binds admission to the same Store/Registry pair and payload
// store as this coordinator's provisions. A nil payload store yields an
// admitter that stores nothing.
func (authority *ProvisionCoordinator) PayloadAdmitter() PayloadAdmitter {
	if !authority.Valid() {
		return PayloadAdmitter{}
	}
	return PayloadAdmitter{coordinator: authority.coordinator, payloads: authority.payloads}
}

// Admit stores body as leaseUUID's payload unless a restore owns the lease or
// the lease already has a deployment. The caller has verified the lease is
// PENDING on chain and that body matches its meta_hash.
func (admitter PayloadAdmitter) Admit(
	ctx context.Context,
	leaseUUID string,
	body []byte,
) PayloadAdmission {
	if admitter.coordinator == nil || !admitter.coordinator.Valid() ||
		ctx == nil || leaseUUID == "" || len(body) == 0 {
		return PayloadAdmissionInvalid
	}
	if admitter.payloads == nil {
		slog.Warn("payload store not configured, cannot store payload", "lease_uuid", leaseUUID)
		return PayloadNotStored
	}
	ticker := time.NewTicker(payloadAdmissionPollInterval)
	defer ticker.Stop()
	for {
		claimed := admitter.coordinator.operations.TryClaimLeaseNow(leaseUUID)
		if claimed.Acquired() {
			return admitter.admitClaimed(leaseUUID, body, claimed.Claim())
		}
		if claimed.Outcome() != operation.LeaseClaimBusy {
			// The Registry is draining: new lifecycle work is closed.
			return PayloadLeaseUnavailable
		}
		if verdict, decided := admitter.ownerVerdict(leaseUUID); decided {
			return verdict
		}
		select {
		case <-ctx.Done():
			return PayloadLeaseUnavailable
		case <-ticker.C:
		}
	}
}

// ownerVerdict decides a busy claim from the operation that holds the lease,
// if any. A claim held without an operation is transient lease work, including
// a restore that has not yet registered its operation, so the caller waits.
func (admitter PayloadAdmitter) ownerVerdict(leaseUUID string) (PayloadAdmission, bool) {
	metadata, exists := admitter.coordinator.operations.Lookup(leaseUUID)
	if !exists {
		return PayloadAdmissionInvalid, false
	}
	switch metadata.Kind() {
	case operation.KindRestore:
		return PayloadRestoreOwned, true
	case operation.KindProvision:
		// A payload-bearing lease is provisioned only from its stored payload,
		// so a stored one settles the upload now. A provision still waiting for
		// a lost payload is waited out, since an upload is how it is repaired.
		stored, err := admitter.payloads.Has(leaseUUID)
		if err == nil && stored {
			return PayloadAlreadyStored, true
		}
		return PayloadAdmissionInvalid, false
	default:
		return PayloadAdmissionInvalid, true
	}
}

// admitClaimed decides and inserts while holding the exact lease claim, so no
// restore can be admitted for the lease in between.
func (admitter PayloadAdmitter) admitClaimed(
	leaseUUID string,
	body []byte,
	claim operation.LeaseClaim,
) PayloadAdmission {
	defer func() {
		if !admitter.coordinator.operations.ReleaseLease(claim) {
			slog.Error("failed to release payload admission lease claim", "lease_uuid", leaseUUID)
		}
	}()
	current := admitter.coordinator.store.Lookup(leaseUUID)
	switch current.State() {
	case StateAbsent:
	case StateAttempting:
		if current.AttemptMetadata().Kind() == operation.KindRestore {
			return PayloadRestoreOwned
		}
		// A provision attempt is redelivered with its exact stored payload.
		// If that payload was lost, an upload of the same bytes repairs it.
	case StateConfirmed:
		return PayloadLeaseDeployed
	case StateUnusable:
		return PayloadPlacementUnusable
	default:
		return PayloadPlacementUnusable
	}
	if admitter.payloads.Store(leaseUUID, body) {
		return PayloadAdmitted
	}
	// The only other insert, the reconciler's rebuild, also holds this lease's
	// claim, so a payload present now was stored before it.
	if stored, err := admitter.payloads.Has(leaseUUID); err == nil && stored {
		return PayloadAlreadyStored
	}
	return PayloadNotStored
}
