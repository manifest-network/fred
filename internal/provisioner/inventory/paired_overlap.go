package inventory

import "github.com/manifest-network/fred/internal/backendidentity"

// PairedOverlapObservation carries a provision-to-retention overlap from one
// identity-valid backend pair. Its row is available only for checking whether
// existing durable authority already represents it. It cannot establish an
// owner, settle an attempt, or grant lifecycle/absence authority.
type PairedOverlapObservation struct {
	provision ProvisionObservation
}

func (observation PairedOverlapObservation) Provision() ProvisionObservation {
	return observation.provision
}

// PairedOverlap requires the exact current epoch, a pinned physical identity,
// and a sole reporter. Explicit rejection removes this observation during
// construction, including rejection recorded before or after paired reads.
func (snapshot Snapshot) PairedOverlap(
	binding Binding,
	backendName, leaseUUID string,
	storageID backendidentity.ID,
) (PairedOverlapObservation, bool) {
	if !snapshot.ValidFor(binding) || !storageID.Valid() {
		return PairedOverlapObservation{}, false
	}
	provision, provisioned := snapshot.provision[backendName]
	retention, retained := snapshot.retention[backendName]
	if !provisioned || !retained || provision.storageID != storageID || retention.storageID != storageID {
		return PairedOverlapObservation{}, false
	}
	row, overlaps := snapshot.overlaps[backendName][leaseUUID]
	reporters := snapshot.LeaseReporters(binding, leaseUUID)
	if !overlaps || len(reporters) != 1 || reporters[0] != backendName {
		return PairedOverlapObservation{}, false
	}
	return PairedOverlapObservation{provision: row}, true
}
