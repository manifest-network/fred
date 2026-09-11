package docker

import (
	"maps"
	"slices"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// recoveredProvision is a fully-built, NOT-YET-PUBLISHED provision snapshot.
// It is the typed payload the bootstrap paths (recover.go, provision.go)
// construct off-map; it reaches b.provisions only via materialize() at the two
// publish points (the provision reservation and the recover swap). It has no
// method that mutates a map-resident *provision, so bootstrap code cannot
// publish a half-built pointer or hold a writable handle to a published
// *provision (ENG-193, ENG-229 category B).
//
// It embeds leasesm.ProvisionState exactly as *provision does (no
// method-promotion downside — ProvisionState has no methods), so materialize is
// a trivial struct copy. The construction literals carry a //exhaustruct:enforce
// directive (exhaustruct runs in directive-only mode — exclude: ['.*'] in
// .golangci.yml), so a newly-added ProvisionState/wrapper field forces the
// ANNOTATED publish-input literals (the reservation, the recover build loop,
// materialize, recoveredFromProvision) to set it. A new, UN-annotated
// construction site is not field-checked — it must carry its own
// //exhaustruct:enforce to get the same protection.
type recoveredProvision struct {
	leasesm.ProvisionState
}

// materialize is the ONLY function that turns recovery/creation data into a
// heap *provision. Caller publishes the result into b.provisions under
// provisionsMu.
func (rec recoveredProvision) materialize() *provision {
	state := rec.ProvisionState
	state.ResourceProfiles = shared.CloneSKUResourceSnapshot(rec.ResourceProfiles)
	return &provision{ProvisionState: state} //exhaustruct:enforce
}

// recoveredFromProvision snapshots a live *provision into an off-map value,
// deep-cloning the reference fields (Items, ContainerIDs, ServiceContainers)
// because worker goroutines re-point those headers off-actor; the materialized
// copy must not alias the live struct. Used by recover to rebuild a kept entry
// as a value instead of mutating the published struct in place. slices.Clone
// preserves nil-vs-empty exactly, so a kept entry's reference fields keep the
// same nil-ness they had before normalization (byte-equivalent to the prior
// preserve-by-pointer path).
func recoveredFromProvision(p *provision) recoveredProvision {
	rec := recoveredProvision{ //exhaustruct:enforce
		ProvisionState: p.ProvisionState,
	}
	rec.ResourceProfiles = shared.CloneSKUResourceSnapshot(p.ResourceProfiles)
	rec.Items = slices.Clone(p.Items)
	rec.ContainerIDs = slices.Clone(p.ContainerIDs)
	if p.ServiceContainers != nil {
		sc := make(map[string][]string, len(p.ServiceContainers))
		for k, v := range p.ServiceContainers {
			sc[k] = slices.Clone(v)
		}
		rec.ServiceContainers = sc
	}
	return rec
}

// provisionMatchesRecovered compares a published, lock-protected projection
// with a previously captured deep snapshot. Pointer identity alone cannot
// detect the intentional in-place mutations performed by lease actors. Compare
// the live value directly with the already-cloned baseline: cloning it again
// under provisionsMu would allocate in the fleet-sized exclusive-lock loop.
func provisionMatchesRecovered(p *provision, snapshot recoveredProvision) bool {
	return p != nil && provisionStateMatches(p.ProvisionState, snapshot.ProvisionState)
}

// exactSliceEqual preserves reflect.DeepEqual's nil-versus-empty distinction
// without reflection or allocation. That distinction is part of the recovery
// compare-and-swap: changing a published slice header from nil to a non-nil
// empty slice is still an in-place mutation that must invalidate the snapshot.
func exactSliceEqual[S ~[]E, E comparable](a, b S) bool {
	return (a == nil) == (b == nil) && slices.Equal(a, b)
}

func serviceContainersEqual(a, b map[string][]string) bool {
	return (a == nil) == (b == nil) && maps.EqualFunc(a, b, exactSliceEqual[[]string, string])
}

// provisionStateMatches is deliberately field-explicit and allocation-free:
// changedProvisionRecoveryLeases calls it once per tracked lease while holding
// provisionsMu exclusively. StackManifest is immutable after publication, so
// pointer identity is the complete comparison for that otherwise large tree.
// Keep this list synchronized with leasesm.ProvisionState; the companion test
// pins the complete field vocabulary so an added field cannot pass unnoticed.
func provisionStateMatches(a, b leasesm.ProvisionState) bool {
	return a.LeaseUUID == b.LeaseUUID &&
		a.Tenant == b.Tenant &&
		a.ProviderUUID == b.ProviderUUID &&
		a.SKU == b.SKU &&
		a.Status == b.Status &&
		a.Quantity == b.Quantity &&
		// Recovery CAS compares the exact stored value, including location and
		// monotonic metadata rather than only the represented instant.
		//nolint:staticcheck
		a.CreatedAt == b.CreatedAt &&
		a.FailCount == b.FailCount &&
		a.LastError == b.LastError &&
		a.Reason == b.Reason &&
		a.Message == b.Message &&
		a.CallbackURL == b.CallbackURL &&
		a.LifecycleCallbackURL == b.LifecycleCallbackURL &&
		a.ActiveReleaseVersion == b.ActiveReleaseVersion &&
		a.ActiveOperationID == b.ActiveOperationID &&
		exactSliceEqual(a.Items, b.Items) &&
		exactSliceEqual(a.ResourceProfiles, b.ResourceProfiles) &&
		exactSliceEqual(a.ContainerIDs, b.ContainerIDs) &&
		a.StackManifest == b.StackManifest &&
		serviceContainersEqual(a.ServiceContainers, b.ServiceContainers)
}

// enrichReserved sets the post-validation workload metadata on a reserved
// provision (the slot is a Provisioning marker). It is the ONLY place SKU /
// StackManifest are written outside the actor; the caller holds b.provisionsMu.
//
// Items are deliberately NOT written here. They are the lease's ownership claim
// on its canonical volume names (snapshotVolumeClaims, volume_destroy.go), so
// the reservation publishes them atomically with the entry itself — deferring
// them to this call left every provision's volumes unclaimed for the whole
// validation window, and made the re-provision arm retract a live claim
// (ENG-681). Both publish sites deep-copy, so the published provision never
// aliases the caller's request slice (NormalizeProvisionRequest mutates it in
// place).
func (p *provision) enrichReserved(sku string, sm *manifest.StackManifest) {
	p.SKU = sku
	p.StackManifest = sm
}
