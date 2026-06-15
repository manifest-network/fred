package docker

import (
	"slices"

	"github.com/manifest-network/fred/internal/backend"
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
	volumeCleanupAttempts int
}

// materialize is the ONLY function that turns recovery/creation data into a
// heap *provision. Caller publishes the result into b.provisions under
// provisionsMu.
func (rec recoveredProvision) materialize() *provision {
	return &provision{ //exhaustruct:enforce
		ProvisionState:        rec.ProvisionState,
		VolumeCleanupAttempts: rec.volumeCleanupAttempts,
	}
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
		ProvisionState:        p.ProvisionState,
		volumeCleanupAttempts: p.VolumeCleanupAttempts,
	}
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

// enrichReserved sets the post-validation workload metadata on a reserved
// provision (the slot is a Provisioning marker). It is the ONLY place SKU /
// Items / StackManifest are written outside the actor; the caller holds
// b.provisionsMu. Items is deep-copied so the published provision does not
// alias the caller's request slice (NormalizeProvisionRequest mutates it in
// place).
func (p *provision) enrichReserved(sku string, items []backend.LeaseItem, sm *manifest.StackManifest) {
	p.SKU = sku
	p.Items = slices.Clone(items)
	p.StackManifest = sm
}
