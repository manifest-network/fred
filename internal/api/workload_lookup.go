package api

import (
	"context"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// workloadLookupBatch is a read-only request derived from current placement.
// Confirmed leases go only to their recorded owner. Discovery still queries
// every backend for absent or ambiguous placement; those observations cannot
// promote an owner or grant lifecycle authority.
type workloadLookupBatch struct {
	client backend.Backend
	leases []string
}

// lookup owns both the assigned request and its response projection. Even a
// discovery backend cannot contribute a row assigned only to a confirmed owner.
// Reject the whole off-contract batch so it cannot masquerade as a complete read.
func (batch workloadLookupBatch) lookup(ctx context.Context) ([]backend.ProvisionInfo, error) {
	requested := make(map[string]struct{}, len(batch.leases))
	for _, id := range batch.leases {
		requested[id] = struct{}{}
	}
	rows, err := batch.client.LookupProvisions(ctx, append([]string(nil), batch.leases...))
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if _, ok := requested[row.LeaseUUID]; !ok {
			return nil, fmt.Errorf("workload lookup returned unrequested lease %q", row.LeaseUUID)
		}
	}
	return rows, nil
}

func (h *Handlers) planWorkloadLookup(leases []string) ([]workloadLookupBatch, []string) {
	warnings := []string{}
	if h.backendRouter == nil {
		return nil, warnings
	}
	backends := h.backendRouter.Backends()
	requested := make(map[string][]string, len(backends))
	missing := make(map[string]struct{})
	for _, lease := range leases {
		if h.placementLookup != nil {
			owner := h.placementLookup.Lookup(lease)
			if owner.State() == placement.StateConfirmed && owner.Attempt == "" {
				if h.backendRouter.GetBackendByName(owner.Backend) == nil {
					if _, seen := missing[owner.Backend]; !seen {
						warnings = append(warnings, fmt.Sprintf("backend %q unavailable", owner.Backend))
						missing[owner.Backend] = struct{}{}
					}
				} else {
					requested[owner.Backend] = append(requested[owner.Backend], lease)
				}
				continue
			}
		}
		for _, client := range backends {
			requested[client.Name()] = append(requested[client.Name()], lease)
		}
	}
	batches := make([]workloadLookupBatch, 0, len(requested))
	for _, client := range backends {
		if ids := requested[client.Name()]; len(ids) != 0 {
			batches = append(batches, workloadLookupBatch{client: client, leases: ids})
		}
	}
	return batches, warnings
}
