package placement

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

var (
	ErrRepairInventoryIncomplete = errors.New("repair inventory is incomplete")
	ErrRepairLeasePresent        = errors.New("repair target is present in live inventory")
	ErrRepairConflictEvidence    = errors.New("repair inventory does not prove the selected conflict owner")
)

// RepairBackendInventory is one full identity-bearing backend observation.
// It is raw evidence, not authority; NewRepairInventorySnapshot validates and
// defensively copies it before producing an opaque snapshot.
type RepairBackendInventory struct {
	StorageIdentity backendidentity.ID
	Provisions      []backend.ProvisionInfo
	Retentions      []backend.RetainedLease
}

// RepairInventorySnapshot is an opaque, immutable complete-fleet inventory.
// The zero value is invalid. Only this package can inspect its observations or
// digest, so callers cannot replace a target verdict with a magic boolean.
type RepairInventorySnapshot struct {
	backends    []string
	inventories map[string]RepairBackendInventory
	digest      [sha256.Size]byte
}

// NewRepairInventorySnapshot validates complete topology coverage, canonical
// lease identities, endpoint overlap, and unique storage identities, then
// copies all wire evidence into an opaque snapshot. Candidate-specific absence
// and owner checks intentionally remain in AttemptRepair, which owns the
// durable candidate and is the only component allowed to mint mutation proof.
func NewRepairInventorySnapshot(
	configuredBackends []string,
	inventories map[string]RepairBackendInventory,
) (RepairInventorySnapshot, error) {
	if len(configuredBackends) == 0 {
		return RepairInventorySnapshot{}, fmt.Errorf("%w: configured topology is empty", ErrRepairInventoryIncomplete)
	}
	backends := slices.Clone(configuredBackends)
	for _, name := range backends {
		if strings.TrimSpace(name) == "" {
			return RepairInventorySnapshot{}, fmt.Errorf("%w: backend name is blank", ErrRepairInventoryIncomplete)
		}
	}
	slices.Sort(backends)
	if len(slices.Compact(slices.Clone(backends))) != len(backends) {
		return RepairInventorySnapshot{}, fmt.Errorf("%w: configured topology contains duplicate names", ErrRepairInventoryIncomplete)
	}
	if len(inventories) != len(backends) {
		return RepairInventorySnapshot{}, fmt.Errorf("%w: inventory count does not match configured topology", ErrRepairInventoryIncomplete)
	}

	storageOwners := make(map[backendidentity.ID]string, len(backends))
	cloned := make(map[string]RepairBackendInventory, len(backends))
	for _, backendName := range backends {
		inventory, exists := inventories[backendName]
		if !exists || !inventory.StorageIdentity.Valid() ||
			inventory.Provisions == nil || inventory.Retentions == nil {
			return RepairInventorySnapshot{}, fmt.Errorf(
				"%w: backend %q lacks complete identity-bearing provision and retention evidence",
				ErrRepairInventoryIncomplete, backendName,
			)
		}
		if owner, duplicate := storageOwners[inventory.StorageIdentity]; duplicate {
			return RepairInventorySnapshot{}, fmt.Errorf(
				"%w: backends %q and %q share storage identity %s",
				ErrRepairInventoryIncomplete, owner, backendName, inventory.StorageIdentity,
			)
		}
		storageOwners[inventory.StorageIdentity] = backendName
		seen := make(map[string]string, len(inventory.Provisions)+len(inventory.Retentions))
		for _, provision := range inventory.Provisions {
			if !canonicalLeaseUUID(provision.LeaseUUID) {
				return RepairInventorySnapshot{}, fmt.Errorf(
					"%w: backend %q provisions contains non-canonical lease %q",
					ErrRepairInventoryIncomplete, backendName, provision.LeaseUUID,
				)
			}
			if previous, duplicate := seen[provision.LeaseUUID]; duplicate {
				return RepairInventorySnapshot{}, fmt.Errorf(
					"%w: backend %q reports lease %q in %s and provisions",
					ErrRepairInventoryIncomplete, backendName, provision.LeaseUUID, previous,
				)
			}
			seen[provision.LeaseUUID] = "provisions"
		}
		for _, retention := range inventory.Retentions {
			if !canonicalLeaseUUID(retention.LeaseUUID) {
				return RepairInventorySnapshot{}, fmt.Errorf(
					"%w: backend %q retentions contains non-canonical lease %q",
					ErrRepairInventoryIncomplete, backendName, retention.LeaseUUID,
				)
			}
			if previous, duplicate := seen[retention.LeaseUUID]; duplicate {
				return RepairInventorySnapshot{}, fmt.Errorf(
					"%w: backend %q reports lease %q in %s and retentions",
					ErrRepairInventoryIncomplete, backendName, retention.LeaseUUID, previous,
				)
			}
			seen[retention.LeaseUUID] = "retentions"
		}
		cloned[backendName] = RepairBackendInventory{
			StorageIdentity: inventory.StorageIdentity,
			Provisions:      cloneRepairProvisions(inventory.Provisions),
			Retentions:      slices.Clone(inventory.Retentions),
		}
	}
	for backendName := range inventories {
		if _, exists := cloned[backendName]; !exists {
			return RepairInventorySnapshot{}, fmt.Errorf(
				"%w: inventory supplied for unconfigured backend %q",
				ErrRepairInventoryIncomplete, backendName,
			)
		}
	}
	digest, err := repairInventoryDigest(backends, cloned)
	if err != nil {
		return RepairInventorySnapshot{}, err
	}
	return RepairInventorySnapshot{
		backends:    backends,
		inventories: cloned,
		digest:      digest,
	}, nil
}

func cloneRepairProvisions(source []backend.ProvisionInfo) []backend.ProvisionInfo {
	cloned := make([]backend.ProvisionInfo, len(source))
	for index, provision := range source {
		cloned[index] = provision
		cloned[index].Items = slices.Clone(provision.Items)
		if provision.ServiceImages != nil {
			cloned[index].ServiceImages = make(map[string]string, len(provision.ServiceImages))
			for service, image := range provision.ServiceImages {
				cloned[index].ServiceImages[service] = image
			}
		}
		if provision.LifecycleGeneration != nil {
			observation := *provision.LifecycleGeneration
			cloned[index].LifecycleGeneration = &observation
		}
	}
	return cloned
}

func repairInventoryDigest(
	backends []string,
	inventories map[string]RepairBackendInventory,
) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(struct {
		Backends    []string                          `json:"backends"`
		Inventories map[string]RepairBackendInventory `json:"inventories"`
	}{Backends: backends, Inventories: inventories})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode complete repair inventory: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func repairLifecycleObservation(observation *backend.LifecycleGenerationObservation) LifecycleObservation {
	if observation == nil {
		return LifecycleObservation{Kind: LifecycleObservationUnknown}
	}
	switch observation.Kind {
	case backend.LifecycleGenerationUnknown:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationUnknown}
		}
	case backend.LifecycleGenerationLegacy:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationLegacy}
		}
	case backend.LifecycleGenerationTyped:
		id, err := lifecycle.ParseID(observation.ID)
		if err == nil {
			return LifecycleObservation{Kind: LifecycleObservationTyped, ID: id}
		}
	case backend.LifecycleGenerationUnusable:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationUnusable}
		}
	}
	return LifecycleObservation{Kind: LifecycleObservationUnusable}
}
