package shared

import (
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/manifest-network/fred/internal/backend"
)

// SKUResourceSnapshot is the immutable resource definition of one SKU at the
// time a durable workflow acquires authority over a lease. Persisting the
// values, rather than resolving the mutable backend configuration during
// recovery, keeps reservations and retained-disk accounting exact after an
// operator renames, removes, or resizes a SKU profile.
//
// A snapshot slice is canonical when it contains exactly one row for every
// distinct SKU referenced by Items and is strictly sorted by SKU. Use
// BuildSKUResourceSnapshot to construct one and ValidateSKUResourceSnapshot at
// every durable decode boundary.
type SKUResourceSnapshot struct {
	SKU           string  `json:"sku"`
	CPUCores      float64 `json:"cpu_cores"`
	MemoryMB      int64   `json:"memory_mb"`
	DiskMB        int64   `json:"disk_mb"`
	ScratchDiskMB int64   `json:"scratch_disk_mb,omitempty"`
}

// Profile returns the resource profile represented by this immutable row.
func (s SKUResourceSnapshot) Profile() SKUProfile {
	return SKUProfile{
		CPUCores: s.CPUCores,
		MemoryMB: s.MemoryMB,
		DiskMB:   s.DiskMB,
	}
}

// EffectiveDiskMB returns the physical disk capacity held by one live
// instance. DiskMB is durable, retainable SKU storage. ScratchDiskMB is the
// separate conservative host-volume ceiling Docker pins and charges for every
// diskless instance; a physical scratch volume is created only when the image
// needs writable-path scaffolding. They are mutually exclusive by construction:
// a stateful instance's writable paths share its durable volume.
func (s SKUResourceSnapshot) EffectiveDiskMB() (int64, error) {
	if err := validateSKUResourceSnapshotRow(s); err != nil {
		return 0, err
	}
	if s.DiskMB > math.MaxInt64-s.ScratchDiskMB {
		return 0, fmt.Errorf("SKU resource snapshot for %q effective disk overflows int64", s.SKU)
	}
	return s.DiskMB + s.ScratchDiskMB, nil
}

// BuildSKUResourceSnapshot resolves every distinct item SKU exactly once and
// returns a canonical snapshot sorted by the original SKU identifier. The
// original identifier is load-bearing: a configured SKU mapping may change
// independently from the profile name it resolved to.
func BuildSKUResourceSnapshot(items []backend.LeaseItem, resolver SKUResolver) ([]SKUResourceSnapshot, error) {
	if resolver == nil {
		return nil, fmt.Errorf("build SKU resource snapshot: resolver is required")
	}

	skus := make(map[string]struct{}, len(items))
	for i, item := range items {
		if item.SKU == "" {
			return nil, fmt.Errorf("build SKU resource snapshot: item %d has an empty SKU", i)
		}
		skus[item.SKU] = struct{}{}
	}
	ordered := make([]string, 0, len(skus))
	for sku := range skus {
		ordered = append(ordered, sku)
	}
	slices.Sort(ordered)

	snapshot := make([]SKUResourceSnapshot, 0, len(ordered))
	for _, sku := range ordered {
		profile, err := resolver(sku)
		if err != nil {
			return nil, fmt.Errorf("build SKU resource snapshot for %q: %w", sku, err)
		}
		row := SKUResourceSnapshot{
			SKU:           sku,
			CPUCores:      profile.CPUCores,
			MemoryMB:      profile.MemoryMB,
			DiskMB:        profile.DiskMB,
			ScratchDiskMB: 0,
		}
		if err := validateSKUResourceSnapshotRow(row); err != nil {
			return nil, fmt.Errorf("build SKU resource snapshot: %w", err)
		}
		snapshot = append(snapshot, row)
	}
	return snapshot, nil
}

// ValidateSKUResourceSnapshot verifies both the resource values and exact,
// canonical coverage of items. Empty items and an empty snapshot are a valid
// empty topology; durable owners that require a non-empty topology enforce that
// separately.
func ValidateSKUResourceSnapshot(items []backend.LeaseItem, snapshot []SKUResourceSnapshot) error {
	expected := make(map[string]struct{}, len(items))
	for i, item := range items {
		if item.SKU == "" {
			return fmt.Errorf("SKU resource snapshot item %d has an empty SKU", i)
		}
		expected[item.SKU] = struct{}{}
	}
	if len(snapshot) != len(expected) {
		return fmt.Errorf(
			"SKU resource snapshot has %d rows, want exactly %d distinct item SKUs",
			len(snapshot), len(expected),
		)
	}

	for i, row := range snapshot {
		if err := validateSKUResourceSnapshotRow(row); err != nil {
			return err
		}
		if i > 0 && snapshot[i-1].SKU >= row.SKU {
			return fmt.Errorf("SKU resource snapshot must be strictly sorted by SKU without duplicates")
		}
		if _, ok := expected[row.SKU]; !ok {
			return fmt.Errorf("SKU resource snapshot contains unreferenced SKU %q", row.SKU)
		}
	}
	return nil
}

func validateSKUResourceSnapshotRow(row SKUResourceSnapshot) error {
	if row.SKU == "" {
		return fmt.Errorf("SKU resource snapshot contains an empty SKU")
	}
	if math.IsNaN(row.CPUCores) || math.IsInf(row.CPUCores, 0) {
		return fmt.Errorf("SKU resource snapshot for %q has non-finite cpu_cores", row.SKU)
	}
	if err := row.Profile().Validate(); err != nil {
		return fmt.Errorf("SKU resource snapshot for %q: %w", row.SKU, err)
	}
	if row.ScratchDiskMB < 0 {
		return fmt.Errorf("SKU resource snapshot for %q has negative scratch_disk_mb", row.SKU)
	}
	if row.DiskMB > 0 && row.ScratchDiskMB > 0 {
		return fmt.Errorf("SKU resource snapshot for %q cannot combine durable and scratch disk", row.SKU)
	}
	return nil
}

// LookupSKUResourceSnapshotRow returns the complete immutable authority for
// sku. Callers doing capacity accounting must use this rather than Profile,
// which intentionally exposes only the durable SKU resources and omits
// ephemeral scratch.
func LookupSKUResourceSnapshotRow(snapshot []SKUResourceSnapshot, sku string) (SKUResourceSnapshot, bool) {
	i := sort.Search(len(snapshot), func(i int) bool {
		return snapshot[i].SKU >= sku
	})
	if i == len(snapshot) || snapshot[i].SKU != sku {
		return SKUResourceSnapshot{}, false
	}
	return snapshot[i], true
}

// LookupSKUResourceSnapshot returns the immutable profile for sku. The input
// must already have passed ValidateSKUResourceSnapshot; canonical ordering makes
// lookup logarithmic and avoids rebuilding a map in recovery/accounting loops.
func LookupSKUResourceSnapshot(snapshot []SKUResourceSnapshot, sku string) (SKUProfile, bool) {
	row, ok := LookupSKUResourceSnapshotRow(snapshot, sku)
	if !ok {
		return SKUProfile{}, false
	}
	return row.Profile(), true
}

// SumSKUResourceSnapshotDiskMB returns the exact durable, retainable disk
// footprint for items using only persisted snapshot authority. It deliberately
// excludes ScratchDiskMB; callers that model physical live capacity use
// EffectiveDiskMB, while retention accounting may add an exact conservatively
// retained scratch-volume name separately. It rejects invalid quantities,
// incomplete snapshots, and int64 overflow so corrupt durable data can never
// wrap a conservative accounting projection into the under-count direction.
func SumSKUResourceSnapshotDiskMB(items []backend.LeaseItem, snapshot []SKUResourceSnapshot) (int64, error) {
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return 0, fmt.Errorf("sum SKU resource snapshot disk: %w", err)
	}
	if err := ValidateSKUResourceSnapshot(items, snapshot); err != nil {
		return 0, fmt.Errorf("sum SKU resource snapshot disk: %w", err)
	}

	var total int64
	for _, item := range items {
		profile, ok := LookupSKUResourceSnapshot(snapshot, item.SKU)
		if !ok {
			// ValidateSKUResourceSnapshot proved exact coverage; retain this guard so
			// future changes cannot turn a broken invariant into an under-count.
			return 0, fmt.Errorf("sum SKU resource snapshot disk: missing SKU %q", item.SKU)
		}
		quantity := int64(item.Quantity)
		if profile.DiskMB > math.MaxInt64/quantity {
			return 0, fmt.Errorf("sum SKU resource snapshot disk: SKU %q quantity multiplication overflows int64", item.SKU)
		}
		term := profile.DiskMB * quantity
		if total > math.MaxInt64-term {
			return 0, fmt.Errorf("sum SKU resource snapshot disk: aggregate overflows int64")
		}
		total += term
	}
	return total, nil
}

// CloneSKUResourceSnapshot returns a copy safe for an opaque durable claim.
func CloneSKUResourceSnapshot(snapshot []SKUResourceSnapshot) []SKUResourceSnapshot {
	return slices.Clone(snapshot)
}
