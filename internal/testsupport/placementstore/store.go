// Package placementstore contains explicit test-fixture construction for the
// placement authority. Production packages must never import this package.
package placementstore

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	// ProviderUUID is the canonical provider identity bound to fixture stores.
	ProviderUUID = "bc19c267-ddbd-47c8-84ca-c944b9a9c74f"
	seedBackend  = "__fred_test_fixture_seed__"
	seedStorage  = "5cbac2aa-63f3-49eb-a177-bbc9b5c98e38"
)

type emptyChainSnapshot struct {
	providerUUID string
}

func (emptyChainSnapshot) Valid() bool { return true }
func (snapshot emptyChainSnapshot) ProviderUUID() string {
	return snapshot.providerUUID
}
func (emptyChainSnapshot) BlockHeight() int64      { return 1 }
func (emptyChainSnapshot) TotalLeases() int        { return 0 }
func (emptyChainSnapshot) BlockingLeaseCount() int { return 0 }

type legacyChainSnapshot struct {
	providerUUID string
	leaseUUIDs   []string
	leaseItems   map[string][]backend.LeaseItem
}

func (snapshot legacyChainSnapshot) Valid() bool             { return true }
func (snapshot legacyChainSnapshot) ProviderUUID() string    { return snapshot.providerUUID }
func (snapshot legacyChainSnapshot) BlockHeight() int64      { return 1 }
func (snapshot legacyChainSnapshot) TotalLeases() int        { return len(snapshot.leaseUUIDs) }
func (snapshot legacyChainSnapshot) BlockingLeaseCount() int { return 0 }
func (snapshot legacyChainSnapshot) LeaseUUIDs() []string {
	leaseUUIDs := make([]string, len(snapshot.leaseUUIDs))
	copy(leaseUUIDs, snapshot.leaseUUIDs)
	return leaseUUIDs
}
func (snapshot legacyChainSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	items := make(map[string][]backend.LeaseItem, len(snapshot.leaseItems))
	for leaseUUID, leaseItems := range snapshot.leaseItems {
		items[leaseUUID] = append([]backend.LeaseItem(nil), leaseItems...)
	}
	return items
}

// LegacyUpgradeChainProof constructs exact immutable provider-membership
// evidence for tests that deliberately exercise the v0.13 migration boundary.
func LegacyUpgradeChainProof(
	providerUUID string,
	leaseUUIDs ...string,
) (placement.LegacyUpgradeChainProof, error) {
	observed := make([]string, len(leaseUUIDs))
	copy(observed, leaseUUIDs)
	items := make(map[string][]backend.LeaseItem, len(observed))
	for _, leaseUUID := range observed {
		items[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: "app",
		}}
	}
	return placement.NewLegacyUpgradeChainProof(legacyChainSnapshot{
		providerUUID: providerUUID,
		leaseUUIDs:   observed,
		leaseItems:   items,
	})
}

// NewStore explicitly initializes an absent test database through the same
// proof-backed API used by the offline command, or reopens an existing fixture.
// Its removable seed topology carries complete empty-inventory evidence; tests
// remain responsible for configuring their real topology before constructing
// runtime components.
func NewStore(dbPath string, opts ...placement.Option) (*placement.Store, error) {
	return NewStoreForProvider(dbPath, ProviderUUID, opts...)
}

// NewStoreForProvider is NewStore with an explicit provider identity. Use it
// when the surrounding fixture already has a canonical provider UUID: the
// initialized database and every runtime component must share that authority.
func NewStoreForProvider(
	dbPath string,
	providerUUID string,
	opts ...placement.Option,
) (*placement.Store, error) {
	store, err := placement.OpenStore(dbPath, providerUUID, opts...)
	if err == nil {
		return store, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	storageID, err := backendidentity.Parse(seedStorage)
	if err != nil {
		return nil, err
	}
	chainProof, err := placement.NewFreshChainProof(emptyChainSnapshot{
		providerUUID: providerUUID,
	})
	if err != nil {
		return nil, err
	}
	backendProof, err := placement.NewFreshBackendProof(
		[]string{seedBackend},
		map[string]placement.BackendInventory{
			seedBackend: {
				StorageIdentity:        storageID,
				Provisions:             []string{},
				ProvisionProviderUUIDs: map[string]string{},
				ProvisionItems:         map[string][]backend.LeaseItem{},
				Retentions:             []string{},
			},
		},
	)
	if err != nil {
		return nil, err
	}
	target, err := placement.NewFreshInitializationTarget(
		dbPath, providerUUID, []string{seedBackend},
	)
	if err != nil {
		return nil, err
	}
	quiescence, err := placement.ConfirmFreshQuiescence(target, target.Confirmation())
	if err != nil {
		return nil, err
	}
	proofCtx, cancelProof := context.WithTimeout(context.Background(), time.Minute)
	defer cancelProof()
	plan, err := placement.NewFreshInitializationPlan(
		proofCtx,
		target,
		chainProof,
		backendProof,
		quiescence,
	)
	if err != nil {
		return nil, err
	}
	if err := placement.InitializeFreshStoreContext(proofCtx, plan); err != nil &&
		!errors.Is(err, placement.ErrPlacementStoreExists) {
		return nil, err
	}
	return placement.OpenStore(dbPath, providerUUID, opts...)
}
