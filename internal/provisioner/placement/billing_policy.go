package placement

import (
	"fmt"

	"github.com/google/uuid"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// BillingPolicy classifies leases by the SKUs they bill. A billing-only SKU
// (config billing_only_skus) is billed on chain but never deployed: its
// service is delivered outside Fred. The policy is opaque and immutable, and
// it is bound once into the ExecutionCoordinator, so the event provision,
// reconciliation and restore facets all classify through the same instance.
// Build it with NewBillingPolicy, or with NoBillingOnlySKUs when the provider
// lists none. A nil policy classifies nothing, so it can neither deploy a
// lease nor acknowledge one as billing-only.
type BillingPolicy struct {
	billingOnly map[string]struct{}
}

// NewBillingPolicy builds the policy for config billing_only_skus. Every entry
// must be a lowercase canonical UUID, listed once: leases match by exact
// string against the chain's canonical SKU UUIDs, so any other spelling would
// silently match nothing. An empty list builds the explicit empty policy.
func NewBillingPolicy(billingOnlySKUs []string) (*BillingPolicy, error) {
	set := make(map[string]struct{}, len(billingOnlySKUs))
	for i, sku := range billingOnlySKUs {
		parsed, err := uuid.Parse(sku)
		if err != nil || parsed.String() != sku {
			return nil, fmt.Errorf("billing-only SKU %d: %q is not a lowercase canonical UUID", i, sku)
		}
		if _, duplicate := set[sku]; duplicate {
			return nil, fmt.Errorf("billing-only SKU %d: duplicate SKU %s", i, sku)
		}
		set[sku] = struct{}{}
	}
	return &BillingPolicy{billingOnly: set}, nil
}

// NoBillingOnlySKUs returns the explicit empty policy: every lease is
// deployable.
func NoBillingOnlySKUs() *BillingPolicy {
	return &BillingPolicy{billingOnly: map[string]struct{}{}}
}

func (p *BillingPolicy) valid() bool { return p != nil && p.billingOnly != nil }

// leaseKind is the outcome of classifying one lease. The zero value is
// invalid: it authorizes neither a deployment nor an acknowledgment.
type leaseKind uint8

const (
	leaseKindInvalid leaseKind = iota
	leaseKindDeployable
	leaseKindBillingOnly
)

// LeaseClass is the result of BillingPolicy.Classify. Its zero value is
// neither deployable nor billing-only.
type LeaseClass struct {
	kind       leaseKind
	deployable DeployableLease
}

// Classify sorts a lease. A lease that has items, all of them billing-only,
// is billing-only. Every other lease is deployable, including one that mixes
// billing-only and deployable SKUs: it takes the normal path, where the
// backend's typed refusal of the SKU it cannot serve rejects the lease. A
// lease without items is deployable too, so the request validation that
// refuses it stays as before. A nil policy, a nil lease, or a lease without an
// identity is neither.
func (p *BillingPolicy) Classify(lease *billingtypes.Lease) LeaseClass {
	if !p.valid() || lease == nil || lease.Uuid == "" {
		return LeaseClass{}
	}
	skus := make([]string, 0, len(lease.Items))
	for _, item := range lease.Items {
		skus = append(skus, item.SkuUuid)
	}
	if p.coversAll(skus) {
		return LeaseClass{kind: leaseKindBillingOnly}
	}
	return LeaseClass{kind: leaseKindDeployable, deployable: DeployableLease{lease: detachLease(lease)}}
}

// permits reports whether a backend request with these items may be sent. It
// applies Classify's rule to the request itself, so a request rebuilt from a
// durable snapshot is judged by the current policy rather than by the one it
// was minted under. An invalid policy permits nothing.
func (p *BillingPolicy) permits(items []backend.LeaseItem) bool {
	if !p.valid() {
		return false
	}
	skus := make([]string, 0, len(items))
	for _, item := range items {
		skus = append(skus, item.SKU)
	}
	return !p.coversAll(skus)
}

// coversAll reports whether there is at least one SKU and every one is
// billing-only. A request without items is never billing-only, so the
// validation that refuses it stays as before.
func (p *BillingPolicy) coversAll(skus []string) bool {
	if len(skus) == 0 {
		return false
	}
	for _, sku := range skus {
		if _, billingOnly := p.billingOnly[sku]; !billingOnly {
			return false
		}
	}
	return true
}

// BillingOnly reports whether the lease is billed but never deployed.
func (c LeaseClass) BillingOnly() bool { return c.kind == leaseKindBillingOnly }

// Deployable returns the capability to deploy the lease. It reports false for
// a billing-only lease and for one that could not be classified.
func (c LeaseClass) Deployable() (DeployableLease, bool) {
	return c.deployable, c.kind == leaseKindDeployable
}

// DeployableLease is a lease the billing policy classified as deployable. Only
// BillingPolicy.Classify issues one. Store.MintBackendRequestSnapshot accepts
// nothing else, and every provision and restore request, including one
// redelivered from a durable attempt, is rebuilt from such a snapshot, so no
// path can deploy a billing-only lease. It holds a detached copy, so later
// edits to the caller's lease cannot change what was classified. The zero
// value deploys nothing.
type DeployableLease struct {
	lease billingtypes.Lease
}

// Lease returns a detached copy of the classified lease.
func (d DeployableLease) Lease() billingtypes.Lease { return detachLease(&d.lease) }

func (d DeployableLease) valid() bool { return d.lease.Uuid != "" }

func detachLease(lease *billingtypes.Lease) billingtypes.Lease {
	detached := *lease
	detached.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	detached.MetaHash = append([]byte(nil), lease.MetaHash...)
	return detached
}
