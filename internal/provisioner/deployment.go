package provisioner

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// This file is providerd's deployment boundary. A workload reaches a backend
// only through a provision or a restore. Both are sent from here, by an
// operation that takes a DeployableLease and builds the backend request
// internally, so no caller ever holds a request it could re-target. Only
// BillingPolicy.Classify issues a DeployableLease, so every path that would
// deploy a lease must first prove the lease is not billing-only. Go has no
// file-level visibility, so forbidigo rules in .golangci.yml keep both the
// backend request types and the raw Provision and Restore calls out of every
// other providerd file.

// BillingPolicy classifies leases by the SKUs they bill. A billing-only SKU
// (config billing_only_skus) is billed on chain but never deployed: its
// service is delivered outside Fred. The policy is opaque and immutable.
// Build it with NewBillingPolicy, or with NoBillingOnlySKUs when the provider
// lists none. A nil policy classifies nothing, so a component missing its
// policy can neither deploy a lease nor acknowledge one as billing-only.
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
// backend refuses the SKU it cannot serve and the lease is rejected. A lease
// without items is deployable too, so its backend validation stays as before.
// A nil policy, a nil lease, or a lease without an identity is neither.
func (p *BillingPolicy) Classify(lease *billingtypes.Lease) LeaseClass {
	if p == nil || p.billingOnly == nil || lease == nil || lease.Uuid == "" {
		return LeaseClass{}
	}
	if len(lease.Items) == 0 {
		return LeaseClass{kind: leaseKindDeployable, deployable: DeployableLease{lease: detachLease(lease)}}
	}
	for _, item := range lease.Items {
		if _, billingOnly := p.billingOnly[item.SkuUuid]; !billingOnly {
			return LeaseClass{kind: leaseKindDeployable, deployable: DeployableLease{lease: detachLease(lease)}}
		}
	}
	return LeaseClass{kind: leaseKindBillingOnly}
}

// BillingOnly reports whether the lease is billed but never deployed.
func (c LeaseClass) BillingOnly() bool { return c.kind == leaseKindBillingOnly }

// Deployable returns the capability to deploy the lease. It reports false for
// a billing-only lease and for one that could not be classified.
func (c LeaseClass) Deployable() (DeployableLease, bool) {
	return c.deployable, c.kind == leaseKindDeployable
}

// DeployableLease is a lease the billing policy classified as deployable. Only
// BillingPolicy.Classify issues one, and the provision and restore requests
// are built only from one, so no path can deploy a billing-only lease. It
// holds a detached copy, so later edits to the caller's lease cannot change
// what was classified. The zero value deploys nothing.
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

// errNotDeployable reports a deployment for a lease the billing policy never
// classified as deployable.
var errNotDeployable = errors.New("lease was not classified as deployable")

// provisionDeployable sends a deployable lease's provision request to the
// backend. The request is built and sent here and never leaves this function,
// so the lease the backend receives is exactly the lease the policy
// classified. The payload hash is sent only with the payload it describes.
func provisionDeployable(
	ctx context.Context, client backend.Backend, lease DeployableLease,
	providerUUID, callbackURL string, payload []byte, payloadHash string,
) error {
	if !lease.valid() {
		return errNotDeployable
	}
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.lease.Uuid,
		Tenant:       lease.lease.Tenant,
		ProviderUUID: providerUUID,
		Items:        ExtractLeaseItems(&lease.lease),
		CallbackURL:  callbackURL,
		Payload:      payload,
	}
	if payload != nil && payloadHash != "" {
		req.PayloadHash = payloadHash
	}
	return client.Provision(ctx, req)
}

// RestoreDeployable sends the request that restores the retained data of
// fromLeaseUUID into a deployable lease. A restore deploys a workload just as
// a provision does, so it takes the same capability, and its request likewise
// never leaves this function.
func RestoreDeployable(
	ctx context.Context, client backend.Backend, lease DeployableLease,
	providerUUID, fromLeaseUUID, callbackURL string,
) error {
	if !lease.valid() {
		return errNotDeployable
	}
	return client.Restore(ctx, backend.RestoreRequest{
		LeaseUUID:     lease.lease.Uuid,
		FromLeaseUUID: fromLeaseUUID,
		Tenant:        lease.lease.Tenant,
		ProviderUUID:  providerUUID,
		Items:         ExtractLeaseItems(&lease.lease),
		CallbackURL:   callbackURL,
	})
}
