package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// ProvisionManifestAdmission owns either a strictly admitted new payload or an
// exact historical payload derived from this journal's current active release.
// A historical admission carries its private release fence through acceptance;
// no caller flag can select stored-policy validation.
type ProvisionManifestAdmission struct {
	issuer  *OperationSettlement
	payload []byte
	replay  *provisionManifestReplay
}

type provisionManifestReplay struct {
	source   ReleaseClaim
	tenant   string
	provider string
	items    []backend.LeaseItem
}

// AdmitProvisionManifest keeps new submissions on current tenant policy. A
// historical exception is available only for the identical active payload,
// principal and topology, observed under the journal's lease transition lock.
func (s *OperationSettlement) AdmitProvisionManifest(ctx context.Context, lease, tenant, provider string, items []backend.LeaseItem, payload []byte) (ProvisionManifestAdmission, error) {
	if ctx == nil || s == nil || !s.valid() {
		return ProvisionManifestAdmission{}, errors.New("provision manifest admission requires an open journal pair")
	}
	if err := ctx.Err(); err != nil {
		return ProvisionManifestAdmission{}, err
	}
	payload = slices.Clone(payload)
	items = slices.Clone(items)
	if _, err := manifest.ParsePayload(payload); err == nil {
		return ProvisionManifestAdmission{issuer: s, payload: payload}, nil
	} else {
		policyErr := fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
		unlock, lockErr := s.lockLeaseContext(ctx, lease)
		if lockErr != nil {
			return ProvisionManifestAdmission{}, lockErr
		}
		defer unlock()
		active, readErr := s.releases.LatestActive(lease)
		if readErr != nil {
			return ProvisionManifestAdmission{}, readErr
		}
		if active == nil {
			return ProvisionManifestAdmission{}, fmt.Errorf("manifest requires current admission: %w", policyErr)
		}
		release, source, readErr := s.releases.claimLatestActive(lease)
		if readErr != nil {
			return ProvisionManifestAdmission{}, readErr
		}
		requested, storedErr := manifest.ParseStoredPayload(payload)
		if storedErr != nil {
			return ProvisionManifestAdmission{}, fmt.Errorf("%w: %w", backend.ErrInvalidManifest, storedErr)
		}
		stored, storedErr := manifest.ParseStoredPayload(release.Manifest)
		if storedErr != nil {
			return ProvisionManifestAdmission{}, storedErr
		}
		requestedJSON, marshalErr := json.Marshal(requested)
		if marshalErr != nil {
			return ProvisionManifestAdmission{}, marshalErr
		}
		storedJSON, marshalErr := json.Marshal(stored)
		if marshalErr != nil {
			return ProvisionManifestAdmission{}, marshalErr
		}
		identity, valid := release.RuntimeIdentity()
		if !valid || identity.Tenant() != tenant || identity.ProviderUUID() != provider ||
			!sameProvisionTopology(items, release.Items) || !bytes.Equal(requestedJSON, storedJSON) {
			return ProvisionManifestAdmission{}, fmt.Errorf("manifest differs from exact historical replay authority: %w", policyErr)
		}
		return ProvisionManifestAdmission{issuer: s, payload: slices.Clone(release.Manifest), replay: &provisionManifestReplay{
			source: source, tenant: identity.Tenant(), provider: identity.ProviderUUID(), items: items,
		}}, nil
	}
}

// Payload returns the owned bytes chosen by admission. Historical replay uses
// the durable release bytes even when the request used equivalent JSON syntax.
func (a ProvisionManifestAdmission) Payload() []byte { return slices.Clone(a.payload) }

// Stack returns a detached structural projection of the admitted bytes.
func (a ProvisionManifestAdmission) Stack() (*manifest.StackManifest, error) {
	if a.issuer == nil || !a.issuer.valid() {
		return nil, errors.New("provision manifest admission is unavailable")
	}
	return manifest.ParseStoredPayload(a.payload)
}

// Bind preserves the admission's private replay fence in the store-minted
// candidate. Acceptance must consume both authorities as one transition.
func (a ProvisionManifestAdmission) Bind(candidate OperationIntentCandidate) (OperationIntentCandidate, error) {
	if a.issuer == nil || !a.issuer.valid() || candidate.settlement != a.issuer ||
		candidate.spec.Kind != OperationIntentProvision || !bytes.Equal(candidate.spec.Manifest, a.payload) {
		return OperationIntentCandidate{}, errors.New("provision candidate differs from its admitted manifest")
	}
	if a.replay != nil {
		if candidate.spec.LeaseUUID != a.replay.source.LeaseUUID() || candidate.spec.Tenant != a.replay.tenant ||
			candidate.spec.ProviderUUID != a.replay.provider || !sameProvisionItemMultiset(candidate.spec.Items, a.replay.items) {
			return OperationIntentCandidate{}, errors.New("historical provision candidate differs from its exact release authority")
		}
		source := a.replay.source
		candidate.provisionReplay = &source
	}
	return candidate, nil
}

// Release items contain effective routing, while the chain request carries
// desired routing. Historical payload admission depends on the same instance
// topology, not on whether the old backend could publish the desired domain.
// This projection deliberately has no domain field. The replay capability
// separately captures the exact admitted request items for candidate binding.
type provisionTopologyItem struct {
	service string
	sku     string
	count   int
}

func sameProvisionTopology(left, right []backend.LeaseItem) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[provisionTopologyItem]int, len(left))
	for _, item := range left {
		counts[provisionTopologyItem{service: item.ServiceName, sku: item.SKU, count: item.Quantity}]++
	}
	for _, item := range right {
		key := provisionTopologyItem{service: item.ServiceName, sku: item.SKU, count: item.Quantity}
		if counts[key] == 0 {
			return false
		}
		counts[key]--
	}
	return true
}

// sameProvisionItemMultiset preserves the entire admitted request, including
// its desired routing. Counts matter: a set comparison would hide duplicates.
func sameProvisionItemMultiset(left, right []backend.LeaseItem) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[backend.LeaseItem]int, len(left))
	for _, item := range left {
		counts[item]++
	}
	for _, item := range right {
		if counts[item] == 0 {
			return false
		}
		counts[item]--
	}
	return true
}

// verifyProvisionReplay runs only while Begin holds the existing lease lock.
// No other release/close/maintenance publication may cross this re-attestation
// and the callback journal's Pending write.
func (candidate OperationIntentCandidate) verifyProvisionReplay() error {
	if candidate.provisionReplay == nil {
		return nil
	}
	source := *candidate.provisionReplay
	if candidate.settlement == nil || !candidate.settlement.valid() ||
		source.issuer != candidate.settlement.releases || source.LeaseUUID() != candidate.spec.LeaseUUID {
		return errors.New("historical provision replay has foreign release authority")
	}
	_, current, err := source.issuer.claimLatestActive(source.LeaseUUID())
	if err != nil {
		return err
	}
	if current != source {
		return errors.New("historical provision release changed before admission")
	}
	return nil
}
