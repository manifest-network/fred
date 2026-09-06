package shared

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// ErrCloseAuthorityMissing means neither release history nor an operation
// journal can authorize destructive close work. Cleanup-only callers may treat
// this as an effect-free absence; projected closes must fail closed.
var ErrCloseAuthorityMissing = errors.New("close has no durable cleanup authority")

// CloseRequest contains policy and substrate-cleanup evidence only. The
// security- and causality-bearing principal, callback route, topology, and
// release fence are derived from the bound journals by BeginClose; callers
// cannot splice those authorities together themselves.
type CloseRequest struct {
	issuer        *CloseSettlement
	leaseUUID     string
	retainOnClose bool
}

// CleanupCloseRequest is the deliberately weaker admission used when no live
// projection exists. It never carries callback or retention authority. When a
// release or pending operation exists, the coordinator still derives cleanup
// topology from that durable source; a request with neither source is refused.
type CleanupCloseRequest struct {
	issuer    *CloseSettlement
	leaseUUID string
}

// CloseSettlement is the sole phase-safe boundary for close admission,
// cleanup progress, release retirement, and lifecycle settlement. Its zero
// value is invalid. All three journals must be the exact open instances for
// one backend storage lineage and share the same terminal authority gate.
type CloseSettlement struct {
	journalPair
	recoveryCoordinator *RecoveryCoordinator
	retentions          *RetentionStore
	mutation            *substratemutation.Protocol[ClosePhysicalSubject]
	recovery            *substratemutation.RecoveryAttestor[ClosePhysicalSubject, ClosePhysicalEvidence]
	execute             func(context.Context, substratemutation.LiveExecution[ClosePhysicalSubject]) substratemutation.Result[ClosePhysicalSubject, ClosePhysicalEvidence]
}

// NewCloseSettlement binds the complete close authority set. Matching backend
// names are not enough: pointer identity, immutable storage identity, and the
// backend-lifetime authority gate must all agree.
func NewCloseSettlement(
	callbacks *CallbackStore,
	releases *ReleaseStore,
	retentions *RetentionStore,
) (*CloseSettlement, error) {
	pair, err := newJournalPair(callbacks, releases)
	if err != nil {
		return nil, fmt.Errorf("close %w", err)
	}
	if !retentionStoreIsOpen(retentions) ||
		retentions.backendAuthorityGate != callbacks.backendAuthorityGate ||
		retentions.binding.backendName != callbacks.binding.backendName ||
		retentions.binding.storageID != callbacks.binding.storageID {
		return nil, errors.New(
			"close settlement requires the exact identity-bound retention journal",
		)
	}
	return &CloseSettlement{
		journalPair: pair,
		retentions:  retentions,
		mutation:    substratemutation.NewProtocol[ClosePhysicalSubject](),
	}, nil
}

func (s *CloseSettlement) valid() bool {
	return s != nil && s.journalPair.valid() && retentionStoreIsOpen(s.retentions) &&
		s.retentions.backendAuthorityGate == s.callbacks.backendAuthorityGate &&
		s.retentions.binding.backendName == s.callbacks.binding.backendName &&
		s.retentions.binding.storageID == s.callbacks.binding.storageID
}

// BindCloseSubstrateExecutor atomically binds the only live close workflow and
// its restart classifier to this exact journal set. The builder alone receives
// Runner, and both functions receive the journal-minted subject, so callers can
// neither inject a mutator nor substitute physical targets at execution time.
func BindCloseSubstrateExecutor[T any](
	s *CloseSettlement,
	authorize substratemutation.Authorize,
	complete substratemutation.Complete,
	build func(substratemutation.Runner, ClosePhysicalSubject) T,
	run func(context.Context, T, ClosePhysicalSubject) error,
	classify func(context.Context, ClosePhysicalSubject) (ClosePhysicalEvidence, error),
) error {
	if !s.valid() || s.mutation == nil {
		return errors.New("close settlement is invalid")
	}
	binding, err := s.mutation.NewGuardBinding()
	if err != nil {
		return err
	}
	guard, recovery, err := substratemutation.NewExecutor(
		binding, authorize, complete, build, run, classify,
	)
	if err != nil {
		return err
	}
	s.recovery = recovery
	s.execute = func(
		ctx context.Context,
		execution substratemutation.LiveExecution[ClosePhysicalSubject],
	) substratemutation.Result[ClosePhysicalSubject, ClosePhysicalEvidence] {
		return guard.Execute(execution, ctx)
	}
	return nil
}

// NewCloseRequest validates and detaches the only caller-observed cleanup
// evidence before binding the request to this exact settlement. Principal,
// runtime identity, and release fencing are intentionally not parameters.
func (s *CloseSettlement) NewCloseRequest(
	leaseUUID string,
	retainOnClose bool,
) (CloseRequest, error) {
	if err := s.validateRequestInput(leaseUUID); err != nil {
		return CloseRequest{}, err
	}
	return CloseRequest{
		issuer: s, leaseUUID: leaseUUID, retainOnClose: retainOnClose,
	}, nil
}

// NewCleanupCloseRequest constructs the separately typed, callbackless and
// non-retaining request used only when the volatile projection is absent.
func (s *CloseSettlement) NewCleanupCloseRequest(
	leaseUUID string,
) (CleanupCloseRequest, error) {
	if err := s.validateRequestInput(leaseUUID); err != nil {
		return CleanupCloseRequest{}, err
	}
	return CleanupCloseRequest{issuer: s, leaseUUID: leaseUUID}, nil
}

func (s *CloseSettlement) validateRequestInput(
	leaseUUID string,
) error {
	if !s.valid() {
		return errors.New("close settlement is invalid")
	}
	if !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return errors.New("close request requires a canonical lease UUID")
	}
	return nil
}

// ActiveRetentionProof is an opaque snapshot of one exact active retention
// record. A proof is useful only to the CloseSettlement bound to its issuing
// store; settlement re-reads the canonical bytes while holding both the
// per-lease transition gate and the retention store's read lock.
type ActiveRetentionProof struct {
	issuer           *RetentionStore
	leaseUUID        string
	backend          string
	storageID        backendidentity.ID
	generation       int
	tenant           string
	providerUUID     string
	items            []backend.LeaseItem
	resourceProfiles []SKUResourceSnapshot
	retainedVolumes  []string
	manifest         []byte
	callbackURL      string
	digest           [sha256.Size]byte
}

// Valid reports whether the proof has a complete opaque shape. It does not
// establish durable liveness; CompleteClose performs that re-attestation.
func (proof ActiveRetentionProof) Valid() bool {
	return proof.issuer != nil && backend.IsCanonicalLeaseUUID(proof.leaseUUID) &&
		proof.backend != "" && proof.storageID.Valid() && proof.generation >= 0 &&
		proof.tenant != "" && proof.providerUUID != "" && len(proof.items) != 0 &&
		len(proof.resourceProfiles) != 0 && len(proof.retainedVolumes) != 0 &&
		len(proof.manifest) != 0 &&
		proof.digest != ([sha256.Size]byte{})
}

// RetainedVolumeNames returns the detached exact volume cohort carried by this
// active-retention generation. It is observation-only input to the
// construction-bound physical classifier; terminal settlement re-attests the
// canonical row before accepting retained completion.
func (proof ActiveRetentionProof) RetainedVolumeNames() []string {
	if !proof.Valid() {
		return nil
	}
	return slices.Clone(proof.retainedVolumes)
}

// ProveActive returns exact, store-issued evidence for the current active
// retention generation. It is intentionally separate from PutActiveMerged:
// Docker obtains it only after the physical retained-volume rename has
// completed, so pre-side-effect snapshots cannot authorize retained success.
func (s *RetentionStore) proveActive(leaseUUID string) (ActiveRetentionProof, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return ActiveRetentionProof{}, errors.New(
			"active retention proof requires an identity-bound retention journal",
		)
	}
	if !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return ActiveRetentionProof{}, errors.New("active retention proof requires a canonical lease UUID")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.proveActiveLocked(leaseUUID)
}

// RecordRetention is the close-owned soft-delete writer. Every identity and
// topology field comes from the exact close claim; callers contribute only
// the physical retained-volume observations and optional partition policy.
// It deliberately returns no completion capability: only ProveRetention,
// called after physical rename success, can mint ActiveRetentionProof.
func (s *CloseSettlement) RecordRetention(
	claim CloseIntentClaim,
	partition string,
	retainedVolumeNames []string,
) (bool, error) {
	if err := s.requireClaim(claim); err != nil {
		return false, err
	}
	if !claim.RetainOnClose() || claim.CleanupOnly() {
		return false, errors.New(
			"recording retention requires a retained projected close",
		)
	}
	if len(retainedVolumeNames) == 0 || len(retainedVolumeNames) > backend.MaxOperationQuantity {
		return false, errors.New(
			"close retention requires a bounded non-empty retained-volume set",
		)
	}
	seenVolumes := make(map[string]struct{}, len(retainedVolumeNames))
	for i, name := range retainedVolumeNames {
		if err := validateClosePhysicalName(i, "retained volume name", name); err != nil {
			return false, err
		}
		if _, exists := seenVolumes[name]; exists {
			return false, fmt.Errorf(
				"close retention volume %q is duplicated", name,
			)
		}
		seenVolumes[name] = struct{}{}
	}
	retainedVolumeNames = slices.Clone(retainedVolumeNames)
	stack, err := manifest.ParsePayload(claim.Manifest())
	if err != nil {
		return false, fmt.Errorf("parse close retention manifest: %w", err)
	}
	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCloseIntent(claim); err != nil {
		return false, err
	}
	ok, err := s.retentions.putActiveMerged(RetentionEntry{
		OriginalLeaseUUID:   claim.LeaseUUID(),
		Tenant:              claim.Tenant(),
		Partition:           partition,
		ProviderUUID:        claim.ProviderUUID(),
		Items:               claim.Items(),
		ResourceProfiles:    claim.ResourceProfiles(),
		StackManifest:       stack,
		CallbackURL:         claim.CallbackURL(),
		RetainedVolumeNames: retainedVolumeNames,
		Status:              RetentionStatusActive,
		CreatedAt:           time.Now(),
	}, func(stored RetentionEntry) error {
		return retentionEntryMatchesClose(stored, claim)
	})
	if err != nil || !ok {
		return ok, err
	}
	return true, nil
}

func retentionEntryMatchesClose(entry RetentionEntry, claim CloseIntentClaim) error {
	stack, err := manifest.ParsePayload(claim.Manifest())
	if err != nil {
		return fmt.Errorf("parse close retention manifest: %w", err)
	}
	wantManifest, err := json.Marshal(stack)
	if err != nil {
		return fmt.Errorf("marshal close manifest for retention: %w", err)
	}
	gotManifest, err := json.Marshal(entry.StackManifest)
	if err != nil {
		return fmt.Errorf("marshal retention manifest: %w", err)
	}
	if entry.OriginalLeaseUUID != claim.LeaseUUID() ||
		entry.Tenant != claim.Tenant() || entry.ProviderUUID != claim.ProviderUUID() ||
		!slices.Equal(entry.Items, claim.Items()) ||
		!slices.Equal(entry.ResourceProfiles, claim.ResourceProfiles()) ||
		!bytes.Equal(gotManifest, wantManifest) || entry.CallbackURL != claim.CallbackURL() {
		return errors.New("retention entry differs from close authority")
	}
	return nil
}

// ProveRetention reissues active-retention evidence for a retry after Docker
// has independently re-attested every persisted retained volume.
func (s *CloseSettlement) ProveRetention(
	claim CloseIntentClaim,
) (ActiveRetentionProof, error) {
	if err := s.requireClaim(claim); err != nil {
		return ActiveRetentionProof{}, err
	}
	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCloseIntent(claim); err != nil {
		return ActiveRetentionProof{}, err
	}
	proof, err := s.retentions.proveActive(claim.LeaseUUID())
	if err != nil {
		return ActiveRetentionProof{}, err
	}
	if err := retentionMatchesClose(proof, claim); err != nil {
		return ActiveRetentionProof{}, err
	}
	return proof, nil
}

func retentionMatchesClose(proof ActiveRetentionProof, claim CloseIntentClaim) error {
	stack, err := manifest.ParsePayload(claim.Manifest())
	if err != nil {
		return fmt.Errorf("parse close manifest for retention proof: %w", err)
	}
	manifestBytes, err := json.Marshal(stack)
	if err != nil {
		return fmt.Errorf("marshal close manifest for retention proof: %w", err)
	}
	if proof.leaseUUID != claim.LeaseUUID() || proof.tenant != claim.Tenant() ||
		proof.providerUUID != claim.ProviderUUID() ||
		!slices.Equal(proof.items, claim.Items()) ||
		!slices.Equal(proof.resourceProfiles, claim.ResourceProfiles()) ||
		!bytes.Equal(proof.manifest, manifestBytes) ||
		proof.callbackURL != claim.CallbackURL() {
		return errors.New("active retention proof differs from close authority")
	}
	return nil
}

func (s *RetentionStore) proveActiveLocked(leaseUUID string) (ActiveRetentionProof, error) {
	var proof ActiveRetentionProof
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		if bucket == nil {
			return errors.New("retention bucket is missing")
		}
		raw := bucket.Get([]byte(leaseUUID))
		if raw == nil {
			return fmt.Errorf("%w %q", ErrNoRetention, leaseUUID)
		}
		entry, err := decodeRetentionEntry(raw)
		if err != nil {
			return fmt.Errorf("decode retention record %q: %w", leaseUUID, err)
		}
		if err := validateAuthoritativeRetentionIdentity([]byte(leaseUUID), &entry); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", leaseUUID, err)
		}
		if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", leaseUUID, err)
		}
		if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", leaseUUID, err)
		}
		if entry.Status != RetentionStatusActive {
			return fmt.Errorf("%w: retention record %q is %q", ErrNotRestorable, leaseUUID, entry.Status)
		}
		proof = ActiveRetentionProof{
			issuer: s, leaseUUID: leaseUUID,
			backend: s.binding.backendName, storageID: s.binding.storageID,
			generation: entry.Generation, tenant: entry.Tenant,
			providerUUID: entry.ProviderUUID, items: slices.Clone(entry.Items),
			resourceProfiles: CloneSKUResourceSnapshot(entry.ResourceProfiles),
			retainedVolumes:  slices.Clone(entry.RetainedVolumeNames),
			callbackURL:      entry.CallbackURL,
			digest:           sha256.Sum256(raw),
		}
		proof.manifest, err = json.Marshal(entry.StackManifest)
		if err != nil {
			return fmt.Errorf("marshal retention manifest %q: %w", leaseUUID, err)
		}
		return nil
	})
	return proof, err
}

func (s *RetentionStore) requireActiveProofLocked(proof ActiveRetentionProof) error {
	if !proof.Valid() || proof.issuer != s || s.binding == nil ||
		proof.backend != s.binding.backendName || proof.storageID != s.binding.storageID {
		return errors.New("active retention proof belongs to another journal lineage")
	}
	current, err := s.proveActiveLocked(proof.leaseUUID)
	if err != nil {
		return err
	}
	if current.generation != proof.generation || current.digest != proof.digest ||
		current.tenant != proof.tenant || current.providerUUID != proof.providerUUID ||
		!slices.Equal(current.items, proof.items) ||
		!slices.Equal(current.resourceProfiles, proof.resourceProfiles) ||
		!slices.Equal(current.retainedVolumes, proof.retainedVolumes) ||
		!bytes.Equal(current.manifest, proof.manifest) ||
		current.callbackURL != proof.callbackURL {
		return errors.New("active retention generation changed before close settlement")
	}
	return nil
}

// closeIntentDerivation is the sealed result of reading the release/callback
// journal pair under the lease gate. A Failed successor over an older active
// Release and a Failed initial operation with total Release absence are
// distinct variants; neither can be mistaken for ordinary terminal-operation
// lineage.
type closeIntentDerivation interface {
	spec() closeIntentSpec
	isCloseIntentDerivation()
}

type directCloseIntentDerivation struct{ value closeIntentSpec }

func (directCloseIntentDerivation) isCloseIntentDerivation() {}
func (derivation directCloseIntentDerivation) spec() closeIntentSpec {
	return derivation.value
}

type failedSuccessorCloseIntentDerivation struct {
	value       closeIntentSpec
	predecessor failedOperationOverRelease
}

func (failedSuccessorCloseIntentDerivation) isCloseIntentDerivation() {}
func (derivation failedSuccessorCloseIntentDerivation) spec() closeIntentSpec {
	return derivation.value
}

type failedWithoutReleaseCleanupCloseIntentDerivation struct {
	value   closeIntentSpec
	absence failedOperationWithoutRelease
}

func (failedWithoutReleaseCleanupCloseIntentDerivation) isCloseIntentDerivation() {}
func (derivation failedWithoutReleaseCleanupCloseIntentDerivation) spec() closeIntentSpec {
	return derivation.value
}

// BeginClose derives the complete destructive and observational authority from
// the exact release selected under the journal pair's per-lease gate. If no
// release exists, an exact pending operation may supply the same authority.
func (s *CloseSettlement) BeginClose(request CloseRequest) (CloseIntentAdmission, error) {
	if request.issuer != s {
		return CloseIntentAdmission{}, errors.New("close request belongs to another settlement")
	}
	return s.begin(
		request.leaseUUID, request.retainOnClose, false,
	)
}

// BeginCleanupClose admits the no-projection path with deliberately reduced
// authority. It never emits a lifecycle callback or retains volumes. An exact
// current Failed operation may supply topology only when its terminal record
// sealed predecessor absence and Release history is still wholly absent.
func (s *CloseSettlement) BeginCleanupClose(
	request CleanupCloseRequest,
) (CloseIntentAdmission, error) {
	if request.issuer != s {
		return CloseIntentAdmission{}, errors.New("cleanup close request belongs to another settlement")
	}
	return s.begin(request.leaseUUID, false, true)
}

func (s *CloseSettlement) begin(
	leaseUUID string,
	retainOnClose bool,
	cleanupOnly bool,
) (CloseIntentAdmission, error) {
	if !s.valid() {
		return CloseIntentAdmission{}, errors.New("close settlement is invalid")
	}
	if !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return CloseIntentAdmission{}, errors.New("close admission requires a canonical lease UUID")
	}
	unlock := s.lockLease(leaseUUID)
	defer unlock()

	if existing, found, err := s.callbacks.getCloseIntentLocked(leaseUUID); err != nil {
		return CloseIntentAdmission{}, err
	} else if found {
		if existing.CleanupOnly() != cleanupOnly ||
			existing.RetainOnClose() != retainOnClose {
			return CloseIntentAdmission{}, fmt.Errorf(
				"%w for lease %q", ErrCloseIntentConflict, leaseUUID,
			)
		}
		return s.bindAdmission(CloseIntentAdmission{
			claim: existing, disposition: CloseIntentAdmissionExisting,
		}), nil
	}

	derivation, err := s.deriveCloseSpecLocked(
		leaseUUID, retainOnClose, cleanupOnly,
	)
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	spec := derivation.spec()
	candidate, err := newCloseIntentCandidate(
		s.callbacks, spec, s.callbacks.binding.backendName, s.callbacks.binding.storageID,
	)
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	var admission CloseIntentAdmission
	switch authority := derivation.(type) {
	case directCloseIntentDerivation:
		admission, err = s.callbacks.beginCloseIntentLocked(candidate)
	case failedSuccessorCloseIntentDerivation:
		admission, err = s.callbacks.beginCloseIntentAfterFailedSuccessorLocked(
			candidate, authority.predecessor,
		)
	case failedWithoutReleaseCleanupCloseIntentDerivation:
		admission, err = s.callbacks.beginCleanupCloseAfterFailedOperationLocked(
			candidate, authority.absence,
		)
	default:
		err = fmt.Errorf("unsupported close intent derivation %T", derivation)
	}
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	return s.bindAdmission(admission), nil
}

func (s *CloseSettlement) deriveCloseSpecLocked(
	leaseUUID string,
	retainOnClose bool,
	cleanupOnly bool,
) (closeIntentDerivation, error) {
	release, releaseClaim, found, err := s.releases.claimForClose(leaseUUID)
	if err != nil {
		return nil, err
	}
	if found {
		identity, ok := release.RuntimeIdentity()
		if !ok {
			return nil, fmt.Errorf(
				"release selected for close of %q has no complete runtime authority", leaseUUID,
			)
		}
		if len(release.Items) == 0 || len(release.ResourceProfiles) == 0 || len(release.Manifest) == 0 {
			return nil, fmt.Errorf(
				"release selected for close of %q has incomplete cleanup authority", leaseUUID,
			)
		}
		callbackURL := identity.CallbackURL()
		lifecycleCallbackURL := identity.LifecycleCallbackURL()
		tenant := identity.Tenant()
		providerUUID := identity.ProviderUUID()
		if cleanupOnly {
			callbackURL = ""
			lifecycleCallbackURL = ""
			tenant = ""
			providerUUID = ""
		}
		spec := closeIntentSpec{
			LeaseUUID: leaseUUID,
			Tenant:    tenant, ProviderUUID: providerUUID,
			Items:            slices.Clone(release.Items),
			ResourceProfiles: CloneSKUResourceSnapshot(release.ResourceProfiles),
			Manifest:         slices.Clone(release.Manifest),
			CallbackURL:      callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
			RetainOnClose: retainOnClose, CleanupOnly: cleanupOnly,
			ActiveReleaseVersion:     releaseClaim.Version(),
			ActiveReleaseDigest:      releaseClaim.Digest(),
			ActiveReleaseOperationID: identity.OperationID(),
		}
		var head leaseMutationHead
		err := s.callbacks.view(func(tx *bolt.Tx) error {
			var present bool
			var readErr error
			head, present, readErr = getLeaseMutationHeadTx(tx, leaseUUID)
			if readErr != nil || !present {
				head = nil
			}
			return readErr
		})
		if err != nil {
			return nil, err
		}
		if operation, ok := head.(operationLeaseMutationHead); ok &&
			operation.claim.entry.State == operationIntentFailed {
			predecessor, err := bindFailedOperationOverRelease(
				s.callbacks, s.releases, operation, releaseClaim,
			)
			if err != nil {
				return nil, fmt.Errorf("derive failed-operation close predecessor: %w", err)
			}
			return failedSuccessorCloseIntentDerivation{
				value: spec, predecessor: predecessor,
			}, nil
		}
		return directCloseIntentDerivation{value: spec}, nil
	}

	operationHead, found, err := s.callbacks.currentOperationHeadLocked(leaseUUID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w for lease %q", ErrCloseAuthorityMissing, leaseUUID)
	}
	operation := operationHead.claim.operationAuthority
	if operationHead.claim.entry.State == operationIntentFailed {
		if !cleanupOnly {
			return nil, fmt.Errorf("%w for lease %q", ErrCloseAuthorityMissing, leaseUUID)
		}
		absence, err := bindFailedOperationWithoutRelease(
			s.callbacks, s.releases, operationHead,
		)
		if err != nil {
			return nil, fmt.Errorf("derive failed-operation cleanup authority: %w", err)
		}
		return failedWithoutReleaseCleanupCloseIntentDerivation{
			value: closeSpecForOperationAuthority(
				leaseUUID, operation, false, true,
			),
			absence: absence,
		}, nil
	}
	if operationHead.claim.entry.State != operationIntentPending {
		return nil, fmt.Errorf("%w for lease %q", ErrCloseAuthorityMissing, leaseUUID)
	}
	return directCloseIntentDerivation{value: closeSpecForOperationAuthority(
		leaseUUID, operation, retainOnClose, cleanupOnly,
	)}, nil
}

func closeSpecForOperationAuthority(
	leaseUUID string,
	operation operationAuthority,
	retainOnClose bool,
	cleanupOnly bool,
) closeIntentSpec {
	callbackURL := operation.CallbackURL()
	lifecycleCallbackURL := operation.LifecycleCallbackURL()
	tenant := operation.Tenant()
	providerUUID := operation.ProviderUUID()
	if cleanupOnly {
		callbackURL = ""
		lifecycleCallbackURL = ""
		tenant = ""
		providerUUID = ""
	}
	return closeIntentSpec{
		LeaseUUID: leaseUUID,
		Tenant:    tenant, ProviderUUID: providerUUID,
		Items: operation.EffectiveItems(), ResourceProfiles: operation.ResourceProfiles(),
		Manifest: operation.Manifest(), CallbackURL: callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		RetainOnClose:        retainOnClose, CleanupOnly: cleanupOnly,
	}
}

func (s *ReleaseStore) claimForClose(
	leaseUUID string,
) (Release, ReleaseClaim, bool, error) {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return Release{}, ReleaseClaim{}, false, err
	}
	var release Release
	var claim ReleaseClaim
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		data := tx.Bucket(releasesBucketName).Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}
		history, err := decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("decode release history for close of %q: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(history); err != nil {
			return fmt.Errorf("invalid release history for close of %q: %w", leaseUUID, err)
		}
		selected := releaseForCloseFence(history)
		if selected == nil {
			return fmt.Errorf("release history for close of %q is empty", leaseUUID)
		}
		encoded, err := json.Marshal(selected)
		if err != nil {
			return fmt.Errorf("marshal release fence for close of %q: %w", leaseUUID, err)
		}
		release = cloneRelease(*selected)
		claim = ReleaseClaim{
			issuer: s, leaseUUID: leaseUUID, version: selected.Version,
			digest: sha256.Sum256(encoded),
		}
		found = true
		return nil
	})
	return release, claim, found, err
}

func (s *CallbackStore) currentOperationHeadLocked(
	leaseUUID string,
) (operationLeaseMutationHead, bool, error) {
	var operationHead operationLeaseMutationHead
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
		if err != nil || !present {
			return err
		}
		operation, ok := head.(operationLeaseMutationHead)
		if !ok {
			return nil
		}
		operationHead = operation
		found = true
		return nil
	})
	return operationHead, found, err
}

func (s *CloseSettlement) bindAdmission(admission CloseIntentAdmission) CloseIntentAdmission {
	admission.claim.settlement = s
	return admission
}

// GetCloseIntent returns the exact recovery capability issued by this complete
// journal set. A claim obtained from another open set cannot be mutated here.
func (s *CloseSettlement) GetCloseIntent(
	leaseUUID string,
) (CloseIntentClaim, bool, error) {
	if !s.valid() {
		return CloseIntentClaim{}, false, errors.New("close settlement is invalid")
	}
	unlock := s.lockLease(leaseUUID)
	defer unlock()
	claim, found, err := s.callbacks.getCloseIntentLocked(leaseUUID)
	if found && err == nil {
		claim.settlement = s
	}
	return claim, found, err
}

// ListCloseIntents returns recovery claims bound to this exact settlement.
func (s *CloseSettlement) ListCloseIntents() ([]CloseIntentClaim, error) {
	if !s.valid() {
		return nil, errors.New("close settlement is invalid")
	}
	claims, err := s.callbacks.listCloseIntents()
	if err != nil {
		return nil, err
	}
	for i := range claims {
		claims[i].settlement = s
	}
	return claims, nil
}

type closeCompletion uint8

const (
	closeCompletionDestroyed closeCompletion = iota + 1
	closeCompletionRetained
)

func (s *CloseSettlement) requireClaim(claim CloseIntentClaim) error {
	if !s.valid() || claim.settlement != s {
		return errors.New("close claim belongs to another journal set")
	}
	return validateCloseIntentClaim(claim)
}

func (s *CloseSettlement) requireNoActiveRetentionLocked(leaseUUID string) error {
	err := s.retentions.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(retentionBucketName).Get([]byte(leaseUUID))
		if raw == nil {
			return nil
		}
		entry, err := decodeRetentionEntry(raw)
		if err != nil {
			return fmt.Errorf("decode retention record %q: %w", leaseUUID, err)
		}
		if err := validateAuthoritativeRetentionIdentity([]byte(leaseUUID), &entry); err != nil {
			return err
		}
		return fmt.Errorf(
			"destroyed close requires retention absence; lease %q still has %q authority",
			leaseUUID, entry.Status,
		)
	})
	return err
}

func (s *ReleaseStore) deleteCloseHistory(claim CloseIntentClaim) error {
	leaseUUID := claim.LeaseUUID()
	version := claim.ActiveReleaseVersion()
	digest := claim.ActiveReleaseDigest()
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if version < 0 || (version == 0) != (digest == ([sha256.Size]byte{})) {
		return errors.New("close release fence is invalid")
	}
	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		key := []byte(leaseUUID)
		data := bucket.Get(key)
		if data == nil {
			return nil
		}
		if version == 0 {
			return fmt.Errorf("release history appeared after close admission for %s", leaseUUID)
		}
		history, err := decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(history); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		selected := releaseForCloseFence(history)
		if selected == nil {
			return fmt.Errorf("release history for %s is empty", leaseUUID)
		}
		encoded, err := json.Marshal(selected)
		if err != nil {
			return fmt.Errorf("marshal release fence for %s: %w", leaseUUID, err)
		}
		if selected.Version != version || sha256.Sum256(encoded) != digest {
			return fmt.Errorf("release history changed after close admission for %s", leaseUUID)
		}
		return bucket.Delete(key)
	})
}
