package shared

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// ErrMaintenanceReleaseClaimRequired marks a raw release-store mutation that
// attempted to bypass the exact MaintenanceReleaseClaim API. Maintenance rows
// are causal authority and may only be appended or transitioned by the typed
// methods in this file.
var ErrMaintenanceReleaseClaimRequired = errors.New("maintenance release requires exact claim")

// MaintenanceSettlement is the construction-bound handoff between the
// callback write-ahead journal and the release journal for restart, update,
// and custom-domain replacement. Every maintenance release mutation and its
// terminal callback settlement crosses this exact pair while holding the
// callback journal's per-lease transition gate. The zero value is invalid.
type MaintenanceSettlement struct {
	journalPair
	recoveryCoordinator *RecoveryCoordinator
	mutation            *substratemutation.Protocol[MaintenancePhysicalSubject]
	recovery            *substratemutation.RecoveryAttestor[MaintenancePhysicalSubject, MaintenancePhysicalEvidence]
	execute             func(context.Context, substratemutation.LiveExecution[MaintenancePhysicalSubject]) substratemutation.Result[MaintenancePhysicalSubject, MaintenancePhysicalEvidence]
	executeRecovery     func(context.Context, substratemutation.RecoveryExecution[MaintenancePhysicalSubject]) substratemutation.Result[MaintenancePhysicalSubject, MaintenancePhysicalEvidence]
}

type MaintenanceExecutionClaim struct {
	settlement *MaintenanceSettlement
	target     MaintenanceReleaseClaim
	subject    MaintenancePhysicalSubject
	started    substratemutation.LiveExecution[MaintenancePhysicalSubject]
}

// MaintenanceExecutionSuccess and MaintenanceExecutionFailure are the two
// terminal physical observations produced by ExecuteMaintenance. Their fields
// are private and bind the exact coordinator and target generation; an intent
// or freshly appended target alone is not terminal authority.
type MaintenanceExecutionSuccess struct {
	settlement *MaintenanceSettlement
	execution  MaintenanceExecutionClaim
	ready      MaintenanceTargetReady
}

type MaintenanceExecutionFailure struct {
	settlement *MaintenanceSettlement
	authority  MaintenanceReleaseClaim
	subject    MaintenancePhysicalSubject
	evidence   MaintenancePhysicalEvidence
	refused    bool
	cause      error
}

// MaintenanceExecutionAmbiguous grants no terminal authority. Recovery must
// classify the exact target from strict inventory before another transition.
type MaintenanceExecutionAmbiguous struct {
	settlement *MaintenanceSettlement
	execution  MaintenanceExecutionClaim
	cause      error
}

func (outcome MaintenanceExecutionSuccess) Valid() bool {
	return outcome.settlement != nil &&
		outcome.execution.target.validFor(outcome.settlement) &&
		outcome.execution.subject.validFor(outcome.settlement) &&
		outcome.ready.Valid() &&
		outcome.ready.validForMaintenance(outcome.execution.subject)
}
func (outcome MaintenanceExecutionFailure) Valid() bool {
	if outcome.settlement == nil || !outcome.authority.validFor(outcome.settlement) {
		return false
	}
	if outcome.refused {
		return outcome.evidence.kind == maintenancePhysicalEvidenceInvalid
	}
	return outcome.subject.validFor(outcome.settlement) &&
		validateMaintenancePhysicalEvidence(outcome.subject, outcome.evidence) == nil
}
func (outcome MaintenanceExecutionAmbiguous) Valid() bool {
	return outcome.settlement != nil &&
		outcome.execution.target.validFor(outcome.settlement) &&
		outcome.cause != nil
}
func (outcome MaintenanceExecutionAmbiguous) Error() string {
	if outcome.cause == nil {
		return "ambiguous maintenance execution"
	}
	return outcome.cause.Error()
}

func (outcome MaintenanceExecutionFailure) Cause() error   { return outcome.cause }
func (outcome MaintenanceExecutionAmbiguous) Cause() error { return outcome.cause }

// SourceRecovered reports the only failure shape which is safe to project
// back to Ready: strict inventory attested the complete previous generation.
func (outcome MaintenanceExecutionFailure) SourceRecovered() bool {
	if !outcome.Valid() {
		return false
	}
	return outcome.evidence.kind == maintenancePhysicalEvidenceSourceReady
}

type MaintenanceExecutionOutcome interface {
	maintenanceExecutionOutcome()
}

func (MaintenanceExecutionSuccess) maintenanceExecutionOutcome()   {}
func (MaintenanceExecutionFailure) maintenanceExecutionOutcome()   {}
func (MaintenanceExecutionAmbiguous) maintenanceExecutionOutcome() {}

// NewMaintenanceSettlement binds the exact open journal instances which
// jointly own maintenance state. Matching names alone are insufficient: the
// journals must share both the immutable storage identity and the same
// backend-lifetime storage-authority gate.
func NewMaintenanceSettlement(
	callbacks *CallbackStore,
	releases *ReleaseStore,
) (*MaintenanceSettlement, error) {
	pair, err := newJournalPair(callbacks, releases)
	if err != nil {
		return nil, fmt.Errorf("maintenance %w", err)
	}
	return &MaintenanceSettlement{
		journalPair: pair,
		mutation:    substratemutation.NewProtocol[MaintenancePhysicalSubject](),
	}, nil
}

// BindMaintenanceSubstrateExecutor atomically binds one narrow maintenance
// facade and its construction-fixed exhaustive classifier. The paired restart
// attestor never escapes the settlement.
func BindMaintenanceSubstrateExecutor[T any](
	s *MaintenanceSettlement,
	authorize substratemutation.Authorize,
	complete substratemutation.Complete,
	build func(substratemutation.Runner, MaintenancePhysicalSubject) T,
	run func(context.Context, T, MaintenancePhysicalSubject) error,
	classify func(context.Context, MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error),
) error {
	if s == nil || s.mutation == nil {
		return errors.New("maintenance settlement is invalid")
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
		execution substratemutation.LiveExecution[MaintenancePhysicalSubject],
	) substratemutation.Result[MaintenancePhysicalSubject, MaintenancePhysicalEvidence] {
		return guard.Execute(execution, ctx)
	}
	s.executeRecovery = func(
		ctx context.Context,
		execution substratemutation.RecoveryExecution[MaintenancePhysicalSubject],
	) substratemutation.Result[MaintenancePhysicalSubject, MaintenancePhysicalEvidence] {
		return guard.ExecuteRecovery(execution, ctx)
	}
	return nil
}

// MaintenanceReleaseActive is store-issued proof that the exact bound target
// generation is currently active. Its fields are private and bind both open
// journals, the precise maintenance intent, and the terminal release bytes.
// Copies are safe because settlement rechecks liveness under the pair gate.
type MaintenanceReleaseActive struct {
	settlement *MaintenanceSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	intent     MaintenanceIntentClaim
	target     MaintenanceReleaseClaim
	terminal   ReleaseClaim
	ready      MaintenanceTargetReady
}

// Valid reports whether the active proof has a complete opaque shape. Durable
// liveness and exact journal identity are rechecked by MaintenanceSettlement.
func (proof MaintenanceReleaseActive) Valid() bool {
	return proof.settlement != nil && proof.callbacks == proof.settlement.callbacks &&
		proof.releases == proof.settlement.releases && proof.intent.Valid() &&
		proof.intent.settlement == proof.settlement && proof.target.settlement == proof.settlement &&
		proof.target.valid() && proof.terminal.valid()
}

func (proof MaintenanceReleaseActive) LeaseUUID() string { return proof.intent.LeaseUUID() }
func (proof MaintenanceReleaseActive) MaintenanceID() MaintenanceID {
	return proof.intent.MaintenanceID()
}
func (proof MaintenanceReleaseActive) Version() int {
	if !proof.Valid() {
		return 0
	}
	return proof.target.Version()
}
func (proof MaintenanceReleaseActive) Intent() MaintenanceIntentClaim {
	return cloneMaintenanceIntentClaim(proof.intent)
}

// MatchesIntent reports whether this terminal proof completes the exact
// immutable maintenance generation named by claim. The proof necessarily
// carries the post-effect successor of the actor's pre-effect claim.
func (proof MaintenanceReleaseActive) MatchesIntent(claim MaintenanceIntentClaim) bool {
	return proof.Valid() && proof.intent.MatchesIntent(claim)
}

// TargetRelease returns the exact committed maintenance generation. The
// intent's pre-append target deliberately has Version zero; callers updating a
// live projection must use this proof-bound copy after activation instead.
func (proof MaintenanceReleaseActive) TargetRelease() (Release, bool) {
	if !proof.Valid() {
		return Release{}, false
	}
	release := proof.intent.TargetRelease()
	release.Version = proof.target.Version()
	return release, true
}

func (proof MaintenanceReleaseActive) TargetReady() (MaintenanceTargetReady, bool) {
	if !proof.Valid() || !proof.ready.Valid() ||
		proof.ready.LeaseUUID() != proof.LeaseUUID() ||
		proof.ready.MaintenanceID() != proof.MaintenanceID() {
		return MaintenanceTargetReady{}, false
	}
	return proof.ready, true
}

// MaintenanceReleaseFailure is store-issued proof that the exact maintenance
// target is terminal failed, or that no target was ever appended. A zero
// terminal claim denotes the latter; callers cannot manufacture either form.
type MaintenanceReleaseFailure struct {
	settlement *MaintenanceSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	intent     MaintenanceIntentClaim
	target     MaintenanceReleaseClaim
	terminal   ReleaseClaim
	absent     bool
	evidence   MaintenancePhysicalEvidence
}

// MaintenanceSourceClaim is pair-bound authority for the active generation
// from which a maintenance replacement may be admitted. A plain ReleaseClaim
// remains useful to internal journal validation, but cannot cross the public
// maintenance boundary: only MaintenanceSettlement can bind one to its exact
// open ReleaseStore instance. Reopening the same file therefore requires a
// fresh claim.
type MaintenanceSourceClaim struct {
	settlement *MaintenanceSettlement
	releases   *ReleaseStore
	claim      ReleaseClaim
}

func (source MaintenanceSourceClaim) Valid() bool {
	return source.settlement != nil && source.releases == source.settlement.releases &&
		source.claim.issuer == source.releases && source.claim.valid()
}

func (source MaintenanceSourceClaim) LeaseUUID() string         { return source.claim.LeaseUUID() }
func (source MaintenanceSourceClaim) Version() int              { return source.claim.Version() }
func (source MaintenanceSourceClaim) Digest() [sha256.Size]byte { return source.claim.Digest() }

// MaintenanceSourceSnapshot is an immutable observation of the exact active
// source generation sealed into one maintenance intent. It carries no release
// mutation authority: consumers may use its cloned topology to validate an
// actor projection, while terminal settlement still requires a distinct
// MaintenanceExecution outcome.
//
// The snapshot is pinned by the source release's version and full encoded-row
// digest. Release history never rewrites a generation's topology or runtime
// identity; only its terminal status may advance. Callers that need liveness
// must therefore consume a current settlement proof as well as this snapshot.
type MaintenanceSourceSnapshot struct {
	settlement *MaintenanceSettlement
	intent     MaintenanceIntentClaim
	source     MaintenanceSourceClaim
	release    Release
}

func (snapshot MaintenanceSourceSnapshot) Valid() bool {
	return snapshot.settlement != nil && snapshot.source.Valid() &&
		snapshot.source.releases == snapshot.settlement.releases &&
		snapshot.settlement.callbacks != nil && snapshot.settlement.callbacks.ctx != nil &&
		snapshot.settlement.callbacks.ctx.Err() == nil &&
		snapshot.settlement.releases != nil && snapshot.settlement.releases.ctx != nil &&
		snapshot.settlement.releases.ctx.Err() == nil &&
		snapshot.intent.Valid() && snapshot.intent.callbacks == snapshot.settlement.callbacks &&
		snapshot.intent.releases == snapshot.settlement.releases &&
		snapshot.source.LeaseUUID() == snapshot.intent.LeaseUUID() &&
		snapshot.source.Version() == snapshot.intent.SourceRelease().Version() &&
		snapshot.source.Digest() == snapshot.intent.SourceRelease().Digest()
}

func (snapshot MaintenanceSourceSnapshot) LeaseUUID() string {
	return snapshot.source.LeaseUUID()
}

// MaintenanceID binds the snapshot to the exact intent that requested the
// replacement. Two maintenance generations may legitimately share one source
// release, so lease/source identity alone is not sufficient at consumption.
func (snapshot MaintenanceSourceSnapshot) MaintenanceID() MaintenanceID {
	return snapshot.intent.MaintenanceID()
}

// Release returns a detached observation. Mutating it cannot alter the
// snapshot or any durable journal authority.
func (snapshot MaintenanceSourceSnapshot) Release() Release {
	return cloneRelease(snapshot.release)
}

// SnapshotMaintenanceSource reads the exact current active source named by
// intent and binds the detached bytes to this settlement/store lineage. It
// cannot silently fall through to a predecessor or successor generation.
func (s *MaintenanceSettlement) SnapshotMaintenanceSource(
	intent MaintenanceIntentClaim,
) (MaintenanceSourceSnapshot, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceSourceSnapshot{}, err
	}
	unlock := s.lockLease(intent.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(intent); err != nil {
		return MaintenanceSourceSnapshot{}, err
	}
	return s.snapshotMaintenanceSourceLocked(intent)
}

// snapshotMaintenanceSourceLocked is used by the Started CAS while the shared
// per-lease gate is already held. It deliberately does not reacquire that gate
// or reread the callback row; the caller supplies the exact current claim from
// the same transition.
func (s *MaintenanceSettlement) snapshotMaintenanceSourceLocked(
	intent MaintenanceIntentClaim,
) (MaintenanceSourceSnapshot, error) {
	release, source, err := s.ClaimLatestActive(intent.LeaseUUID())
	if err != nil {
		return MaintenanceSourceSnapshot{}, err
	}
	expected := intent.SourceRelease()
	if source.Version() != expected.Version() || source.Digest() != expected.Digest() {
		return MaintenanceSourceSnapshot{}, errors.New(
			"current active release is not the maintenance source generation",
		)
	}
	snapshot := MaintenanceSourceSnapshot{
		settlement: s,
		intent:     cloneMaintenanceIntentClaim(intent),
		source:     source,
		release:    cloneRelease(release),
	}
	if !snapshot.Valid() {
		return MaintenanceSourceSnapshot{}, errors.New("maintenance source snapshot is invalid")
	}
	return snapshot, nil
}

// Valid reports whether the failure proof has one complete sealed variant.
func (proof MaintenanceReleaseFailure) Valid() bool {
	if proof.settlement == nil || proof.callbacks != proof.settlement.callbacks ||
		proof.releases != proof.settlement.releases || !proof.intent.Valid() ||
		proof.intent.settlement != proof.settlement {
		return false
	}
	if proof.absent {
		return !proof.target.valid() && !proof.terminal.valid()
	}
	return proof.target.settlement == proof.settlement &&
		proof.target.valid() && proof.terminal.valid()
}

func (proof MaintenanceReleaseFailure) LeaseUUID() string { return proof.intent.LeaseUUID() }
func (proof MaintenanceReleaseFailure) MaintenanceID() MaintenanceID {
	return proof.intent.MaintenanceID()
}
func (proof MaintenanceReleaseFailure) Intent() MaintenanceIntentClaim {
	return cloneMaintenanceIntentClaim(proof.intent)
}

// MatchesIntent reports whether this terminal proof completes the exact
// immutable maintenance generation named by claim.
func (proof MaintenanceReleaseFailure) MatchesIntent(claim MaintenanceIntentClaim) bool {
	return proof.Valid() && proof.intent.MatchesIntent(claim)
}

// PhysicalEvidence returns the strict source/target classification which
// produced this terminal failure. Callback-only reconstructed proofs omit it.
func (proof MaintenanceReleaseFailure) PhysicalEvidence() (MaintenancePhysicalEvidence, bool) {
	if !proof.Valid() || proof.evidence.kind == maintenancePhysicalEvidenceInvalid {
		return MaintenancePhysicalEvidence{}, false
	}
	switch proof.evidence.kind {
	case maintenancePhysicalEvidenceSourceReady:
		return proof.evidence, proof.evidence.sourceReady.Valid() && proof.evidence.sourceReady.MaintenanceID() == proof.MaintenanceID()
	case maintenancePhysicalEvidenceTargetDivergent:
		return proof.evidence, proof.evidence.targetDivergent.Valid() && proof.evidence.targetDivergent.MaintenanceID() == proof.MaintenanceID()
	case maintenancePhysicalEvidenceTargetAbsent:
		return proof.evidence, proof.evidence.targetAbsent.Valid() && proof.evidence.targetAbsent.MaintenanceID() == proof.MaintenanceID()
	default:
		return MaintenancePhysicalEvidence{}, false
	}
}

// ReleaseClaim is an opaque compare-and-swap capability for one exact durable
// release row. It is intentionally not constructible outside this package.
type ReleaseClaim struct {
	issuer    *ReleaseStore
	leaseUUID string
	version   int
	digest    [sha256.Size]byte
}

func (c ReleaseClaim) LeaseUUID() string         { return c.leaseUUID }
func (c ReleaseClaim) Version() int              { return c.version }
func (c ReleaseClaim) Digest() [sha256.Size]byte { return c.digest }
func (c ReleaseClaim) valid() bool {
	return backend.IsCanonicalLeaseUUID(c.leaseUUID) && c.version > 0 &&
		c.digest != ([sha256.Size]byte{})
}

// MaintenanceReleaseClaim is the exact target-generation capability returned
// by AppendMaintenance or recovery lookup. Its immutable digest deliberately
// excludes terminal state fields, so the same claim remains valid across the
// deploying -> active/failed transition and cannot match a different cohort.
type MaintenanceReleaseClaim struct {
	settlement      *MaintenanceSettlement
	callbacks       *CallbackStore
	releases        *ReleaseStore
	intent          MaintenanceIntentClaim
	releaseClaim    ReleaseClaim
	maintenanceID   MaintenanceID
	immutableDigest [sha256.Size]byte
}

func (c MaintenanceReleaseClaim) LeaseUUID() string { return c.releaseClaim.leaseUUID }
func (c MaintenanceReleaseClaim) Version() int      { return c.releaseClaim.version }
func (c MaintenanceReleaseClaim) MaintenanceID() MaintenanceID {
	return c.maintenanceID
}
func (c MaintenanceReleaseClaim) Intent() MaintenanceIntentClaim {
	return cloneMaintenanceIntentClaim(c.intent)
}
func (c MaintenanceReleaseClaim) Digest() [sha256.Size]byte { return c.immutableDigest }

func (c MaintenanceReleaseClaim) Valid() bool { return c.valid() }

func (c MaintenanceReleaseClaim) valid() bool {
	return c.settlement != nil && c.callbacks == c.settlement.callbacks &&
		c.releases == c.settlement.releases && c.intent.Valid() &&
		c.intent.settlement == c.settlement &&
		c.releaseClaim.valid() && c.maintenanceID.Valid() &&
		c.immutableDigest != ([sha256.Size]byte{})
}

func (c MaintenanceReleaseClaim) validFor(s *MaintenanceSettlement) bool {
	return s != nil && c.valid() && c.settlement == s
}

// ClaimLatestActive returns a deep-cloned active release and an opaque exact
// fence from the same bbolt snapshot.
func (s *ReleaseStore) claimLatestActive(leaseUUID string) (Release, ReleaseClaim, error) {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return Release{}, ReleaseClaim{}, err
	}
	var release Release
	var claim ReleaseClaim
	err := s.view(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, leaseUUID)
		if err != nil {
			return err
		}
		index := latestActiveReleaseIndex(releases)
		if index < 0 {
			return fmt.Errorf("active release for %s does not exist", leaseUUID)
		}
		release = cloneRelease(releases[index])
		encoded, err := json.Marshal(releases[index])
		if err != nil {
			return fmt.Errorf("marshal active release fence for %s: %w", leaseUUID, err)
		}
		claim = ReleaseClaim{
			issuer:    s,
			leaseUUID: leaseUUID,
			version:   releases[index].Version,
			digest:    sha256.Sum256(encoded),
		}
		return nil
	})
	if err != nil {
		return Release{}, ReleaseClaim{}, err
	}
	return release, claim, nil
}

// ClaimLatestActive returns the active release and the only source capability
// accepted by this maintenance protocol. The capability is bound to the exact
// open ReleaseStore, so a claim from another file or a prior open cannot be
// admitted even when its bytes happen to be identical.
func (s *MaintenanceSettlement) ClaimLatestActive(
	leaseUUID string,
) (Release, MaintenanceSourceClaim, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return Release{}, MaintenanceSourceClaim{}, errors.New("maintenance settlement is invalid")
	}
	release, claim, err := s.releases.claimLatestActive(leaseUUID)
	if err != nil {
		return Release{}, MaintenanceSourceClaim{}, err
	}
	return release, MaintenanceSourceClaim{settlement: s, releases: s.releases, claim: claim}, nil
}

// CheckAppendMaintenanceCapacity proves that the exact source is still active
// and that both terminal forms of target fit before any substrate mutation.
// The check is advisory, but it still consumes pair-bound dispatch authority so
// a replay or a claim from a reopened journal cannot reserve capacity.
func (s *MaintenanceSettlement) CheckAppendMaintenanceCapacity(
	dispatch MaintenanceIntentDispatch,
) error {
	if s == nil || s.callbacks == nil || s.releases == nil || dispatch.settlement != s ||
		dispatch.issuer != s.callbacks ||
		dispatch.releases != s.releases {
		return errors.New("maintenance dispatch belongs to another journal pair")
	}
	if err := validateMaintenanceIntentDispatch(dispatch); err != nil {
		return err
	}
	unlock := s.lockLease(dispatch.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(dispatch.intent); err != nil {
		return err
	}
	source, target, err := s.releases.maintenanceIntentAuthority(dispatch.intent)
	if err != nil {
		return err
	}
	if err := validateMaintenanceAppendInput(source, target); err != nil {
		return err
	}
	return s.releases.view(func(tx *bolt.Tx) error {
		_, _, err := planMaintenanceAppendTx(
			tx,
			source,
			target,
			releaseHistoryCapacityCutoff(s.releases.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		return err
	})
}

// AppendMaintenance appends the exact MaintenanceID-bearing deploying target
// only while the source claim still names the active release. The returned
// capability includes the store-assigned version and immutable target digest.
func (s *MaintenanceSettlement) AppendMaintenance(
	appendClaim MaintenanceAppendClaim,
) (MaintenanceReleaseClaim, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return MaintenanceReleaseClaim{}, errors.New("maintenance settlement is invalid")
	}
	if err := validateMaintenanceAppendClaim(appendClaim); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	if appendClaim.settlement != s || appendClaim.issuer != s.callbacks || appendClaim.releases != s.releases {
		return MaintenanceReleaseClaim{}, errors.New(
			"maintenance append belongs to another journal pair",
		)
	}
	unlock := s.lockLease(appendClaim.intent.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(appendClaim.intent); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	source, target, err := s.releases.maintenanceIntentAuthority(appendClaim.intent)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	if err := validateMaintenanceAppendInput(source, target); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	var claim MaintenanceReleaseClaim
	err = s.releases.update(func(tx *bolt.Tx) error {
		releases, candidate, err := planMaintenanceAppendTx(
			tx,
			source,
			target,
			releaseHistoryCapacityCutoff(s.releases.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return err
		}
		encoded, err := encodeReleaseHistory(releases)
		if err != nil {
			return fmt.Errorf("encode maintenance release history: %w", err)
		}
		if err := tx.Bucket(releasesBucketName).Put([]byte(source.leaseUUID), encoded); err != nil {
			return err
		}
		claim, err = s.newMaintenanceReleaseClaim(appendClaim.intent, candidate)
		return err
	})
	return claim, err
}

func (s *ReleaseStore) maintenanceIntentAuthority(
	intent MaintenanceIntentClaim,
) (ReleaseClaim, Release, error) {
	if err := validateMaintenanceIntentClaim(intent); err != nil {
		return ReleaseClaim{}, Release{}, err
	}
	if s != nil && s.binding != nil && intent.BackendStorageID() != s.binding.storageID {
		return ReleaseClaim{}, Release{}, errors.New(
			"maintenance intent and release journal have different storage identities",
		)
	}
	return intent.SourceRelease(), intent.TargetRelease(), nil
}

// FindMaintenanceRelease finds by the unguessable maintenance identity rather
// than by tail position. It returns (zero, zero, false, nil) when no target was
// ever appended.
func (s *MaintenanceSettlement) FindMaintenanceRelease(
	leaseUUID string,
	maintenanceID MaintenanceID,
) (Release, MaintenanceReleaseClaim, bool, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return Release{}, MaintenanceReleaseClaim{}, false, errors.New("maintenance settlement is invalid")
	}
	if err := s.releases.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	if !maintenanceID.Valid() {
		return Release{}, MaintenanceReleaseClaim{}, false, errors.New("maintenance ID must be a canonical UUIDv4")
	}
	unlock := s.lockLease(leaseUUID)
	defer unlock()
	var found Release
	err := s.releases.view(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, leaseUUID)
		if err != nil {
			return err
		}
		for _, release := range releases {
			if release.MaintenanceID == maintenanceID {
				found = cloneRelease(release)
				return nil
			}
		}
		return nil
	})
	if err != nil || found.MaintenanceID.IsZero() {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	intent, intentFound, err := s.GetMaintenanceIntent(leaseUUID)
	if err != nil {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	if !intentFound || intent.MaintenanceID() != maintenanceID {
		return Release{}, MaintenanceReleaseClaim{}, false,
			errors.New("maintenance target has no current intent in this journal pair")
	}
	claim, err := s.newMaintenanceReleaseClaim(intent, found)
	if err != nil {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	return found, claim, true, nil
}

// StartMaintenanceExecution durably advances a bound target across the
// irreversible-effect boundary. A copied target from before this transaction
// becomes stale and can no longer authorize a synchronous refusal.
func (s *MaintenanceSettlement) StartMaintenanceExecution(
	target MaintenanceReleaseClaim,
) (MaintenanceExecutionClaim, error) {
	if !target.validFor(s) || !target.intent.entry.EffectNotStarted {
		return MaintenanceExecutionClaim{}, errors.New(
			"maintenance target is not in the pre-effect phase",
		)
	}
	var refreshedTarget MaintenanceReleaseClaim
	var subject MaintenancePhysicalSubject
	started, err := s.mutation.BeginAfter(func() (MaintenancePhysicalSubject, error) {
		unlock := s.lockLease(target.LeaseUUID())
		defer unlock()
		intent, err := s.currentIntentForTargetLocked(target)
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		err = s.callbacks.update(func(tx *bolt.Tx) error {
			entry := cloneMaintenanceIntentEntry(intent.entry)
			entry.EffectNotStarted = false
			data, err := marshalMaintenanceIntent(entry)
			if err != nil {
				return err
			}
			next, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
			if err != nil {
				return err
			}
			transition, err := newStartMaintenanceExecutionLeaseMutation(intent, next)
			if err != nil {
				return err
			}
			written, err := applyLeaseMutationTx(tx, transition)
			if err != nil {
				return err
			}
			refreshed, err := s.mintMaintenanceIntentClaim(
				written.(maintenanceLeaseMutationHead).claim,
			)
			if err != nil {
				return err
			}
			var ok bool
			refreshedTarget, ok = s.targetReleaseClaim(refreshed)
			if !ok {
				return errors.New("started maintenance execution lost its target")
			}
			source, err := s.snapshotMaintenanceSourceLocked(refreshed)
			if err != nil {
				return err
			}
			subject = newMaintenancePhysicalSubject(s, refreshedTarget, source)
			return nil
		})
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		return subject, nil
	})
	if err != nil {
		return MaintenanceExecutionClaim{}, err
	}
	return MaintenanceExecutionClaim{
		settlement: s, target: refreshedTarget, subject: subject, started: started,
	}, nil
}

// RefuseMaintenanceExecution mints pre-effect failure authority. It fails
// after StartMaintenanceExecution because the exact target intent digest has
// advanced durably.
func (s *MaintenanceSettlement) RefuseMaintenanceExecution(
	target MaintenanceReleaseClaim,
) (MaintenanceExecutionFailure, error) {
	if !target.validFor(s) || !target.intent.entry.EffectNotStarted {
		return MaintenanceExecutionFailure{}, errors.New(
			"maintenance target is not in the pre-effect phase",
		)
	}
	unlock := s.lockLease(target.LeaseUUID())
	defer unlock()
	current, err := s.currentIntentForTargetLocked(target)
	if err != nil {
		return MaintenanceExecutionFailure{}, err
	}
	if !current.entry.EffectNotStarted {
		return MaintenanceExecutionFailure{}, errors.New(
			"maintenance execution has already started",
		)
	}
	return MaintenanceExecutionFailure{
		settlement: s, authority: target, refused: true,
	}, nil
}

// ExecuteMaintenance is the normal physical boundary for restart and update.
// It does not hold the per-lease journal lock while the external action runs.
// Only after that action returns does it mint an outcome-specific capability;
// panic or an explicitly ambiguous mutation outcome leaves the intent and
// deploying release non-terminal for strict-inventory recovery.
func (s *MaintenanceSettlement) ExecuteMaintenance(
	ctx context.Context,
	execution MaintenanceExecutionClaim,
) (outcome MaintenanceExecutionOutcome) {
	if s == nil || s.execute == nil || ctx == nil || execution.settlement != s ||
		!execution.target.validFor(s) || execution.target.intent.entry.EffectNotStarted ||
		!execution.subject.validFor(s) {
		return MaintenanceExecutionAmbiguous{
			settlement: s, execution: execution,
			cause: errors.New("maintenance execution boundary is invalid"),
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome = MaintenanceExecutionAmbiguous{
				settlement: s,
				execution:  execution,
				cause:      fmt.Errorf("maintenance worker panic: %v\n%s", recovered, debug.Stack()),
			}
		}
	}()
	physical := s.execute(ctx, execution.started)
	if err := substratemutation.ValidateLiveResult(s.mutation, execution.started, physical); err != nil {
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: err}
	}
	switch physical.Kind() {
	case substratemutation.Attested:
		evidence, ok := physical.Evidence()
		if !ok {
			return MaintenanceExecutionAmbiguous{settlement: s, execution: execution,
				cause: errors.New("attested maintenance result has no evidence")}
		}
		if err := validateMaintenancePhysicalEvidence(execution.subject, evidence); err != nil {
			return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: err}
		}
		switch evidence.kind {
		case maintenancePhysicalEvidenceTargetReady:
			return MaintenanceExecutionSuccess{
				settlement: s, execution: execution, ready: evidence.targetReady,
			}
		case maintenancePhysicalEvidenceSourceReady, maintenancePhysicalEvidenceTargetDivergent,
			maintenancePhysicalEvidenceTargetAbsent:
			return MaintenanceExecutionFailure{
				settlement: s, authority: execution.target,
				subject: execution.subject, evidence: evidence,
			}
		default:
			return MaintenanceExecutionAmbiguous{settlement: s, execution: execution,
				cause: errors.New("unknown maintenance evidence")}
		}
	case substratemutation.Refused:
		return MaintenanceExecutionFailure{
			settlement: s, authority: execution.target, refused: true,
			cause: physical.Err(),
		}
	case substratemutation.Ambiguous:
		cause := physical.Err()
		if cause == nil {
			cause = errors.New("maintenance mutation outcome is ambiguous")
		}
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: cause}
	default:
		return MaintenanceExecutionAmbiguous{
			settlement: s, execution: execution,
			cause: fmt.Errorf("invalid maintenance mutation outcome %s", physical.Kind()),
		}
	}
}

func (s *MaintenanceSettlement) recoveryMaintenanceTarget(
	intent MaintenanceIntentClaim,
) (MaintenanceReleaseClaim, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	target, ok := s.targetReleaseClaim(intent)
	if !ok {
		return MaintenanceReleaseClaim{}, errors.New("maintenance intent has no bound target")
	}
	return target, nil
}

// RecoverMaintenanceExecution is the sole post-restart physical classifier.
// Its strict inventory implementation is fixed when the settlement is bound;
// callers cannot inject a success/failure verifier.
func (s *MaintenanceSettlement) RecoverMaintenanceExecution(
	ctx context.Context,
	scope LeaseRecoveryScope,
	intent MaintenanceIntentClaim,
) (MaintenanceExecutionOutcome, error) {
	if s == nil {
		return nil, errors.New("maintenance recovery requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, intent.LeaseUUID())
	if !valid {
		return nil, errors.New("maintenance recovery requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.recovery == nil {
		return nil, errors.New("maintenance substrate recovery attestor is not bound")
	}
	target, err := s.recoveryMaintenanceTarget(intent)
	if err != nil {
		return nil, err
	}
	if target.intent.entry.EffectNotStarted {
		return s.RefuseMaintenanceExecution(target)
	}
	var currentTarget MaintenanceReleaseClaim
	var subject MaintenancePhysicalSubject
	recovered, err := s.mutation.RecoverAfter(func() (MaintenancePhysicalSubject, error) {
		unlock := s.lockLease(intent.LeaseUUID())
		defer unlock()
		current, err := s.currentIntentForTargetLocked(target)
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		if current.entry.EffectNotStarted {
			return MaintenancePhysicalSubject{}, errors.New(
				"maintenance has not crossed the durable execution boundary",
			)
		}
		var ok bool
		currentTarget, ok = s.targetReleaseClaim(current)
		if !ok {
			return MaintenancePhysicalSubject{}, errors.New("maintenance recovery lost its target")
		}
		source, err := s.snapshotMaintenanceSourceLocked(current)
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		subject = newMaintenancePhysicalSubject(s, currentTarget, source)
		return subject, nil
	})
	if err != nil {
		return nil, err
	}
	result := s.recovery.Inspect(recovered, ctx)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, recovered, result); err != nil {
		return nil, err
	}
	execution := MaintenanceExecutionClaim{
		settlement: s, target: currentTarget, subject: subject,
	}
	switch result.Kind() {
	case substratemutation.Attested:
		evidence, ok := result.Evidence()
		if !ok {
			return nil, errors.New("attested maintenance recovery has no evidence")
		}
		if err := validateMaintenancePhysicalEvidence(subject, evidence); err != nil {
			return nil, err
		}
		switch evidence.kind {
		case maintenancePhysicalEvidenceTargetReady:
			return MaintenanceExecutionSuccess{
				settlement: s, execution: execution, ready: evidence.targetReady,
			}, nil
		case maintenancePhysicalEvidenceSourceReady, maintenancePhysicalEvidenceTargetDivergent,
			maintenancePhysicalEvidenceTargetAbsent:
			return MaintenanceExecutionFailure{
				settlement: s, authority: currentTarget,
				subject: subject, evidence: evidence,
			}, nil
		default:
			return nil, errors.New("unknown maintenance recovery evidence")
		}
	case substratemutation.Ambiguous:
		cause := result.Err()
		if cause == nil {
			cause = errors.New("maintenance recovery remained ambiguous")
		}
		return MaintenanceExecutionAmbiguous{
			settlement: s, execution: execution, cause: cause,
		}, nil
	default:
		return nil, fmt.Errorf("invalid maintenance recovery result %s: %w", result.Kind(), result.Err())
	}
}

// CleanupRecoveredMaintenance runs the construction-bound destructive
// recovery handler for one exact current Started maintenance intent. No caller
// supplies a container ID, project, volume name, or cleanup closure; those are
// derived from the opaque subject minted by the durable journal re-read.
func (s *MaintenanceSettlement) CleanupRecoveredMaintenance(
	ctx context.Context,
	scope LeaseRecoveryScope,
	intent MaintenanceIntentClaim,
) (MaintenanceExecutionOutcome, error) {
	if s == nil {
		return nil, errors.New("maintenance cleanup requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, intent.LeaseUUID())
	if !valid {
		return nil, errors.New("maintenance cleanup requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.executeRecovery == nil {
		return nil, errors.New("maintenance substrate recovery executor is not bound")
	}
	target, err := s.recoveryMaintenanceTarget(intent)
	if err != nil {
		return nil, err
	}
	if target.intent.entry.EffectNotStarted {
		return s.RefuseMaintenanceExecution(target)
	}
	var currentTarget MaintenanceReleaseClaim
	var subject MaintenancePhysicalSubject
	execution, err := s.mutation.RecoverAfter(func() (MaintenancePhysicalSubject, error) {
		unlock := s.lockLease(intent.LeaseUUID())
		defer unlock()
		current, err := s.currentIntentForTargetLocked(target)
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		if current.entry.EffectNotStarted {
			return MaintenancePhysicalSubject{}, errors.New(
				"maintenance has not crossed the durable execution boundary",
			)
		}
		var ok bool
		currentTarget, ok = s.targetReleaseClaim(current)
		if !ok {
			return MaintenancePhysicalSubject{}, errors.New("maintenance recovery lost its target")
		}
		source, err := s.snapshotMaintenanceSourceLocked(current)
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		subject = newMaintenancePhysicalSubjectForMode(s, currentTarget, source, true)
		return subject, nil
	})
	if err != nil {
		return nil, err
	}
	result := s.executeRecovery(ctx, execution)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, execution, result); err != nil {
		return nil, err
	}
	claimResult := MaintenanceExecutionClaim{
		settlement: s, target: currentTarget, subject: subject,
	}
	switch result.Kind() {
	case substratemutation.Attested:
		evidence, ok := result.Evidence()
		if !ok {
			return nil, errors.New("attested maintenance cleanup has no evidence")
		}
		if err := validateMaintenancePhysicalEvidence(subject, evidence); err != nil {
			return nil, err
		}
		switch evidence.kind {
		case maintenancePhysicalEvidenceTargetReady:
			return MaintenanceExecutionSuccess{
				settlement: s, execution: claimResult, ready: evidence.targetReady,
			}, nil
		case maintenancePhysicalEvidenceSourceReady, maintenancePhysicalEvidenceTargetDivergent,
			maintenancePhysicalEvidenceTargetAbsent:
			return MaintenanceExecutionFailure{
				settlement: s, authority: currentTarget, subject: subject, evidence: evidence,
			}, nil
		default:
			return nil, errors.New("unknown maintenance cleanup evidence")
		}
	case substratemutation.Refused, substratemutation.Ambiguous:
		cause := result.Err()
		if cause == nil {
			cause = errors.New("maintenance recovery cleanup remained ambiguous")
		}
		return MaintenanceExecutionAmbiguous{
			settlement: s, execution: claimResult, cause: cause,
		}, nil
	default:
		return nil, fmt.Errorf("invalid maintenance cleanup result %s: %w", result.Kind(), result.Err())
	}
}

// CleanupFailedMaintenanceReceipt invokes only the construction-bound
// late-arrival cleanup workflow for one exact permanent failure receipt. A
// fresh receipt re-read occurs after lease exclusion is held, so neither a
// caller-selected ID nor a receipt removed by close can authorize mutation.
func (s *MaintenanceSettlement) CleanupFailedMaintenanceReceipt(
	ctx context.Context,
	scope LeaseRecoveryScope,
	receipt FailedMaintenanceReceipt,
) error {
	if s == nil {
		return errors.New("failed-maintenance cleanup requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, receipt.LeaseUUID())
	if !valid {
		return errors.New("failed-maintenance cleanup requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.executeRecovery == nil || !s.ownsFailedMaintenanceReceipt(receipt) {
		return errors.New("failed-maintenance cleanup receipt belongs to another journal pair")
	}
	var subject MaintenancePhysicalSubject
	execution, err := s.mutation.RecoverAfter(func() (MaintenancePhysicalSubject, error) {
		current, err := s.ListFailedMaintenanceReceipts()
		if err != nil {
			return MaintenancePhysicalSubject{}, err
		}
		found := false
		for _, candidate := range current {
			if sameFailedMaintenanceReceipt(candidate, receipt) {
				found = true
				break
			}
		}
		if !found {
			return MaintenancePhysicalSubject{}, errors.New(
				"failed-maintenance cleanup receipt is no longer durable",
			)
		}
		subject = newFailedMaintenanceCleanupSubject(s, receipt)
		return subject, nil
	})
	if err != nil {
		return err
	}
	result := s.executeRecovery(ctx, execution)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, execution, result); err != nil {
		return err
	}
	if result.Kind() != substratemutation.Attested {
		return fmt.Errorf("failed-maintenance cleanup is %s: %w", result.Kind(), result.Err())
	}
	evidence, ok := result.Evidence()
	if !ok {
		return errors.New("attested failed-maintenance cleanup has no evidence")
	}
	if err := validateMaintenancePhysicalEvidence(subject, evidence); err != nil {
		return err
	}
	if evidence.kind != maintenancePhysicalEvidenceFailedReceiptAbsent {
		return errors.New("failed-maintenance cleanup has wrong evidence")
	}
	return nil
}

func (s *MaintenanceSettlement) ownsFailedMaintenanceReceipt(receipt FailedMaintenanceReceipt) bool {
	return s != nil && receipt.Valid() && receipt.settlement == s &&
		receipt.callbacks == s.callbacks && receipt.releases == s.releases
}

func sameFailedMaintenanceReceipt(left, right FailedMaintenanceReceipt) bool {
	return left.Valid() && right.Valid() && left.settlement == right.settlement &&
		left.record == right.record && left.target.Digest() == right.target.Digest()
}

// ActivateMaintenance consumes physical-success evidence for one exact
// replacement generation. Replaying the same outcome after commit is
// idempotent.
func (s *MaintenanceSettlement) ActivateMaintenance(
	outcome MaintenanceExecutionSuccess,
) (MaintenanceReleaseActive, error) {
	if outcome.settlement != s || !outcome.Valid() {
		return MaintenanceReleaseActive{}, errors.New(
			"maintenance success outcome belongs to another execution boundary",
		)
	}
	target := outcome.execution.target
	if !target.validFor(s) {
		return MaintenanceReleaseActive{}, errors.New(
			"maintenance target belongs to another journal pair",
		)
	}
	unlock := s.lockLease(target.LeaseUUID())
	defer unlock()
	intent, err := s.currentIntentForTargetLocked(target)
	if err != nil {
		return MaintenanceReleaseActive{}, err
	}
	if err := s.releases.transitionMaintenance(target, "active", "", ""); err != nil {
		return MaintenanceReleaseActive{}, err
	}
	proof, err := s.proveActiveLocked(intent, target)
	if err != nil {
		return MaintenanceReleaseActive{}, err
	}
	release, ok := proof.TargetRelease()
	if !ok {
		return MaintenanceReleaseActive{}, errors.New("active maintenance release cannot be reconstructed")
	}
	proof.ready = outcome.ready.withCommittedRelease(release)
	if !proof.ready.Valid() {
		return MaintenanceReleaseActive{}, errors.New("active maintenance ready evidence cannot be rebound")
	}
	return proof, nil
}

// FailMaintenance records a terminal failure against one exact target and
// returns the only capability that can settle its maintenance callback as
// failed. Replaying the same failed target is idempotent.
func (s *MaintenanceSettlement) FailMaintenance(
	outcome MaintenanceExecutionFailure,
	reason backend.Reason,
	message string,
) (MaintenanceReleaseFailure, error) {
	if outcome.settlement != s || !outcome.Valid() {
		return MaintenanceReleaseFailure{}, errors.New(
			"maintenance failure outcome belongs to another execution boundary",
		)
	}
	target := outcome.authority
	if !target.validFor(s) {
		return MaintenanceReleaseFailure{}, errors.New(
			"maintenance target belongs to another journal pair",
		)
	}
	unlock := s.lockLease(target.LeaseUUID())
	defer unlock()
	intent, err := s.currentIntentForTargetLocked(target)
	if err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	if err := s.releases.transitionMaintenance(target, "failed", reason, message); err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	proof, err := s.proveFailureLocked(intent)
	if err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	proof.evidence = outcome.evidence
	return proof, nil
}

func (s *ReleaseStore) transitionMaintenance(
	claim MaintenanceReleaseClaim,
	status string,
	reason backend.Reason,
	message string,
) error {
	if !claim.valid() || claim.releases != s {
		return errors.New("maintenance release claim has no durable capability")
	}
	if status != "active" && status != "failed" {
		return fmt.Errorf("invalid maintenance terminal status %q", status)
	}
	return s.update(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, claim.LeaseUUID())
		if err != nil {
			return err
		}
		index := maintenanceReleaseIndex(releases, claim.MaintenanceID())
		if index < 0 {
			return fmt.Errorf("maintenance release %s no longer exists for lease %s",
				claim.MaintenanceID(), claim.LeaseUUID())
		}
		candidate := &releases[index]
		if candidate.Version != claim.Version() {
			return errors.New("maintenance release version changed before precise mutation")
		}
		digest, err := maintenanceReleaseDigest(*candidate)
		if err != nil {
			return err
		}
		if digest != claim.Digest() {
			return errors.New("maintenance release changed before precise mutation")
		}
		if candidate.Status == status {
			return nil
		}
		if candidate.Status != "deploying" {
			return fmt.Errorf("maintenance release is already terminal with status %q", candidate.Status)
		}
		if status == "active" {
			for i := range releases {
				if i != index && releases[i].Status == "active" {
					releases[i].Status = "superseded"
				}
			}
			candidate.Status = "active"
			candidate.Reason = ""
			candidate.Message = ""
			candidate.Error = ""
		} else {
			candidate.Status = "failed"
			candidate.Reason = reason
			candidate.Message = message
			candidate.Error = ""
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid maintenance terminal history: %w", err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil && status == "failed" && errors.Is(err, ErrReleaseHistoryCapacity) &&
			(reason != "" || message != "") {
			candidate.Reason = ""
			candidate.Message = ""
			encoded, err = compactAndEncodeReleaseHistory(
				releases,
				releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
				backend.MaxStoredReleaseHistoryBytes,
			)
		}
		if err != nil {
			return fmt.Errorf("encode maintenance terminal history: %w", err)
		}
		return tx.Bucket(releasesBucketName).Put([]byte(claim.LeaseUUID()), encoded)
	})
}

// ProveMaintenanceActive reconstructs success authority after a process crash
// or an ambiguous callback-journal commit. The target must be bound into the
// still-current intent and remain the exact active generation.
func (s *MaintenanceSettlement) ProveMaintenanceActive(
	intent MaintenanceIntentClaim,
) (MaintenanceReleaseActive, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceReleaseActive{}, err
	}
	target, ok := s.targetReleaseClaim(intent)
	if !ok {
		return MaintenanceReleaseActive{}, errors.New(
			"unbound maintenance intent cannot prove an active target release",
		)
	}
	unlock := s.lockLease(intent.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(intent); err != nil {
		return MaintenanceReleaseActive{}, err
	}
	return s.proveActiveLocked(intent, target)
}

// ProveMaintenanceFailure reconstructs failure authority after restart. It
// grants exactly one of two facts: the bound target is terminal failed, or an
// unbound intent has no target bearing its MaintenanceID anywhere in history.
func (s *MaintenanceSettlement) ProveMaintenanceFailure(
	intent MaintenanceIntentClaim,
) (MaintenanceReleaseFailure, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	unlock := s.lockLease(intent.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(intent); err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	return s.proveFailureLocked(intent)
}

func (s *MaintenanceSettlement) validateIntent(intent MaintenanceIntentClaim) error {
	if s == nil || !s.valid() {
		return errors.New("maintenance settlement is invalid")
	}
	if err := validateMaintenanceIntentClaim(intent); err != nil {
		return err
	}
	if intent.settlement != s || intent.callbacks != s.callbacks || intent.releases != s.releases {
		return errors.New("maintenance intent was not minted by this journal pair")
	}
	if s.callbacks.binding == nil && s.releases.binding == nil {
		// Package-local unbound stores exist only for corruption and schema tests;
		// NewMaintenanceSettlement never admits this shape in production.
		return nil
	}
	if s.callbacks.binding == nil || s.releases.binding == nil ||
		s.callbacks.backendAuthorityGate == nil ||
		s.callbacks.backendAuthorityGate != s.releases.backendAuthorityGate ||
		intent.Backend() != s.callbacks.binding.backendName ||
		intent.Backend() != s.releases.binding.backendName ||
		intent.BackendStorageID() != s.callbacks.binding.storageID ||
		intent.BackendStorageID() != s.releases.binding.storageID {
		return errors.New("maintenance intent belongs to another journal pair")
	}
	return nil
}

func (s *MaintenanceSettlement) validateIntentTarget(
	intent MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) error {
	if err := s.validateIntent(intent); err != nil {
		return err
	}
	if !target.validFor(s) || intent.MaintenanceID() != target.intent.MaintenanceID() ||
		intent.LeaseUUID() != target.intent.LeaseUUID() ||
		intent.BackendStorageID() != target.intent.BackendStorageID() ||
		intent.sourceDigest != target.intent.sourceDigest ||
		intent.entry.RequestDigest != target.intent.entry.RequestDigest {
		return errors.New("maintenance target belongs to another intent or journal pair")
	}
	return validateMaintenanceIntentTargetBinding(intent, target)
}

// currentIntentForTargetLocked refreshes the callback-side phase after target
// binding. MaintenanceReleaseClaim seals the immutable intent lineage, while
// BindMaintenanceIntentTarget necessarily advances the callback-row digest;
// terminal mutation must consume that exact current successor rather than a
// stale pre-bind snapshot supplied by the caller.
func (s *MaintenanceSettlement) currentIntentForTargetLocked(
	target MaintenanceReleaseClaim,
) (MaintenanceIntentClaim, error) {
	intent, found, err := s.GetMaintenanceIntent(target.LeaseUUID())
	if err != nil {
		return MaintenanceIntentClaim{}, err
	}
	if !found {
		return MaintenanceIntentClaim{}, errors.New("maintenance intent no longer exists")
	}
	if err := s.validateIntentTarget(intent, target); err != nil {
		return MaintenanceIntentClaim{}, err
	}
	return intent, nil
}

func (s *MaintenanceSettlement) proveActiveLocked(
	intent MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (MaintenanceReleaseActive, error) {
	terminal, err := s.requireMaintenanceTerminal(target, "active")
	if err != nil {
		return MaintenanceReleaseActive{}, err
	}
	return MaintenanceReleaseActive{
		settlement: s,
		callbacks:  s.callbacks,
		releases:   s.releases,
		intent:     cloneMaintenanceIntentClaim(intent),
		target:     target,
		terminal:   terminal,
	}, nil
}

func (s *MaintenanceSettlement) proveFailureLocked(
	intent MaintenanceIntentClaim,
) (MaintenanceReleaseFailure, error) {
	target, bound := s.targetReleaseClaim(intent)
	if !bound {
		found, err := s.releases.hasMaintenanceRelease(
			intent.LeaseUUID(), intent.MaintenanceID(),
		)
		if err != nil {
			return MaintenanceReleaseFailure{}, err
		}
		if found {
			return MaintenanceReleaseFailure{}, errors.New(
				"unbound maintenance intent has a durable target release",
			)
		}
		return MaintenanceReleaseFailure{
			settlement: s,
			callbacks:  s.callbacks,
			releases:   s.releases,
			intent:     cloneMaintenanceIntentClaim(intent),
			absent:     true,
		}, nil
	}
	terminal, err := s.requireMaintenanceTerminal(target, "failed")
	if err != nil {
		return MaintenanceReleaseFailure{}, err
	}
	return MaintenanceReleaseFailure{
		settlement: s,
		callbacks:  s.callbacks,
		releases:   s.releases,
		intent:     cloneMaintenanceIntentClaim(intent),
		target:     target,
		terminal:   terminal,
	}, nil
}

func (s *ReleaseStore) hasMaintenanceRelease(
	leaseUUID string,
	maintenanceID MaintenanceID,
) (bool, error) {
	var found bool
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		data := bucket.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}
		history, err := decodeReleaseHistory(data)
		if err != nil {
			return err
		}
		if err := validateReleaseHistory(history); err != nil {
			return err
		}
		found = maintenanceReleaseIndex(history, maintenanceID) >= 0
		return nil
	})
	return found, err
}

func (s *MaintenanceSettlement) requireMaintenanceTerminal(
	target MaintenanceReleaseClaim,
	status string,
) (ReleaseClaim, error) {
	if !target.validFor(s) {
		return ReleaseClaim{}, errors.New("maintenance target claim has no durable capability")
	}
	var terminal ReleaseClaim
	err := s.releases.view(func(tx *bolt.Tx) error {
		history, err := readReleaseHistoryTx(tx, target.LeaseUUID())
		if err != nil {
			return err
		}
		index := maintenanceReleaseIndex(history, target.MaintenanceID())
		if index < 0 {
			return errors.New("exact maintenance target release no longer exists")
		}
		release := history[index]
		if release.Version != target.Version() {
			return errors.New("maintenance target version changed before terminal proof")
		}
		immutable, err := maintenanceReleaseDigest(release)
		if err != nil {
			return err
		}
		if immutable != target.Digest() {
			return errors.New("maintenance target changed before terminal proof")
		}
		if release.Status != status {
			return fmt.Errorf("maintenance target is %q, not %q", release.Status, status)
		}
		encoded, err := json.Marshal(release)
		if err != nil {
			return fmt.Errorf("marshal terminal maintenance release: %w", err)
		}
		terminal = ReleaseClaim{
			issuer:    s.releases,
			leaseUUID: target.LeaseUUID(),
			version:   target.Version(),
			digest:    sha256.Sum256(encoded),
		}
		return nil
	})
	if err != nil {
		return ReleaseClaim{}, err
	}
	return terminal, nil
}

func cloneMaintenanceIntentClaim(claim MaintenanceIntentClaim) MaintenanceIntentClaim {
	claim.entry = cloneMaintenanceIntentEntry(claim.entry)
	return claim
}

func maintenanceIntentClaimsEqual(left, right MaintenanceIntentClaim) bool {
	return left.digest == right.digest && left.maintenanceID == right.maintenanceID &&
		left.storageID == right.storageID && left.callbacks == right.callbacks &&
		left.releases == right.releases && left.settlement == right.settlement
}

func (s *MaintenanceSettlement) validateActiveProofLocked(
	intent MaintenanceIntentClaim,
	proof MaintenanceReleaseActive,
) error {
	if !proof.Valid() || proof.settlement != s || proof.callbacks != s.callbacks || proof.releases != s.releases ||
		!maintenanceIntentClaimsEqual(intent, proof.intent) {
		return errors.New("maintenance success proof belongs to another intent or journal pair")
	}
	current, err := s.requireMaintenanceTerminal(proof.target, "active")
	if err != nil {
		return err
	}
	if current != proof.terminal {
		return errors.New("active maintenance release changed after proof issuance")
	}
	return nil
}

func (s *MaintenanceSettlement) validateFailureProofLocked(
	intent MaintenanceIntentClaim,
	proof MaintenanceReleaseFailure,
) error {
	if !proof.Valid() || proof.settlement != s || proof.callbacks != s.callbacks || proof.releases != s.releases ||
		!maintenanceIntentClaimsEqual(intent, proof.intent) {
		return errors.New("maintenance failure proof belongs to another intent or journal pair")
	}
	if proof.absent {
		found, err := s.releases.hasMaintenanceRelease(intent.LeaseUUID(), intent.MaintenanceID())
		if err != nil {
			return err
		}
		if found {
			return errors.New("maintenance target appeared after absence proof issuance")
		}
		return nil
	}
	current, err := s.requireMaintenanceTerminal(proof.target, "failed")
	if err != nil {
		return err
	}
	if current != proof.terminal {
		return errors.New("failed maintenance release changed after proof issuance")
	}
	return nil
}

func validateMaintenanceAppendInput(source ReleaseClaim, target Release) error {
	if !source.valid() {
		return errors.New("maintenance source release claim has no durable capability")
	}
	if target.Version != 0 {
		return errors.New("maintenance target version must be store-assigned")
	}
	if target.Status != "deploying" {
		return errors.New("maintenance target must start deploying")
	}
	if !target.MaintenanceID.Valid() {
		return errors.New("maintenance target requires a canonical UUIDv4 maintenance ID")
	}
	if _, ok := releaseRuntimeIdentityFor(target); !ok {
		return errors.New("maintenance target requires durable runtime authority")
	}
	if err := validateStoredRelease(target); err != nil {
		return fmt.Errorf("invalid maintenance release: %w", err)
	}
	if len(target.Items) > 0 && len(target.ResourceProfiles) == 0 {
		return errors.New("invalid maintenance release: desired items require exact resource profiles")
	}
	return nil
}

func planMaintenanceAppendTx(
	tx *bolt.Tx,
	source ReleaseClaim,
	target Release,
	cutoff time.Time,
	limitBytes int,
) ([]Release, Release, error) {
	if source.LeaseUUID() == "" {
		return nil, Release{}, errors.New("maintenance source lease is empty")
	}
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return nil, Release{}, errors.New("releases bucket missing")
	}
	data := bucket.Get([]byte(source.LeaseUUID()))
	if data == nil {
		return nil, Release{}, fmt.Errorf("release history for %s does not exist", source.LeaseUUID())
	}
	current, err := decodeReleaseHistory(data)
	if err != nil {
		return nil, Release{}, fmt.Errorf("corrupted release data for %s: %w", source.LeaseUUID(), err)
	}
	if err := validateReleaseHistory(current); err != nil {
		return nil, Release{}, fmt.Errorf("invalid release data for %s: %w", source.LeaseUUID(), err)
	}
	sourceRelease, err := verifySourceRelease(current, source)
	if err != nil {
		return nil, Release{}, err
	}
	sourceAuthority, sourceOK := releaseRuntimeIdentityFor(sourceRelease)
	targetAuthority, targetOK := releaseRuntimeIdentityFor(target)
	if !sourceOK || !targetOK {
		return nil, Release{}, errors.New("maintenance source and target require runtime authority")
	}
	if sourceAuthority.class != targetAuthority.class {
		return nil, Release{}, errors.New("maintenance target changes runtime authority class")
	}
	if target.OperationID != sourceRelease.OperationID ||
		targetAuthority.operationID != sourceAuthority.operationID {
		return nil, Release{}, errors.New("maintenance target changes operation lineage")
	}
	if targetAuthority.tenant != sourceAuthority.tenant {
		return nil, Release{}, errors.New("maintenance target changes tenant authority")
	}
	if targetAuthority.providerUUID != sourceAuthority.providerUUID {
		return nil, Release{}, errors.New("maintenance target changes provider authority")
	}
	// Callback route bases are trusted configuration and may intentionally rotate
	// during maintenance. The class-specific constructors have already proven a
	// coherent typed identity or a wholly tokenless legacy pair, so byte-equal
	// URLs are neither required nor a stronger identity fence here.
	if maintenanceReleaseIndex(current, target.MaintenanceID) >= 0 {
		return nil, Release{}, errors.New("maintenance target ID already exists")
	}
	planned, _, err := planMaintenanceAppendedReleaseHistory(
		data,
		source.LeaseUUID(),
		cloneRelease(target),
		false,
		cutoff,
		limitBytes,
	)
	if err != nil {
		return nil, Release{}, err
	}
	index := maintenanceReleaseIndex(planned, target.MaintenanceID)
	if index < 0 {
		return nil, Release{}, errors.New("maintenance target was not retained by append plan")
	}
	return planned, cloneRelease(planned[index]), nil
}

func readReleaseHistoryTx(tx *bolt.Tx, leaseUUID string) ([]Release, error) {
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return nil, errors.New("releases bucket missing")
	}
	data := bucket.Get([]byte(leaseUUID))
	if data == nil {
		return nil, fmt.Errorf("release history for %s does not exist", leaseUUID)
	}
	releases, err := decodeReleaseHistory(data)
	if err != nil {
		return nil, fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
	}
	if err := validateReleaseHistory(releases); err != nil {
		return nil, fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
	}
	return releases, nil
}

func verifySourceRelease(releases []Release, source ReleaseClaim) (Release, error) {
	for _, release := range releases {
		if release.Version != source.Version() {
			continue
		}
		encoded, err := json.Marshal(release)
		if err != nil {
			return Release{}, err
		}
		if sha256.Sum256(encoded) != source.Digest() || release.Status != "active" {
			return Release{}, errors.New("maintenance source release changed before admission")
		}
		return release, nil
	}
	return Release{}, errors.New("maintenance source release no longer exists")
}

func maintenanceReleaseIndex(releases []Release, maintenanceID MaintenanceID) int {
	for index := range releases {
		if releases[index].MaintenanceID == maintenanceID {
			return index
		}
	}
	return -1
}

func (s *MaintenanceSettlement) newMaintenanceReleaseClaim(
	intent MaintenanceIntentClaim,
	release Release,
) (MaintenanceReleaseClaim, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	if release.MaintenanceID != intent.MaintenanceID() {
		return MaintenanceReleaseClaim{}, errors.New("maintenance release belongs to another intent")
	}
	digest, err := maintenanceReleaseDigest(release)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	return MaintenanceReleaseClaim{
		settlement:      s,
		callbacks:       s.callbacks,
		releases:        s.releases,
		intent:          cloneMaintenanceIntentClaim(intent),
		releaseClaim:    ReleaseClaim{issuer: s.releases, leaseUUID: intent.LeaseUUID(), version: release.Version, digest: digest},
		maintenanceID:   release.MaintenanceID,
		immutableDigest: digest,
	}, nil
}

func maintenanceReleaseDigest(release Release) ([sha256.Size]byte, error) {
	release = cloneRelease(release)
	release.Status = ""
	release.Error = ""
	release.Reason = ""
	release.Message = ""
	encoded, err := json.Marshal(release)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal maintenance release digest: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
