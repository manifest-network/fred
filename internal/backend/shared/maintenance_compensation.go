package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

var maintenanceCompensationBucket = []byte("maintenance-compensation-v1")

// MaintenanceTargetLaunchStep names the sole concrete effect whose receipt
// can authorize compensation. Image helper or other mutation receipts cannot.
const MaintenanceTargetLaunchStep = "launch prepared managed compose project"
const MaintenanceSourceLaunchStep = "settle frozen compensation source effects"

// Compensation observations are subordinate to the current maintenance head.
// Neither a stored plan nor an elapsed recovery deadline grants execution.
type compensationPhase string

const (
	compensationPrepared          compensationPhase = "prepared"
	compensationUnavailable       compensationPhase = "source-unavailable"
	compensationTargetDispatching compensationPhase = "target-dispatching"
	compensationTargetSettled     compensationPhase = "target-settled"
	compensationRestoring         compensationPhase = "restoring"
	compensationSourceDispatching compensationPhase = "source-dispatching"
	compensationSourceSettled     compensationPhase = "source-effects-settled"
	compensationSourceReady       compensationPhase = "source-ready"
	compensationSourceFailed      compensationPhase = "source-failed"
)

type maintenanceCompensationRecord struct {
	Version       uint8             `json:"version"`
	LeaseUUID     string            `json:"lease_uuid"`
	MaintenanceID MaintenanceID     `json:"maintenance_id"`
	SourceDigest  string            `json:"source_digest"`
	TargetDigest  string            `json:"target_digest"`
	Phase         compensationPhase `json:"phase"`
	Plan          json.RawMessage   `json:"plan,omitempty"`
}

// MaintenanceSourceCapture is an observation from the construction-bound
// source inspector. An unavailable prior source preserves Restart-from-Failed;
// it never acquires restoration authority. The zero observation is invalid.
type MaintenanceSourceCapture struct {
	kind uint8
	plan []byte
}

func CapturedMaintenanceSource(plan []byte) (MaintenanceSourceCapture, error) {
	if len(plan) == 0 || !json.Valid(plan) {
		return MaintenanceSourceCapture{}, errors.New("source execution plan must be valid JSON")
	}
	return MaintenanceSourceCapture{kind: 1, plan: slices.Clone(plan)}, nil
}

func UnavailableMaintenanceSource() MaintenanceSourceCapture {
	return MaintenanceSourceCapture{kind: 2}
}

// MaintenanceCompensationSubject authorizes only replay of the durably captured
// source of one failed, uncommitted target. It cannot be used as a normal target
// execution or cleanup subject. Its copies share the execution protocol's
// one-shot, callback-scoped mutation capability.
type MaintenanceCompensationSubject struct{ state *maintenanceCompensationState }

type maintenanceCompensationState struct {
	settlement *MaintenanceSettlement
	physical   MaintenancePhysicalSubject
	record     maintenanceCompensationRecord
}

func (s MaintenanceCompensationSubject) Valid() bool {
	return s.state != nil && s.state.settlement != nil && s.state.physical.validFor(s.state.settlement) &&
		(s.state.record.Phase == compensationRestoring || s.state.record.Phase == compensationSourceDispatching || s.state.record.Phase == compensationSourceSettled || s.state.record.Phase == compensationSourceReady || s.state.record.Phase == compensationSourceFailed)
}
func (s MaintenanceCompensationSubject) Intent() MaintenanceIntentClaim {
	if !s.Valid() {
		return MaintenanceIntentClaim{}
	}
	return s.state.physical.Intent()
}
func (s MaintenanceCompensationSubject) SourceRelease() (Release, bool) {
	if !s.Valid() {
		return Release{}, false
	}
	return s.state.physical.SourceRelease()
}
func (s MaintenanceCompensationSubject) TargetRelease() (Release, bool) {
	if !s.Valid() {
		return Release{}, false
	}
	return s.state.physical.TargetRelease()
}

// Plan is a detached observation; only the construction-bound decoder and
// compensation sink consume it, together with this exact subject.
func (s MaintenanceCompensationSubject) Plan() []byte {
	if !s.Valid() {
		return nil
	}
	return slices.Clone(s.state.record.Plan)
}

// FailedTarget supplies only exact retirement/diagnostic authority. It cannot
// authorize source creation; that requires MaintenanceCompensationSubject.
func (s MaintenanceCompensationSubject) FailedTarget() MaintenancePhysicalSubject {
	if !s.Valid() {
		return MaintenancePhysicalSubject{}
	}
	return s.state.physical
}

type maintenanceCompensationBinding struct {
	protocol *substratemutation.Protocol[MaintenanceCompensationSubject]
	prepare  func(context.Context, MaintenancePhysicalSubject) (MaintenanceSourceCapture, error)
	validate func(MaintenancePhysicalSubject, []byte) error
	live     func(context.Context, substratemutation.LiveExecution[MaintenanceCompensationSubject]) substratemutation.Result[MaintenanceCompensationSubject, MaintenancePhysicalEvidence]
	recover  func(context.Context, substratemutation.RecoveryExecution[MaintenanceCompensationSubject]) substratemutation.Result[MaintenanceCompensationSubject, MaintenancePhysicalEvidence]
}

// BindMaintenanceCompensationExecutor binds source capture, strict plan decoding,
// execution and classification together. There is no caller-supplied rollback
// closure at admission or recovery, and no use of cleanup authority to launch.
func BindMaintenanceCompensationExecutor[T any](
	s *MaintenanceSettlement,
	lifetime context.Context,
	authorize substratemutation.Authorize,
	complete substratemutation.Complete,
	capture func(context.Context, MaintenancePhysicalSubject) (MaintenanceSourceCapture, error),
	validate func(MaintenancePhysicalSubject, []byte) error,
	build func(substratemutation.Runner, MaintenanceCompensationSubject) T,
	run func(context.Context, T, MaintenanceCompensationSubject) error,
	classify func(context.Context, MaintenanceCompensationSubject) (MaintenancePhysicalEvidence, error),
) error {
	if s == nil || lifetime == nil || s.compensation != nil || capture == nil || validate == nil {
		return errors.New("maintenance compensation requires one complete bound executor")
	}
	p := substratemutation.NewProtocol[MaintenanceCompensationSubject]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		return err
	}
	guard, _, err := substratemutation.NewExecutor(binding, authorize, complete, build, run, classify)
	if err != nil {
		return err
	}
	s.compensation = &maintenanceCompensationBinding{
		protocol: p, validate: validate,
		prepare: func(ctx context.Context, subject MaintenancePhysicalSubject) (plan MaintenanceSourceCapture, err error) {
			result := substratemutation.RunStep(ctx, "capture exact maintenance source", authorize, complete,
				func(ctx context.Context) error { plan, err = capture(ctx, subject); return err })
			return plan, result.Err()
		},
		live: func(ctx context.Context, claim substratemutation.LiveExecution[MaintenanceCompensationSubject]) substratemutation.Result[MaintenanceCompensationSubject, MaintenancePhysicalEvidence] {
			sourceCtx, cancel := compensationExecutionContext(ctx, lifetime)
			defer cancel()
			return guard.Execute(claim, sourceCtx)
		},
		recover: func(ctx context.Context, claim substratemutation.RecoveryExecution[MaintenanceCompensationSubject]) substratemutation.Result[MaintenanceCompensationSubject, MaintenancePhysicalEvidence] {
			return guard.ExecuteRecovery(claim, ctx)
		},
	}
	return nil
}

// The fixed source executor remains inside the original lease worker. Close
// cancels and drains that worker before replacing its durable head; a drain
// timeout refuses close rather than granting concurrent cleanup authority.
// Source recovery gets its own bounded budget after target startup timeout,
// while explicit cancellation and backend shutdown still revoke execution.
func compensationExecutionContext(target, lifetime context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(lifetime, 2*time.Minute)
	if errors.Is(target.Err(), context.Canceled) {
		cancel()
	}
	stop := context.AfterFunc(target, func() {
		if errors.Is(target.Err(), context.Canceled) {
			cancel()
		}
	})
	return ctx, func() { stop(); cancel() }
}

func (s *MaintenanceSettlement) prepareCompensation(ctx context.Context, execution MaintenanceExecutionClaim) error {
	if s.compensation == nil {
		return nil
	}
	// Capture runs before the target executor, including before image pulls.
	plan, err := s.compensation.prepare(ctx, execution.subject)
	if err != nil {
		return fmt.Errorf("capture maintenance source: %w", err)
	}
	if plan.kind != 1 && plan.kind != 2 {
		return errors.New("source inspector returned no classified source observation")
	}
	if plan.kind == 1 {
		if err := s.compensation.validate(execution.subject, plan.plan); err != nil {
			return err
		}
	}
	unlock := s.lockLease(execution.target.LeaseUUID())
	defer unlock()
	intent, err := s.currentIntentForTargetLocked(execution.target)
	if err != nil {
		return err
	}
	if _, err := s.snapshotMaintenanceSourceLocked(intent); err != nil {
		return err
	}
	phase := compensationPrepared
	if plan.kind == 2 {
		phase = compensationUnavailable
	}
	record := compensationRecordFor(intent, phase, plan.plan)
	return s.callbacks.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, intent); err != nil {
			return err
		}
		previous, err := readCompensationTx(tx, intent)
		if err != nil {
			return err
		}
		if previous != nil {
			return errors.New("maintenance source was already captured")
		}
		return writeCompensationTx(tx, record)
	})
}

func compensationRecordFor(intent MaintenanceIntentClaim, phase compensationPhase, plan []byte) maintenanceCompensationRecord {
	return maintenanceCompensationRecord{Version: 1, LeaseUUID: intent.LeaseUUID(), MaintenanceID: intent.MaintenanceID(),
		SourceDigest: intent.entry.SourceReleaseDigest, TargetDigest: intent.entry.TargetReleaseDigest,
		Phase: phase, Plan: slices.Clone(plan)}
}

func readCompensationTx(tx *bolt.Tx, intent MaintenanceIntentClaim) (*maintenanceCompensationRecord, error) {
	bucket := tx.Bucket(maintenanceCompensationBucket)
	if bucket == nil {
		return nil, nil
	}
	data := bucket.Get([]byte(intent.MaintenanceID().String()))
	if data == nil {
		return nil, nil
	}
	var record maintenanceCompensationRecord
	if err := decodeStrictAuthoritativeObject(data, maxMaintenanceIntentEntryBytes, &record); err != nil {
		return nil, err
	}
	if record.Version != 1 || record.LeaseUUID != intent.LeaseUUID() || record.MaintenanceID != intent.MaintenanceID() ||
		record.SourceDigest != intent.entry.SourceReleaseDigest || record.TargetDigest != intent.entry.TargetReleaseDigest ||
		(record.Phase != compensationUnavailable && (len(record.Plan) == 0 || !json.Valid(record.Plan))) ||
		(record.Phase == compensationUnavailable && len(record.Plan) != 0) {
		return nil, errors.New("maintenance compensation record differs from exact intent")
	}
	switch record.Phase {
	case compensationUnavailable, compensationPrepared, compensationTargetDispatching, compensationTargetSettled, compensationRestoring, compensationSourceDispatching, compensationSourceSettled, compensationSourceReady, compensationSourceFailed:
	default:
		return nil, errors.New("maintenance compensation phase is invalid")
	}
	return &record, nil
}

func writeCompensationTx(tx *bolt.Tx, record maintenanceCompensationRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data) > maxMaintenanceIntentEntryBytes {
		return errors.New("maintenance source plan exceeds journal capacity")
	}
	bucket, err := tx.CreateBucketIfNotExists(maintenanceCompensationBucket)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(record.MaintenanceID.String()), data)
}

func deleteCompensationTx(tx *bolt.Tx, intent MaintenanceIntentClaim) error {
	bucket := tx.Bucket(maintenanceCompensationBucket)
	if bucket == nil {
		return nil
	}
	return bucket.Delete([]byte(intent.MaintenanceID().String()))
}

// The launch journal atomically advances this phase with its dispatch debt.
// Prepared therefore proves no target workload launch has been issued; neither
// a wall-clock deadline nor a transport error can manufacture that fact.
func startMaintenanceTargetLaunchTx(tx *bolt.Tx, subject MaintenancePhysicalSubject) error {
	if !subject.Valid() || subject.RecoveryCleanup() {
		return errors.New("target launch has invalid maintenance subject")
	}
	if subject.state.settlement.compensation == nil {
		return nil
	}
	if err := verifyMaintenanceIntentTx(tx, subject.Intent()); err != nil {
		return err
	}
	record, err := readCompensationTx(tx, subject.Intent())
	if err != nil {
		return err
	}
	if record != nil && record.Phase == compensationUnavailable {
		return nil
	}
	if record == nil || record.Phase != compensationPrepared {
		return errors.New("target launch has no undispatched prepared source plan")
	}
	record.Phase = compensationTargetDispatching
	return writeCompensationTx(tx, *record)
}

// Called only by the volume launch journal's atomic receipt settlement.
// The complete target launch and storage attestation have returned successfully;
// a transport error cannot cross this durable transition.
func recordMaintenanceTargetEffectsTx(tx *bolt.Tx, subject MaintenancePhysicalSubject) error {
	if !subject.Valid() || subject.RecoveryCleanup() {
		return errors.New("target receipt has invalid maintenance subject")
	}
	s := subject.state.settlement
	if s.compensation == nil {
		return nil
	}
	intent := subject.Intent()
	if err := verifyMaintenanceIntentTx(tx, intent); err != nil {
		return err
	}
	record, err := readCompensationTx(tx, intent)
	if err != nil {
		return err
	}
	if record != nil && record.Phase == compensationUnavailable {
		return nil
	}
	if record == nil || record.Phase != compensationTargetDispatching {
		return errors.New("target effects have no exact dispatch phase")
	}
	record.Phase = compensationTargetSettled
	return writeCompensationTx(tx, *record)
}

// CompensationPending is an observation for recovery routing. The dispatch
// phase retains unknown remote effects independently of recovery deadlines.
func (s *MaintenanceSettlement) CompensationPending(intent MaintenanceIntentClaim) (bool, error) {
	if err := s.validateIntent(intent); err != nil {
		return false, err
	}
	var found bool
	err := s.callbacks.view(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, intent)
		found = record != nil && record.Phase != compensationUnavailable
		return err
	})
	return found, err
}

// CompensationStarted is a read-only routing observation. Once restoration
// starts, a late ready target is never eligible for activation.
func (s *MaintenanceSettlement) CompensationStarted(intent MaintenanceIntentClaim) (bool, error) {
	if err := s.validateIntent(intent); err != nil {
		return false, err
	}
	var started bool
	err := s.callbacks.view(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, intent)
		started = record != nil && record.Phase != compensationUnavailable && record.Phase != compensationPrepared && record.Phase != compensationTargetDispatching && record.Phase != compensationTargetSettled
		return err
	})
	return started, err
}

func (s *MaintenanceSettlement) beginCompensation(target MaintenanceReleaseClaim) (MaintenanceCompensationSubject, error) {
	if s.compensation == nil {
		return MaintenanceCompensationSubject{}, errors.New("maintenance compensation executor is unavailable")
	}
	unlock := s.lockLease(target.LeaseUUID())
	defer unlock()
	intent, err := s.currentIntentForTargetLocked(target)
	if err != nil {
		return MaintenanceCompensationSubject{}, err
	}
	// Source must still be the active release. Activation irreversibly excludes
	// compensation, including the cross-store crash window before callback ack.
	source, err := s.snapshotMaintenanceSourceLocked(intent)
	if err != nil {
		return MaintenanceCompensationSubject{}, err
	}
	if err := s.releases.view(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, intent.LeaseUUID())
		if err != nil {
			return err
		}
		for _, release := range releases {
			if release.MaintenanceID == intent.MaintenanceID() && release.Status == "deploying" {
				return nil
			}
		}
		return errors.New("compensation target is not an uncommitted deploying release")
	}); err != nil {
		return MaintenanceCompensationSubject{}, err
	}
	physical := newMaintenancePhysicalSubject(s, target, source)
	var subject MaintenanceCompensationSubject
	err = s.callbacks.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, intent); err != nil {
			return err
		}
		record, err := readCompensationTx(tx, intent)
		if err != nil {
			return err
		}
		if record == nil || record.Phase == compensationUnavailable || record.Phase == compensationTargetDispatching {
			return errors.New("target effects are not durably settled; compensation remains pending")
		}
		if err := s.compensation.validate(physical, record.Plan); err != nil {
			return err
		}
		if record.Phase == compensationPrepared || record.Phase == compensationTargetSettled {
			record.Phase = compensationRestoring
		}
		if err := writeCompensationTx(tx, *record); err != nil {
			return err
		}
		subject = MaintenanceCompensationSubject{state: &maintenanceCompensationState{s, physical, *record}}
		return nil
	})
	return subject, err
}

func (s *MaintenanceSettlement) finishCompensation(subject MaintenanceCompensationSubject, result substratemutation.Result[MaintenanceCompensationSubject, MaintenancePhysicalEvidence], cause error) MaintenanceExecutionOutcome {
	execution := MaintenanceExecutionClaim{settlement: s, target: subject.state.physical.state.target, subject: subject.state.physical}
	failed := func(err error) MaintenanceExecutionOutcome {
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: errors.Join(cause, err)}
	}
	if result.Kind() != substratemutation.Attested {
		return failed(result.Err())
	}
	evidence, ok := result.Evidence()
	if !ok || (evidence.kind != maintenancePhysicalEvidenceSourceReady && evidence.kind != maintenancePhysicalEvidenceSourceFailed) || validateMaintenancePhysicalEvidence(execution.subject, evidence) != nil {
		return failed(errors.New("compensation did not attest the complete ready source"))
	}
	unlock := s.lockLease(execution.target.LeaseUUID())
	defer unlock()
	intent, err := s.currentIntentForTargetLocked(execution.target)
	if err != nil {
		return failed(err)
	}
	err = s.callbacks.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, intent); err != nil {
			return err
		}
		record, err := readCompensationTx(tx, intent)
		if err != nil {
			return err
		}
		if record == nil || (record.Phase != compensationRestoring && record.Phase != compensationSourceDispatching && record.Phase != compensationSourceSettled && record.Phase != compensationSourceReady && record.Phase != compensationSourceFailed) || !bytes.Equal(record.Plan, subject.state.record.Plan) {
			return errors.New("compensation authority changed before source readiness")
		}
		if evidence.kind == maintenancePhysicalEvidenceSourceFailed {
			if record.Phase != compensationSourceSettled && record.Phase != compensationSourceFailed {
				return errors.New("failed source requires settled source launch receipt")
			}
			record.Phase = compensationSourceFailed
		} else {
			record.Phase = compensationSourceReady
		}
		return writeCompensationTx(tx, *record)
	})
	if err != nil {
		return failed(err)
	}
	return MaintenanceExecutionFailure{settlement: s, authority: execution.target, subject: execution.subject, evidence: evidence, cause: cause}
}

func (s *MaintenanceSettlement) compensateLive(ctx context.Context, execution MaintenanceExecutionClaim, cause error) MaintenanceExecutionOutcome {
	if s.compensation == nil {
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: cause}
	}
	var subject MaintenanceCompensationSubject
	claim, err := s.compensation.protocol.BeginAfter(func() (MaintenanceCompensationSubject, error) {
		var err error
		subject, err = s.beginCompensation(execution.target)
		return subject, err
	})
	if err != nil {
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: errors.Join(cause, err)}
	}
	result := s.compensation.live(ctx, claim)
	if err := substratemutation.ValidateLiveResult(s.compensation.protocol, claim, result); err != nil {
		return MaintenanceExecutionAmbiguous{settlement: s, execution: execution, cause: errors.Join(cause, err)}
	}
	return s.finishCompensation(subject, result, cause)
}

// SourceLaunchRequired distinguishes a never-issued compensation launch from
// a prior request which may still reach Docker. The latter is observation-only.
func (subject MaintenanceCompensationSubject) SourceLaunchRequired() bool {
	return subject.Valid() && subject.state.record.Phase == compensationRestoring
}

func transitionCompensationSourceTx(tx *bolt.Tx, subject MaintenanceCompensationSubject, before, after compensationPhase) error {
	if !subject.Valid() {
		return errors.New("invalid source launch subject")
	}
	intent := subject.Intent()
	if err := verifyMaintenanceIntentTx(tx, intent); err != nil {
		return err
	}
	record, err := readCompensationTx(tx, intent)
	if err != nil {
		return err
	}
	if record == nil || record.Phase != before || !bytes.Equal(record.Plan, subject.state.record.Plan) {
		return errors.New("source launch phase changed")
	}
	record.Phase = after
	return writeCompensationTx(tx, *record)
}

func startCompensationSourceLaunchTx(tx *bolt.Tx, subject MaintenanceCompensationSubject) error {
	return transitionCompensationSourceTx(tx, subject, compensationRestoring, compensationSourceDispatching)
}

func verifyCompensationSourcePreparationTx(tx *bolt.Tx, subject MaintenanceCompensationSubject) error {
	if !subject.Valid() || !subject.SourceLaunchRequired() {
		return errors.New("source preparation requires undispatched compensation authority")
	}
	if err := verifyMaintenanceIntentTx(tx, subject.Intent()); err != nil {
		return err
	}
	record, err := readCompensationTx(tx, subject.Intent())
	if err != nil {
		return err
	}
	if record == nil || record.Phase != compensationRestoring || !bytes.Equal(record.Plan, subject.state.record.Plan) {
		return errors.New("source preparation no longer owns the frozen execution plan")
	}
	return nil
}

func completeCompensationSourceLaunchTx(tx *bolt.Tx, subject MaintenanceCompensationSubject) error {
	return transitionCompensationSourceTx(tx, subject, compensationSourceDispatching, compensationSourceSettled)
}

func (s *MaintenanceSettlement) requireCompensationTerminal(intent MaintenanceIntentClaim, success, sourceReady, refused bool) error {
	return s.callbacks.view(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, intent)
		if err != nil || record == nil {
			return err
		}
		if record.Phase == compensationUnavailable {
			return nil
		}
		if success {
			if record.Phase == compensationPrepared || record.Phase == compensationTargetDispatching || record.Phase == compensationTargetSettled {
				return nil
			}
		} else if record.Phase == compensationSourceFailed || (refused && record.Phase == compensationPrepared) || sourceReady && (record.Phase == compensationPrepared || record.Phase == compensationTargetSettled || record.Phase == compensationSourceReady) {
			return nil
		}
		return errors.New("unresolved compensation excludes this terminal transition")
	})
}

// NewMaintenanceCompensationSourceFailed is available only to the bound
// compensation classifier after the source launch settled durably. An unhealthy
// source after an ambiguous create/start cannot manufacture this terminal fact.
func NewMaintenanceCompensationSourceFailed(subject MaintenanceCompensationSubject, ids []string, services map[string][]string) (MaintenancePhysicalEvidence, error) {
	if !subject.Valid() {
		return MaintenancePhysicalEvidence{}, errors.New("source failure requires compensation authority")
	}
	s := subject.state.settlement
	err := s.callbacks.view(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, subject.Intent()); err != nil {
			return err
		}
		record, err := readCompensationTx(tx, subject.Intent())
		if err != nil {
			return err
		}
		if record == nil || (record.Phase != compensationSourceSettled && record.Phase != compensationSourceFailed) {
			return errors.New("source launch remains ambiguous")
		}
		return nil
	})
	if err != nil {
		return MaintenancePhysicalEvidence{}, err
	}
	state, err := newMaintenanceProjection(subject.FailedTarget(), ids, services, true)
	if err != nil {
		return MaintenancePhysicalEvidence{}, err
	}
	evidence := MaintenanceSourceFailed{state: state}
	if !evidence.validForMaintenance(subject.FailedTarget()) {
		return MaintenancePhysicalEvidence{}, errors.New("failed source projection is incomplete")
	}
	return MaintenancePhysicalEvidence{kind: maintenancePhysicalEvidenceSourceFailed, sourceFailed: evidence}, nil
}

// validateMaintenanceCompensationsTx verifies the subordinate extension against
// the canonical lease head; orphan plans are corruption, never restored claims.
func validateMaintenanceCompensationsTx(tx *bolt.Tx) error {
	bucket := tx.Bucket(maintenanceCompensationBucket)
	if bucket == nil {
		return nil
	}
	return bucket.ForEach(func(key, value []byte) error {
		var record maintenanceCompensationRecord
		if err := decodeStrictAuthoritativeObject(value, maxMaintenanceIntentEntryBytes, &record); err != nil {
			return err
		}
		if string(key) != record.MaintenanceID.String() {
			return errors.New("compensation key differs from maintenance identity")
		}
		head, present, err := getLeaseMutationHeadTx(tx, record.LeaseUUID)
		if err != nil {
			return err
		}
		maintenance, ok := head.(maintenanceLeaseMutationHead)
		if !present || !ok {
			return errors.New("compensation has no current maintenance owner")
		}
		_, err = readCompensationTx(tx, maintenance.claim)
		return err
	})
}

// RecoverMaintenanceCompensation requires the existing exclusive actor/recovery
// scope and rereads every durable fact before minting its one-shot executor.
func (s *MaintenanceSettlement) RecoverMaintenanceCompensation(ctx context.Context, scope LeaseRecoveryScope, intent MaintenanceIntentClaim) (MaintenanceExecutionOutcome, error) {
	if s == nil || s.compensation == nil {
		return nil, errors.New("maintenance compensation executor is unavailable")
	}
	release, valid := scope.enter(s.recoveryCoordinator, intent.LeaseUUID())
	if !valid {
		return nil, errors.New("compensation requires exclusive lease recovery authority")
	}
	defer release()
	target, err := s.recoveryMaintenanceTarget(intent)
	if err != nil {
		return nil, err
	}
	var subject MaintenanceCompensationSubject
	claim, err := s.compensation.protocol.RecoverAfter(func() (MaintenanceCompensationSubject, error) {
		var err error
		subject, err = s.beginCompensation(target)
		return subject, err
	})
	if err != nil {
		return nil, err
	}
	result := s.compensation.recover(ctx, claim)
	if err := substratemutation.ValidateRecoveryResult(s.compensation.protocol, claim, result); err != nil {
		return nil, err
	}
	return s.finishCompensation(subject, result, errors.New("maintenance target failed; source compensation recovered")), nil
}
