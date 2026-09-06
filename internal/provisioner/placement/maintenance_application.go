package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/util"
)

const (
	defaultMaintenanceRecoveryTimeout            = 30 * time.Second
	maxMaintenanceRecoveryCommandsPerBackendPass = 32
)

// MaintenanceApplicationOutcome is the closed transport-neutral projection
// of one complete maintenance transaction. Its zero value is invalid.
type MaintenanceApplicationOutcome uint8

const (
	MaintenanceApplicationInvalid MaintenanceApplicationOutcome = iota
	MaintenanceApplicationAccepted
	MaintenanceApplicationNotFound
	MaintenanceApplicationNoLongerActive
	MaintenanceApplicationForbidden
	MaintenanceApplicationAlreadyInProgress
	MaintenanceApplicationCommandConflict
	MaintenanceApplicationBackendInvalidState
	MaintenanceApplicationBackendValidation
	MaintenanceApplicationServiceUnavailable
	MaintenanceApplicationInternalFailure
)

// MaintenanceApplicationRequest is an opaque authenticated intent. The
// durable command, routing target, callback, and terminal outcome are derived
// by the construction-bound application coordinator.
type MaintenanceApplicationRequest struct {
	id        maintenanceid.ID
	leaseUUID string
	tenant    string
	kind      MaintenanceCommandKind
	payload   []byte
}

func NewMaintenanceApplicationRequest(
	id maintenanceid.ID,
	leaseUUID, tenant string,
	kind MaintenanceCommandKind,
	payload []byte,
) (MaintenanceApplicationRequest, error) {
	if !id.Valid() || leaseUUID == "" || tenant == "" ||
		(kind != MaintenanceCommandRestart && kind != MaintenanceCommandUpdate) ||
		(kind == MaintenanceCommandRestart && len(payload) != 0) ||
		(kind == MaintenanceCommandUpdate && len(payload) == 0) {
		return MaintenanceApplicationRequest{}, ErrInvalidMaintenanceCommand
	}
	return MaintenanceApplicationRequest{
		id: id, leaseUUID: leaseUUID, tenant: tenant, kind: kind,
		payload: append([]byte(nil), payload...),
	}, nil
}

func (request MaintenanceApplicationRequest) valid() bool {
	_, err := NewMaintenanceApplicationRequest(
		request.id, request.leaseUUID, request.tenant, request.kind, request.payload,
	)
	return err == nil
}

type MaintenanceApplicationResult struct {
	outcome MaintenanceApplicationOutcome
	err     error
}

func (result MaintenanceApplicationResult) Outcome() MaintenanceApplicationOutcome {
	return result.outcome
}
func (result MaintenanceApplicationResult) Err() error { return result.err }

// MaintenanceOrderedEvents preserves the per-lease ordering boundary between
// the visible starting event and the exact backend call plus durable receipt.
type MaintenanceOrderedEvents interface {
	DispatchWithOrderedSettlement(
		backend.LeaseStatusEvent,
		func() (accepted bool, err error),
	) (bool, error)
}

// MaintenanceApplication is the only public command/recovery boundary. It
// owns the process-local lease claims and dispatch lanes needed to keep a
// Pending write-ahead command fenced across ambiguous outcomes and restarts.
type MaintenanceApplication struct {
	coordinator     *MaintenanceCoordinator
	issuer          *maintenanceCoordinatorMarker
	events          MaintenanceOrderedEvents
	recoveryTimeout time.Duration
	heldMu          sync.Mutex
	held            map[string]*maintenanceHeld
	recoveryMu      sync.Mutex
	recoveryCursor  map[string]string
}

type maintenanceHeld struct {
	id           maintenanceid.ID
	leaseClaim   operation.LeaseClaim
	journalClaim MaintenanceCommandClaim
	dispatchMu   sync.Mutex
}

func (authority *MaintenanceCoordinator) Application(
	events MaintenanceOrderedEvents,
	recoveryTimeout time.Duration,
) (*MaintenanceApplication, error) {
	if !authority.Valid() || recoveryTimeout < 0 {
		return nil, ErrInvalidMaintenanceCoordinator
	}
	if util.IsNilInterface(events) {
		events = nil
	}
	if recoveryTimeout == 0 {
		recoveryTimeout = defaultMaintenanceRecoveryTimeout
	}
	application := &MaintenanceApplication{
		coordinator: authority, issuer: authority.marker, events: events,
		recoveryTimeout: recoveryTimeout,
		held:            make(map[string]*maintenanceHeld),
		recoveryCursor:  make(map[string]string),
	}
	if err := application.rehydrate(); err != nil {
		return nil, err
	}
	return application, nil
}

func (application *MaintenanceApplication) Valid() bool {
	return application != nil && application.coordinator != nil &&
		application.coordinator.Valid() && application.issuer == application.coordinator.marker &&
		application.held != nil && application.recoveryCursor != nil &&
		application.recoveryTimeout > 0
}

func (application *MaintenanceApplication) rehydrate() error {
	claims, err := application.coordinator.pendingMaintenanceCommands()
	if err != nil {
		return fmt.Errorf("load pending maintenance commands: %w", err)
	}
	acquired := make([]*maintenanceHeld, 0, len(claims))
	for _, journalClaim := range claims {
		command := journalClaim.Command()
		if !journalClaim.Valid() || !command.Valid() {
			err = errors.New("pending maintenance journal returned an invalid claim")
			break
		}
		if _, duplicate := application.held[command.LeaseUUID()]; duplicate {
			err = fmt.Errorf("multiple pending maintenance claims for lease %s", command.LeaseUUID())
			break
		}
		claim := application.coordinator.tryClaimLeaseNow(command.LeaseUUID())
		if !claim.Acquired() {
			err = fmt.Errorf(
				"pending maintenance for lease %s overlaps an already-rehydrated lifecycle operation",
				command.LeaseUUID(),
			)
			break
		}
		held := &maintenanceHeld{id: command.ID(), leaseClaim: claim.Claim(), journalClaim: journalClaim}
		application.held[command.LeaseUUID()] = held
		acquired = append(acquired, held)
	}
	if err == nil {
		return nil
	}
	for index := len(acquired) - 1; index >= 0; index-- {
		held := acquired[index]
		delete(application.held, held.journalClaim.Command().LeaseUUID())
		if !application.coordinator.releaseLease(held.leaseClaim) {
			err = errors.Join(err, errors.New("unwind pending maintenance lifecycle claim"))
		}
	}
	return err
}

// Execute owns chain authorization, lifecycle exclusion, immutable WAL
// admission/replay, exact backend invocation, and classifier-selected durable
// settlement. No call token or terminal choice escapes this method.
func (application *MaintenanceApplication) Execute(
	ctx context.Context,
	input MaintenanceApplicationRequest,
) MaintenanceApplicationResult {
	if !application.Valid() || !input.valid() {
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure, ErrInvalidMaintenanceCommand)
	}
	record, found, err := application.coordinator.lookupMaintenanceCommand(input.leaseUUID, input.id)
	if err != nil {
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	}
	if found {
		if record.Command().Tenant() != input.tenant {
			return maintenanceApplicationResult(MaintenanceApplicationForbidden, nil)
		}
		if !maintenanceRecordMatches(record, input) {
			return maintenanceApplicationResult(MaintenanceApplicationCommandConflict, ErrMaintenanceCommandConflict)
		}
		if record.Outcome() != MaintenanceOutcomePending {
			application.releaseTerminal(input.leaseUUID, input.id)
			return resultForMaintenanceOutcome(record.Outcome())
		}
	}
	var prepared MaintenancePreparation
	if !found {
		prepared = application.coordinator.prepareMaintenanceCommand(
			ctx, input.id, input.leaseUUID, input.tenant, input.kind, input.payload,
		)
		if result := resultForPreparation(prepared); result.outcome != MaintenanceApplicationInvalid {
			return result
		}
	}

	held, acquired := application.acquire(input.leaseUUID, input.id)
	if held == nil {
		return maintenanceApplicationResult(MaintenanceApplicationAlreadyInProgress, nil)
	}
	defer held.dispatchMu.Unlock()

	record, found, err = application.coordinator.lookupMaintenanceCommand(input.leaseUUID, input.id)
	if err != nil {
		if acquired {
			application.release(input.leaseUUID, held)
		}
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	}
	if found {
		if record.Command().Tenant() != input.tenant {
			if acquired {
				application.release(input.leaseUUID, held)
			}
			return maintenanceApplicationResult(MaintenanceApplicationForbidden, nil)
		}
		if !maintenanceRecordMatches(record, input) {
			if acquired {
				application.release(input.leaseUUID, held)
			}
			return maintenanceApplicationResult(MaintenanceApplicationCommandConflict, ErrMaintenanceCommandConflict)
		}
		if record.Outcome() != MaintenanceOutcomePending {
			application.release(input.leaseUUID, held)
			return resultForMaintenanceOutcome(record.Outcome())
		}
		if !held.journalClaim.Valid() || held.journalClaim.Command().ID() != input.id {
			if err := application.attach(held, input.leaseUUID, input.id); err != nil {
				return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
			}
		}
		return application.reauthorizeAndDispatch(ctx, held)
	}
	if !acquired {
		application.release(input.leaseUUID, held)
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable,
			errors.New("maintenance admission left no durable command"))
	}
	admission, err := application.coordinator.beginMaintenanceCommand(prepared)
	if err != nil {
		_, visible, lookupErr := application.coordinator.lookupMaintenanceCommand(input.leaseUUID, input.id)
		if lookupErr != nil {
			return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, errors.Join(err, lookupErr))
		}
		if !visible {
			application.release(input.leaseUUID, held)
			return resultForMaintenanceBeginError(err)
		}
		admission, err = application.coordinator.beginMaintenanceCommand(prepared)
		if err != nil {
			return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable,
				fmt.Errorf("recover ambiguous maintenance admission: %w", err))
		}
	}
	if !admission.Pending() {
		application.release(input.leaseUUID, held)
		return resultForMaintenanceOutcome(admission.Outcome())
	}
	held.journalClaim = admission.Claim()
	return application.reauthorizeAndDispatch(ctx, held)
}

func maintenanceApplicationResult(outcome MaintenanceApplicationOutcome, err error) MaintenanceApplicationResult {
	return MaintenanceApplicationResult{outcome: outcome, err: err}
}

func maintenanceRecordMatches(record MaintenanceCommandRecord, input MaintenanceApplicationRequest) bool {
	if !record.Valid() {
		return false
	}
	command := record.Command()
	digest := sha256.Sum256(input.payload)
	return command.ID() == input.id && command.LeaseUUID() == input.leaseUUID &&
		command.Kind() == input.kind && command.PayloadHash() == hex.EncodeToString(digest[:])
}

func resultForPreparation(preparation MaintenancePreparation) MaintenanceApplicationResult {
	if err := preparation.Err(); err != nil {
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	}
	switch preparation.Outcome() {
	case MaintenanceAuthorizationGranted:
		if preparation.Authorized() {
			return MaintenanceApplicationResult{}
		}
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure,
			errors.New("maintenance coordinator returned invalid preparation"))
	case MaintenanceAuthorizationNotActive, MaintenanceAuthorizationLeaseEnded:
		return maintenanceApplicationResult(MaintenanceApplicationNoLongerActive, nil)
	case MaintenanceAuthorizationRevoked:
		return maintenanceApplicationResult(MaintenanceApplicationForbidden, nil)
	default:
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure,
			errors.New("maintenance coordinator returned invalid authorization"))
	}
}

func resultForMaintenanceBeginError(err error) MaintenanceApplicationResult {
	switch {
	case errors.Is(err, ErrMaintenanceCommandConflict):
		return maintenanceApplicationResult(MaintenanceApplicationCommandConflict, err)
	case errors.Is(err, ErrMaintenanceHistoryFull):
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	case errors.Is(err, ErrInvalidMaintenanceCommand):
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure, err)
	default:
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	}
}

func resultForMaintenanceOutcome(outcome MaintenanceCommandOutcome) MaintenanceApplicationResult {
	switch outcome {
	case MaintenanceOutcomeAccepted:
		return maintenanceApplicationResult(MaintenanceApplicationAccepted, nil)
	case MaintenanceOutcomeNotProvisioned:
		return maintenanceApplicationResult(MaintenanceApplicationNotFound, nil)
	case MaintenanceOutcomeInvalidState:
		return maintenanceApplicationResult(MaintenanceApplicationBackendInvalidState, nil)
	case MaintenanceOutcomeValidationRejected:
		return maintenanceApplicationResult(MaintenanceApplicationBackendValidation, nil)
	case MaintenanceOutcomeLeaseEnded:
		return maintenanceApplicationResult(MaintenanceApplicationNoLongerActive, nil)
	case MaintenanceOutcomeAuthorityRevoked:
		return maintenanceApplicationResult(MaintenanceApplicationForbidden, nil)
	case MaintenanceOutcomeCapacityRefused, MaintenanceOutcomeBackendUnavailable:
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, nil)
	default:
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure,
			errors.New("invalid durable maintenance outcome"))
	}
}

func (application *MaintenanceApplication) attach(held *maintenanceHeld, leaseUUID string, id maintenanceid.ID) error {
	claims, err := application.coordinator.pendingMaintenanceCommands()
	if err != nil {
		return err
	}
	for _, claim := range claims {
		command := claim.Command()
		if claim.Valid() && command.LeaseUUID() == leaseUUID && command.ID() == id {
			held.journalClaim = claim
			return nil
		}
	}
	return errors.New("pending maintenance record has no settlement capability")
}

func (application *MaintenanceApplication) acquire(leaseUUID string, id maintenanceid.ID) (*maintenanceHeld, bool) {
	for {
		application.heldMu.Lock()
		held := application.held[leaseUUID]
		if held == nil {
			result := application.coordinator.tryClaimLeaseNow(leaseUUID)
			if !result.Acquired() {
				application.heldMu.Unlock()
				return nil, false
			}
			held = &maintenanceHeld{id: id, leaseClaim: result.Claim()}
			held.dispatchMu.Lock()
			application.held[leaseUUID] = held
			application.heldMu.Unlock()
			return held, true
		}
		if held.id != id {
			application.heldMu.Unlock()
			return nil, false
		}
		application.heldMu.Unlock()
		held.dispatchMu.Lock()
		application.heldMu.Lock()
		current := application.held[leaseUUID] == held && held.id == id
		application.heldMu.Unlock()
		if current {
			return held, false
		}
		held.dispatchMu.Unlock()
	}
}

func (application *MaintenanceApplication) release(leaseUUID string, held *maintenanceHeld) {
	application.heldMu.Lock()
	if application.held[leaseUUID] != held {
		application.heldMu.Unlock()
		return
	}
	delete(application.held, leaseUUID)
	application.heldMu.Unlock()
	if !application.coordinator.releaseLease(held.leaseClaim) {
		slog.Error("failed to release maintenance lifecycle claim", "lease_uuid", leaseUUID)
	}
}

func (application *MaintenanceApplication) releaseTerminal(leaseUUID string, id maintenanceid.ID) {
	application.heldMu.Lock()
	held := application.held[leaseUUID]
	application.heldMu.Unlock()
	if held == nil || held.id != id {
		return
	}
	held.dispatchMu.Lock()
	defer held.dispatchMu.Unlock()
	application.release(leaseUUID, held)
}

func (application *MaintenanceApplication) reauthorizeAndDispatch(
	ctx context.Context,
	held *maintenanceHeld,
) MaintenanceApplicationResult {
	reauthorization := application.coordinator.reauthorizeMaintenanceCommand(ctx, held.journalClaim)
	if err := reauthorization.Err(); err != nil {
		return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
	}
	if reauthorization.Settled() {
		application.release(held.journalClaim.Command().LeaseUUID(), held)
		return resultForMaintenanceOutcome(reauthorization.Outcome())
	}
	if !reauthorization.Authorized() {
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure,
			errors.New("maintenance coordinator returned invalid reauthorization"))
	}
	return application.dispatch(ctx, held, reauthorization.Authorization())
}

func (application *MaintenanceApplication) dispatch(
	ctx context.Context,
	held *maintenanceHeld,
	authorization AuthorizedMaintenanceCommand,
) MaintenanceApplicationResult {
	command := authorization.Command()
	if !command.Valid() || command.ID() != held.id ||
		command.LeaseUUID() != held.journalClaim.Command().LeaseUUID() {
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure,
			errors.New("maintenance authorization does not match retained command"))
	}
	settled := MaintenanceOutcomePending
	call := func() (bool, error) {
		completion := application.coordinator.executeMaintenance(ctx, authorization)
		if completion.Settled() {
			settled = completion.Outcome()
		}
		if completion.Err() != nil {
			return completion.BackendAccepted(), completion.Err()
		}
		if completion.CallErr() != nil {
			return false, completion.CallErr()
		}
		if !completion.Settled() || completion.Outcome() != MaintenanceOutcomeAccepted {
			return completion.BackendAccepted(), errors.New("maintenance execution did not produce an accepted receipt")
		}
		return true, nil
	}
	status := backend.ProvisionStatusRestarting
	if command.Kind() == MaintenanceCommandUpdate {
		status = backend.ProvisionStatusUpdating
	}
	accepted, err := false, error(nil)
	if application.events == nil {
		accepted, err = call()
	} else {
		accepted, err = application.events.DispatchWithOrderedSettlement(
			backend.LeaseStatusEvent{LeaseUUID: command.LeaseUUID(), Status: status, Timestamp: time.Now()}, call,
		)
	}
	if settled != MaintenanceOutcomePending {
		application.release(command.LeaseUUID(), held)
	}
	if err == nil {
		return maintenanceApplicationResult(MaintenanceApplicationAccepted, nil)
	}
	if settled != MaintenanceOutcomePending && settled != MaintenanceOutcomeAccepted {
		return resultForMaintenanceOutcome(settled)
	}
	if accepted {
		return maintenanceApplicationResult(MaintenanceApplicationInternalFailure, err)
	}
	return maintenanceApplicationResult(MaintenanceApplicationServiceUnavailable, err)
}

func (application *MaintenanceApplication) releaseSettled() error {
	application.heldMu.Lock()
	entries := make(map[string]*maintenanceHeld, len(application.held))
	for leaseUUID, held := range application.held {
		entries[leaseUUID] = held
	}
	application.heldMu.Unlock()
	var errs []error
	for leaseUUID, held := range entries {
		held.dispatchMu.Lock()
		command := held.journalClaim.Command()
		id := held.id
		if command.Valid() {
			id = command.ID()
		}
		record, found, err := application.coordinator.lookupMaintenanceCommand(leaseUUID, id)
		switch {
		case err != nil:
			errs = append(errs, err)
		case !found && command.Valid():
			errs = append(errs, errors.New("retained maintenance claim has no durable command"))
		case !found || record.Outcome() != MaintenanceOutcomePending:
			application.release(leaseUUID, held)
		}
		held.dispatchMu.Unlock()
	}
	return errors.Join(errs...)
}

type maintenanceRecoveryEntry struct {
	claim MaintenanceCommandClaim
	held  *maintenanceHeld
}

func maintenanceRecoveryKey(claim MaintenanceCommandClaim) string {
	command := claim.Command()
	if !command.Valid() {
		return ""
	}
	return command.LeaseUUID() + "\x00" + command.ID().String()
}

func maintenanceRecoveryBatch(
	pending []maintenanceRecoveryEntry,
	after string,
	limit int,
) []maintenanceRecoveryEntry {
	if len(pending) == 0 || limit <= 0 {
		return nil
	}
	ordered := slices.Clone(pending)
	slices.SortFunc(ordered, func(left, right maintenanceRecoveryEntry) int {
		return strings.Compare(maintenanceRecoveryKey(left.claim), maintenanceRecoveryKey(right.claim))
	})
	start := 0
	if after != "" {
		start, _ = slices.BinarySearchFunc(ordered, after, func(entry maintenanceRecoveryEntry, target string) int {
			return strings.Compare(maintenanceRecoveryKey(entry.claim), target)
		})
		for start < len(ordered) && maintenanceRecoveryKey(ordered[start].claim) <= after {
			start++
		}
		if start == len(ordered) {
			start = 0
		}
	}
	count := min(limit, len(ordered))
	batch := make([]maintenanceRecoveryEntry, 0, count)
	for offset := range count {
		batch = append(batch, ordered[(start+offset)%len(ordered)])
	}
	return batch
}

// RecoverPending owns recovery selection, exact reauthorization, backend
// invocation, and settlement. Callers cannot choose a claim or outcome.
func (application *MaintenanceApplication) RecoverPending(ctx context.Context) error {
	if !application.Valid() {
		return ErrInvalidMaintenanceCoordinator
	}
	if !application.recoveryMu.TryLock() {
		return nil
	}
	defer application.recoveryMu.Unlock()
	if err := application.releaseSettled(); err != nil {
		return fmt.Errorf("release durably settled maintenance claims: %w", err)
	}
	claims, err := application.coordinator.pendingMaintenanceCommands()
	if err != nil {
		return err
	}
	byBackend := make(map[string][]maintenanceRecoveryEntry)
	for _, claim := range claims {
		command := claim.Command()
		application.heldMu.Lock()
		held := application.held[command.LeaseUUID()]
		application.heldMu.Unlock()
		if held == nil || held.id != command.ID() {
			return fmt.Errorf("pending maintenance %s for lease %s has no lifecycle claim", command.ID(), command.LeaseUUID())
		}
		byBackend[command.BackendName()] = append(byBackend[command.BackendName()], maintenanceRecoveryEntry{claim: claim, held: held})
	}
	for backendName := range application.recoveryCursor {
		if _, pending := byBackend[backendName]; !pending {
			delete(application.recoveryCursor, backendName)
		}
	}
	type laneResult struct {
		backendName string
		lastKey     string
		errs        []error
	}
	results := make(chan laneResult, len(byBackend))
	var group sync.WaitGroup
	for backendName, entries := range byBackend {
		backendName := backendName
		batch := maintenanceRecoveryBatch(entries, application.recoveryCursor[backendName], maxMaintenanceRecoveryCommandsPerBackendPass)
		group.Go(func() {
			result := laneResult{backendName: backendName}
			laneCtx, cancel := context.WithTimeout(ctx, application.recoveryTimeout)
			defer cancel()
			for _, entry := range batch {
				if laneCtx.Err() != nil {
					break
				}
				result.lastKey = maintenanceRecoveryKey(entry.claim)
				entry.held.dispatchMu.Lock()
				command := entry.claim.Command()
				record, found, recoverErr := application.coordinator.lookupMaintenanceCommand(command.LeaseUUID(), command.ID())
				switch {
				case recoverErr != nil:
				case !found:
					recoverErr = errors.New("retained maintenance claim has no durable command")
				case record.Outcome() != MaintenanceOutcomePending:
					application.release(command.LeaseUUID(), entry.held)
				default:
					entry.held.journalClaim = entry.claim
					applied := application.reauthorizeAndDispatch(laneCtx, entry.held)
					if applied.outcome != MaintenanceApplicationAccepted &&
						applied.outcome != MaintenanceApplicationNotFound &&
						applied.outcome != MaintenanceApplicationBackendInvalidState &&
						applied.outcome != MaintenanceApplicationBackendValidation {
						recoverErr = applied.err
					}
				}
				entry.held.dispatchMu.Unlock()
				if recoverErr != nil {
					result.errs = append(result.errs, fmt.Errorf("recover maintenance %s for lease %s: %w", command.ID(), command.LeaseUUID(), recoverErr))
				}
			}
			results <- result
		})
	}
	group.Wait()
	close(results)
	var errs []error
	for result := range results {
		if result.lastKey != "" {
			application.recoveryCursor[result.backendName] = result.lastKey
		}
		errs = append(errs, result.errs...)
	}
	return errors.Join(errs...)
}
