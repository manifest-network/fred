package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const interruptedOperationFailure = "backend restarted before operation completion"
const refusedOperationFailure = "backend refused operation before asynchronous acceptance"

type operationIntentJournal interface {
	ProbeOperationIntent(shared.OperationIntentProbe) (shared.OperationIntentAdmissionDisposition, error)
	BeginOperationIntent(shared.OperationIntentSpec) (shared.OperationIntentAdmission, error)
	ResolveOperationIntent(shared.OperationIntentClaim, backend.CallbackStatus, string) (shared.CallbackEntry, error)
}

func (b *Backend) probeOperationIntent(
	leaseUUID, callbackURL string,
) (bool, error) {
	if b.operationIntents == nil {
		return false, errors.New("durable callback store is required for asynchronous operation")
	}
	disposition, err := b.operationIntents.ProbeOperationIntent(shared.OperationIntentProbe{
		LeaseUUID:        leaseUUID,
		CallbackURL:      callbackURL,
		Backend:          b.cfg.Name,
		BackendStorageID: b.storageIdentity,
	})
	if err != nil {
		return false, fmt.Errorf("probe exact operation redelivery: %w", err)
	}
	return disposition == shared.OperationIntentAdmissionExisting ||
		disposition == shared.OperationIntentAdmissionCompleted, nil
}

func (b *Backend) beginOperationIntent(
	kind shared.OperationIntentKind,
	leaseUUID, callbackURL, lifecycleCallbackURL, tenant, providerUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	effectiveItems []backend.LeaseItem,
	healthCheckServices []string,
	manifestPayload []byte,
	sourceLeaseUUID string,
	sourceGeneration int,
) (*shared.OperationIntentClaim, bool, error) {
	if b.operationIntents == nil {
		return nil, false, errors.New("durable callback store is required for asynchronous operation")
	}
	if err := validateDockerResourceProfiles(items, resourceProfiles); err != nil {
		return nil, false, fmt.Errorf("validate %s operation resource profiles: %w", kind, err)
	}
	admission, err := b.operationIntents.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 kind,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Backend:              b.cfg.Name,
		BackendStorageID:     b.storageIdentity,
		Tenant:               tenant,
		ProviderUUID:         providerUUID,
		Items:                items,
		ResourceProfiles:     resourceProfiles,
		EffectiveItems:       effectiveItems,
		HealthCheckServices:  healthCheckServices,
		Manifest:             manifestPayload,
		SourceLeaseUUID:      sourceLeaseUUID,
		SourceGeneration:     sourceGeneration,
	})
	if err != nil {
		return nil, false, fmt.Errorf("persist %s operation intent: %w", kind, err)
	}
	if admission.Disposition != shared.OperationIntentAdmissionCreated {
		return nil, false, nil
	}
	return &admission.Claim, true, nil
}

func (b *Backend) refuseOperationIntent(claim *shared.OperationIntentClaim, cause error) error {
	if claim == nil {
		return cause
	}
	if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil ||
		errors.Is(cause, backendidentity.ErrIdentityDrift) ||
		errors.Is(cause, backendidentity.ErrMutationOutcomeAmbiguous) {
		if authorityErr == nil {
			authorityErr = cause
		}
		// Do not turn a lost storage-authority proof into a definitive refusal.
		// Intermediate provisioning helpers may intentionally log-and-default a
		// raw mutation error; the backend-lifetime latch is therefore the source
		// of truth at this final evidence-consuming boundary.
		return errors.Join(cause, fmt.Errorf(
			"%w: preserve %s operation intent for restart recovery: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, claim.Kind(), authorityErr,
		))
	}
	if _, err := b.operationIntents.ResolveOperationIntent(
		*claim, backend.CallbackStatusFailed, refusedOperationFailure,
	); err != nil {
		b.logger.Error("failed to settle refused operation intent",
			"error", err,
			"lease_uuid", claim.LeaseUUID(),
			"operation", claim.Kind(),
		)
		// Do not preserve a clearable validation/conflict sentinel when durable
		// failure settlement failed: the outcome is now ambiguous and Fred must
		// keep its write-ahead attempt for startup recovery.
		return fmt.Errorf("operation refused but durable intent settlement failed: %s: %w", cause.Error(), err)
	}
	return cause
}

// settleUnacceptedRestoreIntent durably settles a restore that never crossed
// the lease actor's acceptance boundary. It deliberately does not accept nil:
// once beginOperationIntent returns proceed=true, the typed claim is mandatory
// authority for every synchronous failure path.
//
// The caller must invoke this only after teardown, re-quarantine, and source
// quota proof, but before handing the retention row back to Active. That order
// keeps the Restoring row as a level-triggered retry owner if this write fails;
// handing ownership back first would strand an Existing intent until restart.
func (b *Backend) settleUnacceptedRestoreIntent(claim shared.OperationIntentClaim) error {
	if claim.Kind() != shared.OperationIntentRestore {
		return fmt.Errorf("unaccepted restore settlement received %s intent", claim.Kind())
	}
	if _, err := b.operationIntents.ResolveOperationIntent(
		claim, backend.CallbackStatusFailed, refusedOperationFailure,
	); err != nil {
		b.logger.Error("failed to settle unaccepted restore intent",
			"error", err,
			"lease_uuid", claim.LeaseUUID(),
		)
		return fmt.Errorf("settle unaccepted restore intent: %w", err)
	}
	return nil
}

type recoveredIntentDecision struct {
	claim  shared.OperationIntentClaim
	status backend.CallbackStatus
	errMsg string
}

// recoverOperationIntents classifies the durable write-ahead window from
// strict Docker substrate evidence. It first classifies every intent and only
// then settles any of them, so one ambiguous lease keeps the complete startup
// evidence set intact and makes readiness fail closed.
func (b *Backend) recoverOperationIntents(ctx context.Context, retentionReconcileErr error) error {
	if b.callbackStore == nil {
		return nil
	}
	claims, err := b.callbackStore.ListOperationIntents()
	if err != nil {
		return fmt.Errorf("list callback operation intents: %w", err)
	}
	if len(claims) == 0 {
		return nil
	}
	if retentionReconcileErr != nil {
		for _, claim := range claims {
			if claim.Kind() == shared.OperationIntentRestore {
				return fmt.Errorf("retention reconciliation failed while %s intent for lease %q is pending: %w",
					claim.Kind(), claim.LeaseUUID(), retentionReconcileErr)
			}
		}
	}

	containers, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return fmt.Errorf("strict managed-container inventory for operation recovery: %w", err)
	}

	decisions := make([]recoveredIntentDecision, 0, len(claims))
	for _, claim := range claims {
		if claim.Backend() != b.cfg.Name || claim.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf("%s operation intent for lease %q belongs to backend %q storage %q",
				claim.Kind(), claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID().String())
		}
		if err := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); err != nil {
			return fmt.Errorf("%s operation intent for lease %q has invalid resource authority: %w",
				claim.Kind(), claim.LeaseUUID(), err)
		}
		status, errMsg, classifyErr := b.classifyOperationIntent(ctx, claim, containers)
		if classifyErr != nil {
			return fmt.Errorf("%s operation intent for lease %q remains unresolved: %w",
				claim.Kind(), claim.LeaseUUID(), classifyErr)
		}
		decisions = append(decisions, recoveredIntentDecision{claim: claim, status: status, errMsg: errMsg})
	}
	// Preflight every success before mutating any release/finalizer or settling
	// any intent. One missing projection/SKU/corrupt store must preserve the
	// complete multi-lease causal evidence set.
	for _, decision := range decisions {
		if decision.status == backend.CallbackStatusSuccess {
			if err := b.validateRecoveredOperationSuccess(decision.claim); err != nil {
				return fmt.Errorf("validate recovered %s success for lease %q: %w",
					decision.claim.Kind(), decision.claim.LeaseUUID(), err)
			}
		}
	}
	for _, decision := range decisions {
		if decision.status == backend.CallbackStatusSuccess {
			if err := b.ensureRecoveredOperationSuccess(decision.claim); err != nil {
				return fmt.Errorf("finalize recovered %s success for lease %q: %w",
					decision.claim.Kind(), decision.claim.LeaseUUID(), err)
			}
		}
		if _, err := b.callbackStore.ResolveOperationIntent(
			decision.claim, decision.status, decision.errMsg,
		); err != nil {
			return fmt.Errorf("settle recovered %s operation intent for lease %q: %w",
				decision.claim.Kind(), decision.claim.LeaseUUID(), err)
		}
	}
	return nil
}

// preflightOperationIntentRecovery rejects ambiguous restore substrate before
// retention reconciliation can tear down containers, rename volumes, or delete
// the source finalizer that explains the interrupted operation. Definite absent,
// failed, and ready states are left for the normal reconciliation/recovery pair.
func (b *Backend) preflightOperationIntentRecovery(ctx context.Context) error {
	if b.callbackStore == nil {
		return nil
	}
	claims, err := b.callbackStore.ListOperationIntents()
	if err != nil {
		return fmt.Errorf("list callback operation intents: %w", err)
	}
	hasRestore := false
	for _, claim := range claims {
		if claim.Kind() == shared.OperationIntentRestore {
			hasRestore = true
			break
		}
	}
	if !hasRestore {
		return nil
	}
	containers, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return fmt.Errorf("strict managed-container inventory for restore preflight: %w", err)
	}
	for _, claim := range claims {
		if claim.Kind() != shared.OperationIntentRestore {
			continue
		}
		if claim.Backend() != b.cfg.Name || claim.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf("%s operation intent for lease %q belongs to backend %q storage %q",
				claim.Kind(), claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID().String())
		}
		if err := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); err != nil {
			return fmt.Errorf("%s operation intent for lease %q has invalid resource authority before retention reconciliation: %w",
				claim.Kind(), claim.LeaseUUID(), err)
		}
		committed, commitErr := b.operationIntentHasCommittedRelease(claim)
		if commitErr != nil {
			return fmt.Errorf("validate committed restore intent for lease %q: %w", claim.LeaseUUID(), commitErr)
		}
		if committed {
			continue
		}
		if _, _, _, err := b.classifyOperationIntentSubstrate(ctx, claim, containers); err != nil {
			return fmt.Errorf("%s operation intent for lease %q is ambiguous before retention reconciliation: %w",
				claim.Kind(), claim.LeaseUUID(), err)
		}
	}
	return nil
}

func (b *Backend) validateRecoveredOperationSuccess(claim shared.OperationIntentClaim) error {
	payload := claim.Manifest()
	if len(payload) == 0 {
		return fmt.Errorf("durable intent has no manifest")
	}
	if _, err := manifest.ParsePayload(payload); err != nil {
		return fmt.Errorf("parse durable operation manifest: %w", err)
	}
	if b.releaseStore == nil {
		return fmt.Errorf("release store is required")
	}
	if err := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); err != nil {
		return fmt.Errorf("validate recovered operation resource profiles: %w", err)
	}
	committedOperation, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return fmt.Errorf("validate committed operation release: %w", err)
	}

	b.provisionsMu.RLock()
	provision, exists := b.provisions[claim.LeaseUUID()]
	if !exists || provision.Tenant != claim.Tenant() ||
		provision.ProviderUUID != claim.ProviderUUID() ||
		provision.CallbackURL != claim.CallbackURL() ||
		provision.LifecycleCallbackURL != claim.LifecycleCallbackURL() ||
		!intentItemsMatchProjection(claim.EffectiveItems(), provision.Items) ||
		!slices.Equal(provision.ResourceProfiles, claim.ResourceProfiles()) {
		b.provisionsMu.RUnlock()
		return fmt.Errorf("strict substrate does not have an exact recovered projection")
	}
	status := provision.Status
	containerCount := len(provision.ContainerIDs)
	b.provisionsMu.RUnlock()
	if committedOperation {
		if status != backend.ProvisionStatusReady && status != backend.ProvisionStatusFailed {
			return fmt.Errorf("committed operation projection remains non-terminal: %s", status)
		}
		return nil
	}
	if status != backend.ProvisionStatusReady ||
		containerCount != expectedIntentQuantity(claim.EffectiveItems()) {
		return fmt.Errorf("strict substrate does not have an exact Ready recovered projection")
	}

	if _, err := b.releaseStore.LatestActive(claim.LeaseUUID()); err != nil {
		return fmt.Errorf("read active release: %w", err)
	}
	if claim.Kind() != shared.OperationIntentRestore {
		return nil
	}
	if b.retentionStore == nil {
		return fmt.Errorf("retention store is required for recovered restore")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("re-read restore source finalizer: %w", err)
	}
	if record != nil && (record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != claim.LeaseUUID() ||
		record.Generation != claim.SourceGeneration()) {
		return fmt.Errorf("restore source finalizer changed before recovered success commit")
	}
	if record != nil && len(record.DestinationItems) > 0 &&
		(!slices.Equal(record.DestinationItems, claim.EffectiveItems()) ||
			!slices.Equal(record.DestinationResourceProfiles, claim.ResourceProfiles())) {
		return fmt.Errorf("restore source destination authority differs from recovered operation intent")
	}
	return nil
}

func (b *Backend) ensureRecoveredOperationSuccess(claim shared.OperationIntentClaim) error {
	if err := b.validateRecoveredOperationSuccess(claim); err != nil {
		return err
	}
	payload := claim.Manifest()
	stack, err := manifest.ParsePayload(payload)
	if err != nil {
		return fmt.Errorf("parse durable operation manifest: %w", err)
	}
	b.provisionsMu.Lock()
	b.provisions[claim.LeaseUUID()].StackManifest = stack
	b.provisions[claim.LeaseUUID()].ResourceProfiles = claim.ResourceProfiles()
	b.provisionsMu.Unlock()
	committedOperation, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return fmt.Errorf("validate committed operation release: %w", err)
	}
	if committedOperation {
		// The Release is already the durable operation-success marker. A Failed
		// zero-survivor projection keeps its non-expiring identity in the same
		// atomic row, so settling the operation intent cannot erase restart
		// authority.
		return nil
	}

	// A byte-identical older generation is still not this operation's commit.
	// Once substrate recovery proves success, publish a new exact lineage row;
	// payload equality alone must never let the callback settle against stale
	// runtime authority.
	release, releaseErr := releaseForOperationIntent(claim)
	if releaseErr != nil {
		return fmt.Errorf("construct recovered active release: %w", releaseErr)
	}
	if err := b.releaseStore.AppendActive(claim.LeaseUUID(), release); err != nil {
		return fmt.Errorf("record recovered active release: %w", err)
	}

	if claim.Kind() != shared.OperationIntentRestore {
		return nil
	}
	// The caller settles the exact operation intent only after this Release write.
	// The source finalizer deliberately remains for the next level-triggered
	// retention pass, which deletes it only after observing that settlement.
	return nil
}

// checkOperationReleaseCapacity proves the exact success record before the
// first tenant-substrate side effect. The durable intent timestamp is reused by
// the live commit and cold recovery so RFC3339Nano's variable-width encoding
// cannot invalidate the byte proof at the final boundary.
func (b *Backend) checkOperationReleaseCapacity(claim shared.OperationIntentClaim) error {
	planner := b.releaseHistoryCapacityPlanner()
	if planner == nil {
		return errors.New("release store is required for asynchronous operation")
	}
	release, err := releaseForOperationIntent(claim)
	if err != nil {
		return err
	}
	return planner.CheckAppendActiveCapacity(claim.LeaseUUID(), release)
}

func (b *Backend) releaseHistoryCapacityPlanner() releaseHistoryCapacityPlanner {
	if b == nil {
		return nil
	}
	if b.releaseCapacityPlanner != nil {
		return b.releaseCapacityPlanner
	}
	if b.releaseStore == nil {
		return nil
	}
	return b.releaseStore
}

func releaseForOperationIntent(claim shared.OperationIntentClaim) (shared.Release, error) {
	runtimeAuthority, err := releaseRuntimeAuthorityForIntent(claim)
	if err != nil {
		return shared.Release{}, fmt.Errorf("construct release runtime authority: %w", err)
	}
	if claim.CreatedAt().IsZero() {
		return shared.Release{}, errors.New("operation intent has no durable admission timestamp")
	}
	return shared.Release{
		Manifest:         claim.Manifest(),
		Image:            "stack",
		OperationID:      claim.OperationID(),
		Items:            claim.EffectiveItems(),
		ResourceProfiles: claim.ResourceProfiles(),
		RuntimeAuthority: runtimeAuthority,
		Status:           "active",
		CreatedAt:        claim.CreatedAt(),
	}, nil
}

func expectedIntentQuantity(items []backend.LeaseItem) int {
	total := 0
	for _, item := range items {
		total += item.Quantity
	}
	return total
}

func intentItemsMatchProjection(expected, actual []backend.LeaseItem) bool {
	type itemShape struct {
		sku          string
		quantity     int
		customDomain string
	}
	shape := func(items []backend.LeaseItem) (map[string]itemShape, bool) {
		result := make(map[string]itemShape, len(items))
		for _, item := range items {
			current, exists := result[item.ServiceName]
			if exists && (current.sku != item.SKU || current.customDomain != item.CustomDomain) {
				return nil, false
			}
			result[item.ServiceName] = itemShape{
				sku: item.SKU, quantity: current.quantity + item.Quantity, customDomain: item.CustomDomain,
			}
		}
		return result, true
	}
	want, ok := shape(expected)
	if !ok {
		return false
	}
	got, ok := shape(actual)
	if !ok || len(want) != len(got) {
		return false
	}
	for service, expectedShape := range want {
		if got[service] != expectedShape {
			return false
		}
	}
	return true
}

func (b *Backend) classifyOperationIntent(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	all []ContainerInfo,
) (backend.CallbackStatus, string, error) {
	committed, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return "", "", err
	}
	if committed {
		return backend.CallbackStatusSuccess, "", nil
	}
	status, errMsg, hasCurrent, err := b.classifyOperationIntentSubstrate(ctx, claim, all)
	if err != nil {
		return "", "", err
	}
	if err := b.validateRestoreIntentSource(claim, hasCurrent); err != nil {
		return "", "", err
	}
	return status, errMsg, nil
}

// operationIntentHasCommittedRelease recognizes the durable success boundary
// independently of current container health. Provision and restore both write
// their exact active Release before the actor publishes Ready and settles the
// operation callback; containers that fail or disappear after that write are a
// lifecycle failure, not proof that the operation itself failed.
//
// An absent Release, an empty legacy OperationID, or a different OperationID is
// evidence only for another generation and therefore remains uncommitted. Once
// the same OperationID is present, every immutable operation field must match:
// divergence is corruption/ambiguity and fails recovery closed rather than
// silently downgrading a committed generation to Failed.
func (b *Backend) operationIntentHasCommittedRelease(
	claim shared.OperationIntentClaim,
) (bool, error) {
	if b.releaseStore == nil {
		return false, errors.New("release store is required for operation intent recovery")
	}
	active, err := b.releaseStore.LatestActive(claim.LeaseUUID())
	if err != nil {
		return false, fmt.Errorf("read operation active release: %w", err)
	}
	committed, err := operationReleaseMatchesIntent(active, claim)
	if err != nil || !committed {
		return committed, err
	}
	if claim.Kind() != shared.OperationIntentRestore {
		return true, nil
	}
	if b.retentionStore == nil {
		return false, errors.New("retention store is required for restore intent recovery")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return false, fmt.Errorf("read restore source finalizer: %w", err)
	}
	if record == nil {
		// The commit may have consumed the source finalizer before callback
		// settlement. The exact Release still proves this operation generation.
		return true, nil
	}
	if record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != claim.LeaseUUID() ||
		record.Generation != claim.SourceGeneration() {
		return false, errors.New("restore source finalizer differs from committed operation intent")
	}
	if err := b.validateRestoreIntentAuthority(claim, *record); err != nil {
		return false, err
	}
	return true, nil
}

// operationReleaseMatchesIntent is the single exact-Release predicate shared by
// startup recovery and close admission. It deliberately distinguishes an older
// or legacy generation (false, nil) from a same-token divergent generation
// (false, error), because only the latter claims to be this exact operation.
func operationReleaseMatchesIntent(
	active *shared.Release,
	claim shared.OperationIntentClaim,
) (bool, error) {
	if !claim.OperationID().Valid() {
		return false, errors.New("operation intent has an invalid operation ID")
	}
	if active == nil || active.OperationID == "" {
		return false, nil
	}
	if !active.OperationID.Valid() {
		return false, errors.New("active release has an invalid operation ID")
	}
	if active.OperationID != claim.OperationID() {
		return false, nil
	}
	if !bytes.Equal(active.Manifest, claim.Manifest()) ||
		!slices.Equal(active.Items, claim.EffectiveItems()) ||
		!slices.Equal(active.ResourceProfiles, claim.ResourceProfiles()) {
		return false, errors.New("active release with matching operation ID differs from operation intent")
	}
	if active.RuntimeAuthority == nil {
		return false, errors.New("active release with matching operation ID has no runtime authority")
	}
	if !releaseRuntimeAuthorityMatchesIntent(active.RuntimeAuthority, claim) {
		return false, errors.New("active release runtime authority differs from operation intent")
	}
	return true, nil
}

func releaseRuntimeAuthorityForIntent(
	claim shared.OperationIntentClaim,
) (*shared.ReleaseRuntimeAuthority, error) {
	return releaseRuntimeAuthorityForOperation(
		claim.OperationID(),
		claim.Tenant(),
		claim.ProviderUUID(),
		claim.CallbackURL(),
		claim.LifecycleCallbackURL(),
	)
}

func releaseRuntimeAuthorityForOperation(
	operationID shared.OperationID,
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) (*shared.ReleaseRuntimeAuthority, error) {
	if !operationID.Valid() {
		if operationID == "" {
			return nil, nil
		}
		return nil, errors.New("release runtime authority requires a valid operation ID")
	}
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if err != nil {
		return nil, err
	}
	return &authority, nil
}

func releaseRuntimeAuthorityMatchesIntent(
	authority *shared.ReleaseRuntimeAuthority,
	claim shared.OperationIntentClaim,
) bool {
	return authority != nil &&
		authority.OperationID() == claim.OperationID() &&
		authority.Tenant() == claim.Tenant() &&
		authority.ProviderUUID() == claim.ProviderUUID() &&
		authority.CallbackURL() == claim.CallbackURL() &&
		authority.LifecycleCallbackURL() == claim.LifecycleCallbackURL()
}

func (b *Backend) classifyOperationIntentSubstrate(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	all []ContainerInfo,
) (backend.CallbackStatus, string, bool, error) {
	var current []ContainerInfo
	oldCount := 0
	for _, container := range all {
		if container.LeaseUUID != claim.LeaseUUID() || strings.HasSuffix(container.Name, "-prev") {
			continue
		}
		callbackMatches := container.CallbackURL == claim.CallbackURL()
		lifecycleMatches := container.LifecycleCallbackURL == claim.LifecycleCallbackURL()
		switch {
		case callbackMatches && lifecycleMatches:
			current = append(current, container)
		case callbackMatches || lifecycleMatches:
			return "", "", false, fmt.Errorf("container %q has a partial current callback identity", container.ContainerID)
		default:
			oldCount++
		}
	}
	if oldCount != 0 {
		return "", "", false, fmt.Errorf("container substrate with another callback generation exists")
	}
	if len(current) == 0 {
		// No container carries this operation's unguessable callback authority;
		// therefore this exact operation created no surviving substrate.
		return backend.CallbackStatusFailed, interruptedOperationFailure, false, nil
	}
	stack, err := manifest.ParsePayload(claim.Manifest())
	if err != nil {
		return "", "", true, fmt.Errorf("parse durable operation manifest: %w", err)
	}

	type instanceKey struct {
		service string
		sku     string
		index   int
	}
	expected := make(map[instanceKey]struct{})
	healthRequired := make(map[string]struct{}, len(claim.HealthCheckServices()))
	for _, service := range claim.HealthCheckServices() {
		healthRequired[service] = struct{}{}
	}
	for _, item := range claim.Items() {
		for index := range item.Quantity {
			key := instanceKey{service: item.ServiceName, sku: item.SKU, index: index}
			if _, duplicate := expected[key]; duplicate {
				return "", "", true, fmt.Errorf("intent contains duplicate expected instance %+v", key)
			}
			expected[key] = struct{}{}
		}
	}
	if len(current) != len(expected) {
		return "", "", true, fmt.Errorf("partial substrate: found %d current containers, expected %d",
			len(current), len(expected))
	}

	seen := make(map[instanceKey]struct{}, len(current))
	effectiveDomains := make(map[string]string, len(claim.EffectiveItems()))
	for _, item := range claim.EffectiveItems() {
		effectiveDomains[item.ServiceName] = item.CustomDomain
	}
	serviceDomains := make(map[string]string, len(effectiveDomains))
	ready, failed := 0, 0
	for _, listed := range current {
		container, err := b.inspectContainerForRecovery(ctx, listed.ContainerID)
		if err != nil {
			return "", "", true, fmt.Errorf("inspect current container %q: %w", listed.ContainerID, err)
		}
		if container.LeaseUUID != claim.LeaseUUID() ||
			container.Tenant != claim.Tenant() ||
			container.ProviderUUID != claim.ProviderUUID() ||
			container.CallbackURL != claim.CallbackURL() ||
			container.LifecycleCallbackURL != claim.LifecycleCallbackURL() {
			return "", "", true, fmt.Errorf("container %q identity changed or does not match the intent", listed.ContainerID)
		}
		serviceManifest, ok := stack.Services[container.ServiceName]
		if !ok || serviceManifest == nil || container.Image != serviceManifest.Image {
			return "", "", true, fmt.Errorf("container %q image does not match the durable manifest", listed.ContainerID)
		}
		if domain, exists := serviceDomains[container.ServiceName]; exists && domain != container.CustomDomain {
			return "", "", true, fmt.Errorf("service %q has inconsistent custom-domain labels", container.ServiceName)
		}
		serviceDomains[container.ServiceName] = container.CustomDomain
		if container.CustomDomain != effectiveDomains[container.ServiceName] {
			return "", "", true, fmt.Errorf("container %q custom domain does not match durable effective items", listed.ContainerID)
		}
		key := instanceKey{service: container.ServiceName, sku: container.SKU, index: container.InstanceIndex}
		if _, ok := expected[key]; !ok {
			return "", "", true, fmt.Errorf("container %q is not in the exact expected instance set", listed.ContainerID)
		}
		if _, duplicate := seen[key]; duplicate {
			return "", "", true, fmt.Errorf("duplicate container for expected instance %+v", key)
		}
		seen[key] = struct{}{}
		switch containerStatusToProvisionStatus(container.Status) {
		case backend.ProvisionStatusReady:
			if _, required := healthRequired[container.ServiceName]; required &&
				container.Health != HealthStatusHealthy {
				if container.Health == HealthStatusUnhealthy {
					failed++
					continue
				}
				return "", "", true, fmt.Errorf("container %q has not completed its required health check", listed.ContainerID)
			}
			ready++
		case backend.ProvisionStatusFailed:
			failed++
		default:
			return "", "", true, fmt.Errorf("container %q remains in non-terminal state %q", listed.ContainerID, container.Status)
		}
	}
	if len(seen) != len(expected) {
		return "", "", true, fmt.Errorf("partial substrate instance set")
	}
	switch {
	case ready == len(expected):
		return backend.CallbackStatusSuccess, "", true, nil
	case failed == len(expected):
		return backend.CallbackStatusFailed, interruptedOperationFailure, true, nil
	default:
		return "", "", true, fmt.Errorf("mixed ready and failed substrate state")
	}
}

func (b *Backend) validateRestoreIntentSource(
	claim shared.OperationIntentClaim,
	hasCurrentDestination bool,
) error {
	if claim.Kind() != shared.OperationIntentRestore {
		return nil
	}
	if b.retentionStore == nil {
		return fmt.Errorf("restore intent has no retention store")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("read restore source finalizer: %w", err)
	}
	if hasCurrentDestination {
		if record == nil {
			// Success finalization may already have deleted the source record.
			return nil
		}
		if record.Status != shared.RetentionStatusRestoring ||
			record.NewLeaseUUID != claim.LeaseUUID() ||
			record.Generation != claim.SourceGeneration() {
			return fmt.Errorf("restore source finalizer does not exactly own the destination")
		}
		if len(record.DestinationItems) > 0 &&
			(!slices.Equal(record.DestinationItems, claim.EffectiveItems()) ||
				!slices.Equal(record.DestinationResourceProfiles, claim.ResourceProfiles())) {
			return fmt.Errorf("restore source destination authority differs from operation intent")
		}
		return nil
	}
	if record == nil {
		return fmt.Errorf("restore source finalizer is absent while destination substrate is absent")
	}
	if record.Status != shared.RetentionStatusActive {
		return fmt.Errorf("restore source remains %q after retention reconciliation", record.Status)
	}
	// The claim either never committed (pre-claim generation) or rollback
	// committed (RevertToActiveWithResourceProfiles increments the claimed generation once).
	if record.Generation != claim.SourceGeneration()-1 &&
		record.Generation != claim.SourceGeneration()+1 {
		return fmt.Errorf("restore source generation %d is not the pre-claim or rolled-back generation for %d",
			record.Generation, claim.SourceGeneration())
	}
	return nil
}
