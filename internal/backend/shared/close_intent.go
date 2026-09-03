package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
)

const (
	maxCloseIntentEntryBytes      = 4 << 20
	maxCloseIntentIdentityBytes   = 4 << 10
	maxCloseRollbackTargetBytes   = 4 << 10
	closeIntentPreemptedOperation = "operation preempted by lease close"
)

// ErrCloseIntentConflict means a lease already has an unresolved close whose
// immutable input differs from the requested close. The existing intent is
// preserved; callers must not perform cleanup for the conflicting request.
var ErrCloseIntentConflict = errors.New("unresolved callback close intent")

// CloseIntentAdmissionDisposition tells a backend whether it created the
// durable close barrier or recovered an exact retry of one already accepted.
type CloseIntentAdmissionDisposition uint8

const (
	CloseIntentAdmissionNone CloseIntentAdmissionDisposition = iota
	CloseIntentAdmissionCreated
	CloseIntentAdmissionExisting
)

// CloseLegacyRollbackTarget identifies one exact pre-Compose rollback
// container. ContainerID is the immutable deletion authority. Name is retained
// alongside it for operator evidence and defense-in-depth identity checks; a
// delayed cleanup must never resolve a reused name to a different container.
type CloseLegacyRollbackTarget struct {
	ContainerID string `json:"container_id"`
	Name        string `json:"name"`
}

// CloseIntentSpec is the immutable input to destructive lease cleanup. It is
// persisted before the first substrate mutation so restart recovery never has
// to reconstruct authority from a partial set of survivors.
type CloseIntentSpec struct {
	LeaseUUID        string
	Backend          string
	BackendStorageID backendidentity.ID
	Tenant           string
	ProviderUUID     string
	Items            []backend.LeaseItem
	ResourceProfiles []SKUResourceSnapshot
	Manifest         []byte

	// CallbackURL and LifecycleCallbackURL are the exact durable pair captured
	// by the active provision. Both may be empty only for a callbackless legacy
	// provision; otherwise both halves must be present and match exactly.
	CallbackURL          string
	LifecycleCallbackURL string

	RetainOnClose bool
	CleanupOnly   bool

	// ActiveReleaseVersion and ActiveReleaseDigest fence cleanup to the exact
	// release inspected before admission. The digest is intentionally supplied
	// by the release owner: this journal binds it but does not reinterpret the
	// release store's canonical encoding.
	ActiveReleaseVersion int
	ActiveReleaseDigest  [sha256.Size]byte

	LegacyRollbackTargets []CloseLegacyRollbackTarget
}

// CloseIntentAdmission is returned only after the close barrier is durably
// committed. Existing is an exact idempotent retry and returns the original
// capability, including its persisted cleanup-attempt count.
type CloseIntentAdmission struct {
	Claim       CloseIntentClaim
	Disposition CloseIntentAdmissionDisposition
	// OperationPreempted reports that this transaction replaced an unresolved
	// operation intent with its exact failed operation callback.
	OperationPreempted bool
	// MaintenancePreempted reports that this same transaction replaced an
	// unresolved maintenance intent with its failed lifecycle completion.
	MaintenancePreempted bool
}

// CloseIntentClaim is an opaque, copy-safe capability for one exact durable
// close. It contains no caller-settable authority. Every mutation verifies the
// lease key and SHA-256 digest against bbolt, so using two copies cannot replay
// a resolve or overwrite a refreshed cleanup-attempt count.
type CloseIntentClaim struct {
	entry               closeIntentEntry
	intentID            uuid.UUID
	storageID           backendidentity.ID
	activeReleaseDigest [sha256.Size]byte
	digest              [sha256.Size]byte
}

func (c CloseIntentClaim) IntentID() string {
	if c.intentID == uuid.Nil {
		return ""
	}
	return c.intentID.String()
}

func (c CloseIntentClaim) LeaseUUID() string { return c.entry.LeaseUUID }
func (c CloseIntentClaim) Backend() string   { return c.entry.Backend }

func (c CloseIntentClaim) BackendStorageID() backendidentity.ID { return c.storageID }
func (c CloseIntentClaim) Tenant() string                       { return c.entry.Tenant }
func (c CloseIntentClaim) ProviderUUID() string                 { return c.entry.ProviderUUID }

func (c CloseIntentClaim) Items() []backend.LeaseItem {
	return slices.Clone(c.entry.Items)
}

func (c CloseIntentClaim) ResourceProfiles() []SKUResourceSnapshot {
	return CloneSKUResourceSnapshot(c.entry.ResourceProfiles)
}

func (c CloseIntentClaim) Manifest() []byte { return bytes.Clone(c.entry.Manifest) }

func (c CloseIntentClaim) CallbackURL() string { return c.entry.CallbackURL }

func (c CloseIntentClaim) LifecycleCallbackURL() string {
	return c.entry.LifecycleCallbackURL
}

func (c CloseIntentClaim) RetainOnClose() bool { return c.entry.RetainOnClose }
func (c CloseIntentClaim) CleanupOnly() bool   { return c.entry.CleanupOnly }

func (c CloseIntentClaim) ActiveReleaseVersion() int {
	return c.entry.ActiveReleaseVersion
}

func (c CloseIntentClaim) ActiveReleaseDigest() [sha256.Size]byte {
	return c.activeReleaseDigest
}

func (c CloseIntentClaim) LegacyRollbackTargets() []CloseLegacyRollbackTarget {
	return slices.Clone(c.entry.LegacyRollbackTargets)
}

func (c CloseIntentClaim) CleanupAttempts() int { return c.entry.CleanupAttempts }
func (c CloseIntentClaim) CreatedAt() time.Time { return c.entry.CreatedAt }

type closeIntentEntry struct {
	IntentID              string                      `json:"intent_id"`
	LeaseUUID             string                      `json:"lease_uuid"`
	Backend               string                      `json:"backend"`
	BackendStorageID      string                      `json:"backend_storage_id"`
	Tenant                string                      `json:"tenant"`
	ProviderUUID          string                      `json:"provider_uuid"`
	Items                 []backend.LeaseItem         `json:"items"`
	ResourceProfiles      []SKUResourceSnapshot       `json:"resource_profiles"`
	Manifest              []byte                      `json:"manifest"`
	CallbackURL           string                      `json:"callback_url,omitempty"`
	LifecycleCallbackURL  string                      `json:"lifecycle_callback_url,omitempty"`
	RetainOnClose         bool                        `json:"retain_on_close"`
	CleanupOnly           bool                        `json:"cleanup_only"`
	ActiveReleaseVersion  int                         `json:"active_release_version"`
	ActiveReleaseDigest   string                      `json:"active_release_digest"`
	LegacyRollbackTargets []CloseLegacyRollbackTarget `json:"legacy_rollback_targets,omitempty"`
	CleanupAttempts       int                         `json:"cleanup_attempts"`
	CreatedAt             time.Time                   `json:"created_at"`
}

// BeginCloseIntent durably publishes a close barrier before destructive work.
// If an asynchronous provision/restore intent still exists, this same bbolt
// transaction first converts it into its exact failed operation callback. A
// crash can therefore expose both durable facts or neither, never a close that
// silently erased the operation completion it preempted.
func (s *CallbackStore) BeginCloseIntent(spec CloseIntentSpec) (CloseIntentAdmission, error) {
	if err := validateCloseIntentSpec(spec); err != nil {
		return CloseIntentAdmission{}, err
	}
	entry := closeIntentEntry{
		LeaseUUID:             spec.LeaseUUID,
		Backend:               spec.Backend,
		BackendStorageID:      spec.BackendStorageID.String(),
		Tenant:                spec.Tenant,
		ProviderUUID:          spec.ProviderUUID,
		Items:                 slices.Clone(spec.Items),
		ResourceProfiles:      CloneSKUResourceSnapshot(spec.ResourceProfiles),
		Manifest:              bytes.Clone(spec.Manifest),
		CallbackURL:           spec.CallbackURL,
		LifecycleCallbackURL:  spec.LifecycleCallbackURL,
		RetainOnClose:         spec.RetainOnClose,
		CleanupOnly:           spec.CleanupOnly,
		ActiveReleaseVersion:  spec.ActiveReleaseVersion,
		ActiveReleaseDigest:   encodeCloseReleaseDigest(spec.ActiveReleaseDigest),
		LegacyRollbackTargets: slices.Clone(spec.LegacyRollbackTargets),
	}

	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	admission := CloseIntentAdmission{}
	err := s.update(func(tx *bolt.Tx) error {
		closeBucket := tx.Bucket(callbackCloseIntentBucketName)
		if closeBucket == nil {
			return fmt.Errorf("callback close intent bucket missing")
		}
		operationBucket := tx.Bucket(callbackOperationIntentBucketName)
		if operationBucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		maintenanceBucket := tx.Bucket(callbackMaintenanceIntentBucketName)
		if maintenanceBucket == nil {
			return fmt.Errorf("callback maintenance intent bucket missing")
		}
		key := []byte(entry.LeaseUUID)
		if closeBucket.Bucket(key) != nil {
			return fmt.Errorf("callback close intent %q is a nested bucket", entry.LeaseUUID)
		}
		if current := closeBucket.Get(key); current != nil {
			claim, decodeErr := decodeCloseIntent(key, current)
			if decodeErr != nil {
				return decodeErr
			}
			if operationBucket.Get(key) != nil || operationBucket.Bucket(key) != nil ||
				maintenanceBucket.Get(key) != nil || maintenanceBucket.Bucket(key) != nil {
				return fmt.Errorf("lease %q has a close intent overlapping earlier work", entry.LeaseUUID)
			}
			if !closeIntentEntryMatchesSpec(claim.entry, entry) {
				return fmt.Errorf("%w for lease %q", ErrCloseIntentConflict, entry.LeaseUUID)
			}
			admission = CloseIntentAdmission{
				Claim: claim, Disposition: CloseIntentAdmissionExisting,
			}
			return nil
		}

		intentID, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("allocate callback close intent ID: %w", err)
		}
		entry.IntentID = intentID.String()
		entry.CreatedAt = time.Now()
		data, err := marshalCloseIntent(entry)
		if err != nil {
			return err
		}

		// Put the close row first so a callback insertion failure exercises the
		// transaction's rollback in the safety-critical direction as well.
		if err := closeBucket.Put(key, data); err != nil {
			return err
		}

		if operationBucket.Bucket(key) != nil {
			return fmt.Errorf("callback operation intent %q is a nested bucket", entry.LeaseUUID)
		}
		if operationBucket.Get(key) != nil && maintenanceBucket.Get(key) != nil {
			return fmt.Errorf("lease %q has simultaneous operation and maintenance intents", entry.LeaseUUID)
		}
		if current := operationBucket.Get(key); current != nil {
			operationClaim, decodeErr := decodeOperationIntent(key, current)
			if decodeErr != nil {
				return decodeErr
			}
			if operationClaim.Backend() != entry.Backend ||
				operationClaim.BackendStorageID().String() != entry.BackendStorageID {
				return fmt.Errorf(
					"operation and close intents have different backend storage authority for lease %q",
					entry.LeaseUUID,
				)
			}
			callback, callbackErr := callbackEntryForIntent(
				*operationClaim.entry, backend.CallbackStatusFailed, closeIntentPreemptedOperation,
			)
			if callbackErr != nil {
				return callbackErr
			}
			preemptedDeliveryID, idErr := uuid.NewRandom()
			if idErr != nil {
				return fmt.Errorf("allocate preempted operation callback delivery ID: %w", idErr)
			}
			callback.DeliveryID = preemptedDeliveryID.String()
			if err := operationIntentMatchesCallback(*operationClaim.entry, callback); err != nil {
				return err
			}
			if _, _, err := putCallbackEntryTx(tx, callback); err != nil {
				return err
			}
			if err := operationBucket.Delete(key); err != nil {
				return err
			}
			admission.OperationPreempted = true
		}

		if maintenanceBucket.Bucket(key) != nil {
			return fmt.Errorf("callback maintenance intent %q is a nested bucket", entry.LeaseUUID)
		}
		if current := maintenanceBucket.Get(key); current != nil {
			maintenanceClaim, decodeErr := decodeMaintenanceIntent(key, current)
			if decodeErr != nil {
				return decodeErr
			}
			if maintenanceClaim.Backend() != entry.Backend ||
				maintenanceClaim.BackendStorageID().String() != entry.BackendStorageID {
				return fmt.Errorf(
					"maintenance and close intents have different backend storage authority for lease %q",
					entry.LeaseUUID,
				)
			}
			if entry.CleanupOnly || entry.ActiveReleaseVersion == 0 ||
				entry.ActiveReleaseVersion != maintenanceClaim.SourceRelease().Version() ||
				entry.ActiveReleaseDigest != maintenanceClaim.entry.SourceReleaseDigest {
				return fmt.Errorf(
					"close intent does not fence the maintenance source release for lease %q",
					entry.LeaseUUID,
				)
			}
			if entry.Tenant != maintenanceClaim.Tenant() ||
				entry.ProviderUUID != maintenanceClaim.ProviderUUID() {
				return fmt.Errorf(
					"maintenance and close intents have different tenant or provider authority for lease %q",
					entry.LeaseUUID,
				)
			}
			deliveryID, idErr := uuid.NewRandom()
			if idErr != nil {
				return fmt.Errorf("allocate preempted maintenance callback delivery ID: %w", idErr)
			}
			callback := callbackEntryForMaintenanceIntent(
				maintenanceClaim.entry,
				deliveryID.String(),
				backend.CallbackStatusFailed,
				"maintenance preempted by lease close",
			)
			if err := validateNewCallbackEntry(callback, time.Now()); err != nil {
				return err
			}
			if _, _, err := putCallbackEntryTx(tx, callback); err != nil {
				return err
			}
			if err := maintenanceBucket.Delete(key); err != nil {
				return err
			}
			admission.MaintenancePreempted = true
		}

		claim, decodeErr := decodeCloseIntent(key, data)
		if decodeErr != nil {
			return decodeErr
		}
		admission.Claim = claim
		admission.Disposition = CloseIntentAdmissionCreated
		return nil
	})
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	if admission.OperationPreempted || admission.MaintenancePreempted {
		s.notifyReplaySubscribers()
	}
	return admission, nil
}

// GetCloseIntent returns the current exact close capability for leaseUUID.
// Absence is reported as (zero, false, nil). The returned digest is a snapshot;
// a concurrent/refreshed mutation makes it safely stale.
func (s *CallbackStore) GetCloseIntent(leaseUUID string) (CloseIntentClaim, bool, error) {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return CloseIntentClaim{}, false, err
	}
	unlock := s.lockDeliveryLease(leaseUUID)
	defer unlock()
	var claim CloseIntentClaim
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackCloseIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback close intent bucket missing")
		}
		key := []byte(leaseUUID)
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback close intent %q is a nested bucket", leaseUUID)
		}
		value := bucket.Get(key)
		if value == nil {
			return nil
		}
		var decodeErr error
		claim, decodeErr = decodeCloseIntent(key, value)
		found = decodeErr == nil
		return decodeErr
	})
	return claim, found, err
}

// ListCloseIntents returns durable recovery capabilities in deterministic
// canonical lease order. Close intents never expire: they are the sole causal
// authority for destructive cleanup after a crash.
func (s *CallbackStore) ListCloseIntents() ([]CloseIntentClaim, error) {
	var claims []CloseIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackCloseIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback close intent bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback close intent %q is a nested bucket", key)
			}
			claim, err := decodeCloseIntent(key, value)
			if err != nil {
				return err
			}
			claims = append(claims, claim)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(claims, func(left, right CloseIntentClaim) int {
		return strings.Compare(left.LeaseUUID(), right.LeaseUUID())
	})
	return claims, nil
}

// IncrementCloseCleanupAttempts atomically persists one cleanup attempt and
// returns the only claim current enough to resolve or increment again. A stale
// copied claim cannot overwrite this counter or reset a restart budget.
func (s *CallbackStore) IncrementCloseCleanupAttempts(
	claim CloseIntentClaim,
) (CloseIntentClaim, error) {
	if err := validateCloseIntentClaim(claim); err != nil {
		return CloseIntentClaim{}, err
	}
	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	var refreshed CloseIntentClaim
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyCloseIntentTx(tx, claim); err != nil {
			return err
		}
		if claim.entry.CleanupAttempts == math.MaxInt {
			return fmt.Errorf("callback close intent cleanup-attempt counter exhausted")
		}
		entry := cloneCloseIntentEntry(claim.entry)
		entry.CleanupAttempts++
		data, err := marshalCloseIntent(entry)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(callbackCloseIntentBucketName)
		if err := bucket.Put([]byte(entry.LeaseUUID), data); err != nil {
			return err
		}
		refreshed, err = decodeCloseIntent([]byte(entry.LeaseUUID), data)
		return err
	})
	if err != nil {
		return CloseIntentClaim{}, err
	}
	return refreshed, nil
}

// ResolveCloseIntent atomically removes one precise close and enqueues its
// terminal lifecycle observation. Callbackless legacy closes delete only the
// intent. Failed and deprovisioned are the only valid close outcomes.
func (s *CallbackStore) ResolveCloseIntent(
	claim CloseIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
	retained bool,
) (CallbackEntry, error) {
	if err := validateCloseIntentClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	switch status {
	case backend.CallbackStatusFailed:
		if retained {
			return CallbackEntry{}, fmt.Errorf("failed close callback cannot be retained")
		}
	case backend.CallbackStatusDeprovisioned:
		if retained && !claim.RetainOnClose() {
			return CallbackEntry{}, fmt.Errorf("close callback cannot retain an unretained close")
		}
	default:
		return CallbackEntry{}, fmt.Errorf("close intent has invalid completion status %q", status)
	}

	callbackless := claim.CallbackURL() == "" && claim.LifecycleCallbackURL() == ""
	var entry CallbackEntry
	if !callbackless {
		deliveryID, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("allocate close callback delivery ID: %w", err)
		}
		entry = CallbackEntry{
			DeliveryID:       deliveryID.String(),
			LeaseUUID:        claim.LeaseUUID(),
			CallbackURL:      claim.LifecycleCallbackURL(),
			DeliveryKind:     CallbackDeliveryKindLifecycle,
			Success:          status != backend.CallbackStatusFailed,
			Status:           status,
			Backend:          claim.Backend(),
			BackendStorageID: claim.BackendStorageID().String(),
			Error:            errMsg,
			Retained:         retained,
			CreatedAt:        time.Now(),
		}
		if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
			return CallbackEntry{}, err
		}
	}

	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	var data []byte
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyCloseIntentTx(tx, claim); err != nil {
			return err
		}
		if !callbackless {
			var err error
			entry, data, err = putCallbackEntryTx(tx, entry)
			if err != nil {
				return err
			}
		}
		return tx.Bucket(callbackCloseIntentBucketName).Delete([]byte(claim.LeaseUUID()))
	})
	if err != nil {
		return CallbackEntry{}, err
	}
	if callbackless {
		return CallbackEntry{}, nil
	}
	entry.storageVersion = callbackStorageV2
	entry.storageLease = entry.LeaseUUID
	entry.storageDeliveryID = entry.DeliveryID
	entry.storageKey = string(callbackSequenceKey(entry.Sequence))
	entry.storageDigest = sha256.Sum256(data)
	s.notifyReplaySubscribers()
	return entry, nil
}

func validateCloseIntentSpec(spec CloseIntentSpec) error {
	if err := validateCanonicalLeaseUUID(spec.LeaseUUID); err != nil {
		return err
	}
	if err := backendname.Validate(spec.Backend); err != nil {
		return fmt.Errorf("callback close intent backend: %w", err)
	}
	if spec.CleanupOnly {
		if spec.Tenant != "" {
			if err := validateCloseIntentIdentity("tenant", spec.Tenant); err != nil {
				return err
			}
		}
		if spec.ProviderUUID != "" {
			if err := validateCloseIntentIdentity("provider", spec.ProviderUUID); err != nil {
				return err
			}
		}
		if spec.CallbackURL != "" || spec.LifecycleCallbackURL != "" {
			return fmt.Errorf("cleanup-only callback close intent cannot carry a callback pair")
		}
		if spec.RetainOnClose {
			return fmt.Errorf("cleanup-only callback close intent cannot retain volumes")
		}
	} else {
		if err := validateCloseIntentIdentity("tenant", spec.Tenant); err != nil {
			return err
		}
		if err := validateCloseIntentIdentity("provider", spec.ProviderUUID); err != nil {
			return err
		}
	}
	if !spec.BackendStorageID.Valid() {
		return fmt.Errorf("callback close intent requires a valid backend storage identity")
	}
	if len(spec.Items) == 0 {
		return fmt.Errorf("callback close intent requires lease items")
	}
	_, err := backend.ValidateOperationQuantities(spec.Items)
	if err != nil {
		return fmt.Errorf("callback close intent quantities: %w", err)
	}
	seenServices := make(map[string]struct{}, len(spec.Items))
	for i, item := range spec.Items {
		if err := validateCloseIntentIdentity(fmt.Sprintf("item %d SKU", i), item.SKU); err != nil {
			return err
		}
		if err := validateCloseIntentIdentity(fmt.Sprintf("item %d service name", i), item.ServiceName); err != nil {
			return err
		}
		if _, exists := seenServices[item.ServiceName]; exists {
			return fmt.Errorf("callback close intent service name %q is duplicated", item.ServiceName)
		}
		seenServices[item.ServiceName] = struct{}{}
	}
	if err := ValidateSKUResourceSnapshot(spec.Items, spec.ResourceProfiles); err != nil {
		return fmt.Errorf("callback close intent resource profiles: %w", err)
	}
	if len(spec.Manifest) == 0 {
		return fmt.Errorf("callback close intent requires its manifest")
	}
	stack, err := manifest.ParsePayload(spec.Manifest)
	if err != nil {
		return fmt.Errorf("callback close intent manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, spec.Items); err != nil {
		return fmt.Errorf("callback close intent topology: %w", err)
	}

	switch {
	case spec.CallbackURL == "" && spec.LifecycleCallbackURL == "":
		// Explicit callbackless legacy close.
	case spec.CallbackURL == "" || spec.LifecycleCallbackURL == "":
		return fmt.Errorf("callback close intent callback pair must be both present or both empty")
	default:
		if err := validateCallbackDestination(spec.CallbackURL); err != nil {
			return err
		}
		if err := validateCallbackDestination(spec.LifecycleCallbackURL); err != nil {
			return err
		}
		if err := backend.ValidateOperationCallbackURL(spec.CallbackURL); err != nil {
			return fmt.Errorf("callback close intent has invalid operation callback: %w", err)
		}
		resolved, err := backend.ResolveLifecycleCallbackURL(
			spec.CallbackURL, spec.LifecycleCallbackURL,
		)
		if err != nil {
			return fmt.Errorf("callback close intent has invalid callback pair: %w", err)
		}
		if resolved != spec.LifecycleCallbackURL {
			return fmt.Errorf("callback close intent lifecycle callback does not match its operation callback")
		}
	}

	switch {
	case spec.ActiveReleaseVersion < 0:
		return fmt.Errorf("callback close intent active release version cannot be negative")
	case spec.ActiveReleaseVersion == 0 && spec.ActiveReleaseDigest != ([sha256.Size]byte{}):
		return fmt.Errorf("callback close intent release fence must be wholly absent or wholly present")
	case spec.ActiveReleaseVersion > 0 && spec.ActiveReleaseDigest == ([sha256.Size]byte{}):
		return fmt.Errorf("callback close intent release fence must be wholly absent or wholly present")
	}
	if spec.CleanupOnly && spec.ActiveReleaseVersion == 0 {
		return fmt.Errorf("cleanup-only callback close intent requires an active release fence")
	}
	if spec.ActiveReleaseVersion == 0 && len(spec.LegacyRollbackTargets) != 0 {
		return fmt.Errorf("callback close intent rollback targets require an active release fence")
	}
	if len(spec.LegacyRollbackTargets) > backend.MaxOperationQuantity {
		return fmt.Errorf(
			"callback close intent has %d rollback targets; maximum is %d",
			len(spec.LegacyRollbackTargets), backend.MaxOperationQuantity,
		)
	}
	seenContainerIDs := make(map[string]struct{}, len(spec.LegacyRollbackTargets))
	seenNames := make(map[string]struct{}, len(spec.LegacyRollbackTargets))
	for i, target := range spec.LegacyRollbackTargets {
		if err := validateCloseRollbackTargetValue(i, "container ID", target.ContainerID); err != nil {
			return err
		}
		if err := validateCloseRollbackTargetValue(i, "name", target.Name); err != nil {
			return err
		}
		if _, exists := seenContainerIDs[target.ContainerID]; exists {
			return fmt.Errorf("callback close intent rollback container ID %q is duplicated", target.ContainerID)
		}
		if _, exists := seenNames[target.Name]; exists {
			return fmt.Errorf("callback close intent rollback name %q is duplicated", target.Name)
		}
		seenContainerIDs[target.ContainerID] = struct{}{}
		seenNames[target.Name] = struct{}{}
	}
	return nil
}

func validateCloseIntentEntry(entry closeIntentEntry, leaseUUID string) error {
	if _, err := parseCloseIntentID(entry.IntentID); err != nil {
		return err
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("callback close intent lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return fmt.Errorf("invalid callback close intent storage identity: %w", err)
	}
	activeDigest, err := parseCloseReleaseDigest(entry.ActiveReleaseDigest)
	if err != nil {
		return err
	}
	if entry.CleanupAttempts < 0 {
		return fmt.Errorf("callback close intent cleanup attempts cannot be negative")
	}
	if err := validateCloseIntentSpec(CloseIntentSpec{
		LeaseUUID:             entry.LeaseUUID,
		Backend:               entry.Backend,
		BackendStorageID:      storageID,
		Tenant:                entry.Tenant,
		ProviderUUID:          entry.ProviderUUID,
		Items:                 entry.Items,
		ResourceProfiles:      entry.ResourceProfiles,
		Manifest:              entry.Manifest,
		CallbackURL:           entry.CallbackURL,
		LifecycleCallbackURL:  entry.LifecycleCallbackURL,
		RetainOnClose:         entry.RetainOnClose,
		CleanupOnly:           entry.CleanupOnly,
		ActiveReleaseVersion:  entry.ActiveReleaseVersion,
		ActiveReleaseDigest:   activeDigest,
		LegacyRollbackTargets: entry.LegacyRollbackTargets,
	}); err != nil {
		return err
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func validateCloseIntentClaim(claim CloseIntentClaim) error {
	if claim.digest == ([sha256.Size]byte{}) || claim.intentID == uuid.Nil {
		return fmt.Errorf("callback close intent claim has no durable capability")
	}
	if !claim.storageID.Valid() || claim.storageID.String() != claim.entry.BackendStorageID {
		return fmt.Errorf("callback close intent claim has invalid storage authority")
	}
	if claim.intentID.String() != claim.entry.IntentID {
		return fmt.Errorf("callback close intent claim has invalid intent authority")
	}
	if encodeCloseReleaseDigest(claim.activeReleaseDigest) != claim.entry.ActiveReleaseDigest {
		return fmt.Errorf("callback close intent claim has invalid release authority")
	}
	return validateCloseIntentEntry(claim.entry, claim.entry.LeaseUUID)
}

func decodeCloseIntent(key, value []byte) (CloseIntentClaim, error) {
	if err := validateUniqueJSONObject(value, maxCloseIntentEntryBytes); err != nil {
		return CloseIntentClaim{}, fmt.Errorf("decode callback close intent %q: %w", key, err)
	}
	var entry closeIntentEntry
	if err := json.Unmarshal(value, &entry); err != nil {
		return CloseIntentClaim{}, fmt.Errorf("decode callback close intent %q: %w", key, err)
	}
	if err := validateCloseIntentEntry(entry, string(key)); err != nil {
		return CloseIntentClaim{}, fmt.Errorf("invalid callback close intent %q: %w", key, err)
	}
	intentID, _ := parseCloseIntentID(entry.IntentID)
	storageID, _ := backendidentity.Parse(entry.BackendStorageID)
	activeDigest, _ := parseCloseReleaseDigest(entry.ActiveReleaseDigest)
	entry = cloneCloseIntentEntry(entry)
	return CloseIntentClaim{
		entry:               entry,
		intentID:            intentID,
		storageID:           storageID,
		activeReleaseDigest: activeDigest,
		digest:              sha256.Sum256(value),
	}, nil
}

func marshalCloseIntent(entry closeIntentEntry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("marshal callback close intent: %w", err)
	}
	if len(data) > maxCloseIntentEntryBytes {
		return nil, fmt.Errorf("callback close intent exceeds %d bytes", maxCloseIntentEntryBytes)
	}
	return data, nil
}

func verifyCloseIntentTx(tx *bolt.Tx, claim CloseIntentClaim) error {
	bucket := tx.Bucket(callbackCloseIntentBucketName)
	if bucket == nil {
		return fmt.Errorf("callback close intent bucket missing")
	}
	key := []byte(claim.LeaseUUID())
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback close intent %q is a nested bucket", claim.LeaseUUID())
	}
	current := bucket.Get(key)
	if current == nil {
		return fmt.Errorf("callback close intent no longer exists for lease %q", claim.LeaseUUID())
	}
	if sha256.Sum256(current) != claim.digest {
		return fmt.Errorf("callback close intent changed before precise mutation")
	}
	return nil
}

// rejectOperationWhileClosingTx is the shared admission fence for new
// operation intents and late operation completions. Callers already hold the
// per-lease journal-mutation lock; keeping the durable check in their bbolt
// transaction prevents either path from recreating operation authority after
// close won.
func rejectOperationWhileClosingTx(tx *bolt.Tx, leaseUUID string) error {
	bucket := tx.Bucket(callbackCloseIntentBucketName)
	if bucket == nil {
		return fmt.Errorf("callback close intent bucket missing")
	}
	key := []byte(leaseUUID)
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback close intent %q is a nested bucket", leaseUUID)
	}
	if bucket.Get(key) != nil {
		return fmt.Errorf("%w for lease %q: close is already admitted",
			ErrOperationIntentConflict, leaseUUID)
	}
	return nil
}

func closeIntentEntryMatchesSpec(left, right closeIntentEntry) bool {
	return left.LeaseUUID == right.LeaseUUID &&
		left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID &&
		slices.Equal(left.Items, right.Items) &&
		slices.Equal(left.ResourceProfiles, right.ResourceProfiles) &&
		bytes.Equal(left.Manifest, right.Manifest) &&
		left.CallbackURL == right.CallbackURL &&
		left.LifecycleCallbackURL == right.LifecycleCallbackURL &&
		left.RetainOnClose == right.RetainOnClose &&
		left.CleanupOnly == right.CleanupOnly &&
		left.ActiveReleaseVersion == right.ActiveReleaseVersion &&
		left.ActiveReleaseDigest == right.ActiveReleaseDigest &&
		slices.Equal(left.LegacyRollbackTargets, right.LegacyRollbackTargets)
}

func cloneCloseIntentEntry(entry closeIntentEntry) closeIntentEntry {
	entry.Items = slices.Clone(entry.Items)
	entry.ResourceProfiles = CloneSKUResourceSnapshot(entry.ResourceProfiles)
	entry.Manifest = bytes.Clone(entry.Manifest)
	entry.LegacyRollbackTargets = slices.Clone(entry.LegacyRollbackTargets)
	return entry
}

func parseCloseIntentID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id.String() != value || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return uuid.Nil, fmt.Errorf("callback close intent ID must be a canonical UUIDv4: %q", value)
	}
	return id, nil
}

func parseCloseReleaseDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if value == "" {
		return digest, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return digest, fmt.Errorf("callback close intent active release digest must be canonical SHA-256")
	}
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return digest, fmt.Errorf("callback close intent absent release digest must use an empty encoding")
	}
	return digest, nil
}

func encodeCloseReleaseDigest(digest [sha256.Size]byte) string {
	if digest == ([sha256.Size]byte{}) {
		return ""
	}
	return hex.EncodeToString(digest[:])
}

func validateCloseIntentIdentity(label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("callback close intent requires %s", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("callback close intent %s must not contain surrounding whitespace", label)
	}
	if len(value) > maxCloseIntentIdentityBytes || !utf8.ValidString(value) {
		return fmt.Errorf("callback close intent %s is invalid or exceeds %d bytes", label, maxCloseIntentIdentityBytes)
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("callback close intent %s contains a non-printable character", label)
		}
	}
	return nil
}

func validateCloseRollbackTargetValue(index int, label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("callback close intent rollback target %d requires %s", index, label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("callback close intent rollback target %d %s has surrounding whitespace", index, label)
	}
	if len(value) > maxCloseRollbackTargetBytes || !utf8.ValidString(value) {
		return fmt.Errorf(
			"callback close intent rollback target %d %s is invalid or exceeds %d bytes",
			index, label, maxCloseRollbackTargetBytes,
		)
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("callback close intent rollback target %d %s contains a non-printable character", index, label)
		}
	}
	return nil
}
