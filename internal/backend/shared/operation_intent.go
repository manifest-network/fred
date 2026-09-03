package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const maxOperationIntentEntryBytes = 4 << 20

// OperationIntentKind identifies the asynchronous operation whose exact
// completion must survive a backend crash.
type OperationIntentKind string

// OperationID is the parsed callback authority. Empty is the explicit
// tokenless compatibility value accepted at the backend boundary; every
// non-empty value has already passed canonical UUIDv4 validation there.
type OperationID string

func (id OperationID) String() string { return string(id) }

func (id OperationID) Valid() bool {
	parsed, err := uuid.Parse(string(id))
	return err == nil && parsed.Version() == uuid.Version(4) &&
		parsed.Variant() == uuid.RFC4122 && parsed.String() == string(id)
}

const (
	OperationIntentProvision OperationIntentKind = "provision"
	OperationIntentRestore   OperationIntentKind = "restore"
)

// ErrOperationIntentConflict means a lease already has an unresolved durable
// operation. The existing intent is preserved; callers must not mutate the
// substrate for the conflicting request.
var ErrOperationIntentConflict = errors.New("unresolved callback operation intent")

// ErrOperationIntentMissing means an exact operation completion has no durable
// intent to consume. The callback is not enqueued: manufacturing completion
// evidence after its write-ahead authority disappeared would let a stale worker
// settle a newer or already-closed lease generation.
var ErrOperationIntentMissing = errors.New("callback operation intent missing")

// OperationIntentAdmissionDisposition tells a backend whether it owns the
// newly-created intent and may start work, or whether an exact retry was
// already durably accepted/completed and should receive an idempotent success.
type OperationIntentAdmissionDisposition uint8

const (
	OperationIntentAdmissionNone OperationIntentAdmissionDisposition = iota
	OperationIntentAdmissionCreated
	OperationIntentAdmissionExisting
	OperationIntentAdmissionCompleted
)

// OperationIntentProbe is the minimal exact authority needed to recognize a
// redelivery before mutable SKU, manifest, or restore-source validation. A
// positive result authorizes only an idempotent acknowledgement, never a new
// substrate mutation.
type OperationIntentProbe struct {
	LeaseUUID        string
	CallbackURL      string
	Backend          string
	BackendStorageID backendidentity.ID
}

type OperationIntentAdmission struct {
	Claim       OperationIntentClaim
	Disposition OperationIntentAdmissionDisposition
}

// OperationIntentSpec is the immutable evidence needed to classify an
// accepted asynchronous operation after restart.
type OperationIntentSpec struct {
	Kind                 OperationIntentKind
	LeaseUUID            string
	CallbackURL          string
	LifecycleCallbackURL string
	Backend              string
	BackendStorageID     backendidentity.ID
	Tenant               string
	ProviderUUID         string
	Items                []backend.LeaseItem
	// ResourceProfiles freezes the resource definition used for admission and
	// substrate creation before either can happen. Operation intents are new in
	// v0.14, so unlike Release there is no deployed legacy format without this
	// authority.
	ResourceProfiles []SKUResourceSnapshot
	// EffectiveItems is the exact item metadata emitted to substrate labels.
	// It may differ from desired Items only where a custom domain was deferred
	// by the DNS-readiness gate.
	EffectiveItems      []backend.LeaseItem
	HealthCheckServices []string
	Manifest            []byte
	SourceLeaseUUID     string
	SourceGeneration    int
}

// OperationIntentClaim is an opaque, precise capability for canceling or
// resolving one durable intent. Values are returned only by CallbackStore;
// callers cannot construct a usable claim from request data alone.
type OperationIntentClaim struct {
	entry     *operationIntentEntry
	storageID backendidentity.ID
	digest    [sha256.Size]byte
}

func (c OperationIntentClaim) Kind() OperationIntentKind {
	if c.entry == nil {
		return ""
	}
	return c.entry.Kind
}
func (c OperationIntentClaim) OperationID() OperationID {
	if c.entry == nil {
		return ""
	}
	return c.entry.OperationID
}
func (c OperationIntentClaim) LeaseUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.LeaseUUID
}
func (c OperationIntentClaim) CallbackURL() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.CallbackURL
}
func (c OperationIntentClaim) LifecycleCallbackURL() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.LifecycleCallbackURL
}
func (c OperationIntentClaim) Backend() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.Backend
}
func (c OperationIntentClaim) BackendStorageID() backendidentity.ID {
	return c.storageID
}
func (c OperationIntentClaim) Tenant() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.Tenant
}
func (c OperationIntentClaim) ProviderUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.ProviderUUID
}
func (c OperationIntentClaim) Items() []backend.LeaseItem {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.Items)
}
func (c OperationIntentClaim) ResourceProfiles() []SKUResourceSnapshot {
	if c.entry == nil {
		return nil
	}
	return CloneSKUResourceSnapshot(c.entry.ResourceProfiles)
}
func (c OperationIntentClaim) EffectiveItems() []backend.LeaseItem {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.EffectiveItems)
}
func (c OperationIntentClaim) HealthCheckServices() []string {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.HealthCheckServices)
}
func (c OperationIntentClaim) Manifest() []byte {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.Manifest)
}
func (c OperationIntentClaim) SourceLeaseUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.SourceLeaseUUID
}
func (c OperationIntentClaim) SourceGeneration() int {
	if c.entry == nil {
		return 0
	}
	return c.entry.SourceGeneration
}
func (c OperationIntentClaim) CreatedAt() time.Time {
	if c.entry == nil {
		return time.Time{}
	}
	return c.entry.CreatedAt
}

type operationIntentEntry struct {
	IntentID             string                `json:"intent_id"`
	OperationID          OperationID           `json:"operation_id,omitempty"`
	Kind                 OperationIntentKind   `json:"kind"`
	LeaseUUID            string                `json:"lease_uuid"`
	CallbackURL          string                `json:"callback_url"`
	LifecycleCallbackURL string                `json:"lifecycle_callback_url"`
	Backend              string                `json:"backend"`
	BackendStorageID     string                `json:"backend_storage_id"`
	Tenant               string                `json:"tenant"`
	ProviderUUID         string                `json:"provider_uuid"`
	Items                []backend.LeaseItem   `json:"items"`
	ResourceProfiles     []SKUResourceSnapshot `json:"resource_profiles"`
	EffectiveItems       []backend.LeaseItem   `json:"effective_items,omitempty"`
	HealthCheckServices  []string              `json:"health_check_services,omitempty"`
	Manifest             []byte                `json:"manifest,omitempty"`
	SourceLeaseUUID      string                `json:"source_lease_uuid,omitempty"`
	SourceGeneration     int                   `json:"source_generation,omitempty"`
	CreatedAt            time.Time             `json:"created_at"`
}

// ProbeOperationIntent recognizes an already-accepted or already-completed
// exact operation without requiring the original semantic inputs to remain
// valid. This is what makes provider-side redelivery safe after a SKU was
// removed or a completed restore deleted its source retention record.
func (s *CallbackStore) ProbeOperationIntent(
	probe OperationIntentProbe,
) (OperationIntentAdmissionDisposition, error) {
	if err := validateCanonicalLeaseUUID(probe.LeaseUUID); err != nil {
		return OperationIntentAdmissionNone, err
	}
	if probe.Backend == "" || !probe.BackendStorageID.Valid() {
		return OperationIntentAdmissionNone, fmt.Errorf("operation intent probe requires backend and storage identity")
	}
	if err := backend.ValidateOperationCallbackURL(probe.CallbackURL); err != nil {
		return OperationIntentAdmissionNone, err
	}

	unlock := s.lockDeliveryLease(probe.LeaseUUID)
	defer unlock()
	disposition := OperationIntentAdmissionNone
	err := s.view(func(tx *bolt.Tx) error {
		if err := rejectOperationWhileClosingTx(tx, probe.LeaseUUID); err != nil {
			return err
		}
		if err := rejectOperationWhileMaintainingTx(tx, probe.LeaseUUID); err != nil {
			return err
		}
		key := []byte(probe.LeaseUUID)
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback operation intent %q is a nested bucket", probe.LeaseUUID)
		}
		if current := bucket.Get(key); current != nil {
			claim, err := decodeOperationIntent(key, current)
			if err != nil {
				return err
			}
			if claim.CallbackURL() != probe.CallbackURL ||
				claim.Backend() != probe.Backend ||
				claim.BackendStorageID() != probe.BackendStorageID {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.LeaseUUID)
			}
			disposition = OperationIntentAdmissionExisting
			return nil
		}

		pending, err := listPendingCallbackEntriesTx(tx, probe.LeaseUUID)
		if err != nil {
			return err
		}
		for _, callback := range pending {
			if callback.DeliveryKind == CallbackDeliveryKindLifecycle {
				continue
			}
			if callback.CallbackURL != probe.CallbackURL ||
				callback.Backend != probe.Backend ||
				callback.BackendStorageID != probe.BackendStorageID.String() {
				return fmt.Errorf("%w for lease %q: an earlier operation completion is pending",
					ErrOperationIntentConflict, probe.LeaseUUID)
			}
			disposition = OperationIntentAdmissionCompleted
		}
		return nil
	})
	return disposition, err
}

// BeginOperationIntent durably records an operation before its first external
// side effect. bbolt's default synchronous commit is the acceptance barrier.
func (s *CallbackStore) BeginOperationIntent(spec OperationIntentSpec) (OperationIntentAdmission, error) {
	if len(spec.EffectiveItems) == 0 {
		spec.EffectiveItems = slices.Clone(spec.Items)
	}
	if err := validateOperationIntentSpec(spec); err != nil {
		return OperationIntentAdmission{}, err
	}
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return OperationIntentAdmission{}, fmt.Errorf("allocate callback operation intent ID: %w", err)
	}
	entry := operationIntentEntry{
		IntentID:             id.String(),
		OperationID:          operationID,
		Kind:                 spec.Kind,
		LeaseUUID:            spec.LeaseUUID,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Backend:              spec.Backend,
		BackendStorageID:     spec.BackendStorageID.String(),
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                slices.Clone(spec.Items),
		ResourceProfiles:     CloneSKUResourceSnapshot(spec.ResourceProfiles),
		EffectiveItems:       slices.Clone(spec.EffectiveItems),
		HealthCheckServices:  slices.Clone(spec.HealthCheckServices),
		Manifest:             slices.Clone(spec.Manifest),
		SourceLeaseUUID:      spec.SourceLeaseUUID,
		SourceGeneration:     spec.SourceGeneration,
		CreatedAt:            time.Now(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return OperationIntentAdmission{}, fmt.Errorf("marshal callback operation intent: %w", err)
	}
	if len(data) > maxOperationIntentEntryBytes {
		return OperationIntentAdmission{}, fmt.Errorf("callback operation intent exceeds %d bytes", maxOperationIntentEntryBytes)
	}

	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	admission := OperationIntentAdmission{}
	err = s.update(func(tx *bolt.Tx) error {
		if err := rejectOperationWhileClosingTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		if err := rejectOperationWhileMaintainingTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		key := []byte(entry.LeaseUUID)
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback operation intent %q is a nested bucket", entry.LeaseUUID)
		}
		if current := bucket.Get(key); current != nil {
			claim, decodeErr := decodeOperationIntent(key, current)
			if decodeErr != nil {
				return decodeErr
			}
			if !operationIntentEntriesEqual(*claim.entry, entry) {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, entry.LeaseUUID)
			}
			admission = OperationIntentAdmission{
				Claim: claim, Disposition: OperationIntentAdmissionExisting,
			}
			return nil
		}
		pending, pendingErr := listPendingCallbackEntriesTx(tx, entry.LeaseUUID)
		if pendingErr != nil {
			return pendingErr
		}
		exactCompletion := false
		for _, callback := range pending {
			if callback.DeliveryKind == CallbackDeliveryKindLifecycle {
				continue
			}
			if callback.CallbackURL == entry.CallbackURL &&
				callback.Backend == entry.Backend &&
				callback.BackendStorageID == entry.BackendStorageID {
				exactCompletion = true
				continue
			}
			return fmt.Errorf("%w for lease %q: an earlier operation completion is pending",
				ErrOperationIntentConflict, entry.LeaseUUID)
		}
		if exactCompletion {
			admission = OperationIntentAdmission{Disposition: OperationIntentAdmissionCompleted}
			return nil
		}
		if err := bucket.Put(key, data); err != nil {
			return err
		}
		admission = OperationIntentAdmission{
			Claim: OperationIntentClaim{
				entry: &entry, storageID: spec.BackendStorageID, digest: sha256.Sum256(data),
			},
			Disposition: OperationIntentAdmissionCreated,
		}
		return nil
	})
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	return admission, nil
}

// ListOperationIntents returns durable recovery capabilities in deterministic
// lease order. Intents never expire: they are unresolved causal evidence.
func (s *CallbackStore) ListOperationIntents() ([]OperationIntentClaim, error) {
	var claims []OperationIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback operation intent %q is a nested bucket", key)
			}
			claim, err := decodeOperationIntent(key, value)
			if err != nil {
				return err
			}
			claims = append(claims, claim)
			return nil
		})
	})
	return claims, err
}

// ResolveOperationIntent atomically replaces one precise intent with the exact
// operation callback that settles it. The durable commit wakes every running
// replay loop; during startup, the initial level-triggered replay observes the
// row after recovery completes.
func (s *CallbackStore) ResolveOperationIntent(
	claim OperationIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	if err := validateOperationIntentClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	entry, err := callbackEntryForIntent(*claim.entry, status, errMsg)
	if err != nil {
		return CallbackEntry{}, err
	}
	unlock := s.lockDeliveryLease(claim.entry.LeaseUUID)
	defer unlock()
	return s.resolveOperationIntentLocked(claim, entry)
}

// FailOperationIntentIfPresent atomically settles the exact durable operation
// currently owned by leaseUUID. It is used when deprovision preempts an
// accepted worker; absence is a no-op so teardown cannot manufacture duplicate
// completions for already-settled operations.
func (s *CallbackStore) FailOperationIntentIfPresent(leaseUUID, errMsg string) (bool, error) {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return false, err
	}
	unlock := s.lockDeliveryLease(leaseUUID)
	defer unlock()
	var claim OperationIntentClaim
	found := false
	if err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		key := []byte(leaseUUID)
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback operation intent %q is a nested bucket", leaseUUID)
		}
		value := bucket.Get(key)
		if value == nil {
			return nil
		}
		var err error
		claim, err = decodeOperationIntent(key, value)
		found = err == nil
		return err
	}); err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	entry, err := callbackEntryForIntent(*claim.entry, backend.CallbackStatusFailed, errMsg)
	if err != nil {
		return false, err
	}
	if _, err := s.resolveOperationIntentLocked(claim, entry); err != nil {
		return false, err
	}
	return true, nil
}

func (s *CallbackStore) resolveOperationIntentLocked(
	claim OperationIntentClaim,
	entry CallbackEntry,
) (CallbackEntry, error) {
	if entry.DeliveryID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("allocate callback delivery ID: %w", err)
		}
		entry.DeliveryID = id.String()
	}
	if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
		return CallbackEntry{}, err
	}
	if err := operationIntentMatchesCallback(*claim.entry, entry); err != nil {
		return CallbackEntry{}, err
	}
	var data []byte
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyOperationIntentTx(tx, claim); err != nil {
			return err
		}
		var err error
		entry, data, err = putCallbackEntryTx(tx, entry)
		if err != nil {
			return err
		}
		return removeOperationIntentTx(tx, claim)
	})
	if err != nil {
		return CallbackEntry{}, err
	}
	entry.storageVersion = callbackStorageV2
	entry.storageLease = entry.LeaseUUID
	entry.storageDeliveryID = entry.DeliveryID
	entry.storageKey = string(callbackSequenceKey(entry.Sequence))
	entry.storageDigest = sha256.Sum256(data)
	s.notifyReplaySubscribers()
	return entry, nil
}

// settleOperationCallbackLocked consumes the current intent only when the
// worker presents its exact callback identity. An absent intent is a causal
// invariant violation, not permission to manufacture an outbox entry.
func (s *CallbackStore) settleOperationCallbackLocked(entry CallbackEntry) error {
	var claim OperationIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		if err := rejectOperationWhileClosingTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		if err := rejectOperationWhileMaintainingTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		if bucket == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		key := []byte(entry.LeaseUUID)
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback operation intent %q is a nested bucket", entry.LeaseUUID)
		}
		value := bucket.Get(key)
		if value == nil {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentMissing, entry.LeaseUUID)
		}
		var err error
		claim, err = decodeOperationIntent(key, value)
		return err
	})
	if err != nil {
		return err
	}
	_, err = s.resolveOperationIntentLocked(claim, entry)
	return err
}

func callbackEntryForIntent(
	intent operationIntentEntry,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	entry := CallbackEntry{
		LeaseUUID:        intent.LeaseUUID,
		CallbackURL:      intent.CallbackURL,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Success:          status != backend.CallbackStatusFailed,
		Status:           status,
		Backend:          intent.Backend,
		BackendStorageID: intent.BackendStorageID,
		Error:            errMsg,
		CreatedAt:        time.Now(),
	}
	if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
		return CallbackEntry{}, fmt.Errorf("operation intent has invalid completion status %q", status)
	}
	return entry, nil
}

func validateOperationIntentSpec(spec OperationIntentSpec) error {
	// Manifest is only one field in the encoded row, so a manifest this large
	// necessarily makes the durable intent exceed its total entry budget. Reject
	// it before doing an expensive semantic parse.
	if len(spec.Manifest) >= maxOperationIntentEntryBytes {
		return fmt.Errorf("callback operation intent exceeds %d bytes", maxOperationIntentEntryBytes)
	}
	if err := validateCanonicalLeaseUUID(spec.LeaseUUID); err != nil {
		return err
	}
	if spec.Kind != OperationIntentProvision && spec.Kind != OperationIntentRestore {
		return fmt.Errorf("invalid callback operation intent kind %q", spec.Kind)
	}
	if !spec.BackendStorageID.Valid() {
		return fmt.Errorf("callback operation intent requires a valid backend storage identity")
	}
	if spec.Backend == "" || spec.Tenant == "" || spec.ProviderUUID == "" {
		return fmt.Errorf("callback operation intent requires backend, tenant, and provider identities")
	}
	if !backend.IsCanonicalLeaseUUID(spec.ProviderUUID) {
		return fmt.Errorf("callback operation intent provider UUID is not canonical")
	}
	if len(spec.Items) == 0 {
		return fmt.Errorf("callback operation intent requires lease items")
	}
	if err := ValidateSKUResourceSnapshot(spec.Items, spec.ResourceProfiles); err != nil {
		return fmt.Errorf("callback operation intent resource profiles: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(spec.Items); err != nil {
		return fmt.Errorf("callback operation intent quantities: %w", err)
	}
	if len(spec.Manifest) == 0 {
		return fmt.Errorf("callback operation intent requires its manifest")
	}
	stack, err := manifest.ParsePayload(spec.Manifest)
	if err != nil {
		return fmt.Errorf("callback operation intent manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, spec.Items); err != nil {
		return fmt.Errorf("callback operation intent manifest topology: %w", err)
	}
	for i, item := range spec.Items {
		if item.SKU == "" || item.ServiceName == "" {
			return fmt.Errorf("callback operation intent item %d requires SKU and service name", i)
		}
	}
	if len(spec.EffectiveItems) != len(spec.Items) {
		return fmt.Errorf("callback operation intent effective items must match desired item count")
	}
	for i, effective := range spec.EffectiveItems {
		desired := spec.Items[i]
		if effective.SKU != desired.SKU || effective.ServiceName != desired.ServiceName ||
			effective.Quantity != desired.Quantity ||
			(effective.CustomDomain != desired.CustomDomain && effective.CustomDomain != "") {
			return fmt.Errorf("callback operation intent effective item %d diverges from desired item", i)
		}
	}
	seenHealthServices := make(map[string]struct{}, len(spec.HealthCheckServices))
	for _, service := range spec.HealthCheckServices {
		if service == "" {
			return fmt.Errorf("callback operation intent health-check service is empty")
		}
		if _, exists := seenHealthServices[service]; exists {
			return fmt.Errorf("callback operation intent health-check service %q is duplicated", service)
		}
		seenHealthServices[service] = struct{}{}
	}
	switch spec.Kind {
	case OperationIntentProvision:
		if spec.SourceLeaseUUID != "" || spec.SourceGeneration != 0 {
			return fmt.Errorf("provision operation intent cannot carry restore source authority")
		}
	case OperationIntentRestore:
		if err := validateCanonicalLeaseUUID(spec.SourceLeaseUUID); err != nil {
			return fmt.Errorf("restore source lease: %w", err)
		}
		if spec.SourceLeaseUUID == spec.LeaseUUID || spec.SourceGeneration <= 0 {
			return fmt.Errorf("restore operation intent requires a distinct source and positive generation")
		}
	}
	if err := validateCallbackDestination(spec.CallbackURL); err != nil {
		return err
	}
	if _, err := backend.ResolveLifecycleCallbackURL(spec.CallbackURL, spec.LifecycleCallbackURL); err != nil {
		return fmt.Errorf("callback operation intent has invalid callback pair: %w", err)
	}
	return nil
}

func validateOperationIntentClaim(claim OperationIntentClaim) error {
	if claim.digest == ([sha256.Size]byte{}) || claim.entry == nil || claim.entry.IntentID == "" {
		return fmt.Errorf("callback operation intent claim has no durable capability")
	}
	if !claim.storageID.Valid() || claim.storageID.String() != claim.entry.BackendStorageID {
		return fmt.Errorf("callback operation intent claim has invalid storage authority")
	}
	if err := validateOperationIntentEntry(*claim.entry, claim.entry.LeaseUUID); err != nil {
		return err
	}
	return nil
}

func validateOperationIntentEntry(entry operationIntentEntry, leaseUUID string) error {
	id, err := uuid.Parse(entry.IntentID)
	if err != nil || id.String() != entry.IntentID || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return fmt.Errorf("callback operation intent ID must be a canonical UUIDv4: %q", entry.IntentID)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return fmt.Errorf("invalid callback operation intent storage identity: %w", err)
	}
	if err := validateOperationIntentSpec(OperationIntentSpec{
		Kind:                 entry.Kind,
		LeaseUUID:            entry.LeaseUUID,
		CallbackURL:          entry.CallbackURL,
		LifecycleCallbackURL: entry.LifecycleCallbackURL,
		Backend:              entry.Backend,
		BackendStorageID:     storageID,
		Tenant:               entry.Tenant,
		ProviderUUID:         entry.ProviderUUID,
		Items:                entry.Items,
		ResourceProfiles:     entry.ResourceProfiles,
		EffectiveItems:       entry.EffectiveItems,
		HealthCheckServices:  entry.HealthCheckServices,
		Manifest:             entry.Manifest,
		SourceLeaseUUID:      entry.SourceLeaseUUID,
		SourceGeneration:     entry.SourceGeneration,
	}); err != nil {
		return err
	}
	wantOperationID, err := parseOperationCallbackID(entry.CallbackURL)
	if err != nil {
		return err
	}
	if entry.OperationID != wantOperationID {
		return fmt.Errorf("callback operation intent ID %q does not match callback authority %q",
			entry.OperationID, wantOperationID)
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("callback operation intent lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func parseOperationCallbackID(callbackURL string) (OperationID, error) {
	parsed, err := url.Parse(callbackURL)
	if err != nil {
		return "", fmt.Errorf("parse operation callback authority: %w", err)
	}
	values, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return "", fmt.Errorf("parse operation callback authority: %w", err)
	}
	ids := values[backend.CallbackOperationIDQueryParameter]
	if len(ids) == 0 {
		// Tokenless compatibility is intentional at the request boundary, so an
		// explicitly recorded tokenless intent remains readable and comparable.
		// It does not authorize completion when the durable intent is absent.
		return "", nil
	}
	if len(ids) != 1 {
		return "", fmt.Errorf("operation callback authority occurs %d times", len(ids))
	}
	return OperationID(ids[0]), nil
}

func operationIntentEntriesEqual(left, right operationIntentEntry) bool {
	return left.Kind == right.Kind &&
		left.OperationID == right.OperationID &&
		left.LeaseUUID == right.LeaseUUID &&
		left.CallbackURL == right.CallbackURL &&
		left.LifecycleCallbackURL == right.LifecycleCallbackURL &&
		left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID &&
		slices.Equal(left.Items, right.Items) &&
		slices.Equal(left.ResourceProfiles, right.ResourceProfiles) &&
		slices.Equal(left.EffectiveItems, right.EffectiveItems) &&
		slices.Equal(left.HealthCheckServices, right.HealthCheckServices) &&
		bytes.Equal(left.Manifest, right.Manifest) &&
		left.SourceLeaseUUID == right.SourceLeaseUUID &&
		left.SourceGeneration == right.SourceGeneration
}

func decodeOperationIntent(key, value []byte) (OperationIntentClaim, error) {
	if err := validateUniqueJSONObject(value, maxOperationIntentEntryBytes); err != nil {
		return OperationIntentClaim{}, fmt.Errorf("decode callback operation intent %q: %w", key, err)
	}
	var entry operationIntentEntry
	if err := json.Unmarshal(value, &entry); err != nil {
		return OperationIntentClaim{}, fmt.Errorf("decode callback operation intent %q: %w", key, err)
	}
	if len(entry.EffectiveItems) == 0 {
		// Compatibility with intent rows written before effective custom-domain
		// labels were journaled separately from desired lease items.
		entry.EffectiveItems = slices.Clone(entry.Items)
	}
	if err := validateOperationIntentEntry(entry, string(key)); err != nil {
		return OperationIntentClaim{}, fmt.Errorf("invalid callback operation intent %q: %w", key, err)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return OperationIntentClaim{}, fmt.Errorf("decode callback operation intent %q storage identity: %w", key, err)
	}
	return OperationIntentClaim{
		entry: &entry, storageID: storageID, digest: sha256.Sum256(value),
	}, nil
}

func operationIntentMatchesCallback(intent operationIntentEntry, entry CallbackEntry) error {
	if entry.DeliveryKind != CallbackDeliveryKindOperation ||
		entry.LeaseUUID != intent.LeaseUUID ||
		entry.CallbackURL != intent.CallbackURL ||
		entry.Backend != intent.Backend ||
		entry.BackendStorageID != intent.BackendStorageID {
		return fmt.Errorf("operation callback does not match durable intent for lease %q", intent.LeaseUUID)
	}
	return nil
}

func verifyOperationIntentTx(tx *bolt.Tx, claim OperationIntentClaim) error {
	bucket := tx.Bucket(callbackOperationIntentBucketName)
	if bucket == nil {
		return fmt.Errorf("callback operation intent bucket missing")
	}
	key := []byte(claim.entry.LeaseUUID)
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback operation intent %q is a nested bucket", claim.entry.LeaseUUID)
	}
	current := bucket.Get(key)
	if current == nil {
		return fmt.Errorf("callback operation intent no longer exists for lease %q", claim.entry.LeaseUUID)
	}
	if sha256.Sum256(current) != claim.digest {
		return fmt.Errorf("callback operation intent changed before precise mutation")
	}
	return nil
}

func removeOperationIntentTx(tx *bolt.Tx, claim OperationIntentClaim) error {
	if err := verifyOperationIntentTx(tx, claim); err != nil {
		return err
	}
	return tx.Bucket(callbackOperationIntentBucketName).Delete([]byte(claim.entry.LeaseUUID))
}
