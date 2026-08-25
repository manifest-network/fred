package placement

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("placements")

// State describes what is durably known about one lease placement.
type State uint8

const (
	// StateAbsent means no record exists for the lease.
	StateAbsent State = iota
	// StateAttempting means a backend call may have happened but has not yet
	// been confirmed or authoritatively disproved.
	StateAttempting
	// StateConfirmed means Backend is the last positively observed owner. A
	// confirmed placement may also carry an unresolved Attempt.
	StateConfirmed
	// StateUnusable means a record exists but cannot safely be interpreted.
	StateUnusable
)

// String returns the stable log representation of a placement state.
func (s State) String() string {
	switch s {
	case StateAbsent:
		return "absent"
	case StateAttempting:
		return "attempting"
	case StateConfirmed:
		return "confirmed"
	case StateUnusable:
		return "unusable"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

var (
	// ErrInvalidPlacement means a required lease or backend identifier is empty.
	ErrInvalidPlacement = errors.New("invalid placement")
	// ErrAttemptConflict means an unresolved attempt already exists. Retrying
	// even the same target is unsafe because the first backend outcome is unknown.
	ErrAttemptConflict = errors.New("placement attempt already exists")
	// ErrBackendConflict means an operation targeted a backend other than the
	// lease's confirmed owner.
	ErrBackendConflict = errors.New("placement backend conflicts with target")
	// ErrUnusablePlacement means an on-disk record exists but cannot safely be
	// used as either affinity or an attempt gate.
	ErrUnusablePlacement = errors.New("placement record is unusable")
	// ErrAttemptMismatch means Confirm or ClearAttempt did not name the current
	// unresolved attempt.
	ErrAttemptMismatch = errors.New("placement attempt does not match target")
)

// Placement is an immutable snapshot of one cached placement record. Backend
// is the last positively observed owner. Attempt is a write-ahead record of a
// backend call whose outcome is not yet known. SetAt is the record's first-seen
// time and gates orphan pruning.
//
// State and revision deliberately remain opaque. Callers use State and
// Revision to make decisions without being able to mutate store-owned metadata.
type Placement struct {
	Backend string
	Attempt string
	SetAt   time.Time
	// Conflict is a durable quarantine marker: more than one backend reported
	// positive ownership, so no individual backend/status is authoritative.
	Conflict bool
	// ConflictBackends is the sorted durable set of backends that have been
	// positively identified as possible owners while Conflict is true. Keeping
	// the names is essential across restart/reconfiguration: a backend removed
	// from the current router must remain an unresolved deprovision candidate,
	// rather than disappearing from the evidence simply because it is offline.
	ConflictBackends []string
	// ConflictOwnersUnknown marks a legacy or malformed conflict whose complete
	// candidate set was not persisted. Such a record remains fail-closed until an
	// operator resolves it; a complete view of only the current configuration
	// cannot prove that a former owner is gone.
	ConflictOwnersUnknown bool

	unusable bool
	revision uint64
}

// State returns the placement's derived state. The zero Placement is Absent.
func (p Placement) State() State {
	if p.unusable || p.Conflict {
		return StateUnusable
	}
	if p.Backend != "" {
		return StateConfirmed
	}
	if p.Attempt != "" {
		return StateAttempting
	}
	// Only the completely empty public snapshot is Absent. A timestamp without
	// either placement fact is structurally present but cannot be interpreted.
	if !p.SetAt.IsZero() || p.revision != 0 {
		return StateUnusable
	}
	return StateAbsent
}

// Revision returns the opaque per-record revision used by conditional writes.
func (p Placement) Revision() uint64 { return p.revision }

// record is the version-tolerant bbolt representation. The backend and set_at
// field names retain compatibility with the ENG-335 JSON format. Before that
// change, values were raw backend names; decodeRecord still accepts them.
type record struct {
	Backend               string    `json:"backend"`
	Attempt               string    `json:"attempt,omitempty"`
	SetAt                 time.Time `json:"set_at"`
	Revision              uint64    `json:"revision,omitempty"`
	Conflict              bool      `json:"conflict,omitempty"`
	ConflictBackends      []string  `json:"conflict_backends,omitempty"`
	ConflictOwnersUnknown bool      `json:"conflict_owners_unknown,omitempty"`
}

// Store is a bbolt-backed placement store with an in-memory read cache. All
// writes commit to bbolt before the cache or revision clock is changed.
type Store struct {
	db           *bolt.DB
	cache        map[string]Placement
	lastMutation map[string]uint64 // includes process-local tombstones for deleted keys
	now          func() time.Time
	revision     uint64
	mu           sync.RWMutex
	closeOnce    sync.Once
	closeErr     error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithClock injects the clock used to stamp SetAt. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// NewStore opens or creates a bbolt database and loads all existing placement
// records into memory. Corrupt records remain present as StateUnusable so a
// missing placement can never be inferred from unreadable durable state.
func NewStore(dbPath string, opts ...Option) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open placement db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create placement bucket: %w", err)
	}

	cache := make(map[string]Placement)
	lastMutation := make(map[string]uint64)
	var revision uint64
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.ForEach(func(k, v []byte) error {
			p := decodeRecord(string(k), v)
			cache[string(k)] = p
			lastMutation[string(k)] = p.revision
			if p.revision > revision {
				revision = p.revision
			}
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placements into cache: %w", err)
	}

	s := &Store{
		db:           db,
		cache:        cache,
		lastMutation: lastMutation,
		now:          time.Now,
		revision:     revision,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// decodeRecord parses the current JSON representation or the pre-ENG-335 raw
// backend-name representation. An empty raw value, malformed JSON object, or
// JSON object with neither Backend nor Attempt remains present but Unusable.
func decodeRecord(leaseUUID string, v []byte) Placement {
	if len(v) > 0 && v[0] != '{' {
		return Placement{Backend: string(v)}
	}

	var r record
	if len(v) == 0 {
		return unusableRecord(leaseUUID, errors.New("empty value"))
	}
	if err := json.Unmarshal(v, &r); err != nil {
		return unusableRecord(leaseUUID, err)
	}

	p := Placement{
		Backend:               r.Backend,
		Attempt:               r.Attempt,
		SetAt:                 r.SetAt,
		Conflict:              r.Conflict,
		ConflictBackends:      normalizeBackendNames(r.ConflictBackends),
		ConflictOwnersUnknown: r.ConflictOwnersUnknown,
		revision:              r.Revision,
	}
	if p.Conflict {
		// Records written before conflict candidates were introduced contain only
		// conflict=true. Preserve them, but never mistake the missing owner set for
		// proof that the current router represents the whole historical fleet.
		if len(p.ConflictBackends) < 2 {
			p.ConflictOwnersUnknown = true
		}
	} else {
		// Candidate metadata has meaning only while the quarantine is active.
		p.ConflictBackends = nil
		p.ConflictOwnersUnknown = false
	}
	if p.Backend == "" && p.Attempt == "" && !p.Conflict {
		p.unusable = true
		slog.Warn("placement: loaded record with no backend or attempt",
			"lease_uuid", leaseUUID)
	}
	return p
}

func unusableRecord(leaseUUID string, err error) Placement {
	slog.Warn("placement: loaded unparseable record",
		"lease_uuid", leaseUUID, "error", err)
	return Placement{unusable: true}
}

func encodePlacement(p Placement) ([]byte, error) {
	return json.Marshal(record{
		Backend:               p.Backend,
		Attempt:               p.Attempt,
		SetAt:                 p.SetAt,
		Revision:              p.revision,
		Conflict:              p.Conflict,
		ConflictBackends:      normalizeBackendNames(p.ConflictBackends),
		ConflictOwnersUnknown: p.ConflictOwnersUnknown,
	})
}

func normalizeBackendNames(names []string) []string {
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name != "" {
			unique[name] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(unique))
}

// Lookup returns an immutable placement snapshot. A missing key returns the
// zero Placement (StateAbsent).
func (s *Store) Lookup(leaseUUID string) Placement {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := s.cache[leaseUUID]
	p.ConflictBackends = slices.Clone(p.ConflictBackends)
	return p
}

// List returns a point-in-time copy of every cached placement, including
// StateUnusable records. Mutating the returned map cannot affect the store.
func (s *Store) List() map[string]Placement {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := maps.Clone(s.cache)
	for leaseUUID, p := range out {
		p.ConflictBackends = slices.Clone(p.ConflictBackends)
		out[leaseUUID] = p
	}
	return out
}

// SnapshotRevision returns the store's current global revision. Records written
// after this call receive a greater revision, allowing a fleet inventory sweep
// to reject placement decisions newer than the inventory it fetched.
func (s *Store) SnapshotRevision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Get returns only a confirmed backend. Deprecated: use Lookup.
func (s *Store) Get(leaseUUID string) string {
	p := s.Lookup(leaseUUID)
	if p.State() != StateConfirmed {
		return ""
	}
	return p.Backend
}

// SetAt returns the snapshot's first-seen time and whether a record exists.
// Deprecated: use Lookup.
func (s *Store) SetAt(leaseUUID string) (time.Time, bool) {
	p := s.Lookup(leaseUUID)
	return p.SetAt, p.State() != StateAbsent
}

// Set records a positive backend observation with the legacy overwrite
// semantics. Deprecated: use Confirm or SetBatch as appropriate.
func (s *Store) Set(leaseUUID, backendName string) error {
	return s.SetBatch(map[string]string{leaseUUID: backendName})
}

// SetAttempting durably records the target before any backend call. It refuses
// to overwrite every unresolved attempt, including one for the same target.
func (s *Store) SetAttempting(leaseUUID, backendName string) error {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.cache[leaseUUID]
	if exists && existing.State() == StateUnusable {
		return fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if existing.Attempt != "" {
		return fmt.Errorf("%w: lease %q targets %q", ErrAttemptConflict, leaseUUID, existing.Attempt)
	}
	if existing.Backend != "" && existing.Backend != backendName {
		return fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, existing.Backend, backendName)
	}

	revision, err := s.nextRevision()
	if err != nil {
		return err
	}
	p := existing
	if !exists {
		p.SetAt = s.now().UTC()
	}
	p.Attempt = backendName
	p.revision = revision
	if err := s.put(leaseUUID, p, "set attempting placement"); err != nil {
		return err
	}
	s.revision = revision
	return nil
}

// Confirm records a positive observation. It promotes and clears a matching
// attempt, creates a confirmed record when absent, and is idempotent for an
// already-confirmed target with no attempt.
func (s *Store) Confirm(leaseUUID, backendName string) error {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	p, exists := s.cache[leaseUUID]
	if exists && p.State() == StateUnusable {
		return fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Attempt != "" && p.Attempt != backendName {
		return fmt.Errorf("%w: lease %q targets %q, not %q",
			ErrAttemptMismatch, leaseUUID, p.Attempt, backendName)
	}
	if p.Backend != "" && p.Backend != backendName {
		return fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, p.Backend, backendName)
	}
	if p.Backend == backendName && p.Attempt == "" {
		return nil
	}

	revision, err := s.nextRevision()
	if err != nil {
		return err
	}
	if !exists {
		p.SetAt = s.now().UTC()
	}
	p.Backend = backendName
	p.Attempt = ""
	p.revision = revision
	if err := s.put(leaseUUID, p, "confirm placement"); err != nil {
		return err
	}
	s.revision = revision
	return nil
}

// ConfirmAttemptIfRevision promotes only the exact write-ahead attempt observed
// by its caller. It returns false without writing when a callback, inventory
// sync, or terminal cleanup already changed/deleted the record. This prevents a
// synchronous 202 response that arrives after a fast callback from recreating
// stale confirmed ownership.
func (s *Store) ConfirmAttemptIfRevision(
	leaseUUID, backendName string,
	revision uint64,
) (bool, error) {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	p, exists := s.cache[leaseUUID]
	if !exists || p.revision != revision {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Attempt != backendName {
		return false, nil
	}
	if p.Backend != "" && p.Backend != backendName {
		return false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, p.Backend, backendName)
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Backend = backendName
	p.Attempt = ""
	p.revision = next
	if err := s.put(leaseUUID, p, "conditionally confirm placement attempt"); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// ClearAttempt clears only the named unresolved target. An absent placement or
// an already-confirmed target with no attempt is an idempotent no-op.
func (s *Store) ClearAttempt(leaseUUID, backendName string) error {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.clearAttemptLocked(leaseUUID, backendName, nil)
	return err
}

// ClearAttemptIfRevision is ClearAttempt with an exact per-record CAS. It
// returns false without writing when the record is absent or its revision is
// stale. A matching attempt-only record is deleted; a confirmed record keeps
// Backend and clears only Attempt.
func (s *Store) ClearAttemptIfRevision(
	leaseUUID, backendName string,
	revision uint64,
) (bool, error) {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clearAttemptLocked(leaseUUID, backendName, &revision)
}

func (s *Store) clearAttemptLocked(
	leaseUUID, backendName string,
	expectedRevision *uint64,
) (bool, error) {
	p, exists := s.cache[leaseUUID]
	if !exists {
		return false, nil
	}
	if expectedRevision != nil && p.revision != *expectedRevision {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Attempt == "" {
		if p.Backend == backendName {
			return false, nil
		}
		return false, fmt.Errorf("%w: lease %q has no attempt for %q",
			ErrAttemptMismatch, leaseUUID, backendName)
	}
	if p.Attempt != backendName {
		return false, fmt.Errorf("%w: lease %q targets %q, not %q",
			ErrAttemptMismatch, leaseUUID, p.Attempt, backendName)
	}

	if p.Backend == "" {
		if err := s.deleteLocked(leaseUUID, "clear placement attempt"); err != nil {
			return false, err
		}
		return true, nil
	}

	revision, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Attempt = ""
	p.revision = revision
	if err := s.put(leaseUUID, p, "clear placement attempt"); err != nil {
		return false, err
	}
	s.revision = revision
	return true, nil
}

// Delete removes any placement record after the durable delete commits.
func (s *Store) Delete(leaseUUID string) error {
	if leaseUUID == "" {
		return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.cache[leaseUUID]; !exists {
		return nil
	}
	if err := s.deleteLocked(leaseUUID, "delete placement"); err != nil {
		return err
	}
	return nil
}

// DeleteIfRevision deletes a placement only when its current per-record
// revision matches the supplied snapshot revision.
func (s *Store) DeleteIfRevision(leaseUUID string, revision uint64) (bool, error) {
	if leaseUUID == "" {
		return false, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.cache[leaseUUID]
	if !exists || p.revision != revision {
		return false, nil
	}
	if err := s.deleteLocked(leaseUUID, "delete placement conditionally"); err != nil {
		return false, err
	}
	return true, nil
}

// SetBatch records positive backend inventory without a generation cutoff. It
// is retained for compatibility helpers and tests; reconciliation must use
// SetBatchIfNotNewer so an inventory fetched before a concurrent attempt cannot
// overwrite or confirm that newer record.
func (s *Store) SetBatch(placements map[string]string) error {
	return s.SetBatchIfNotNewer(placements, math.MaxUint64)
}

// SetBatchIfNotNewer records positive backend inventory in one bbolt
// transaction, but only for records whose revision is no newer than
// maxRevision. It preserves SetAt for existing usable records, repairs unusable
// records, and resolves Attempt only when the reporting backend matches it. A
// mismatched Attempt is retained as unresolved evidence rather than silently
// discarded.
//
// The generation cutoff is the write-side half of the reconciler's inventory
// snapshot: a SetAttempting/Confirm that raced the fetch must win over its stale
// result and remain for a later sweep.
func (s *Store) SetBatchIfNotNewer(placements map[string]string, maxRevision uint64) error {
	for leaseUUID, backendName := range placements {
		if err := validateIDs(leaseUUID, backendName); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// An empty backend inventory is still the durable synchronization point
	// that lets the reconciler trust record absence. Do not report success
	// without touching bbolt: a closed or otherwise unavailable store must keep
	// the process-wide trust latch disarmed.
	if len(placements) == 0 {
		if err := s.db.View(func(tx *bolt.Tx) error {
			if tx.Bucket(bucketName) == nil {
				return errors.New("placements bucket missing")
			}
			return nil
		}); err != nil {
			return mutationFailure("verify empty placement sync", err)
		}
		return nil
	}

	keys := slices.Sorted(maps.Keys(placements))
	eligible := keys[:0]
	for _, leaseUUID := range keys {
		if s.lastMutation[leaseUUID] > maxRevision {
			continue
		}
		eligible = append(eligible, leaseUUID)
	}
	keys = eligible
	if len(keys) == 0 {
		// Every reported record was newer than the inventory. This is a valid,
		// conservative sync, but still verify the store before allowing callers to
		// treat it as the process's durable synchronization point.
		if err := s.db.View(func(tx *bolt.Tx) error {
			if tx.Bucket(bucketName) == nil {
				return errors.New("placements bucket missing")
			}
			return nil
		}); err != nil {
			return mutationFailure("verify generation-filtered placement sync", err)
		}
		return nil
	}
	now := s.now().UTC()
	nextRevision := s.revision
	merged := make(map[string]Placement, len(placements))
	encoded := make(map[string][]byte, len(placements))
	for _, leaseUUID := range keys {
		if nextRevision == math.MaxUint64 {
			return fmt.Errorf("placement revision exhausted")
		}
		nextRevision++

		backendName := placements[leaseUUID]
		p, exists := s.cache[leaseUUID]
		if !exists || p.State() == StateUnusable {
			p = Placement{SetAt: now}
		}
		p.Backend = backendName
		if p.Attempt == backendName {
			p.Attempt = ""
		}
		p.unusable = false
		p.revision = nextRevision
		enc, err := encodePlacement(p)
		if err != nil {
			return mutationFailure("encode batch placements", err)
		}
		merged[leaseUUID] = p
		encoded[leaseUUID] = enc
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, leaseUUID := range keys {
			if err := b.Put([]byte(leaseUUID), encoded[leaseUUID]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return mutationFailure("set batch placements", err)
	}

	maps.Copy(s.cache, merged)
	for leaseUUID, p := range merged {
		s.lastMutation[leaseUUID] = p.revision
	}
	s.revision = nextRevision
	return nil
}

// SetConflictsIfNotNewer durably quarantines leases positively reported by
// multiple backends. It replaces any individual Backend/Attempt as the selected
// owner, but preserves those names together with every reporting backend in a
// durable candidate set. No individual status may drive chain actions while the
// conflict remains, yet deprovision and later reconciliation can still account
// for every backend that was ever positively identified.
func (s *Store) SetConflictsIfNotNewer(conflicts map[string][]string, maxRevision uint64) error {
	for leaseUUID, backendNames := range conflicts {
		if leaseUUID == "" {
			return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
		if len(normalizeBackendNames(backendNames)) < 2 {
			return fmt.Errorf("%w: conflict for lease %q requires at least two backends",
				ErrInvalidPlacement, leaseUUID)
		}
	}
	if len(conflicts) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	keys := slices.Sorted(maps.Keys(conflicts))
	eligible := keys[:0]
	for _, leaseUUID := range keys {
		if s.lastMutation[leaseUUID] <= maxRevision {
			eligible = append(eligible, leaseUUID)
		}
	}
	keys = eligible
	if len(keys) == 0 {
		return nil
	}

	now := s.now().UTC()
	nextRevision := s.revision
	quarantined := make(map[string]Placement, len(keys))
	encoded := make(map[string][]byte, len(keys))
	for _, leaseUUID := range keys {
		if nextRevision == math.MaxUint64 {
			return fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		existing, exists := s.cache[leaseUUID]
		setAt := existing.SetAt
		if setAt.IsZero() {
			setAt = now
		}
		candidateSet := make(map[string]struct{}, len(conflicts[leaseUUID])+len(existing.ConflictBackends)+2)
		for _, backendName := range conflicts[leaseUUID] {
			if backendName != "" {
				candidateSet[backendName] = struct{}{}
			}
		}
		for _, backendName := range existing.ConflictBackends {
			candidateSet[backendName] = struct{}{}
		}
		if existing.Backend != "" {
			candidateSet[existing.Backend] = struct{}{}
		}
		if existing.Attempt != "" {
			candidateSet[existing.Attempt] = struct{}{}
		}
		unknownOwners := existing.ConflictOwnersUnknown ||
			(existing.Conflict && len(existing.ConflictBackends) < 2) ||
			(exists && existing.State() == StateUnusable && !existing.Conflict)
		p := Placement{
			SetAt:                 setAt,
			Conflict:              true,
			ConflictBackends:      slices.Sorted(maps.Keys(candidateSet)),
			ConflictOwnersUnknown: unknownOwners,
			revision:              nextRevision,
		}
		enc, err := encodePlacement(p)
		if err != nil {
			return mutationFailure("encode placement conflicts", err)
		}
		quarantined[leaseUUID] = p
		encoded[leaseUUID] = enc
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, leaseUUID := range keys {
			if err := b.Put([]byte(leaseUUID), encoded[leaseUUID]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return mutationFailure("set placement conflicts", err)
	}

	maps.Copy(s.cache, quarantined)
	for leaseUUID, p := range quarantined {
		s.lastMutation[leaseUUID] = p.revision
	}
	s.revision = nextRevision
	return nil
}

// ClearConflictsIfNotNewer removes conflict markers only after a complete
// inventory reports no owner. Unique owners are repaired by SetBatchIfNotNewer;
// this method handles the complete-absence case.
func (s *Store) ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error {
	for leaseUUID := range leases {
		if leaseUUID == "" {
			return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
	}
	if len(leases) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	keys := slices.Sorted(maps.Keys(leases))
	eligible := keys[:0]
	for _, leaseUUID := range keys {
		p, exists := s.cache[leaseUUID]
		if exists && p.Conflict && s.lastMutation[leaseUUID] <= maxRevision {
			eligible = append(eligible, leaseUUID)
		}
	}
	keys = eligible
	if len(keys) == 0 {
		return nil
	}

	nextRevision := s.revision
	tombstones := make(map[string]uint64, len(keys))
	for _, leaseUUID := range keys {
		if nextRevision == math.MaxUint64 {
			return fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		tombstones[leaseUUID] = nextRevision
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, leaseUUID := range keys {
			if err := b.Delete([]byte(leaseUUID)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return mutationFailure("clear placement conflicts", err)
	}
	for _, leaseUUID := range keys {
		delete(s.cache, leaseUUID)
		s.lastMutation[leaseUUID] = tombstones[leaseUUID]
	}
	s.revision = nextRevision
	return nil
}

// Count returns the number of durable keys represented in the cache, including
// StateUnusable records.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cache)
}

// Healthy checks that the bbolt database and placement bucket are accessible.
func (s *Store) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		return nil
	})
}

// Close closes the bbolt database. It is safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func validateIDs(leaseUUID, backendName string) error {
	if leaseUUID == "" {
		return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}
	if backendName == "" {
		return fmt.Errorf("%w: backend name is required", ErrInvalidPlacement)
	}
	return nil
}

func (s *Store) nextRevision() (uint64, error) {
	if s.revision == math.MaxUint64 {
		return 0, fmt.Errorf("placement revision exhausted")
	}
	return s.revision + 1, nil
}

// put writes one record to bbolt and updates the cache only after commit.
// Caller holds s.mu.
func (s *Store) put(leaseUUID string, p Placement, operation string) error {
	enc, err := encodePlacement(p)
	if err != nil {
		return mutationFailure("encode placement for "+operation, err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(leaseUUID), enc)
	}); err != nil {
		return mutationFailure(operation, err)
	}
	s.cache[leaseUUID] = p
	s.lastMutation[leaseUUID] = p.revision
	return nil
}

// deleteLocked durably removes one key and advances its process-local mutation
// generation. The tombstone is intentionally in memory only: no inventory
// snapshot survives a process restart, while retaining it in-process prevents
// an older snapshot from recreating a record deleted by a concurrent callback.
// Caller holds s.mu.
func (s *Store) deleteLocked(leaseUUID, operation string) error {
	revision, err := s.nextRevision()
	if err != nil {
		return err
	}
	if err := s.deleteDurable(leaseUUID, operation); err != nil {
		return err
	}
	delete(s.cache, leaseUUID)
	s.lastMutation[leaseUUID] = revision
	s.revision = revision
	return nil
}

// deleteDurable deletes one bbolt key without touching the cache. Caller holds
// s.mu and performs the cache delete after this succeeds.
func (s *Store) deleteDurable(leaseUUID, operation string) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(leaseUUID))
	}); err != nil {
		return mutationFailure(operation, err)
	}
	return nil
}

func mutationFailure(operation string, err error) error {
	metrics.PlacementWriteFailuresTotal.Inc()
	return fmt.Errorf("failed to %s: %w", operation, err)
}
