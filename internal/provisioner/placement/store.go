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
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
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
	db    *bolt.DB
	cache map[string]Placement
	// deleteRevisions fences stale inventory from recreating an exact key that
	// was deleted after its snapshot began. Entries exist only while at least one
	// registered inventory snapshot could still need them, so unrelated keys do
	// not share a global fence and tombstones do not grow for the process lifetime.
	deleteRevisions map[string]uint64
	// activeSnapshots counts registered inventory cutoffs. More than one caller
	// may begin at the same revision, so a refcount is required before tombstones
	// at that cutoff can be pruned.
	activeSnapshots map[uint64]uint64
	now             func() time.Time
	revision        uint64
	mu              sync.RWMutex
	closeOnce       sync.Once
	closeErr        error
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
	var revision uint64
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.ForEach(func(k, v []byte) error {
			p := decodeRecord(string(k), v)
			cache[string(k)] = p
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
		db:              db,
		cache:           cache,
		deleteRevisions: make(map[string]uint64),
		activeSnapshots: make(map[uint64]uint64),
		now:             time.Now,
		revision:        revision,
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
		if !validLegacyBackendName(v) {
			return unusableRecord(leaseUUID, errors.New("legacy backend name is not printable UTF-8"))
		}
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

func validLegacyBackendName(v []byte) bool {
	if !utf8.Valid(v) {
		return false
	}
	for _, r := range string(v) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
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

func equalPlacementIgnoringRevision(a, b Placement) bool {
	return a.Backend == b.Backend &&
		a.Attempt == b.Attempt &&
		a.SetAt.Equal(b.SetAt) &&
		a.Conflict == b.Conflict &&
		slices.Equal(a.ConflictBackends, b.ConflictBackends) &&
		a.ConflictOwnersUnknown == b.ConflictOwnersUnknown &&
		a.unusable == b.unusable
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

// SnapshotRevision returns the store's current global revision for immediate
// identity/CAS checks on present records. It does not register deletion fences;
// callers that will fetch external inventory or conditionally recreate absent
// keys must keep that work inside BeginInventorySnapshot/EndInventorySnapshot.
func (s *Store) SnapshotRevision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// BeginInventorySnapshot registers the current revision as the causal cutoff
// for one fleet inventory. Deletions committed while that snapshot is active
// retain exact-key tombstones until EndInventorySnapshot releases the cutoff.
// Ordinary identity checks that cannot recreate an absent key may use
// SnapshotRevision instead.
func (s *Store) BeginInventorySnapshot() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	revision := s.revision
	s.activeSnapshots[revision]++
	return revision
}

// EndInventorySnapshot releases a cutoff returned by BeginInventorySnapshot
// and prunes deletion tombstones that no remaining inventory can predate.
func (s *Store) EndInventorySnapshot(revision uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := s.activeSnapshots[revision]
	switch count {
	case 0:
		return
	case 1:
		delete(s.activeSnapshots, revision)
	default:
		s.activeSnapshots[revision] = count - 1
	}
	s.pruneDeleteRevisionsLocked()
}

// SetAttempting durably records the target before any backend call. It refuses
// to overwrite every unresolved attempt, including one for the same target.
// On success it returns the exact revision committed with the attempt so the
// caller can conditionally settle that write without a racy follow-up Lookup.
func (s *Store) SetAttempting(leaseUUID, backendName string) (uint64, error) {
	revision, _, err := s.setAttemptingIfNotNewer(leaseUUID, backendName, math.MaxUint64)
	return revision, err
}

// SetAttemptingIfNotNewer is the reconciler's write-ahead fence. It records an
// attempt only when the lease has not changed since the inventory revision
// supplied by the caller. The revision check and attempt write share the store
// lock, so an operation or placement transition that raced the inventory wins
// before any backend side effect is sent.
func (s *Store) SetAttemptingIfNotNewer(
	leaseUUID, backendName string,
	maxRevision uint64,
) (uint64, bool, error) {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return 0, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setAttemptingIfNotNewerLocked(leaseUUID, backendName, maxRevision)
}

func (s *Store) setAttemptingIfNotNewer(
	leaseUUID, backendName string,
	maxRevision uint64,
) (uint64, bool, error) {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setAttemptingIfNotNewerLocked(leaseUUID, backendName, maxRevision)
}

// Caller holds s.mu.
func (s *Store) setAttemptingIfNotNewerLocked(
	leaseUUID, backendName string,
	maxRevision uint64,
) (uint64, bool, error) {
	if s.mutationRevisionLocked(leaseUUID) > maxRevision {
		return 0, false, nil
	}

	existing, exists := s.cache[leaseUUID]
	if exists && existing.State() == StateUnusable {
		return 0, false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if existing.Attempt != "" {
		return 0, false, fmt.Errorf("%w: lease %q targets %q", ErrAttemptConflict, leaseUUID, existing.Attempt)
	}
	if existing.Backend != "" && existing.Backend != backendName {
		return 0, false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, existing.Backend, backendName)
	}

	revision, err := s.nextRevision()
	if err != nil {
		return 0, false, err
	}
	p := existing
	if !exists {
		p.SetAt = s.now().UTC()
	}
	p.Attempt = backendName
	p.revision = revision
	if err := s.put(leaseUUID, p, "set attempting placement"); err != nil {
		return 0, false, err
	}
	s.revision = revision
	return revision, true, nil
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

// SetBatchIfNotNewer records positive backend inventory in one bbolt
// transaction, but only for records whose revision is no newer than
// maxRevision. It preserves SetAt for existing usable records, repairs unusable
// records, and resolves Attempt only when the reporting backend matches it. A
// mismatched Attempt is retained as unresolved evidence rather than silently
// discarded.
//
// The generation cutoff is the write-side half of the reconciler's inventory
// snapshot: a SetAttempting/Confirm that raced the fetch must win over its stale
// result and remain for a later sweep. The first returned map identifies the
// exact committed revision of every record this call changed. The second names
// observations rejected by the revision fence, which callers must keep as
// lease-local untrusted-absence exceptions. Semantic no-ops appear in neither.
func (s *Store) SetBatchIfNotNewer(
	placements map[string]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	for leaseUUID, backendName := range placements {
		if err := validateIDs(leaseUUID, backendName); err != nil {
			return nil, nil, err
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
			return nil, nil, mutationFailure("verify empty placement sync", err)
		}
		return nil, nil, nil
	}

	keys := slices.Sorted(maps.Keys(placements))
	eligible := keys[:0]
	fenced := make(map[string]struct{})
	for _, leaseUUID := range keys {
		if s.mutationRevisionLocked(leaseUUID) > maxRevision {
			fenced[leaseUUID] = struct{}{}
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
			return nil, fenced, mutationFailure("verify generation-filtered placement sync", err)
		}
		return nil, fenced, nil
	}
	now := s.now().UTC()
	nextRevision := s.revision
	merged := make(map[string]Placement, len(placements))
	encoded := make(map[string][]byte, len(placements))
	mutated := keys[:0]
	for _, leaseUUID := range keys {
		backendName := placements[leaseUUID]
		existing, exists := s.cache[leaseUUID]
		p := existing
		if !exists || p.State() == StateUnusable {
			p = Placement{SetAt: now}
		}
		p.Backend = backendName
		if p.Attempt == backendName {
			p.Attempt = ""
		}
		p.unusable = false
		if exists && equalPlacementIgnoringRevision(p, existing) {
			continue
		}
		if nextRevision == math.MaxUint64 {
			return nil, fenced, fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		p.revision = nextRevision
		enc, err := encodePlacement(p)
		if err != nil {
			return nil, fenced, mutationFailure("encode batch placements", err)
		}
		mutated = append(mutated, leaseUUID)
		merged[leaseUUID] = p
		encoded[leaseUUID] = enc
	}
	keys = mutated
	if len(keys) == 0 {
		// Exact confirmed inventory is idempotent. Verify bbolt even though no
		// revision needs to move: callers use successful sync as authority for
		// placement absence.
		if err := s.db.View(func(tx *bolt.Tx) error {
			if tx.Bucket(bucketName) == nil {
				return errors.New("placements bucket missing")
			}
			return nil
		}); err != nil {
			return nil, fenced, mutationFailure("verify idempotent placement sync", err)
		}
		return nil, fenced, nil
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
		return nil, fenced, mutationFailure("set batch placements", err)
	}

	maps.Copy(s.cache, merged)
	for leaseUUID := range merged {
		delete(s.deleteRevisions, leaseUUID)
	}
	s.revision = nextRevision
	applied := make(map[string]uint64, len(merged))
	for leaseUUID, p := range merged {
		applied[leaseUUID] = p.revision
	}
	return applied, fenced, nil
}

// SetConflictsIfNotNewer durably quarantines leases positively reported by
// multiple backends. It replaces any individual Backend/Attempt as the selected
// owner, but preserves those names together with every reporting backend in a
// durable candidate set. No individual status may drive chain actions while the
// conflict remains, yet deprovision and later reconciliation can still account
// for every backend that was ever positively identified. The first returned map
// contains exact revisions committed by this call. The returned set names
// conflicts rejected by the revision fence so callers can preserve lease-local
// untrusted-absence state instead of treating a filtered quarantine as durable.
func (s *Store) SetConflictsIfNotNewer(
	conflicts map[string][]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	for leaseUUID, backendNames := range conflicts {
		if leaseUUID == "" {
			return nil, nil, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
		if len(normalizeBackendNames(backendNames)) < 2 {
			return nil, nil, fmt.Errorf("%w: conflict for lease %q requires at least two backends",
				ErrInvalidPlacement, leaseUUID)
		}
	}
	if len(conflicts) == 0 {
		return nil, nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	keys := slices.Sorted(maps.Keys(conflicts))
	eligible := keys[:0]
	fenced := make(map[string]struct{})
	for _, leaseUUID := range keys {
		if s.mutationRevisionLocked(leaseUUID) <= maxRevision {
			eligible = append(eligible, leaseUUID)
		} else {
			fenced[leaseUUID] = struct{}{}
		}
	}
	keys = eligible
	if len(keys) == 0 {
		if err := s.verifyBucket(); err != nil {
			return nil, fenced, mutationFailure("verify generation-filtered placement conflict sync", err)
		}
		return nil, fenced, nil
	}

	now := s.now().UTC()
	nextRevision := s.revision
	quarantined := make(map[string]Placement, len(keys))
	encoded := make(map[string][]byte, len(keys))
	mutated := keys[:0]
	for _, leaseUUID := range keys {
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
		}
		if exists && equalPlacementIgnoringRevision(p, existing) {
			continue
		}
		if nextRevision == math.MaxUint64 {
			return nil, fenced, fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		p.revision = nextRevision
		enc, err := encodePlacement(p)
		if err != nil {
			return nil, fenced, mutationFailure("encode placement conflicts", err)
		}
		mutated = append(mutated, leaseUUID)
		quarantined[leaseUUID] = p
		encoded[leaseUUID] = enc
	}
	keys = mutated
	if len(keys) == 0 {
		if err := s.verifyBucket(); err != nil {
			return nil, fenced, mutationFailure("verify idempotent placement conflict sync", err)
		}
		return nil, fenced, nil
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
		return nil, fenced, mutationFailure("set placement conflicts", err)
	}

	maps.Copy(s.cache, quarantined)
	for leaseUUID := range quarantined {
		delete(s.deleteRevisions, leaseUUID)
	}
	s.revision = nextRevision
	applied := make(map[string]uint64, len(quarantined))
	for leaseUUID, p := range quarantined {
		applied[leaseUUID] = p.revision
	}
	return applied, fenced, nil
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
		if exists && p.Conflict && p.revision <= maxRevision {
			eligible = append(eligible, leaseUUID)
		}
	}
	keys = eligible
	if len(keys) == 0 {
		return nil
	}

	nextRevision := s.revision
	for range keys {
		if nextRevision == math.MaxUint64 {
			return fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
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
		if len(s.activeSnapshots) > 0 {
			s.deleteRevisions[leaseUUID] = nextRevision
		}
	}
	s.revision = nextRevision
	return nil
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

// mutationRevisionLocked returns the per-record revision for a present key or
// the exact key's deletion revision while an older registered inventory remains
// active. Unrelated deletions never fence this lease.
// Caller holds at least s.mu.RLock.
func (s *Store) mutationRevisionLocked(leaseUUID string) uint64 {
	if p, exists := s.cache[leaseUUID]; exists {
		return p.revision
	}
	return s.deleteRevisions[leaseUUID]
}

// verifyBucket proves the durable store is readable even when an idempotent or
// fully fenced synchronization has no mutation to commit.
// Caller holds at least s.mu.RLock.
func (s *Store) verifyBucket() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		return nil
	})
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
	delete(s.deleteRevisions, leaseUUID)
	return nil
}

// deleteLocked durably removes one key and retains its deletion revision only
// while a registered inventory snapshot could otherwise recreate it.
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
	if len(s.activeSnapshots) > 0 {
		s.deleteRevisions[leaseUUID] = revision
	}
	s.revision = revision
	return nil
}

// pruneDeleteRevisionsLocked drops tombstones that are no newer than every
// active inventory cutoff. Such inventories already observed the key absent.
// Caller holds s.mu.
func (s *Store) pruneDeleteRevisionsLocked() {
	if len(s.activeSnapshots) == 0 {
		clear(s.deleteRevisions)
		return
	}
	oldest := uint64(math.MaxUint64)
	for revision := range s.activeSnapshots {
		if revision < oldest {
			oldest = revision
		}
	}
	for leaseUUID, revision := range s.deleteRevisions {
		if revision <= oldest {
			delete(s.deleteRevisions, leaseUUID)
		}
	}
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
