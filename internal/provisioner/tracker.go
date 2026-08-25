package provisioner

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// ProvisionKind distinguishes a fresh provision from a restore. A restore IS a
// provisioning operation — it brings a lease's deployment up from retained
// volumes instead of a fresh manifest — so it shares the provisioning metrics
// and is differentiated by an operation label rather than a separate metric,
// per Prometheus naming guidance (sum/avg across the dimension stays meaningful).
// ENG-358 tracks restores in-flight so their callback is acknowledged inline;
// the Kind keeps restore latency/outcomes labeled distinctly from fresh
// provisions (which ENG-357 deliberately kept separable).
type ProvisionKind uint8

const (
	KindProvision ProvisionKind = iota // zero value: a fresh provision
	KindRestore                        // a restore from retained volumes (ENG-358)
)

type inFlightSettlementOwner uint8

const (
	inFlightSettlementUnclaimed inFlightSettlementOwner = iota
	inFlightSettlementTerminal
	inFlightSettlementDeprovision
)

// operationLabel maps the kind to the Prometheus `operation` label value used on
// provisioning_total / provisioning_duration_seconds.
func (k ProvisionKind) operationLabel() string {
	if k == KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}

// InFlightProvision represents a lease that is currently being provisioned.
type InFlightProvision struct {
	LeaseUUID  string
	Tenant     string
	Items      []backend.LeaseItem // All items being provisioned
	Backend    string
	Generation uint64 // Operation identity for conditional cleanup (ENG-632)
	// GenerationRequired is set by token-aware production entry points. Their
	// callbacks must echo Generation through the HMAC-authenticated callback URL;
	// legacy/test registrations leave it false for rolling compatibility.
	GenerationRequired bool
	StartTime          time.Time               // For duration metrics
	Kind               ProvisionKind           // Provision vs restore — labels callback metrics (ENG-358)
	settlementClaimed  bool                    // serializes callback/timeout/deprovision terminal work
	settlementOwner    inFlightSettlementOwner // distinguishes close RPC ownership from callback/timeout ownership
}

// RoutingSKU returns the first SKU for backend routing decisions.
//
// Used during deprovision when we need to determine which backend handled
// a lease but only have the in-flight tracking data. Since all items in a
// lease belong to the same provider, any SKU works for routing.
//
// This should NOT be used for resource calculations - use Items directly.
func (p InFlightProvision) RoutingSKU() string {
	if len(p.Items) == 0 {
		return ""
	}
	return p.Items[0].SKU
}

// InFlightTracker defines the interface for tracking in-flight provisions.
// This is used by handlers, orchestrator, timeout checker, and reconciler.
type InFlightTracker interface {
	// TryTrackInFlightWithGeneration atomically tracks an absent lease and returns
	// its operation token. The token lets the initiating call clean up only its own
	// tracker entry when a fast callback has already allowed a replacement to start.
	TryTrackInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool)

	// SnapshotMutationRevision captures the operation boundary used by one
	// reconciliation sweep.
	SnapshotMutationRevision() uint64

	// TryTrackInFlightWithGenerationIfNotNewer tracks only when this lease has
	// not started or finished an operation after maxRevision. The third return
	// value distinguishes a stale snapshot from an ordinary live-entry conflict.
	TryTrackInFlightWithGenerationIfNotNewer(
		leaseUUID, tenant string,
		items []backend.LeaseItem,
		backendName string,
		maxRevision uint64,
	) (generation uint64, tracked, snapshotStale bool)

	// TryClaimLeaseActionIfNotNewer fences every non-overlapping lifecycle
	// action for one lease. Ordinary event/restore tracking refuses the lease
	// until ReleaseLeaseAction, while conditional reconciler tracking
	// atomically adds an in-flight operation without dropping the claim. Keeping
	// both guards closes the preflight-failure gap until the worker completes its
	// terminal chain decision.
	TryClaimLeaseActionIfNotNewer(leaseUUID string, maxRevision uint64) (claimed, snapshotStale bool)
	TryClaimLeaseAction(leaseUUID string) bool
	ReleaseLeaseAction(leaseUUID string) bool

	// TryTrackRestoreInFlightWithGeneration is the restore variant. It marks the
	// entry KindRestore and returns the operation token that the backend must echo.
	TryTrackRestoreInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool)

	// UntrackInFlightIfGeneration removes the entry only when it still belongs to
	// the named operation.
	UntrackInFlightIfGeneration(leaseUUID string, generation uint64) bool
	// Terminal settlement can contain slow chain and storage calls. A claim
	// prevents timeout/callback actors from replacing the generation mid-flight.
	TryClaimInFlight(leaseUUID string, generation uint64) (InFlightProvision, bool)
	// TryClaimInFlightForDeprovision marks the exact claim as close-owned. Backend
	// callbacks can then report status without waiting for or consuming a claim
	// that the close path must retain across all candidate RPCs.
	TryClaimInFlightForDeprovision(leaseUUID string, generation uint64) (InFlightProvision, bool)
	ReleaseInFlightClaim(leaseUUID string, generation uint64) bool
	FinishClaimedInFlight(leaseUUID string, generation uint64) bool

	// GetInFlight returns the in-flight provision info without removing it.
	// Returns the provision info and true if found, or zero value and false if not found.
	GetInFlight(leaseUUID string) (InFlightProvision, bool)

	// IsInFlight checks if a lease is currently being provisioned.
	IsInFlight(leaseUUID string) bool

	// InFlightCount returns the number of provisions currently in flight.
	InFlightCount() int

	// InFlightCountsByBackend returns a snapshot of the number of in-flight
	// provisions per backend name. Used by the router's burst guard to spread
	// concurrent provisions that observe an identical backend load snapshot.
	InFlightCountsByBackend() map[string]int

	// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
	GetInFlightLeases() []string

	// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
	// Returns the number of provisions that were still in-flight when the timeout expired.
	WaitForDrain(ctx context.Context, timeout time.Duration) int

	// GetTimedOutProvisions returns provisions that have exceeded the given timeout.
	GetTimedOutProvisions(timeout time.Duration) []InFlightProvision
}

// ReconcilerTracker extends InFlightTracker with payload-related methods
// needed by the reconciler for coordinating with the event-driven path.
type ReconcilerTracker interface {
	InFlightTracker

	// HasPayload checks if a payload exists for a lease.
	// Returns an error if the underlying store read fails.
	HasPayload(leaseUUID string) (bool, error)

	// PayloadStore returns the payload store for direct access.
	// May return nil if payload store is not configured.
	PayloadStore() *payload.Store
}

// DefaultInFlightTracker is the default implementation of InFlightTracker.
// It uses a sync.RWMutex for thread-safe tracking of in-flight provisions.
type DefaultInFlightTracker struct {
	inFlight             map[string]InFlightProvision
	nextGeneration       uint64
	mutationRevision     uint64
	lastMutation         map[string]uint64
	lastSnapshotRevision uint64
	reconcileClaims      map[string]struct{}
	mu                   sync.RWMutex
}

// NewInFlightTracker creates a new DefaultInFlightTracker.
func NewInFlightTracker() *DefaultInFlightTracker {
	return &DefaultInFlightTracker{
		inFlight:        make(map[string]InFlightProvision),
		nextGeneration:  randomGenerationSeed(),
		lastMutation:    make(map[string]uint64),
		reconcileClaims: make(map[string]struct{}),
	}
}

func randomGenerationSeed() uint64 {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err == nil {
		if seed := binary.BigEndian.Uint64(raw[:]); seed != 0 {
			return seed
		}
	}
	// crypto/rand failure is exceptionally unlikely. The wall clock still keeps
	// generation reuse across process restarts far less likely than restarting at
	// one, while allocateGenerationLocked preserves the non-zero invariant.
	return uint64(time.Now().UnixNano())
}

// Compile-time check that DefaultInFlightTracker implements InFlightTracker.
var _ InFlightTracker = (*DefaultInFlightTracker)(nil)

// TryTrackInFlightWithGeneration atomically tracks a provision and returns its
// operation generation.
func (t *DefaultInFlightTracker) TryTrackInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool) {
	return t.tryTrack(leaseUUID, tenant, items, backendName, KindProvision, true)
}

func (t *DefaultInFlightTracker) TryTrackInFlightWithGenerationIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	maxRevision uint64,
) (uint64, bool, bool) {
	return t.tryTrackIfNotNewer(
		leaseUUID, tenant, items, backendName, KindProvision, true, maxRevision, true,
	)
}

// TryTrackRestoreInFlightWithGeneration atomically tracks a restore and returns
// its operation generation.
func (t *DefaultInFlightTracker) TryTrackRestoreInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool) {
	return t.tryTrack(leaseUUID, tenant, items, backendName, KindRestore, true)
}

// tryTrack is the shared atomic track-if-absent implementation.
func (t *DefaultInFlightTracker) tryTrack(leaseUUID, tenant string, items []backend.LeaseItem, backendName string, kind ProvisionKind, generationRequired bool) (uint64, bool) {
	generation, tracked, _ := t.tryTrackIfNotNewer(
		leaseUUID, tenant, items, backendName, kind, generationRequired, ^uint64(0), false,
	)
	return generation, tracked
}

func (t *DefaultInFlightTracker) tryTrackIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	kind ProvisionKind,
	generationRequired bool,
	maxRevision uint64,
	requireReconcileClaim bool,
) (uint64, bool, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if requireReconcileClaim && t.lastMutation[leaseUUID] > maxRevision {
		return 0, false, true
	}
	_, reconcileClaimed := t.reconcileClaims[leaseUUID]
	if requireReconcileClaim {
		if !reconcileClaimed {
			return 0, false, false
		}
	} else if reconcileClaimed {
		return 0, false, false
	}
	if _, exists := t.inFlight[leaseUUID]; exists {
		return 0, false, false
	}
	generation := t.allocateGenerationLocked()
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:          leaseUUID,
		Tenant:             tenant,
		Items:              items,
		Backend:            backendName,
		Generation:         generation,
		GenerationRequired: generationRequired,
		StartTime:          time.Now(),
		Kind:               kind,
	}
	t.markMutationLocked(leaseUUID)
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
	return generation, true, false
}

func (t *DefaultInFlightTracker) TryClaimLeaseActionIfNotNewer(
	leaseUUID string,
	maxRevision uint64,
) (bool, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastMutation[leaseUUID] > maxRevision {
		return false, true
	}
	if _, exists := t.inFlight[leaseUUID]; exists {
		return false, false
	}
	if _, exists := t.reconcileClaims[leaseUUID]; exists {
		return false, false
	}
	t.reconcileClaims[leaseUUID] = struct{}{}
	return true, false
}

// TryClaimLeaseAction is the event-path variant used by deprovision. Claiming
// marks a mutation so a reconciliation snapshot that predates even a complete
// claim/release interval will defer the lease.
func (t *DefaultInFlightTracker) TryClaimLeaseAction(leaseUUID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.inFlight[leaseUUID]; exists {
		return false
	}
	if _, exists := t.reconcileClaims[leaseUUID]; exists {
		return false
	}
	t.reconcileClaims[leaseUUID] = struct{}{}
	t.markMutationLocked(leaseUUID)
	return true
}

func (t *DefaultInFlightTracker) ReleaseLeaseAction(leaseUUID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.reconcileClaims[leaseUUID]; !exists {
		return false
	}
	delete(t.reconcileClaims, leaseUUID)
	t.markMutationLocked(leaseUUID)
	return true
}

// SnapshotMutationRevision returns a causal boundary for per-lease conditional
// tracking. ReconcileAll is single-flight, so mutations older than the prior
// boundary can be discarded here; the map remains bounded to leases touched
// between adjacent sweeps without weakening an active sweep.
func (t *DefaultInFlightTracker) SnapshotMutationRevision() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	for leaseUUID, revision := range t.lastMutation {
		if revision <= t.lastSnapshotRevision {
			delete(t.lastMutation, leaseUUID)
		}
	}
	t.lastSnapshotRevision = t.mutationRevision
	return t.mutationRevision
}

func (t *DefaultInFlightTracker) markMutationLocked(leaseUUID string) {
	t.mutationRevision++
	if t.mutationRevision == 0 {
		t.mutationRevision++
	}
	t.lastMutation[leaseUUID] = t.mutationRevision
}

func (t *DefaultInFlightTracker) allocateGenerationLocked() uint64 {
	t.nextGeneration++
	if t.nextGeneration == 0 {
		t.nextGeneration++
	}
	return t.nextGeneration
}

// UntrackInFlightIfGeneration conditionally removes one operation without
// disturbing a newer replacement for the same lease.
func (t *DefaultInFlightTracker) UntrackInFlightIfGeneration(leaseUUID string, generation uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, exists := t.inFlight[leaseUUID]
	if !exists || p.Generation != generation || p.settlementClaimed {
		return false
	}
	delete(t.inFlight, leaseUUID)
	t.markMutationLocked(leaseUUID)
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
	return true
}

// TryClaimInFlight atomically claims one generation for terminal settlement.
func (t *DefaultInFlightTracker) TryClaimInFlight(leaseUUID string, generation uint64) (InFlightProvision, bool) {
	return t.tryClaimInFlight(leaseUUID, generation, inFlightSettlementTerminal)
}

func (t *DefaultInFlightTracker) TryClaimInFlightForDeprovision(
	leaseUUID string,
	generation uint64,
) (InFlightProvision, bool) {
	return t.tryClaimInFlight(leaseUUID, generation, inFlightSettlementDeprovision)
}

func (t *DefaultInFlightTracker) tryClaimInFlight(
	leaseUUID string,
	generation uint64,
	owner inFlightSettlementOwner,
) (InFlightProvision, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, exists := t.inFlight[leaseUUID]
	if !exists || p.Generation != generation || p.settlementClaimed {
		return InFlightProvision{}, false
	}
	p.settlementClaimed = true
	p.settlementOwner = owner
	t.inFlight[leaseUUID] = p
	return p, true
}

// ReleaseInFlightClaim returns a generation to retryable in-flight state.
func (t *DefaultInFlightTracker) ReleaseInFlightClaim(leaseUUID string, generation uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, exists := t.inFlight[leaseUUID]
	if !exists || p.Generation != generation || !p.settlementClaimed {
		return false
	}
	p.settlementClaimed = false
	p.settlementOwner = inFlightSettlementUnclaimed
	t.inFlight[leaseUUID] = p
	return true
}

// FinishClaimedInFlight removes a generation only for its settlement owner.
func (t *DefaultInFlightTracker) FinishClaimedInFlight(leaseUUID string, generation uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, exists := t.inFlight[leaseUUID]
	if !exists || p.Generation != generation || !p.settlementClaimed {
		return false
	}
	delete(t.inFlight, leaseUUID)
	t.markMutationLocked(leaseUUID)
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
	return true
}

// IsInFlight checks if a lease is currently being provisioned.
func (t *DefaultInFlightTracker) IsInFlight(leaseUUID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.inFlight[leaseUUID]
	return exists
}

// GetInFlight returns the in-flight provision info without removing it.
func (t *DefaultInFlightTracker) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	provision, exists := t.inFlight[leaseUUID]
	return provision, exists
}

// InFlightCount returns the number of provisions currently in flight.
func (t *DefaultInFlightTracker) InFlightCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.inFlight)
}

// InFlightCountsByBackend returns a snapshot of in-flight provision counts keyed
// by backend name.
func (t *DefaultInFlightTracker) InFlightCountsByBackend() map[string]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	counts := make(map[string]int, len(t.inFlight))
	for _, p := range t.inFlight {
		counts[p.Backend]++
	}
	return counts
}

// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
func (t *DefaultInFlightTracker) GetInFlightLeases() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return slices.Collect(maps.Keys(t.inFlight))
}

// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
// Returns the number of provisions that were still in-flight when the timeout expired.
func (t *DefaultInFlightTracker) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if t.InFlightCount() == 0 {
		return 0
	}

	slog.Info("waiting for in-flight provisions to drain",
		"count", t.InFlightCount(),
		"timeout", timeout,
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			remaining := t.InFlightCount()
			if remaining > 0 {
				slog.Warn("drain interrupted by context cancellation",
					"remaining", remaining,
					"leases", t.GetInFlightLeases(),
				)
			}
			return remaining

		case <-ticker.C:
			count := t.InFlightCount()
			if count == 0 {
				slog.Info("all in-flight provisions drained successfully")
				return 0
			}

			if time.Now().After(deadline) {
				slog.Warn("drain timeout expired with provisions still in-flight",
					"remaining", count,
					"leases", t.GetInFlightLeases(),
				)
				return count
			}

			slog.Debug("waiting for provisions to drain",
				"remaining", count,
				"time_left", time.Until(deadline).Round(time.Second),
			)
		}
	}
}

// GetTimedOutProvisions returns provisions that have exceeded the given timeout.
func (t *DefaultInFlightTracker) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	now := time.Now()
	var timedOut []InFlightProvision

	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, p := range t.inFlight {
		if now.Sub(p.StartTime) > timeout {
			timedOut = append(timedOut, p)
		}
	}
	return timedOut
}
