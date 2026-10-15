package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics/background"
)

// A runtime refresh recreates every healthy lease whose containers were
// created under an older runtime contract, with the same release, volumes and
// lease, so it runs under the current one. An operator starts each run through
// the authenticated POST /runtime_refresh; nothing starts one automatically.
// Each lease is refreshed through the same journaled, restart-shaped
// maintenance a custom-domain change uses, under its own maintenance kind.

const (
	// runtimeRefreshRunTimeout bounds one run, including its retries of leases
	// that were busy.
	runtimeRefreshRunTimeout = time.Hour
	// runtimeRefreshRetryInterval separates passes when every remaining lease
	// was busy.
	runtimeRefreshRetryInterval = 10 * time.Second
	// runtimeRefreshPollInterval is how often a lease's durable settlement is
	// read while its refresh runs.
	runtimeRefreshPollInterval = time.Second
)

var (
	// ErrRuntimeRefreshDisabled reports that runtime_refresh_max_concurrency is 0.
	ErrRuntimeRefreshDisabled = errors.New("runtime refresh is disabled (runtime_refresh_max_concurrency is 0)")
	// ErrRuntimeRefreshActive reports that a run is already active; a new run
	// can start once it has finished.
	ErrRuntimeRefreshActive = errors.New("a runtime refresh run is already active")
)

// RuntimeRefreshOutcome is one lease's result in a run.
type RuntimeRefreshOutcome string

const (
	// RuntimeRefreshRefreshed: the lease's refresh target release is durably
	// active. Its new containers carry the attested maintenance ID in
	// fred.maintenance_id.
	RuntimeRefreshRefreshed RuntimeRefreshOutcome = "refreshed"
	// RuntimeRefreshFailed: the refresh ran and did not become the active
	// release. A rolled-back refresh leaves the lease on its old containers.
	RuntimeRefreshFailed RuntimeRefreshOutcome = "failed"
	// RuntimeRefreshSkipped: the lease was not refreshed. It was busy with
	// other lifecycle work, or stopped being eligible, when last attempted.
	RuntimeRefreshSkipped RuntimeRefreshOutcome = "skipped"
)

var runtimeRefreshOutcomes = []RuntimeRefreshOutcome{
	RuntimeRefreshRefreshed, RuntimeRefreshFailed, RuntimeRefreshSkipped,
}

// RuntimeRefreshLease attests one lease's outcome. MaintenanceID is the
// runtime_refresh maintenance whose containers carry it in fred.maintenance_id;
// it is empty when no refresh was admitted.
type RuntimeRefreshLease struct {
	LeaseUUID     string                `json:"lease_uuid"`
	MaintenanceID string                `json:"maintenance_id,omitempty"`
	Outcome       RuntimeRefreshOutcome `json:"outcome"`
}

// RuntimeRefreshStatus is the GET /runtime_refresh response. Stale counts the
// Ready leases with a container under an older contract, read from Docker for
// each response; a lease whose refresh failed and rolled back stays counted.
// Active is true from the POST that starts a run until that run has finished
// every lease. The remaining fields describe the current run, or the most
// recent one when none is active. Leases holds one row per lease the run
// attested, sorted and unique by lease_uuid, so its size is bounded by the
// backend's tracked leases, and its per-outcome row counts always equal
// Refreshed, Failed and Skipped.
type RuntimeRefreshStatus struct {
	Contract  uint32                `json:"contract"`
	Active    bool                  `json:"active"`
	Stale     int                   `json:"stale"`
	InFlight  int                   `json:"in_flight"`
	Refreshed int                   `json:"refreshed"`
	Failed    int                   `json:"failed"`
	Skipped   int                   `json:"skipped"`
	Leases    []RuntimeRefreshLease `json:"leases"`
}

// runtimeRefreshController owns at most one run at a time. The zero value is
// ready to use.
type runtimeRefreshController struct {
	mu     sync.Mutex
	active bool
	// leases holds the current or most recent run's attestations.
	leases   map[string]RuntimeRefreshLease
	inFlight map[string]struct{}
}

// runtimeRefreshEnabled reports whether the operator endpoint may start runs.
func (b *Backend) runtimeRefreshEnabled() bool {
	return b.cfg.GetRuntimeRefreshMaxConcurrency() > 0
}

// StartRuntimeRefresh starts a run and returns the resulting status. It
// refuses with ErrRuntimeRefreshActive while a run is active. The run belongs to the backend's lifetime, never to
// the request that started it.
func (b *Backend) StartRuntimeRefresh(ctx context.Context) (RuntimeRefreshStatus, error) {
	if !b.runtimeRefreshEnabled() {
		return RuntimeRefreshStatus{}, ErrRuntimeRefreshDisabled
	}
	if b.releaseStore == nil || b.callbackStore == nil || b.maintenanceSettlement == nil {
		return RuntimeRefreshStatus{}, errors.New("runtime refresh requires the durable release and callback stores")
	}
	if err := b.requireMutationAdmission(ctx, "runtime refresh"); err != nil {
		return RuntimeRefreshStatus{}, fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	if b.stopCtx.Err() != nil {
		return RuntimeRefreshStatus{}, fmt.Errorf("%w: backend is stopping", backend.ErrInvalidState)
	}
	// Read the inventory first: a run starts only with a stale count to report.
	candidates, err := b.runtimeRefreshCandidates(ctx)
	if err != nil {
		return RuntimeRefreshStatus{}, err
	}
	controller := &b.runtimeRefresh
	controller.mu.Lock()
	if controller.active {
		controller.mu.Unlock()
		return RuntimeRefreshStatus{}, ErrRuntimeRefreshActive
	}
	controller.active = true
	controller.leases = make(map[string]RuntimeRefreshLease)
	controller.inFlight = make(map[string]struct{})
	b.wg.Go(b.runRuntimeRefresh)
	controller.mu.Unlock()
	status := controller.snapshot()
	status.Stale = len(candidates)
	return status, nil
}

// RuntimeRefreshStatus reports the stale leases Docker shows now and the
// current or most recent run. A Docker inventory error is returned, never
// reported as zero stale leases.
func (b *Backend) RuntimeRefreshStatus(ctx context.Context) (RuntimeRefreshStatus, error) {
	candidates, err := b.runtimeRefreshCandidates(ctx)
	if err != nil {
		return RuntimeRefreshStatus{}, err
	}
	status := b.runtimeRefresh.snapshot()
	status.Stale = len(candidates)
	return status, nil
}

func (c *runtimeRefreshController) snapshot() RuntimeRefreshStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := RuntimeRefreshStatus{
		Contract: uint32(currentRuntimeContract),
		Active:   c.active,
		InFlight: len(c.inFlight),
		Leases:   make([]RuntimeRefreshLease, 0, len(c.leases)),
	}
	for _, lease := range slices.Sorted(maps.Keys(c.leases)) {
		attestation := c.leases[lease]
		status.Leases = append(status.Leases, attestation)
		switch attestation.Outcome {
		case RuntimeRefreshRefreshed:
			status.Refreshed++
		case RuntimeRefreshFailed:
			status.Failed++
		case RuntimeRefreshSkipped:
			status.Skipped++
		}
	}
	return status
}

func (c *runtimeRefreshController) begin(lease string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[lease] = struct{}{}
}

func (c *runtimeRefreshController) record(attestation RuntimeRefreshLease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, attestation.LeaseUUID)
	c.leases[attestation.LeaseUUID] = attestation
}

// settled reports whether lease reached a terminal outcome in this run. A
// skipped lease is retried by a later pass.
func (c *runtimeRefreshController) settled(lease string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcome := c.leases[lease].Outcome
	return outcome == RuntimeRefreshRefreshed || outcome == RuntimeRefreshFailed
}

// finish ends the run and counts its final outcomes.
func (c *runtimeRefreshController) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	clear(c.inFlight)
	for _, attestation := range c.leases {
		runtimeRefreshTotal.WithLabelValues(string(attestation.Outcome)).Inc()
	}
}

// runtimeRefreshCandidates reads Docker and returns the leases eligible for a
// refresh now; see selectRuntimeRefreshCandidates.
func (b *Backend) runtimeRefreshCandidates(ctx context.Context) ([]string, error) {
	containers, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers for runtime refresh: %w", err)
	}
	b.provisionsMu.RLock()
	ready := make(map[string]struct{}, len(b.provisions))
	for lease, provision := range b.provisions {
		if provision.Status == backend.ProvisionStatusReady && provision.StackManifest != nil {
			ready[lease] = struct{}{}
		}
	}
	b.provisionsMu.RUnlock()
	candidates := selectRuntimeRefreshCandidates(ready, containers)
	runtimeStaleLeases.Set(float64(len(candidates)))
	return candidates, nil
}

// selectRuntimeRefreshCandidates returns, sorted, the leases of ready with at
// least one container created under an older runtime contract. Only recorded
// labels decide: a lease without a listed container is not a candidate.
func selectRuntimeRefreshCandidates(ready map[string]struct{}, containers []ContainerInfo) []string {
	stale := make(map[string]struct{})
	for _, container := range containers {
		if _, ok := ready[container.LeaseUUID]; ok && container.RuntimeContract.predatesCurrent() {
			stale[container.LeaseUUID] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(stale))
}

// runRuntimeRefresh refreshes candidates in passes until none remains
// unsettled, the run times out, or the backend stops.
func (b *Backend) runRuntimeRefresh() {
	controller := &b.runtimeRefresh
	defer controller.finish()
	defer func() {
		if recovered := recover(); recovered != nil {
			background.CleanupPanicsTotal.WithLabelValues("docker_runtime_refresh").Inc()
			b.logger.Error("runtime refresh run panicked", "panic", recovered, "stack", string(debug.Stack()))
		}
	}()
	ctx, cancel := context.WithTimeout(b.stopCtx, runtimeRefreshRunTimeout)
	defer cancel()
	concurrency := b.cfg.GetRuntimeRefreshMaxConcurrency()
	b.logger.Info("runtime refresh started", "contract", currentRuntimeContract, "max_concurrency", concurrency)
	for ctx.Err() == nil {
		candidates, err := b.runtimeRefreshCandidates(ctx)
		if err != nil {
			b.logger.Warn("runtime refresh could not read stale leases; retrying", "error", err)
			if !sleepContext(ctx, runtimeRefreshRetryInterval) {
				break
			}
			continue
		}
		pending := make([]string, 0, len(candidates))
		for _, lease := range candidates {
			if !controller.settled(lease) {
				pending = append(pending, lease)
			}
		}
		if len(pending) == 0 {
			break
		}
		var group errgroup.Group
		group.SetLimit(concurrency)
		var progressed atomic.Bool
		for _, lease := range pending {
			group.Go(func() error {
				attestation := b.refreshLeaseRuntime(ctx, lease)
				controller.record(attestation)
				if attestation.Outcome != RuntimeRefreshSkipped {
					progressed.Store(true)
				}
				return nil
			})
		}
		_ = group.Wait()
		if !progressed.Load() && !sleepContext(ctx, runtimeRefreshRetryInterval) {
			break
		}
	}
	status := controller.snapshot()
	b.logger.Info("runtime refresh finished", "refreshed", status.Refreshed,
		"failed", status.Failed, "skipped", status.Skipped)
}

// refreshLeaseRuntime refreshes one lease and attests its outcome. A panic in
// one lease fails only that lease.
func (b *Backend) refreshLeaseRuntime(ctx context.Context, lease string) (attestation RuntimeRefreshLease) {
	attestation = RuntimeRefreshLease{LeaseUUID: lease, Outcome: RuntimeRefreshSkipped}
	b.runtimeRefresh.begin(lease)
	defer func() {
		if recovered := recover(); recovered != nil {
			background.CleanupPanicsTotal.WithLabelValues("docker_runtime_refresh").Inc()
			b.logger.Error("runtime refresh panicked", "lease_uuid", lease,
				"panic", recovered, "stack", string(debug.Stack()))
			attestation.Outcome = RuntimeRefreshFailed
		}
	}()
	logger := b.logger.With("lease_uuid", lease)

	b.provisionsMu.RLock()
	provision, ok := b.provisions[lease]
	eligible := ok && provision.Status == backend.ProvisionStatusReady && provision.StackManifest != nil
	var callbackURL, lifecycleCallbackURL string
	if eligible {
		callbackURL, lifecycleCallbackURL = provision.CallbackURL, provision.LifecycleCallbackURL
	}
	b.provisionsMu.RUnlock()
	if !eligible {
		return attestation
	}
	// No request URL: this autonomous path never treats a lifecycle_id URL as a
	// fresh operation callback.
	_, lifecycleCallbackURL, err := resolveMaintenanceCallbackURLs(callbackURL, lifecycleCallbackURL, "")
	if err != nil {
		logger.Warn("runtime refresh has no usable lifecycle route", "error", err)
		attestation.Outcome = RuntimeRefreshFailed
		return attestation
	}
	id, err := maintenanceid.New()
	if err != nil {
		logger.Warn("allocate runtime refresh maintenance identity", "error", err)
		attestation.Outcome = RuntimeRefreshFailed
		return attestation
	}
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		id, shared.MaintenanceIntentRuntimeRefresh, lease, lifecycleCallbackURL, nil,
	)
	if err != nil {
		logger.Warn("construct runtime refresh authority", "error", err)
		attestation.Outcome = RuntimeRefreshFailed
		return attestation
	}
	routeErr := b.routeReplaceRestart(ctx, request, runtimeRefreshRoute())
	if errors.Is(routeErr, errLeaseCommandBusy) || shared.IsLifecyclePending(routeErr) {
		return attestation
	}
	// Whatever the route returned, only the durable settlement of this
	// maintenance decides: an admitted refresh continues even when its
	// acknowledgment was lost.
	outcome, err := b.awaitRuntimeRefresh(ctx, lease, id)
	switch {
	case err != nil:
		logger.Warn("runtime refresh settlement unknown; not attested", "maintenance_id", id, "error", err)
		attestation.Outcome = RuntimeRefreshFailed
		attestation.MaintenanceID = id.String()
	case outcome == runtimeRefreshNotAdmitted:
		switch {
		case routeErr == nil:
			logger.Warn("runtime refresh accepted but never admitted", "maintenance_id", id)
			attestation.Outcome = RuntimeRefreshFailed
		case errors.Is(routeErr, backend.ErrInvalidState) || errors.Is(routeErr, backend.ErrNotProvisioned):
			// The lease stopped being a healthy, tracked lease before admission.
			attestation.Outcome = RuntimeRefreshSkipped
		default:
			logger.Warn("runtime refresh refused", "error", routeErr)
			attestation.Outcome = RuntimeRefreshFailed
		}
	case outcome == runtimeRefreshActive:
		attestation.Outcome = RuntimeRefreshRefreshed
		attestation.MaintenanceID = id.String()
		logger.Info("runtime refreshed", "maintenance_id", id)
	default:
		attestation.Outcome = RuntimeRefreshFailed
		attestation.MaintenanceID = id.String()
		logger.Warn("runtime refresh did not become active", "maintenance_id", id, "route_error", routeErr)
	}
	return attestation
}

// runtimeRefreshSettlement is the durable state of one refresh maintenance.
type runtimeRefreshSettlement uint8

const (
	runtimeRefreshPending runtimeRefreshSettlement = iota
	runtimeRefreshNotAdmitted
	runtimeRefreshActive
	runtimeRefreshInactive
)

// awaitRuntimeRefresh waits for the durable settlement of maintenance id, at
// most one provision timeout past its admission.
func (b *Backend) awaitRuntimeRefresh(ctx context.Context, lease string, id shared.MaintenanceID) (runtimeRefreshSettlement, error) {
	waitCtx, cancel := context.WithTimeout(ctx, b.provisionOperationTimeout()+time.Minute)
	defer cancel()
	for {
		settlement, err := b.readRuntimeRefreshSettlement(lease, id)
		if err != nil || settlement != runtimeRefreshPending {
			return settlement, err
		}
		if !sleepContext(waitCtx, runtimeRefreshPollInterval) {
			return runtimeRefreshPending, fmt.Errorf("runtime refresh of lease %q did not settle: %w", lease, context.Cause(waitCtx))
		}
	}
}

// readRuntimeRefreshSettlement reads the durable state of maintenance id.
// Only a target release that is the lease's active release attests success:
// its containers are the running cohort and carry the maintenance ID.
func (b *Backend) readRuntimeRefreshSettlement(lease string, id shared.MaintenanceID) (runtimeRefreshSettlement, error) {
	releases, err := b.releaseStore.List(lease)
	if err != nil {
		return runtimeRefreshPending, fmt.Errorf("read release history: %w", err)
	}
	for _, release := range releases {
		if release.MaintenanceID != id {
			continue
		}
		switch release.Status {
		case "deploying":
			return runtimeRefreshPending, nil
		case "active":
			return runtimeRefreshActive, nil
		default:
			// failed, or superseded without having been observed active.
			return runtimeRefreshInactive, nil
		}
	}
	intent, found, err := b.maintenanceSettlement.GetMaintenanceIntent(lease)
	if err != nil {
		return runtimeRefreshPending, fmt.Errorf("read maintenance intent: %w", err)
	}
	if found && intent.MaintenanceID() == id {
		return runtimeRefreshPending, nil
	}
	return runtimeRefreshNotAdmitted, nil
}

// sleepContext waits d, reporting false when ctx ended first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
