package docker

import (
	"context"
	"encoding/json"
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
	"github.com/manifest-network/fred/internal/util"
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
	// RuntimeRefreshFailed: the refresh did not become the active release.
	// When its settlement proved the pre-refresh release the running, ready
	// cohort, the row carries Restored: a compensation recreated it (copies with
	// new container IDs) or the target failed before displacing it.
	RuntimeRefreshFailed RuntimeRefreshOutcome = "failed"
	// RuntimeRefreshSkipped: the lease was not refreshed. It was busy with
	// other lifecycle work, or stopped being eligible, when last attempted.
	RuntimeRefreshSkipped RuntimeRefreshOutcome = "skipped"
)

var runtimeRefreshOutcomes = []RuntimeRefreshOutcome{
	RuntimeRefreshRefreshed, RuntimeRefreshFailed, RuntimeRefreshSkipped,
}

// RuntimeRefreshLease attests one lease's outcome. MaintenanceID is the
// runtime_refresh maintenance; a refreshed lease's containers carry it in
// fred.maintenance_id. It is empty when no refresh was admitted. Rows are
// built only by runtimeRefreshSettledRow, from a lease's durable receipt, and
// by the unsettled-row constructors, so refreshed and Restored rows always
// name their maintenance and Restored appears only on a failed row.
type RuntimeRefreshLease struct {
	LeaseUUID     string                  `json:"lease_uuid"`
	MaintenanceID string                  `json:"maintenance_id,omitempty"`
	Outcome       RuntimeRefreshOutcome   `json:"outcome"`
	Restored      *RuntimeRefreshRestored `json:"restored,omitempty"`
}

// RuntimeRefreshRestored names the release a failed refresh settled back onto
// as the lease's running, ready cohort.
type RuntimeRefreshRestored struct {
	ReleaseVersion int    `json:"release_version"`
	ReleaseDigest  string `json:"release_digest"`
}

// runtimeRefreshSettledRow attests a lease from its settled refresh receipt.
func runtimeRefreshSettledRow(settled shared.SettledMaintenance) RuntimeRefreshLease {
	row := RuntimeRefreshLease{
		LeaseUUID: settled.LeaseUUID(), MaintenanceID: settled.MaintenanceID().String(), Outcome: RuntimeRefreshFailed,
	}
	if settled.Succeeded() {
		row.Outcome = RuntimeRefreshRefreshed
	} else if source, ready := settled.SourceReady(); ready {
		row.Restored = &RuntimeRefreshRestored{ReleaseVersion: source.Version, ReleaseDigest: source.Digest}
	}
	return row
}

// runtimeRefreshSkippedRow attests a lease this run did not refresh.
func runtimeRefreshSkippedRow(lease string) RuntimeRefreshLease {
	return RuntimeRefreshLease{LeaseUUID: lease, Outcome: RuntimeRefreshSkipped}
}

// runtimeRefreshUnsettledFailureRow attests a refresh that failed without a
// settled receipt: it was refused, or its settlement is unknown. id is the
// maintenance it may have admitted, or empty.
func runtimeRefreshUnsettledFailureRow(lease, id string) RuntimeRefreshLease {
	return RuntimeRefreshLease{LeaseUUID: lease, MaintenanceID: id, Outcome: RuntimeRefreshFailed}
}

// RuntimeRefreshStatus is the GET /runtime_refresh response.
//
// Stale, StaleLeases and SharedNetworkLeases are read from Docker for each
// response. StaleLeases lists, sorted, the Ready leases with a container under
// an older contract, and Stale is its length; a lease whose refresh failed and
// was restored stays listed. SharedNetworkLeases lists, sorted, the leases
// with a container on a Docker network that a container of another lease is
// also on, in any state.
//
// Leases holds one row per lease, sorted and unique by lease_uuid. A lease's
// refreshed or failed row comes from its latest retained runtime_refresh
// receipt, so it survives a backend restart and later runs; a skipped row, or
// a failure with no receipt, comes from the current or most recent run in this
// process. Active and InFlight describe that run. The per-outcome row counts
// always equal Refreshed, Failed and Skipped, and Restored counts the failed
// rows that carry Restored.
type RuntimeRefreshStatus struct {
	Contract            uint32                `json:"contract"`
	Active              bool                  `json:"active"`
	Stale               int                   `json:"stale"`
	StaleLeases         []string              `json:"stale_leases"`
	InFlight            int                   `json:"in_flight"`
	Refreshed           int                   `json:"refreshed"`
	Failed              int                   `json:"failed"`
	Restored            int                   `json:"restored"`
	Skipped             int                   `json:"skipped"`
	SharedNetworkLeases []string              `json:"shared_network_leases"`
	Leases              []RuntimeRefreshLease `json:"leases"`
}

// runtimeRefreshController owns at most one run at a time. The zero value is
// ready to use.
type runtimeRefreshController struct {
	mu     sync.Mutex
	active bool
	// leases holds the current or most recent run's outcomes.
	leases   map[string]runtimeRefreshRunOutcome
	inFlight map[string]struct{}
}

// runtimeRefreshRunOutcome is one lease's outcome in a run. A settled outcome
// was read from the lease's receipt: the status always reads it from the
// retained receipt again, so a receipt deleted with its lease is never
// republished from memory. Only an unsettled outcome (skipped, refused, or a
// settlement this run could not read) is reported from memory.
type runtimeRefreshRunOutcome struct {
	row     RuntimeRefreshLease
	settled bool
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
	// Read the inventory and the durable outcomes first: a run starts only
	// with a status to report.
	inventory, err := b.readRuntimeRefreshInventory(ctx)
	if err != nil {
		return RuntimeRefreshStatus{}, err
	}
	settled, err := b.settledRuntimeRefreshes()
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
	controller.leases = make(map[string]runtimeRefreshRunOutcome)
	controller.inFlight = make(map[string]struct{})
	b.wg.Go(b.runRuntimeRefresh)
	controller.mu.Unlock()
	return controller.snapshot(inventory, settled, b.trackedLeases()), nil
}

// RuntimeRefreshStatus reports what Docker shows now, every lease's durable
// refresh outcome, and the current or most recent run. A Docker inventory or
// receipt read error is returned, never reported as zero.
func (b *Backend) RuntimeRefreshStatus(ctx context.Context) (RuntimeRefreshStatus, error) {
	inventory, err := b.readRuntimeRefreshInventory(ctx)
	if err != nil {
		return RuntimeRefreshStatus{}, err
	}
	settled, err := b.settledRuntimeRefreshes()
	if err != nil {
		return RuntimeRefreshStatus{}, err
	}
	return b.runtimeRefresh.snapshot(inventory, settled, b.trackedLeases()), nil
}

// trackedLeases is the set of leases this backend tracks now.
func (b *Backend) trackedLeases() map[string]struct{} {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	tracked := make(map[string]struct{}, len(b.provisions))
	for lease := range b.provisions {
		tracked[lease] = struct{}{}
	}
	return tracked
}

// settledRuntimeRefreshes returns each live lease's latest retained
// runtime_refresh receipt.
func (b *Backend) settledRuntimeRefreshes() ([]shared.SettledMaintenance, error) {
	if b.maintenanceSettlement == nil {
		return nil, errors.New("runtime refresh requires the maintenance settlement")
	}
	settled, err := b.maintenanceSettlement.LatestSettledMaintenance(shared.MaintenanceIntentRuntimeRefresh)
	if err != nil {
		return nil, fmt.Errorf("read runtime refresh receipts: %w", err)
	}
	return settled, nil
}

// snapshot assembles the status. Every refreshed or failed outcome with a
// receipt is read from the retained receipt. This run's unsettled outcomes, for
// leases still tracked, fill in a lease without a receipt, and replace a
// receipt only when they name a newer maintenance, whose settlement is unknown.
func (c *runtimeRefreshController) snapshot(inventory runtimeRefreshInventory, settled []shared.SettledMaintenance, tracked map[string]struct{}) RuntimeRefreshStatus {
	rows := make(map[string]RuntimeRefreshLease, len(settled))
	for _, receipt := range settled {
		rows[receipt.LeaseUUID()] = runtimeRefreshSettledRow(receipt)
	}
	c.mu.Lock()
	status := RuntimeRefreshStatus{
		Contract:            uint32(currentRuntimeContract),
		Active:              c.active,
		InFlight:            len(c.inFlight),
		Stale:               len(inventory.stale),
		StaleLeases:         slices.Clone(inventory.stale),
		SharedNetworkLeases: slices.Clone(inventory.sharedNetwork),
	}
	for lease, outcome := range c.leases {
		if _, live := tracked[lease]; outcome.settled || !live {
			continue
		}
		durable, ok := rows[lease]
		if !ok || (outcome.row.MaintenanceID != "" && outcome.row.MaintenanceID != durable.MaintenanceID) {
			rows[lease] = outcome.row
		}
	}
	c.mu.Unlock()
	status.Leases = make([]RuntimeRefreshLease, 0, len(rows))
	for _, lease := range slices.Sorted(maps.Keys(rows)) {
		row := rows[lease]
		status.Leases = append(status.Leases, row)
		switch row.Outcome {
		case RuntimeRefreshRefreshed:
			status.Refreshed++
		case RuntimeRefreshFailed:
			status.Failed++
			if row.Restored != nil {
				status.Restored++
			}
		case RuntimeRefreshSkipped:
			status.Skipped++
		}
	}
	return status
}

// MarshalJSON writes nil lists as empty arrays: the cutover reads both as
// lists, so the wire never carries null for either.
func (s RuntimeRefreshStatus) MarshalJSON() ([]byte, error) {
	type wire RuntimeRefreshStatus
	w := wire(s)
	if w.StaleLeases == nil {
		w.StaleLeases = []string{}
	}
	if w.SharedNetworkLeases == nil {
		w.SharedNetworkLeases = []string{}
	}
	if w.Leases == nil {
		w.Leases = []RuntimeRefreshLease{}
	}
	return json.Marshal(w)
}

func (c *runtimeRefreshController) begin(lease string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[lease] = struct{}{}
}

func (c *runtimeRefreshController) record(outcome runtimeRefreshRunOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, outcome.row.LeaseUUID)
	c.leases[outcome.row.LeaseUUID] = outcome
}

// settled reports whether lease reached a terminal outcome in this run. A
// skipped lease is retried by a later pass.
func (c *runtimeRefreshController) settled(lease string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcome := c.leases[lease].row.Outcome
	return outcome == RuntimeRefreshRefreshed || outcome == RuntimeRefreshFailed
}

// finish ends the run and counts its final outcomes.
func (c *runtimeRefreshController) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	clear(c.inFlight)
	for _, outcome := range c.leases {
		runtimeRefreshTotal.WithLabelValues(string(outcome.row.Outcome)).Inc()
	}
}

// runtimeRefreshInventory is one read of Docker: the leases eligible for a
// refresh, and the leases sharing a network with another lease.
type runtimeRefreshInventory struct {
	stale         []string
	sharedNetwork []string
}

// readRuntimeRefreshInventory reads Docker once and publishes both gauges.
func (b *Backend) readRuntimeRefreshInventory(ctx context.Context) (runtimeRefreshInventory, error) {
	containers, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return runtimeRefreshInventory{}, fmt.Errorf("list containers for runtime refresh: %w", err)
	}
	b.provisionsMu.RLock()
	ready := make(map[string]struct{}, len(b.provisions))
	for lease, provision := range b.provisions {
		if provision.Status == backend.ProvisionStatusReady && provision.StackManifest != nil {
			ready[lease] = struct{}{}
		}
	}
	b.provisionsMu.RUnlock()
	inventory := runtimeRefreshInventory{
		stale:         selectRuntimeRefreshCandidates(ready, containers),
		sharedNetwork: selectSharedNetworkLeases(containers),
	}
	runtimeStaleLeases.Set(float64(len(inventory.stale)))
	runtimeSharedNetworkLeases.Set(float64(len(inventory.sharedNetwork)))
	return inventory, nil
}

// runtimeInventoryTimeout bounds one periodic inventory pass.
const runtimeInventoryTimeout = 30 * time.Second

// runtimeInventoryLoop keeps the stale and shared-network gauges current every
// reconcile_interval, so an alert on them holds after a restart and between
// operator requests. The first pass waits one interval, so it never competes
// with startup recovery. It only reads Docker.
func (b *Backend) runtimeInventoryLoop() {
	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
		}
		util.RunCleanupIteration(func() error {
			b.runRuntimeInventoryPass()
			return nil
		}, "docker_runtime_inventory", func(any) {
			runtimeInventoryTotal.WithLabelValues("error").Inc()
			background.CleanupPanicsTotal.WithLabelValues("docker_runtime_inventory").Inc()
		})
	}
}

// runRuntimeInventoryPass reads the inventory once. A failed pass is counted
// and leaves both gauges at their last completed value.
func (b *Backend) runRuntimeInventoryPass() {
	ctx, cancel := context.WithTimeout(b.stopCtx, runtimeInventoryTimeout)
	defer cancel()
	if _, err := b.readRuntimeRefreshInventory(ctx); err != nil {
		if b.stopCtx.Err() != nil {
			return
		}
		runtimeInventoryTotal.WithLabelValues("error").Inc()
		b.logger.Warn("runtime inventory failed; keeping the last completed counts", "error", err)
		return
	}
	runtimeInventoryTotal.WithLabelValues("ok").Inc()
}

// runtimeRefreshCandidates reads Docker and returns the leases eligible for a
// refresh now; see selectRuntimeRefreshCandidates.
func (b *Backend) runtimeRefreshCandidates(ctx context.Context) ([]string, error) {
	inventory, err := b.readRuntimeRefreshInventory(ctx)
	return inventory.stale, err
}

// selectSharedNetworkLeases returns, sorted, the leases with a container on a
// network that a container of another lease is also on. Membership is what
// the daemon reports for each container, whatever its state: a stopped
// container rejoins its networks when it restarts. A lease's own network holds
// only that lease, so it never counts; "none" is no network.
func selectSharedNetworkLeases(containers []ContainerInfo) []string {
	members := make(map[string]map[string]struct{})
	for _, container := range containers {
		for _, network := range container.Networks {
			if network == "none" || container.LeaseUUID == "" {
				continue
			}
			if members[network] == nil {
				members[network] = make(map[string]struct{})
			}
			members[network][container.LeaseUUID] = struct{}{}
		}
	}
	shared := make(map[string]struct{})
	for _, leases := range members {
		if len(leases) > 1 {
			maps.Copy(shared, leases)
		}
	}
	return slices.Sorted(maps.Keys(shared))
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
				outcome := b.refreshLeaseRuntime(ctx, lease)
				controller.record(outcome)
				if outcome.row.Outcome != RuntimeRefreshSkipped {
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
	refreshed, failed, skipped := controller.runCounts()
	b.logger.Info("runtime refresh finished", "refreshed", refreshed, "failed", failed, "skipped", skipped)
}

// runCounts counts this run's own rows.
func (c *runtimeRefreshController) runCounts() (refreshed, failed, skipped int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, outcome := range c.leases {
		switch outcome.row.Outcome {
		case RuntimeRefreshRefreshed:
			refreshed++
		case RuntimeRefreshFailed:
			failed++
		case RuntimeRefreshSkipped:
			skipped++
		}
	}
	return refreshed, failed, skipped
}

// refreshLeaseRuntime refreshes one lease and attests its outcome. A panic in
// one lease fails only that lease.
func (b *Backend) refreshLeaseRuntime(ctx context.Context, lease string) (outcome runtimeRefreshRunOutcome) {
	outcome = runtimeRefreshRunOutcome{row: runtimeRefreshSkippedRow(lease)}
	var admitted string
	b.runtimeRefresh.begin(lease)
	defer func() {
		if recovered := recover(); recovered != nil {
			background.CleanupPanicsTotal.WithLabelValues("docker_runtime_refresh").Inc()
			b.logger.Error("runtime refresh panicked", "lease_uuid", lease,
				"panic", recovered, "stack", string(debug.Stack()))
			outcome = runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, admitted)}
		}
	}()
	logger := b.logger.With("lease_uuid", lease)

	b.provisionsMu.RLock()
	provision, ok := b.provisions[lease]
	eligible := ok && provision.Status == backend.ProvisionStatusReady && provision.StackManifest != nil
	b.provisionsMu.RUnlock()
	if !eligible {
		return outcome
	}
	// The refresh target is the source release itself, so its request carries
	// the source's own lifecycle route, never a projection's.
	active, err := b.releaseStore.LatestActive(lease)
	if err != nil || active == nil {
		logger.Warn("runtime refresh cannot read the active release", "error", err)
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
	}
	identity, ok := runtimeIdentityForRelease(active)
	if !ok || identity.LifecycleCallbackURL() == "" {
		logger.Warn("runtime refresh source has no lifecycle route")
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
	}
	lifecycleCallbackURL := identity.LifecycleCallbackURL()
	id, err := maintenanceid.New()
	if err != nil {
		logger.Warn("allocate runtime refresh maintenance identity", "error", err)
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
	}
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		id, shared.MaintenanceIntentRuntimeRefresh, lease, lifecycleCallbackURL, nil,
	)
	if err != nil {
		logger.Warn("construct runtime refresh authority", "error", err)
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
	}
	admitted = id.String()
	routeErr := b.routeReplaceRestart(ctx, request, runtimeRefreshRoute())
	if errors.Is(routeErr, errLeaseCommandBusy) || errors.Is(routeErr, errRuntimeRefreshInventoryUnavailable) ||
		shared.IsLifecyclePending(routeErr) {
		return outcome
	}
	if errors.Is(routeErr, errRuntimeRefreshUnpinned) {
		logger.Warn("runtime refresh skipped: the source's exact images are not proved", "error", routeErr)
		return outcome
	}
	if errors.Is(routeErr, errRuntimeRefreshSourceImageDiverged) {
		logger.Error("runtime refresh refused: the source runs other images than its pins record, so a refresh would change the workload",
			"error", routeErr)
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
	}
	// Whatever the route returned, only the durable settlement of this
	// maintenance decides: an admitted refresh continues even when its
	// acknowledgment was lost.
	settlement, err := b.awaitRuntimeRefresh(lease, id)
	switch {
	case err != nil:
		logger.Warn("runtime refresh settlement unknown; not attested", "maintenance_id", id, "error", err)
		return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, admitted)}
	case !settlement.admitted:
		switch {
		case routeErr == nil:
			logger.Warn("runtime refresh accepted but never admitted", "maintenance_id", id)
			return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
		case errors.Is(routeErr, backend.ErrInvalidState) || errors.Is(routeErr, backend.ErrNotProvisioned):
			// The lease stopped being a healthy, tracked, stale lease before
			// admission.
			return outcome
		default:
			logger.Warn("runtime refresh refused", "error", routeErr)
			return runtimeRefreshRunOutcome{row: runtimeRefreshUnsettledFailureRow(lease, "")}
		}
	}
	row := runtimeRefreshSettledRow(settlement.receipt)
	switch {
	case row.Outcome == RuntimeRefreshRefreshed:
		logger.Info("runtime refreshed", "maintenance_id", id)
	case row.Restored != nil:
		logger.Warn("runtime refresh failed; the pre-refresh release is the running cohort",
			"maintenance_id", id, "release_version", row.Restored.ReleaseVersion, "route_error", routeErr)
	default:
		logger.Warn("runtime refresh failed", "maintenance_id", id, "route_error", routeErr)
	}
	return runtimeRefreshRunOutcome{row: row, settled: true}
}

// errRuntimeRefreshInventoryUnavailable is a pre-admission refusal: the
// lease's containers could not be read, so its staleness is unknown. Nothing
// was journaled, and a later pass retries it.
var errRuntimeRefreshInventoryUnavailable = errors.New("runtime refresh could not read the lease's containers")

// requireRuntimeRefreshStale proves, from the containers Docker lists now,
// that the lease still has a container created under an older runtime
// contract. A lease already under the current contract is refused as no
// longer eligible; nothing is recreated.
func (b *Backend) requireRuntimeRefreshStale(ctx context.Context, leaseUUID string) error {
	containers, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errRuntimeRefreshInventoryUnavailable, err)
	}
	for _, container := range containers {
		if container.LeaseUUID == leaseUUID && container.RuntimeContract.predatesCurrent() {
			return nil
		}
	}
	return fmt.Errorf("%w: lease already runs under the current runtime contract", backend.ErrInvalidState)
}

// errRuntimeRefreshUnpinned is a pre-admission refusal: the images the lease's
// running containers execute could not be matched to its pins, even after
// observing them. Nothing was journaled, and a later pass retries it.
var errRuntimeRefreshUnpinned = errors.New("runtime refresh cannot prove the source's exact images")

// errRuntimeRefreshSourceImageDiverged is a definitive pre-admission refusal:
// an image of the lease's active release is pinned to other content than its
// running containers execute, as after a failed restart that pinned what a
// moved tag named. A pin is never replaced, so a refresh could only launch
// that other content; it refuses rather than change the workload. Nothing was
// journaled.
var errRuntimeRefreshSourceImageDiverged = errors.New("runtime refresh source runs other images than its pins record")

// requireRuntimeRefreshSourceImages proves every image of the lease's active
// release is pinned to exactly the image its running containers execute, as
// the same observer as the startup backfill reads them; a missing pin is
// minted from that observation, never from a mutable tag. Execution then
// admits only those images.
func (b *Backend) requireRuntimeRefreshSourceImages(ctx context.Context, leaseUUID string) error {
	capacity := b.imageCapacity
	if capacity == nil || capacity.backfiller == nil {
		return fmt.Errorf("%w: no image pin journal", errRuntimeRefreshUnpinned)
	}
	verification, err := capacity.backfiller.VerifyLease(ctx, leaseUUID)
	switch {
	case err != nil:
		return fmt.Errorf("%w: %w", errRuntimeRefreshUnpinned, err)
	case len(verification.Diverged) > 0:
		return fmt.Errorf("%w: %v", errRuntimeRefreshSourceImageDiverged, verification.Diverged)
	case !verification.Verified:
		return fmt.Errorf("%w: the running images could not be observed", errRuntimeRefreshUnpinned)
	}
	return nil
}

// runtimeRefreshSettlement is the durable state of one refresh maintenance:
// not admitted, or settled with its receipt.
type runtimeRefreshSettlement struct {
	admitted bool
	receipt  shared.SettledMaintenance
}

// awaitRuntimeRefresh waits for the durable settlement of maintenance id. It
// holds the run's slot until the admitted maintenance settles, however long
// its compensation takes: only the receipt completes it, never an observation
// deadline. Only the backend's stop ends the wait early.
func (b *Backend) awaitRuntimeRefresh(lease string, id shared.MaintenanceID) (runtimeRefreshSettlement, error) {
	for {
		settlement, pending, err := b.readRuntimeRefreshSettlement(lease, id)
		if err == nil && !pending {
			return settlement, nil
		}
		if err != nil {
			b.logger.Warn("runtime refresh settlement unreadable; retrying", "lease_uuid", lease, "maintenance_id", id, "error", err)
		}
		if !sleepContext(b.stopCtx, runtimeRefreshPollInterval) {
			return runtimeRefreshSettlement{}, fmt.Errorf("runtime refresh of lease %q did not settle before the backend stopped: %w",
				lease, context.Cause(b.stopCtx))
		}
	}
}

// readRuntimeRefreshSettlement reads the durable state of maintenance id. Its
// completion receipt is the only settlement: it is written in the transaction
// that consumes the intent, so an intent that is gone without a receipt was
// never admitted (or was canceled before any effect).
func (b *Backend) readRuntimeRefreshSettlement(lease string, id shared.MaintenanceID) (settlement runtimeRefreshSettlement, pending bool, err error) {
	receipt, found, err := b.maintenanceSettlement.SettledMaintenanceByID(lease, id)
	if err != nil {
		return runtimeRefreshSettlement{}, true, fmt.Errorf("read runtime refresh receipt: %w", err)
	}
	if found {
		return runtimeRefreshSettlement{admitted: true, receipt: receipt}, false, nil
	}
	intent, found, err := b.maintenanceSettlement.GetMaintenanceIntent(lease)
	if err != nil {
		return runtimeRefreshSettlement{}, true, fmt.Errorf("read maintenance intent: %w", err)
	}
	if found && intent.MaintenanceID() == id {
		return runtimeRefreshSettlement{}, true, nil
	}
	// A settlement can commit between the two reads, consuming the intent and
	// writing the receipt together; read the receipt once more so that is not
	// mistaken for no admission.
	receipt, found, err = b.maintenanceSettlement.SettledMaintenanceByID(lease, id)
	if err != nil {
		return runtimeRefreshSettlement{}, true, fmt.Errorf("read runtime refresh receipt: %w", err)
	}
	return runtimeRefreshSettlement{admitted: found, receipt: receipt}, false, nil
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
