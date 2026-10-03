package docker

import (
	"slices"
	"strings"
)

// launchDegradation names one part of the platform's own launch preparation
// that a launch skipped while still proceeding (ENG-1125). A workload that
// then fails its startup may be failing because of that skip, so a startup
// failure after any of them never counts. It is a closed set: a preparation
// step that falls back must name its degradation here, and the receipt's
// completeness is derived from the set being empty.
//
// The set is still a list of known fallbacks, not a proof that every step
// completed: a future fallback that forgets to mark itself would leave the
// launch complete. One thing bounds that: every Step or Prepare error poisons
// the execution's session, so a fallback taken because a guarded call failed
// can never become a definite outcome (substratemutation.Accept refuses it).
// Only a fallback that swallows no guarded error, such as a per-path
// extraction failure the helper reports in its result, depends on being
// marked here.
type launchDegradation uint8

const (
	// launchVolumeOwnerUndetected: the image's volume owner could not be
	// detected; volumes were prepared for root.
	launchVolumeOwnerUndetected launchDegradation = 1 << iota
	// launchWritablePathsUndetected: the image's writable paths could not be
	// detected; none were mounted.
	launchWritablePathsUndetected
	// launchWritableVolumeUnavailable: a writable-path-only volume could not
	// be created or used; its paths stay on the tmpfs fallback.
	launchWritableVolumeUnavailable
	// launchWritablePathsUnseeded: at least one writable path was not seeded
	// with the image's content (extraction failed, or its bind source failed
	// a confinement check).
	launchWritablePathsUnseeded
	// launchWritablePathsStale: the previous writable-path content could not
	// be cleared before seeding, so the seeded tree may hold stale files.
	launchWritablePathsStale
)

// launchDegradations is a set of launchDegradation values. The zero value is
// the empty set: a complete preparation.
type launchDegradations uint8

func (d *launchDegradations) add(degradation launchDegradation) {
	*d |= launchDegradations(degradation)
}

func (d *launchDegradations) addAll(other launchDegradations) { *d |= other }

func (d launchDegradations) empty() bool { return d == 0 }

// String names the set's members, for logs.
func (d launchDegradations) String() string {
	names := []struct {
		degradation launchDegradation
		name        string
	}{
		{launchVolumeOwnerUndetected, "volume_owner_undetected"},
		{launchWritablePathsUndetected, "writable_paths_undetected"},
		{launchWritableVolumeUnavailable, "writable_volume_unavailable"},
		{launchWritablePathsUnseeded, "writable_paths_unseeded"},
		{launchWritablePathsStale, "writable_paths_stale"},
	}
	var members []string
	for _, entry := range names {
		if d&launchDegradations(entry.degradation) != 0 {
			members = append(members, entry.name)
		}
	}
	if len(members) == 0 {
		return "complete"
	}
	return strings.Join(members, ",")
}

// launchCompletion is the closed account of how completely a launch was
// prepared. A degraded launch skipped part of the platform's own preparation,
// so its startup failure never counts (ENG-1125). The zero value is invalid.
type launchCompletion uint8

const (
	launchComplete launchCompletion = iota + 1
	launchDegraded
)

// launchExchange is the closed account of how a settled launch exchange
// ended. The zero value is invalid.
type launchExchange uint8

const (
	// launchExchangeLaunched: every Create and Start succeeded.
	launchExchangeLaunched launchExchange = iota + 1
	// launchExchangeRejected: the exchange settled, but Compose or the daemon
	// returned an error (a Start the daemon refused, a dependency that exited
	// or turned unhealthy). Only the cohort's own state can tell why.
	launchExchangeRejected
)

// settledLaunch is the receipt of one Compose launch whose every Create and
// Start got a final daemon response, and whose launch-debt journal row was
// cleared through the launch step's own receipt. After it, no container of
// the launch can still appear late, whether the exchange launched everything
// or was rejected. It is bound to the exact per-execution mutation facade that
// launched, and carries the positive facts of that launch: how the exchange
// ended, the containers whose Start the daemon refused with a final response,
// the canonical volumes its Create reported created, and whether its
// preparation completed.
//
// Only the launch dispatch mints one (newSettledLaunch, confined by
// internal/testutil). The zero value is invalid: no startup finding can be
// minted without a real receipt.
type settledLaunch struct{ state *settledLaunchState }

type settledLaunchState struct {
	mutations     *storageMutations
	exchange      launchExchange
	refusedStarts []string
	created       []string
	completion    launchCompletion
	degradations  launchDegradations
}

// newSettledLaunch mints the receipt for q's launch from its settled exchange
// outcome. Only the launch dispatch may call it, and only once that exchange
// and its journal row settled.
func newSettledLaunch(q *quiescedVolumes, outcome daemonLaunchOutcome) settledLaunch {
	created := make([]string, 0, len(q.volumes))
	for name, volume := range q.volumes {
		if volume.created {
			created = append(created, name)
		}
	}
	slices.Sort(created)
	completion := launchComplete
	if !q.degradations.empty() {
		completion = launchDegraded
	}
	exchange := launchExchangeLaunched
	var refused []string
	if outcome.err != nil {
		exchange = launchExchangeRejected
		refused = slices.Clone(outcome.refusedStarts)
		slices.Sort(refused)
		refused = slices.Compact(refused)
	}
	return settledLaunch{state: &settledLaunchState{
		mutations: q.mutations, exchange: exchange, refusedStarts: refused,
		created: created, completion: completion, degradations: q.degradations,
	}}
}

// boundTo reports whether the receipt is real and was minted for mutations.
func (l settledLaunch) boundTo(mutations *storageMutations) bool {
	return l.state != nil && mutations != nil && l.state.mutations == mutations &&
		(l.state.completion == launchComplete || l.state.completion == launchDegraded) &&
		(l.state.exchange == launchExchangeLaunched || l.state.exchange == launchExchangeRejected)
}

// rejected reports whether the settled exchange ended in an error. An invalid
// receipt reads as rejected: it never launched anything.
func (l settledLaunch) rejected() bool {
	return l.state == nil || l.state.exchange != launchExchangeLaunched
}

// startRefused reports whether the daemon answered the Start of containerID
// with a final error during this launch.
func (l settledLaunch) startRefused(containerID string) bool {
	if l.state == nil || containerID == "" {
		return false
	}
	_, found := slices.BinarySearch(l.state.refusedStarts, containerID)
	return found
}

// createdVolumes are the canonical volumes this launch's Create reported as
// created: the only volumes a live rollback may destroy.
func (l settledLaunch) createdVolumes() []string {
	if l.state == nil {
		return nil
	}
	return slices.Clone(l.state.created)
}

// degraded reports whether the launch's preparation was incomplete. An
// invalid receipt reads as degraded, which never counts.
func (l settledLaunch) degraded() bool {
	return l.state == nil || l.state.completion != launchComplete
}

// degradations names what the launch's preparation skipped, for logs.
func (l settledLaunch) degradations() launchDegradations {
	if l.state == nil {
		return 0
	}
	return l.state.degradations
}
