package docker

import "slices"

// launchCompletion is the closed account of how completely a launch was
// prepared. A degraded launch skipped part of the platform's own preparation
// (writable-path detection or seeding, or volume-owner detection), so a
// workload that then fails its startup may be failing because of the
// platform: its failure never counts (ENG-1125). The zero value is invalid.
type launchCompletion uint8

const (
	launchComplete launchCompletion = iota + 1
	launchDegraded
)

// settledLaunch is the receipt of one Compose launch whose every Create and
// Start got a final daemon response, with no business error, and whose
// launch-debt journal row was cleared through the launch step's own receipt.
// After it, no container of the launch can still appear late. It is bound to
// the exact per-execution mutation facade that launched, and carries the
// positive facts of that launch: the canonical volumes its Create reported
// created, and whether its preparation completed.
//
// Only the launch dispatch mints one (newSettledLaunch, confined by
// internal/testutil). The zero value is invalid: no startup finding can be
// minted without a real receipt.
type settledLaunch struct{ state *settledLaunchState }

type settledLaunchState struct {
	mutations  *storageMutations
	created    []string
	completion launchCompletion
}

// newSettledLaunch mints the receipt for q's launch. Only the launch dispatch
// may call it, and only once its exchange and journal row settled.
func newSettledLaunch(q *quiescedVolumes) settledLaunch {
	created := make([]string, 0, len(q.volumes))
	for name, volume := range q.volumes {
		if volume.created {
			created = append(created, name)
		}
	}
	slices.Sort(created)
	completion := launchComplete
	if q.degraded {
		completion = launchDegraded
	}
	return settledLaunch{state: &settledLaunchState{
		mutations: q.mutations, created: created, completion: completion,
	}}
}

// boundTo reports whether the receipt is real and was minted for mutations.
func (l settledLaunch) boundTo(mutations *storageMutations) bool {
	return l.state != nil && mutations != nil && l.state.mutations == mutations &&
		(l.state.completion == launchComplete || l.state.completion == launchDegraded)
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
