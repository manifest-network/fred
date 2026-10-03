package failurecause

// Cause is the sealed attribution of one recorded failure. The zero value is
// the unknown cause, which never counts.
type Cause struct{ kind causeKind }

type causeKind uint8

const (
	causeUnknown causeKind = iota
	causeTenantWorkload
	causeDisruption
	causePlatform
	causeMaintenance
	causeUnhealthy
	// causeSentinel bounds the closed set. It is never a valid cause.
	causeSentinel
)

// Platform attributes a failure to the platform: an internal error, a running
// cohort that diverged from its durable release, a refusal before any substrate
// effect (image admission or pull included), or a startup failure of a launch
// the platform degraded. It never counts.
func Platform() Cause { return Cause{kind: causePlatform} }

// Maintenance attributes the outcome of a restart, update or restore, whether
// it rolled back or not. It never counts, just as a failed Kubernetes rollout
// only sets a condition.
func Maintenance() Cause { return Cause{kind: causeMaintenance} }

// Unhealthy attributes a startup failure whose health check never passed: the
// workload is still running, so no exit of the tenant's process was observed.
// It never counts (ENG-1125), although Kubernetes would restart such a
// container under its own policy.
func Unhealthy() Cause { return Cause{kind: causeUnhealthy} }

// Counts reports whether this failure consumes the terminal budget. It is an
// allowlist: only an observed exit of the tenant's own workload counts.
func (c Cause) Counts() bool { return c.kind == causeTenantWorkload }

// Label is the bounded metric label of this cause. A value outside the closed
// set reads as "unknown".
func (c Cause) Label() string {
	switch c.kind {
	case causeTenantWorkload:
		return "tenant_workload"
	case causeDisruption:
		return "disruption"
	case causePlatform:
		return "platform"
	case causeMaintenance:
		return "maintenance"
	case causeUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// Labels returns every cause label in a fixed order, so a metric can be
// pre-initialized with exactly the closed set.
func Labels() []string {
	labels := make([]string, 0, int(causeSentinel))
	for kind := causeUnknown; kind < causeSentinel; kind++ {
		labels = append(labels, Cause{kind: kind}.Label())
	}
	return labels
}

// Termination is the substrate's positive observation of how a dead instance
// ended. The substrate's own adapter mints it while translating its
// inspection, because only the substrate knows which of its terminal states
// is an observed workload exit (a Kubernetes container terminated with an exit
// code is Exited even when its phase reads failed). The zero value is an
// unknown termination, which never counts.
type Termination struct{ kind terminationKind }

type terminationKind uint8

const (
	terminationUnknown terminationKind = iota
	terminationExited
	terminationGone
)

// Exited observes an instance that exited and reported its exit status. Any
// status counts as the workload's own exit, including 0, 137 and 143, because
// provenance, not the status, rules out an external signal. A kill by the OOM
// killer counts too. Residual: Docker's OOMKilled reflects the cgroup-v2
// oom_kill counter, which counts kills by any OOM killer, so the victim of a
// host-wide OOM is indistinguishable from a workload exceeding its own limit
// and counts as well.
func Exited() Termination { return Termination{kind: terminationExited} }

// Gone observes an instance that is being removed, is dead, or is positively
// absent. Its end was not observed as a workload exit, so it never counts.
func Gone() Termination { return Termination{kind: terminationGone} }

// ClassifyDeath attributes one observed death of instanceID. It is the only
// constructor of a counting Cause: the death must have been delivered live by
// an event stream that observed the whole run and no API signal to it, and the
// substrate must have observed an exit with a status. A provenance minted for
// another instance is ignored, as if the death had no live event.
func ClassifyDeath(instanceID string, provenance Provenance, termination Termination) Cause {
	if instanceID == "" || provenance.instanceID != instanceID {
		provenance = Provenance{}
	}
	switch {
	case provenance.kind == provenanceSignaled, termination.kind == terminationGone:
		return Cause{kind: causeDisruption}
	case provenance.kind == provenanceObservedRun && termination.kind == terminationExited:
		return Cause{kind: causeTenantWorkload}
	default:
		return Cause{}
	}
}
