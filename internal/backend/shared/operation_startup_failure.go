package shared

import (
	"errors"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// OperationStartupFailure is the sealed account of one definite provision
// startup failure (ENG-1125): the provision's launch exchange settled, a
// container of its cohort then positively failed startup, and the attempt was
// rolled back exactly. It carries what the lease state machine needs to
// attribute the failure (what was observed, the substrate's termination of the
// container, the live provenance of its death, and whether the platform
// degraded the launch), never a cause: attribution happens in the state
// machine's provision-failure entry action. The zero value is invalid.
type OperationStartupFailure struct{ state *operationStartupFailureState }

type operationStartupFailureState struct {
	kind        OperationStartupFailureKind
	reason      backend.Reason
	message     string
	detail      string
	instanceID  string
	service     string
	termination failurecause.Termination
	provenance  failurecause.Provenance
	exitCode    int
	oomKilled   bool
	degraded    bool
}

// OperationStartupFailureKind is the closed account of what a definite startup
// failure observed. NewOperationStartupFailure derives it from the validated
// terms; nothing else produces one.
type OperationStartupFailureKind uint8

const (
	// OperationStartupExited: the container exited and the substrate reported
	// its exit status (ContainerExited).
	OperationStartupExited OperationStartupFailureKind = iota + 1
	// OperationStartupUnhealthy: the container is running and its health check
	// never passed (HealthCheckFailed).
	OperationStartupUnhealthy
	// OperationStartupRefused: the container runtime refused to start the
	// container, which never ran (ContainerStartFailed).
	OperationStartupRefused
)

// OperationStartupFailureTerms are the observed facts of one startup failure.
// NewOperationStartupFailure validates them as a whole.
type OperationStartupFailureTerms struct {
	// Reason and Message are the curated tenant surface authored where the
	// failure was observed: ContainerExited for an exit, HealthCheckFailed
	// for a health check that never passed, ContainerStartFailed for a start
	// the container runtime refused.
	Reason  backend.Reason
	Message string
	// Detail is operator-side text (exit status, logs); never tenant-facing.
	Detail     string
	InstanceID string
	Service    string
	// Termination is the substrate adapter's classification of an exited
	// container: failurecause.Exited for an exit with a status. An unhealthy
	// container is still running and has none.
	Termination failurecause.Termination
	// Provenance is the live event stream's account of the container's death,
	// bound to InstanceID, or zero when none was observed.
	Provenance failurecause.Provenance
	ExitCode   int
	OOMKilled  bool
	// Degraded marks a launch the platform did not complete as planned (for
	// example, writable-path seeding skipped): its failure never counts.
	Degraded bool
}

// NewOperationStartupFailure seals one startup failure. An exit must carry the
// substrate's Exited termination (the exit code alone proves nothing: it is an
// int). An unhealthy container and a container whose start was refused must
// carry neither a termination nor a death provenance: neither died. A
// provenance must have been minted for this very container.
func NewOperationStartupFailure(terms OperationStartupFailureTerms) (OperationStartupFailure, error) {
	if terms.InstanceID == "" || terms.Message == "" {
		return OperationStartupFailure{}, errors.New("startup failure requires its container and curated message")
	}
	if terms.Provenance != (failurecause.Provenance{}) && terms.Provenance.InstanceID() != terms.InstanceID {
		return OperationStartupFailure{}, errors.New("startup failure provenance was minted for another container")
	}
	noDeath := terms.Termination == (failurecause.Termination{}) && terms.Provenance == (failurecause.Provenance{})
	var kind OperationStartupFailureKind
	switch terms.Reason {
	case backend.ReasonContainerExited:
		if terms.Termination != failurecause.Exited() {
			return OperationStartupFailure{}, errors.New("an exited startup failure requires an observed exit")
		}
		kind = OperationStartupExited
	case backend.ReasonHealthCheckFailed:
		if !noDeath {
			return OperationStartupFailure{}, errors.New("an unhealthy startup failure is a running container with no death")
		}
		kind = OperationStartupUnhealthy
	case backend.ReasonContainerStartFailed:
		if !noDeath || terms.ExitCode != 0 || terms.OOMKilled {
			return OperationStartupFailure{}, errors.New("a refused start is a container that never ran")
		}
		kind = OperationStartupRefused
	default:
		return OperationStartupFailure{}, errors.New("a startup failure is ContainerExited, HealthCheckFailed or ContainerStartFailed")
	}
	return OperationStartupFailure{state: &operationStartupFailureState{
		kind: kind, reason: terms.Reason, message: terms.Message, detail: terms.Detail,
		instanceID: terms.InstanceID, service: terms.Service,
		termination: terms.Termination, provenance: terms.Provenance,
		exitCode: terms.ExitCode, oomKilled: terms.OOMKilled, degraded: terms.Degraded,
	}}, nil
}

func (f OperationStartupFailure) Valid() bool { return f.state != nil }

// Kind is what the failure observed; zero for the invalid value.
func (f OperationStartupFailure) Kind() OperationStartupFailureKind {
	if f.state == nil {
		return 0
	}
	return f.state.kind
}

func (f OperationStartupFailure) Reason() backend.Reason {
	if f.state == nil {
		return ""
	}
	return f.state.reason
}

func (f OperationStartupFailure) Message() string {
	if f.state == nil {
		return ""
	}
	return f.state.message
}

func (f OperationStartupFailure) Detail() string {
	if f.state == nil {
		return ""
	}
	return f.state.detail
}

func (f OperationStartupFailure) InstanceID() string {
	if f.state == nil {
		return ""
	}
	return f.state.instanceID
}

func (f OperationStartupFailure) Service() string {
	if f.state == nil {
		return ""
	}
	return f.state.service
}

func (f OperationStartupFailure) Termination() failurecause.Termination {
	if f.state == nil {
		return failurecause.Termination{}
	}
	return f.state.termination
}

func (f OperationStartupFailure) Provenance() failurecause.Provenance {
	if f.state == nil {
		return failurecause.Provenance{}
	}
	return f.state.provenance
}

// ExitStatus reports the observed exit code and OOM flag, which only an exit
// has.
func (f OperationStartupFailure) ExitStatus() (code int, oomKilled, exited bool) {
	if f.state == nil || f.state.termination != failurecause.Exited() {
		return 0, false, false
	}
	return f.state.exitCode, f.state.oomKilled, true
}

func (f OperationStartupFailure) Degraded() bool { return f.state != nil && f.state.degraded }

type operationStartupFailedState struct {
	subject OperationPhysicalSubject
	failure OperationStartupFailure
}

// OperationStartupFailed is classifier evidence that a live provision's
// startup failed definitely and its exact substrate is gone: the classifier
// re-read the strict inventory, the launch-debt journal and the volume
// namespace before minting it. It is valid only for the live execution of a
// provision; recovery can never mint or consume it.
type OperationStartupFailed struct{ state *operationStartupFailedState }

func (e OperationStartupFailed) validForOperation(subject OperationPhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && e.state.failure.Valid() &&
		liveProvisionSubject(subject)
}

func (e OperationStartupFailed) Valid() bool {
	return e.state != nil && e.validForOperation(e.state.subject)
}

// liveProvisionSubject reports whether subject was minted for the live
// execution of a provision: not recovery, not a historical receipt, not a
// restore.
func liveProvisionSubject(subject OperationPhysicalSubject) bool {
	return subject.Valid() && subject.state.mode == operationPhysicalExecution &&
		subject.Intent().Kind() == OperationIntentProvision
}

// NewOperationStartupFailed seals classifier evidence of a definite live
// startup failure for the exact subject.
func NewOperationStartupFailed(
	subject OperationPhysicalSubject,
	failure OperationStartupFailure,
) (OperationPhysicalEvidence, error) {
	if !liveProvisionSubject(subject) {
		return OperationPhysicalEvidence{}, errors.New("startup failure evidence requires a live provision execution")
	}
	if !failure.Valid() {
		return OperationPhysicalEvidence{}, errors.New("startup failure evidence requires a sealed startup failure")
	}
	return OperationPhysicalEvidence{
		kind:          operationPhysicalEvidenceStartupFailed,
		startupFailed: OperationStartupFailed{state: &operationStartupFailedState{subject: subject, failure: failure}},
	}, nil
}
