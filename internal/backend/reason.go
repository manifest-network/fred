package backend

// Reason is a stable, machine-readable failure-category code surfaced to
// tenants alongside a human message (K8s condition Reason shape). It is
// AUTHORED at the failure source, never parsed from the human message
// (which may be dynamically composed). A defined type so a stray verbose
// string (e.g. err.Error()) is a compile error at an authoring site.
//
// This is an OPEN, add-only set: consumers MUST tolerate an unrecognized
// value and fall back to the human message. Distinct from
// leasesm.ContainerExitInfo.Reason, which is the raw substrate termination
// reason (e.g. "OOMKilled").
type Reason string

const (
	ReasonContainerExited Reason = "ContainerExited"
	// ReasonHealthCheckFailed marks a container whose health check never passed
	// during startup verification: Docker reported it unhealthy, or it was still
	// not healthy at the startup deadline. It is never counted toward the
	// terminal failure budget (ENG-1125).
	ReasonHealthCheckFailed Reason = "HealthCheckFailed"
	// ReasonContainerStartFailed marks a provision container that the
	// container runtime refused to start: Docker answered its start request
	// with an error and the container never ran (for example, an entrypoint
	// that does not exist in the image). It is never counted toward the
	// terminal failure budget (ENG-1125).
	ReasonContainerStartFailed   Reason = "ContainerStartFailed"
	ReasonImagePullFailed        Reason = "ImagePullFailed"
	ReasonInternal               Reason = "Internal"
	ReasonRestartFailed          Reason = "RestartFailed"
	ReasonUpdateFailed           Reason = "UpdateFailed"
	ReasonRestoreFailed          Reason = "RestoreFailed"
	ReasonVolumeCleanupExhausted Reason = "VolumeCleanupExhausted"
	ReasonCleanupFailed          Reason = "CleanupFailed"
	// ReasonBackendStorageLost marks a lease whose backend an operator
	// retired as irrecoverably lost; it is authored by the placement store.
	ReasonBackendStorageLost Reason = "BackendStorageLost"
	// ReasonVolumeDeletePending marks a provision refused because an earlier
	// deletion of the lease's own volume has not finished yet; the provision
	// can be retried once it has.
	ReasonVolumeDeletePending Reason = "VolumeDeletePending"
	// ReasonUnknown is the read-boundary default for a FAILED lease with no
	// authored reason (a legacy pre-upgrade record or a future-unmapped
	// path). gRPC-UNKNOWN-equivalent: "failed, cause unclassified".
	ReasonUnknown Reason = "Unknown"
)

// Curated human messages for the fixed (non-composed) reasons. Referenced at
// both the CallbackErr write site and the release-path call so the
// (reason, message) pair cannot drift. The composed restart/update messages
// use MsgRestartFailed/MsgUpdateFailed as their base + a runtime rollback
// suffix; container-exited/internal reuse leasesm.errMsg* consts.
const (
	MsgImagePullFailed        = "image pull failed"
	MsgRestartFailed          = "restart failed"
	MsgUpdateFailed           = "update failed"
	MsgRestoreFailed          = "restore failed"
	MsgVolumeCleanupExhausted = "volume cleanup exhausted"
	MsgCleanupFailed          = "cleanup failed"
	MsgBackendStorageLost     = "the backend storage holding this lease was irrecoverably lost"
	MsgVolumeDeletePending    = "an earlier deletion of this lease's volume is still finishing; retry later"
)

// Curated messages for a failed startup verification (ENG-1125). Each one is
// paired with its reason where the failure is observed; only an observed exit
// is ContainerExited.
const (
	// MsgContainerExitedDuringStartup / MsgContainerExitedDuringHealthCheck:
	// ReasonContainerExited.
	MsgContainerExitedDuringStartup     = "container exited during startup"
	MsgContainerExitedDuringHealthCheck = "container exited during health check"
	// MsgContainerUnhealthy / MsgHealthCheckDeadline: ReasonHealthCheckFailed.
	MsgContainerUnhealthy  = "container reported unhealthy"
	MsgHealthCheckDeadline = "container did not become healthy before the startup deadline"
	// MsgContainerStartRefused: ReasonContainerStartFailed.
	MsgContainerStartRefused = "container runtime refused to start the container"
	// MsgStartupUnverified / MsgStartupCanceled: ReasonInternal. Neither is an
	// observation of the tenant's workload.
	MsgStartupUnverified = "container startup could not be verified"
	MsgStartupCanceled   = "container startup verification canceled"
)
