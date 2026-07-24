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
	ReasonContainerExited        Reason = "ContainerExited"
	ReasonImagePullFailed        Reason = "ImagePullFailed"
	ReasonInternal               Reason = "Internal"
	ReasonRestartFailed          Reason = "RestartFailed"
	ReasonUpdateFailed           Reason = "UpdateFailed"
	ReasonRestoreFailed          Reason = "RestoreFailed"
	ReasonVolumeCleanupExhausted Reason = "VolumeCleanupExhausted"
	ReasonCleanupFailed          Reason = "CleanupFailed"
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
	MsgVolumeCleanupExhausted = "volume cleanup exhausted"
	MsgCleanupFailed          = "cleanup failed"
)
