package backend

import (
	"strings"
	"testing"
)

func TestReasonConstants(t *testing.T) {
	cases := map[Reason]string{
		ReasonContainerExited:          "ContainerExited",
		ReasonImagePullFailed:          "ImagePullFailed",
		ReasonInternal:                 "Internal",
		ReasonRestartFailed:            "RestartFailed",
		ReasonUpdateFailed:             "UpdateFailed",
		ReasonRestoreFailed:            "RestoreFailed",
		ReasonVolumeCleanupExhausted:   "VolumeCleanupExhausted",
		ReasonCleanupFailed:            "CleanupFailed",
		ReasonVolumeDeletePending:      "VolumeDeletePending",
		ReasonVolumeDeletionInProgress: "VolumeDeletionInProgress",
		ReasonUnknown:                  "Unknown",
	}
	for r, want := range cases {
		if string(r) != want {
			t.Errorf("reason %q != %q", r, want)
		}
		// Security-relevant: CamelCase, no whitespace/punctuation/host-path chars.
		for _, c := range string(r) {
			if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
				t.Errorf("reason %q has non-CamelCase char %q", r, c)
			}
		}
	}
	// Curated messages must never embed a host path.
	for _, m := range []string{
		MsgImagePullFailed, MsgRestartFailed, MsgUpdateFailed, MsgVolumeCleanupExhausted, MsgCleanupFailed,
		MsgVolumeDeletePending, MsgVolumeDeletionInProgress,
	} {
		if strings.Contains(m, "/") {
			t.Errorf("message %q contains a path separator", m)
		}
	}
	// A provider-side deletion is nothing the tenant can retry or fix.
	for _, m := range []string{MsgVolumeDeletePending, MsgVolumeDeletionInProgress} {
		if strings.Contains(strings.ToLower(m), "retry") || strings.Contains(strings.ToLower(m), "fail") {
			t.Errorf("message %q asks the tenant to retry or reads as a failure", m)
		}
	}
}
