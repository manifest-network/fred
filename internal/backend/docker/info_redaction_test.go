package docker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// Every curated failure message must be host-path-free.
func TestReasonMessages_NoHostPathText(t *testing.T) {
	for _, m := range []string{
		leasesm.ErrMsgContainerExited, leasesm.ErrMsgInternal,
		backend.MsgRestartFailed, backend.MsgImagePullFailed, backend.MsgUpdateFailed,
		backend.MsgVolumeCleanupExhausted, backend.MsgCleanupFailed,
	} {
		assert.False(t, strings.Contains(m, "/"), "message %q contains a path separator", m)
	}
}

// Property: an arbitrary verbose LastError with a host-path sentinel never
// appears in the tenant DTO (provisionToInfo drops it; only Reason/Message ship).
func TestProvisionToInfo_VerboseNeverInDTO(t *testing.T) {
	const sentinel = "/data/fred/volumes/SENTINEL"
	prov := &provision{ProvisionState: leasesm.ProvisionState{
		Status:    backend.ProvisionStatusFailed,
		LastError: "quota " + sentinel + " exit 1",
		Reason:    backend.ReasonContainerExited,
		Message:   "container exited unexpectedly",
	}}
	info := provisionToInfo(prov, "docker-1")
	b, err := json.Marshal(info)
	assert.NoError(t, err)
	assert.NotContains(t, string(b), sentinel)
}

func TestProvisionToInfoReportsPersistedLifecycleGenerationWithoutURL(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	prov := &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID:            "lease-1",
		Status:               backend.ProvisionStatusReady,
		CallbackURL:          "https://secret.internal/callbacks/provision?operation_id=" + id,
		LifecycleCallbackURL: "https://secret.internal/callbacks/provision?lifecycle_id=" + id,
	}}

	info := provisionToInfo(prov, "docker-1")
	assert.Equal(t, &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   id,
	}, info.LifecycleGeneration)
	encoded, err := json.Marshal(info.LifecycleGeneration)
	assert.NoError(t, err)
	assert.NotContains(t, string(encoded), "secret.internal")
	assert.NotContains(t, string(encoded), "callback")
}
