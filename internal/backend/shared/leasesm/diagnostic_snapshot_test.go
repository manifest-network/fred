package leasesm

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/backend"
)

func TestDiagnosticSnapshot_CopiesReasonMessage(t *testing.T) {
	p := &ProvisionState{LeaseUUID: "l1", LastError: "verbose host path", Reason: backend.ReasonContainerExited, Message: "container exited unexpectedly"}
	e := DiagnosticSnapshot(p)
	assert.Equal(t, "verbose host path", e.Error) // operator field unchanged
	assert.Equal(t, backend.ReasonContainerExited, e.Reason)
	assert.Equal(t, "container exited unexpectedly", e.Message)
}

func TestDiagnosticSnapshot_CopiesLifecycleGeneration(t *testing.T) {
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	p := &ProvisionState{
		CallbackURL:          "https://fred.example/callbacks/provision?operation_id=" + lifecycleID,
		LifecycleCallbackURL: "https://fred.example/callbacks/provision?lifecycle_id=" + lifecycleID,
	}

	entry := DiagnosticSnapshot(p)

	assert.Equal(t, &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   lifecycleID,
	}, entry.LifecycleGeneration)
}
