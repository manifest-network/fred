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
