package placement

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestPlacementCapabilityFormattingRedactsNestedAuthority(t *testing.T) {
	operationID := requireOperationID(t, "941")
	pair := testCallbackPair(operationID)
	request, err := newBackendRequestSnapshot(
		"tenant-private", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-private", Quantity: 1, ServiceName: "service-private"}},
	)
	require.NoError(t, err)
	metadata := AttemptMetadata{
		operationID: operationID, operationKind: operation.KindRestore,
		restoreSourceLeaseUUID: "source-private", requestSnapshot: request,
		callbackPair: pair,
	}
	require.True(t, metadata.Valid())

	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	maintenance := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("payload-private"),
	).Command()
	require.True(t, maintenance.Valid())

	tests := []struct {
		name       string
		value      any
		diagnostic string
		sensitive  []string
	}{
		{
			name: "callback pair", value: pair, diagnostic: callbackPairDiagnostic,
			sensitive: []string{pair.OperationURL(), pair.LifecycleURL(), operationID.String()},
		},
		{
			name: "attempt metadata", value: metadata, diagnostic: attemptMetadataDiagnostic,
			sensitive: []string{
				pair.OperationURL(), pair.LifecycleURL(), operationID.String(),
				"source-private", "tenant-private", freshTestProviderUUID, "sku-private",
			},
		},
		{
			name: "maintenance command", value: maintenance,
			diagnostic: maintenanceCommandDiagnostic,
			sensitive: []string{
				maintenance.CallbackURL(), maintenance.ID().String(), maintenance.LeaseUUID(),
				"payload-private", freshTestProviderUUID,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := formattedAndLoggedPlacementCapability(test.value)
			assert.Contains(t, output, test.diagnostic)
			for _, sensitive := range test.sensitive {
				assert.NotEmpty(t, sensitive)
				assert.NotContains(t, output, sensitive)
			}
		})
	}
}

func formattedAndLoggedPlacementCapability(value any) string {
	var output strings.Builder
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X"} {
		output.WriteString(fmt.Sprintf(format, value))
		output.WriteByte('\n')
	}
	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("capability", "value", value)
	output.WriteString(logged.String())
	return output.String()
}
