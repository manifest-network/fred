package callbackwire

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservationFormattingRedactsCallbackContents(t *testing.T) {
	const leaseUUID = "d144291f-a36f-47a4-8ccf-48afe590e29d"
	body := []byte(`{"lease_uuid":"` + leaseUUID + `","status":"failed","error":"private backend failure","backend":"backend-private","backend_storage_id":"` + testStorageID + `"}`)
	proof := verifiedCallback(
		t, "/callbacks/provision?operation_id="+testOperation, body, testStorageID,
	)
	observation, err := DecodeVerified(proof)
	require.NoError(t, err)

	output := formattedAndLoggedObservation(observation)
	assert.Contains(t, output, observationDiagnostic)
	for _, sensitive := range []string{
		leaseUUID, "private backend failure", "backend-private",
		testStorageID, testOperation,
	} {
		assert.NotContains(t, output, sensitive)
	}
}

func formattedAndLoggedObservation(value Observation) string {
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
