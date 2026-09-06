package lifecycle

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteFormattingRedactsCallbackCapability(t *testing.T) {
	factory, err := NewRouteFactory("https://private.example/proxy?opaque=secret")
	require.NoError(t, err)
	id := mustTestID(t, canonicalTestID)
	route, err := factory.For(id)
	require.NoError(t, err)

	output := formattedAndLoggedRoute(route)
	assert.Contains(t, output, routeDiagnostic)
	for _, sensitive := range []string{route.URL(), id.String(), "private.example", "opaque=secret"} {
		assert.NotContains(t, output, sensitive)
	}
}

func formattedAndLoggedRoute(value Route) string {
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
