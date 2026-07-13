package docker

import (
	"bytes"
	"strings"
	"testing"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// multiplexedLog encodes s as a Docker non-TTY stdout frame stream — the same
// multiplexed format the daemon returns to ContainerLogs.
func multiplexedLog(t *testing.T, s string) *bytes.Buffer {
	t.Helper()
	var stream bytes.Buffer
	w := stdcopy.NewStdWriter(&stream, stdcopy.Stdout)
	_, err := w.Write([]byte(s))
	require.NoError(t, err)
	return &stream
}

// TestDemuxContainerLogs_TruncatesOversizedOutput verifies that a container's
// (tenant-controlled) output larger than the cap is not buffered in full: the
// result is bounded to maxContainerLogBytes and carries a truncation marker.
// Without this a container emitting huge output could OOM the provider via
// GET /logs (ENG-499).
func TestDemuxContainerLogs_TruncatesOversizedOutput(t *testing.T) {
	huge := strings.Repeat("A", maxContainerLogBytes+(2<<20)) // cap + 2 MiB

	out, err := demuxContainerLogs(multiplexedLog(t, huge))
	require.NoError(t, err)

	assert.LessOrEqual(t, len(out), maxContainerLogBytes+64,
		"output must be bounded to the cap plus a short marker")
	assert.Contains(t, out, "truncated", "truncated output must carry a marker")
	assert.Equal(t, strings.Repeat("A", maxContainerLogBytes), out[:maxContainerLogBytes],
		"the retained prefix must be the container's real output")
}

// TestDemuxContainerLogs_PassesSmallOutputUnchanged verifies output under the
// cap is returned verbatim with no marker.
func TestDemuxContainerLogs_PassesSmallOutputUnchanged(t *testing.T) {
	out, err := demuxContainerLogs(multiplexedLog(t, "hello\nworld\n"))
	require.NoError(t, err)
	assert.Equal(t, "hello\nworld\n", out)
	assert.NotContains(t, out, "truncated")
}
