package docker

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTrimLogToBudget_DoesNotSplitMultibyteRune pins the code-review finding on
// #198: trimLogToBudget truncates tenant-controlled log output at a raw byte
// offset, which can split a multi-byte UTF-8 rune and leave the tail invalid
// UTF-8 (silently mangled to U+FFFD downstream). It must back off to a rune
// boundary before cutting — the same idiom as shared/partition.go's
// TruncatePartitionRaw.
func TestTrimLogToBudget_DoesNotSplitMultibyteRune(t *testing.T) {
	// Ten 3-byte runes (rune boundaries at every multiple of 3).
	logs := strings.Repeat("世", 10) // 30 bytes
	// remaining=8 lands inside the third rune (bytes 6,7,8) — a raw slice at 8
	// would split it.
	out, consumed := trimLogToBudget(logs, 8)

	assert.True(t, utf8.ValidString(out),
		"truncated output must be valid UTF-8 (must not split a multi-byte rune)")
	assert.LessOrEqual(t, consumed, 8, "must not consume more than the budget")
	assert.Equal(t, 6, consumed, "should back off to the rune boundary at byte 6")
	assert.Equal(t, strings.Repeat("世", 2)+"\n"+aggregateLogLimitMessage, out)
}

// TestTrimLogToBudget_FirstRuneExceedsBudget pins the follow-up finding on #198:
// when the remaining budget is smaller than the first rune, the rune-boundary
// backoff drives end to 0. It must then CONSUME the remaining budget (so callers
// see remaining reach <=0 and stop fetching further containers) and emit just
// the marker — not consumed=0 (budget stuck, reads never skipped) with a
// content-less leading "\n".
func TestTrimLogToBudget_FirstRuneExceedsBudget(t *testing.T) {
	logs := strings.Repeat("世", 5) // first rune is 3 bytes
	// remaining=2 is smaller than the 3-byte first rune: not even one rune fits.
	out, consumed := trimLogToBudget(logs, 2)

	assert.Equal(t, 2, consumed,
		"must consume the remaining budget when not even one rune fits (so callers stop fetching)")
	assert.True(t, utf8.ValidString(out), "output must be valid UTF-8")
	assert.Equal(t, aggregateLogLimitMessage, out,
		"no content fit; emit just the marker, not a content-less leading newline")
}

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
	require.GreaterOrEqual(t, len(out), maxContainerLogBytes,
		"truncated output must retain a full cap's worth of bytes before the marker")

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
