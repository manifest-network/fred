package manifest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthTimingCase is one wire value for a health_check timing field and the
// admission verdict it must get (ENG-1127).
type healthTimingCase struct {
	name     string
	wire     string // JSON literal: a quoted Go duration string or bare nanoseconds
	admitted bool
	override time.Duration // what Override yields; zero means "use the default"
}

func healthTimingCases() []healthTimingCase {
	return []healthTimingCase{
		{"string zero", `"0s"`, true, 0},
		{"string bare zero", `"0"`, true, 0},
		{"string negative", `"-5s"`, false, 0},
		{"string 1ns", `"1ns"`, false, 0},
		{"string just below 1ms", `"999999ns"`, false, 0},
		{"string just below 1ms in us", `"999us"`, false, 0},
		{"string exactly 1ms", `"1ms"`, true, time.Millisecond},
		{"string exactly 1ms in us", `"1000us"`, true, time.Millisecond},
		{"string larger", `"1m30s"`, true, 90 * time.Second},
		{"numeric zero", `0`, true, 0},
		{"numeric negative", `-1`, false, 0},
		{"numeric 1ns", `1`, false, 0},
		{"numeric just below 1ms", `999999`, false, 0},
		{"numeric exactly 1ms", `1000000`, true, time.Millisecond},
		{"numeric larger", `30000000000`, true, 30 * time.Second},
	}
}

func healthTimingPayload(field, wire string) []byte {
	return fmt.Appendf(nil, `{"services":{"web":{"image":"nginx:1","health_check":{"test":["CMD","true"],%q:%s}}}}`, field, wire)
}

func TestHealthDurationAdmissionBounds(t *testing.T) {
	require.Equal(t, time.Millisecond, container.MinimumDuration, "the bound is Docker's own constant")
	for _, field := range []string{"interval", "timeout", "start_period"} {
		for _, tc := range healthTimingCases() {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				payload := healthTimingPayload(field, tc.wire)
				_, err := ParsePayload(payload)
				if tc.admitted {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					assert.ErrorContains(t, err, "invalid health_check: "+field+" must")
				}
				// Flat (legacy) payloads take the same admission path.
				flat := fmt.Appendf(nil, `{"image":"nginx:1","health_check":{"test":["CMD","true"],%q:%s}}`, field, tc.wire)
				_, flatErr := ParsePayload(flat)
				assert.Equal(t, tc.admitted, flatErr == nil, "flat admission must agree: %v", flatErr)
			})
		}
	}
}

// Releases persisted before ENG-1127 may carry timing admission now rejects.
// Recovery decodes them, re-encodes them byte-identically (historical replay
// compares canonical JSON), and reads them only through Override, which never
// yields a value Docker would refuse.
func TestHealthDurationStoredHistoryDecodesAndMapsSafely(t *testing.T) {
	for _, field := range []string{"interval", "timeout", "start_period"} {
		for _, tc := range healthTimingCases() {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				stack, err := ParseStoredPayload(healthTimingPayload(field, tc.wire))
				require.NoError(t, err, "stored history must not reapply current admission")
				hc := stack.Services["web"].HealthCheck
				timing := map[string]HealthDuration{
					"interval": hc.Interval, "timeout": hc.Timeout, "start_period": hc.StartPeriod,
				}[field]

				value, ok := timing.Override()
				assert.Equal(t, tc.override, value)
				assert.Equal(t, tc.override != 0, ok)
				if ok {
					assert.GreaterOrEqual(t, value, container.MinimumDuration)
				}

				// Canonical re-encoding is what the previous Duration produced.
				var legacy Duration
				require.NoError(t, json.Unmarshal([]byte(tc.wire), &legacy))
				encoded, err := json.Marshal(hc)
				require.NoError(t, err)
				if legacy == 0 {
					assert.NotContains(t, string(encoded), field, "zero stays omitted")
				} else {
					legacyJSON, err := json.Marshal(legacy)
					require.NoError(t, err)
					assert.Contains(t, string(encoded), fmt.Sprintf("%q:%s", field, legacyJSON))
				}
				reparsed, err := ParseStoredPayload(fmt.Appendf(nil,
					`{"services":{"web":{"image":"nginx:1","health_check":%s}}}`, encoded))
				require.NoError(t, err)
				assert.Equal(t, *hc, *reparsed.Services["web"].HealthCheck, "round trip is lossless")
			})
		}
	}
}

func TestHealthDurationZeroValueUsesDefault(t *testing.T) {
	var timing HealthDuration
	value, ok := timing.Override()
	assert.False(t, ok)
	assert.Zero(t, value)
	assert.True(t, timing.IsZero())
	assert.NoError(t, timing.admissionError("interval"))
}

func TestHealthDurationRejectsMalformedWireInBothPaths(t *testing.T) {
	for _, wire := range []string{`"abc"`, `"5"`, `1.5`, `true`, `{}`} {
		t.Run(wire, func(t *testing.T) {
			payload := healthTimingPayload("interval", wire)
			_, err := ParsePayload(payload)
			require.Error(t, err)
			_, err = ParseStoredPayload(payload)
			require.Error(t, err, "malformed timing was never admissible, so no history can carry it")
		})
	}
}
