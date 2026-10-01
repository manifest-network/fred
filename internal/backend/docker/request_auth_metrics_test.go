package docker

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const nextRequestKey = "next-request-key-0123456789abcdef!!"

func TestConfig_ValidateCallbackSecretNext(t *testing.T) {
	cfg := validConfig()
	cfg.CallbackSecretNext = nextRequestKey
	require.NoError(t, cfg.Validate())
	keys, err := cfg.RequestKeys()
	require.NoError(t, err)
	assert.True(t, keys.HasRotation())

	cfg.CallbackSecretNext = "short"
	assert.ErrorContains(t, cfg.Validate(), "callback_secret_next must be at least")
	cfg.CallbackSecretNext = config.RotationSecret(cfg.CallbackSecret + "\x00")
	assert.ErrorContains(t, cfg.Validate(), "callback_secret_next must differ from callback_secret",
		"a zero-padded copy is the same HMAC key")
}

// Not parallel: these read process-global counters.
func TestRecordRequestSignatureCountsSlotsAndReasons(t *testing.T) {
	current := requestSignatureKeyTotal.WithLabelValues(requestKeySlotCurrent)
	next := requestSignatureKeyTotal.WithLabelValues(requestKeySlotNext)
	currentBefore, nextBefore := promtestutil.ToFloat64(current), promtestutil.ToFloat64(next)
	RecordRequestSignatureChecked(hmacauth.KeySlotCurrent, nil)
	RecordRequestSignatureChecked(hmacauth.KeySlotRotation, nil)
	assert.Equal(t, currentBefore+1, promtestutil.ToFloat64(current))
	assert.Equal(t, nextBefore+1, promtestutil.ToFloat64(next))

	keys, err := hmacauth.NewVerifyKeys("current-request-key-0123456789abcd!", "")
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/restart", nil)
	refusals := map[string]func() error{
		"mismatch": func() error {
			_, err := hmacauth.VerifyRequestKeys(keys, req, nil,
				hmacauth.SignRequest(nextRequestKey, req, nil), time.Minute)
			return err
		},
		"format": func() error {
			_, err := hmacauth.VerifyRequestKeys(keys, req, nil, "garbage", time.Minute)
			return err
		},
		"expired": func() error {
			_, err := hmacauth.VerifyRequestKeys(keys, req, nil,
				hmacauth.SignWithTime(nextRequestKey, "POST", "/restart", nil, time.Now().Add(-time.Hour)), time.Minute)
			return err
		},
	}
	for reason, refuse := range refusals {
		counter := requestAuthFailuresTotal.WithLabelValues(reason)
		before := promtestutil.ToFloat64(counter)
		RecordRequestSignatureChecked(0, refuse())
		assert.Equal(t, before+1, promtestutil.ToFloat64(counter), reason)
	}
	missing := requestAuthFailuresTotal.WithLabelValues("missing")
	missingBefore := promtestutil.ToFloat64(missing)
	RecordRequestSignatureMissing()
	assert.Equal(t, missingBefore+1, promtestutil.ToFloat64(missing))

	otherBefore := promtestutil.ToFloat64(requestAuthFailuresTotal.WithLabelValues("mismatch"))
	RecordRequestSignatureChecked(0, errors.New("not a verification failure"))
	assert.Equal(t, otherBefore, promtestutil.ToFloat64(requestAuthFailuresTotal.WithLabelValues("mismatch")),
		"an error that is not a signature verdict is not counted")
}

func TestInitRequestKeyMetricsReportsAConfiguredNextKey(t *testing.T) {
	current := validConfig().CallbackSecret
	rotating, err := hmacauth.NewVerifyKeys(string(current), nextRequestKey)
	require.NoError(t, err)
	InitRequestKeyMetrics(rotating)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(requestNextKeyConfigured))
	single, err := hmacauth.NewVerifyKeys(string(current), "")
	require.NoError(t, err)
	InitRequestKeyMetrics(single)
	assert.Equal(t, 0.0, promtestutil.ToFloat64(requestNextKeyConfigured))
}
