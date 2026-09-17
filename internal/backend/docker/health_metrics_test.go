package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthTimingIncludesFailedIdentityWithoutSamplingSkippedStages(t *testing.T) {
	identityFailure := errors.New("storage identity unavailable")
	pinged := false
	b := &Backend{docker: &mockDockerClient{PingFn: func(context.Context) error {
		pinged = true
		return nil
	}}}
	installMutationTestVerifier(t, b, func(context.Context) error { return identityFailure })
	identityBefore := backendHealthTimingSamples(t, backendHealthDuration, string(healthStageIdentity))
	totalBefore := backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageTotal))
	pingBefore := backendHealthTimingSamples(t, backendHealthDuration, string(healthStageDocker))
	require.ErrorIs(t, b.Health(t.Context()), identityFailure)
	assert.False(t, pinged)
	assert.Equal(t, identityBefore+1, backendHealthTimingSamples(t, backendHealthDuration, string(healthStageIdentity)))
	assert.Equal(t, totalBefore, backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageTotal)),
		"the fake verifier does not invoke the production verification stages")
	assert.Equal(t, pingBefore, backendHealthTimingSamples(t, backendHealthDuration, string(healthStageDocker)),
		"a skipped stage must not look like a completed zero-duration probe")
}

func TestStorageIdentityTimingCoversProductionAndCallbackInvocations(t *testing.T) {
	// Invalid authority fails inside the real production verifier, before any
	// substrate access. Both entry paths must still record exactly one total;
	// an outer rejection that never invokes it must record none.
	production := productionDockerStorageIdentityVerifier{backend: &Backend{}}
	callback := dockerCallbackStorageVerifier{verifier: production}
	for _, test := range []struct {
		name         string
		verify       func(context.Context) error
		totalSamples uint64
	}{
		{name: "production", verify: production.Verify, totalSamples: 1},
		{name: "callback", verify: callback.Verify, totalSamples: 1},
		{name: "outer prevalidation", verify: (&Backend{}).VerifyStorageIdentity},
	} {
		t.Run(test.name, func(t *testing.T) {
			totalBefore := backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageTotal))
			lockBefore := backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageLock))
			require.Error(t, test.verify(t.Context()))
			assert.Equal(t, totalBefore+test.totalSamples,
				backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageTotal)))
			assert.Equal(t, lockBefore,
				backendHealthTimingSamples(t, storageIdentityDuration, string(identityStageLock)),
				"authority rejection cannot sample an unexecuted stage")
		})
	}
}

func backendHealthTimingSamples(t *testing.T, histogram *prometheus.HistogramVec, stage string) uint64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, histogram.WithLabelValues(stage).(prometheus.Metric).Write(&metric))
	return metric.GetHistogram().GetSampleCount()
}
