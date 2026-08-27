package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestResolveCallbackApplicationTimeout_PreservesDeliveryDeadline(t *testing.T) {
	tests := []struct {
		name       string
		timeout    time.Duration
		wantErr    bool
		wantChosen time.Duration
	}{
		{
			name:       "zero selects protocol default",
			wantChosen: backend.DefaultCallbackApplicationTimeout,
		},
		{
			name:       "negative selects protocol default",
			timeout:    -time.Nanosecond,
			wantChosen: backend.DefaultCallbackApplicationTimeout,
		},
		{
			name:       "protocol default is accepted",
			timeout:    backend.DefaultCallbackApplicationTimeout,
			wantChosen: backend.DefaultCallbackApplicationTimeout,
		},
		{
			name:       "shorter application deadline is accepted",
			timeout:    backend.DefaultCallbackApplicationTimeout - time.Nanosecond,
			wantChosen: backend.DefaultCallbackApplicationTimeout - time.Nanosecond,
		},
		{
			name:    "above protocol default is rejected even below delivery deadline",
			timeout: backend.DefaultCallbackApplicationTimeout + time.Nanosecond,
			wantErr: true,
		},
		{
			name:    "delivery deadline is rejected",
			timeout: backend.DefaultCallbackDeliveryTimeout,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chosen, err := resolveCallbackApplicationTimeout(tc.timeout)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "must not exceed protocol default")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantChosen, chosen)
		})
	}
}

func TestNewServer_RejectsCallbackApplicationTimeoutAtDeliveryDeadline(t *testing.T) {
	server, err := NewServer(ServerConfig{
		ProviderUUID:               "provider-1",
		Bech32Prefix:               "manifest",
		RateLimitRPS:               100,
		RateLimitBurst:             200,
		CallbackApplicationTimeout: backend.DefaultCallbackDeliveryTimeout,
	}, ServerDeps{})
	require.Error(t, err)
	assert.Nil(t, server)
	assert.ErrorContains(t, err, "must not exceed protocol default")
}
