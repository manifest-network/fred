package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestPrintBackendKeyIDsReportsEveryKeyWithoutTheKeys(t *testing.T) {
	const (
		secretA   = "backend-a-secret-0123456789abcdef"
		secretB   = "backend-b-secret-0123456789abcdef"
		previousB = "backend-b-old-secret-0123456789ab"
	)
	cfg := &config.Config{Backends: []config.BackendConfig{
		{Name: "backend-a", HMACSecret: secretA},
		{Name: "backend-b", HMACSecret: secretB, HMACSecretPrevious: previousB},
	}}
	var out bytes.Buffer
	require.NoError(t, printBackendKeyIDs(&out, cfg))
	for _, secret := range []string{secretA, secretB, previousB} {
		assert.NotContains(t, out.String(), secret)
	}
	var printed struct {
		Backends []backendKeyIDs `json:"backends"`
	}
	decoder := json.NewDecoder(&out)
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&printed))
	assert.Equal(t, []backendKeyIDs{
		{Name: "backend-a", CurrentKeyID: hmacauth.KeyID(secretA)},
		{Name: "backend-b", CurrentKeyID: hmacauth.KeyID(secretB), PreviousKeyID: hmacauth.KeyID(previousB)},
	}, printed.Backends)

	legacy := &config.Config{CallbackSecret: "legacy-shared-secret-0123456789ab", Backends: []config.BackendConfig{{Name: "mock"}}}
	assert.ErrorContains(t, printBackendKeyIDs(&bytes.Buffer{}, legacy), "no per-backend HMAC secret")
}
