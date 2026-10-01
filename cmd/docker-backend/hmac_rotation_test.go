package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	rotationOldKey    = "rotation-old-key-0123456789abcdef!"
	rotationNewKey    = "rotation-new-key-0123456789abcdef!"
	rotationBackend   = "backend-a"
	rotationLeaseUUID = "d144291f-a36f-47a4-8ccf-48afe590e29d"
	rotationCallback  = "/callbacks/provision"
)

// backendRotationConfig is the docker-backend side: callback_secret signs
// callbacks and verifies requests; callback_secret_next only verifies.
type backendRotationConfig struct{ current, next string }

// providerRotationConfig is providerd's side: hmac_secret signs requests and
// verifies callbacks; hmac_secret_previous only verifies.
type providerRotationConfig struct{ current, previous string }

func (b backendRotationConfig) requestHandler(t *testing.T) http.Handler {
	t.Helper()
	cfg := docker.Config{CallbackSecret: config.Secret(b.current), CallbackSecretNext: config.RotationSecret(b.next)}
	keys, err := cfg.RequestKeys()
	require.NoError(t, err)
	return hmacAuthMiddleware(keys, slog.Default(), docker.DefaultMaxRequestBodySize)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	)
}

// callbackKeys are the keys providerd's keyring verifies this backend's
// callbacks with, built through the same config accessor production uses.
// (Importing internal/api here would link providerd's collectors into this
// binary's test and break TestMetricSurfaceIsBackendOwned; the keyring's own
// selection is tested in internal/api.)
func (p providerRotationConfig) callbackKeys(t *testing.T) hmacauth.VerifyKeys {
	t.Helper()
	cfg := config.Config{Backends: []config.BackendConfig{{
		Name: rotationBackend, HMACSecret: config.Secret(p.current), HMACSecretPrevious: config.RotationSecret(p.previous),
	}}}
	keys, err := cfg.BackendCallbackKeys(rotationBackend)
	require.NoError(t, err)
	return keys
}

// requestAccepted signs a request as providerd does, with its current key.
func requestAccepted(t *testing.T, provider providerRotationConfig, backend backendRotationConfig) bool {
	t.Helper()
	body := []byte(`{"lease_uuid":"` + rotationLeaseUUID + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/restart", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(provider.current, req, body))
	recorder := httptest.NewRecorder()
	backend.requestHandler(t).ServeHTTP(recorder, req)
	return recorder.Code == http.StatusOK
}

// callbackAccepted signs a callback as the backend does, with its current key,
// and verifies it as providerd's keyring does.
func callbackAccepted(t *testing.T, backend backendRotationConfig, provider providerRotationConfig) bool {
	t.Helper()
	body := []byte(`{"lease_uuid":"` + rotationLeaseUUID + `","status":"success"}`)
	verifier, _ := hmacauth.NewCallbackProofBoundary()
	now := time.Now()
	_, _, err := verifier.VerifyRoutedKeysWithTime(
		provider.callbackKeys(t), http.MethodPost, rotationCallback, body,
		hmacauth.SignWithTime(backend.current, http.MethodPost, rotationCallback, body, now),
		rotationBackend, rotationCallback, 5*time.Minute, time.Minute, now,
	)
	return err == nil
}

// TestHMACRotationNeverRefusesALiveMessage walks the documented four-step
// rotation. At each step one side restarts, so both its old and new
// configuration can be live at once; every message either of them signs must
// verify on the other side, and every message the other side signs must
// verify on both.
func TestHMACRotationNeverRefusesALiveMessage(t *testing.T) {
	steps := []struct {
		name      string
		backends  []backendRotationConfig
		providers []providerRotationConfig
	}{
		{
			name:      "1: backend adds callback_secret_next",
			backends:  []backendRotationConfig{{rotationOldKey, ""}, {rotationOldKey, rotationNewKey}},
			providers: []providerRotationConfig{{rotationOldKey, ""}},
		},
		{
			name:      "2: providerd signs with the new key and keeps the old as previous",
			backends:  []backendRotationConfig{{rotationOldKey, rotationNewKey}},
			providers: []providerRotationConfig{{rotationOldKey, ""}, {rotationNewKey, rotationOldKey}},
		},
		{
			name:      "3: backend signs with the new key and drops next",
			backends:  []backendRotationConfig{{rotationOldKey, rotationNewKey}, {rotationNewKey, ""}},
			providers: []providerRotationConfig{{rotationNewKey, rotationOldKey}},
		},
		{
			name:      "4: providerd drops previous",
			backends:  []backendRotationConfig{{rotationNewKey, ""}},
			providers: []providerRotationConfig{{rotationNewKey, rotationOldKey}, {rotationNewKey, ""}},
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			for _, backend := range step.backends {
				for _, provider := range step.providers {
					assert.True(t, requestAccepted(t, provider, backend),
						"request signed by providerd %+v refused by backend %+v", provider, backend)
					assert.True(t, callbackAccepted(t, backend, provider),
						"callback signed by backend %+v refused by providerd %+v", backend, provider)
				}
			}
		})
	}
}

// TestHMACRotationOutOfOrderIsRefused pins why the order matters: providerd
// signing with the new key before the backend accepts it is refused, not
// silently accepted.
func TestHMACRotationOutOfOrderIsRefused(t *testing.T) {
	assert.False(t, requestAccepted(t,
		providerRotationConfig{rotationNewKey, rotationOldKey},
		backendRotationConfig{rotationOldKey, ""},
	))
	assert.False(t, callbackAccepted(t,
		backendRotationConfig{rotationNewKey, ""},
		providerRotationConfig{rotationOldKey, ""},
	))
}

func TestApplyEnvOverridesSetsTheNextRequestKey(t *testing.T) {
	t.Setenv("DOCKER_BACKEND_CALLBACK_SECRET_NEXT", rotationNewKey)
	cfg := docker.Config{CallbackSecret: config.Secret(rotationOldKey)}
	applyEnvOverrides(&cfg)
	assert.Equal(t, config.RotationSecret(rotationNewKey), cfg.CallbackSecretNext)
	keys, err := cfg.RequestKeys()
	require.NoError(t, err)
	assert.True(t, keys.HasRotation())
}
