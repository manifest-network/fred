package docker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func newStorageAuthorityLatchTestBackend(t *testing.T) (*Backend, Config) {
	t.Helper()
	cfg := DefaultConfig()
	dir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(dir, "retention.db")
	cfg.VolumeDataPath = ""
	cfg.VolumeMountPath = ""
	cfg.SKUProfiles = map[string]shared.SKUProfile{
		"docker-micro": {CPUCores: 1, MemoryMB: 256, DiskMB: 0},
	}
	cfg.SKUMapping = map[string]string{"docker-micro": "docker-micro"}
	cfg.CallbackSecret = durableCallbackTestSecret
	cfg.HostAddress = "127.0.0.1"

	b, err := newBackendWithTestIdentity(cfg, slog.Default())
	require.NoError(t, err)
	b.operationIntents = b.callbackStore
	t.Cleanup(func() { _ = b.Stop() })
	return b, cfg
}

func TestSiblingAuthoritativeStoreFailureBlocksCallbackSettlement(t *testing.T) {
	tests := []struct {
		name    string
		path    func(Config) string
		trigger func(*Backend, string) error
	}{
		{
			name: "release journal",
			path: func(cfg Config) string { return cfg.ReleasesDBPath },
			trigger: func(b *Backend, leaseUUID string) error {
				_, err := b.releaseStore.LatestActive(leaseUUID)
				return err
			},
		},
		{
			name: "retention journal",
			path: func(cfg Config) string { return cfg.RetentionDBPath },
			trigger: func(b *Backend, leaseUUID string) error {
				_, err := b.retentionStore.Get(leaseUUID)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, cfg := newStorageAuthorityLatchTestBackend(t)
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			admission, err := b.callbackStore.BeginOperationIntent(spec)
			require.NoError(t, err)

			var callbackRequests atomic.Int32
			b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
				Store: b.callbackStore,
				HTTPClient: &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
					callbackRequests.Add(1)
					return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
				})},
				Secret:          durableCallbackTestSecret,
				Logger:          b.logger,
				StopCtx:         b.stopCtx,
				BeforeDelivery:  b.VerifyStorageIdentity,
				BeforeReplay:    b.VerifyStorageIdentity,
				StorageIdentity: b.storageIdentity,
				Backoff:         &zeroBackoff,
				DeliveryTimeout: time.Second,
			})

			storePath := test.path(cfg)
			require.NoError(t, os.Rename(storePath, storePath+".withdrawn"))
			triggerErr := test.trigger(b, spec.LeaseUUID)
			require.Error(t, triggerErr)
			assert.ErrorIs(t, triggerErr, backendidentity.ErrIdentityDrift)

			latched := b.terminalStorageAuthorityError()
			require.Error(t, latched)
			assert.EqualError(t, latched, triggerErr.Error(),
				"the backend latch must retain the store's exact terminal cause")
			select {
			case <-b.stopCtx.Done():
			default:
				t.Fatal("authoritative store failure did not cancel the backend lifetime")
			}

			// Direct operation-intent settlement and the CallbackSender must both
			// fail before entering the callback journal transaction, even though
			// stopCtx is already canceled and a daemon probe would only report
			// context.Canceled.
			_, err = b.operationIntents.ResolveOperationIntent(
				admission.Claim, backend.CallbackStatusFailed, "late terminal failure",
			)
			require.Error(t, err)
			assert.EqualError(t, err, latched.Error())
			b.sendOperationCallbackWithURL(
				spec.LeaseUUID, spec.CallbackURL, backend.CallbackStatusFailed, "late terminal failure",
			)
			assert.Zero(t, callbackRequests.Load())

			// Reopen the callback journal without the process-local latch and prove
			// neither path consumed the write-ahead intent or added a completion.
			require.NoError(t, b.callbackStore.Close())
			restartGate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
			require.NoError(t, err)
			reopened, err := shared.OpenIdentityBoundCallbackStore(
				shared.CallbackStoreConfig{DBPath: cfg.CallbackDBPath}, b.storageAuthority, restartGate,
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			intents, err := reopened.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			assert.Equal(t, admission.Claim.OperationID(), intents[0].OperationID())
			pending, err := reopened.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending)
		})
	}
}

func TestTerminalStorageAuthorityCausePrecedesCanceledMutationLifetime(t *testing.T) {
	b, _ := newStorageAuthorityLatchTestBackend(t)
	terminalCause := fmt.Errorf("%w: %w", backendidentity.ErrIdentityDrift, assert.AnError)
	cause := b.latchTerminalStorageAuthority(terminalCause)
	require.Error(t, cause)

	err := b.requireMutationAdmission(context.Background(), "provision")
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.NotErrorIs(t, err, context.Canceled)
}
